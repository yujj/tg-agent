"""Демон без входа аккаунтом: Telegram видно только глазами бота.

Нет `tg login` (сервер, где личной сессии быть не должно) — демон читает getUpdates бота и складывает
сообщения в свой SQLite. Бот не может запросить историю: `tg read` показывает только то, что демон
получил, и только из чатов, где бот есть (в группе с privacy mode — лишь команды, ответы и упоминания
боту; чтобы видеть всё, бот — админ или privacy выключен в @BotFather).

Снаружи всё как у демона аккаунта: тот же сокет, те же команды и та же строка на сообщение. Чего бот
не умеет (`--as-me`, `create-group`, полный список чатов), — понятная ошибка.

Хранилище: таблицы meta и messages; одна строка — одно обновление Telegram (update_id), правка —
отдельная строка с тем же message_id и "edited": true. Offset getUpdates сохраняется в той же
транзакции, что и сообщения: подтверждаем Telegram только записанное.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from .core import (
    CATCH_UP_LIMIT,
    SOCK,
    STATE_DIR,
    TgError,
    bot_api,
    bot_file,
    bot_upload,
    clean,
    config,
    ensure_writable,
    is_numeric_id,
    parse_query,
    safe_filename,
    secret,
)
from .daemon import LINE_LIMIT, Daemon, Subscriber, describe, log, send

INBOX = STATE_DIR / "bot-inbox.sqlite"
KEEP = 10_000  # обновлений в хранилище, старшие удаляются
LONG_POLL = 25  # getUpdates ждёт новые сообщения столько секунд
MAX_DOWNLOAD = 20 * 1024 * 1024  # getFile отдаёт файлы до 20 МБ
UPDATE_KINDS = ["message", "channel_post", "edited_message", "edited_channel_post"]
# Поля сообщения с файлом, в порядке проверки
FILE_KINDS = ["photo", "voice", "video_note", "audio", "video", "animation", "document", "sticker"]
SERVICE_KINDS = {
    "new_chat_members": "ChatAddUser", "left_chat_member": "ChatDeleteUser", "new_chat_title": "ChatEditTitle",
    "new_chat_photo": "ChatEditPhoto", "delete_chat_photo": "ChatDeletePhoto", "pinned_message": "PinMessage",
    "group_chat_created": "ChatCreate", "supergroup_chat_created": "ChatCreate",
    "migrate_to_chat_id": "ChatMigrateTo", "migrate_from_chat_id": "ChannelMigrateFrom",
}
# Сообщение без текста и файла, но с содержимым — его вид в скобках
CONTENT_KINDS = {"poll": "Poll", "location": "Geo", "venue": "Venue", "contact": "Contact", "dice": "Dice",
                 "game": "Game", "story": "Story"}
NO_ACCOUNT = "нужен вход аккаунтом (tg login); демон работает только от бота"


def open_inbox() -> sqlite3.Connection:
    db = sqlite3.connect(INBOX, timeout=10)
    INBOX.chmod(0o600)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (
            update_id INTEGER PRIMARY KEY, chat_id TEXT NOT NULL,
            message_id INTEGER NOT NULL, data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS chat_updates ON messages(chat_id, update_id);
    """)
    return db


def get_meta(db: sqlite3.Connection, key: str, default=None):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def put_meta(db: sqlite3.Connection, key: str, value) -> None:
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))


def ingest(db: sqlite3.Connection, updates: list[dict], allowed: set[str] | None) -> list[dict]:
    """Записать обновления и сдвинуть offset одной транзакцией. → новые сохранённые сообщения."""
    stored = []
    with db:
        offset = get_meta(db, "offset", 0)
        for u in updates:
            kind = next((k for k in UPDATE_KINDS if k in u), None)
            m = u.get(kind) if kind else None
            if m and (allowed is None or str(m["chat"]["id"]) in allowed):
                m = dict(m, edited=kind.startswith("edited_"))
                cur = db.execute("INSERT OR IGNORE INTO messages VALUES (?,?,?,?)",
                                 (u["update_id"], str(m["chat"]["id"]), m["message_id"], json.dumps(m)))
                if cur.rowcount:
                    stored.append(m)
            offset = max(offset, u["update_id"] + 1)
        put_meta(db, "offset", offset)
        db.execute("DELETE FROM messages WHERE update_id NOT IN "
                   "(SELECT update_id FROM messages ORDER BY update_id DESC LIMIT ?)", (KEEP,))
    return stored


def attachment(m: dict) -> tuple[str, dict] | None:
    for kind in FILE_KINDS:
        item = m.get(kind)
        if kind == "photo" and item:
            return kind, item[-1]  # размеры по возрастанию — берём самый большой
        if isinstance(item, dict) and item.get("file_id"):
            return kind, item
    return None


def title_of(chat: dict) -> str:
    name = chat.get("title") or " ".join(filter(None, [chat.get("first_name"), chat.get("last_name")]))
    return name or str(chat["id"])


def sender_of(m: dict) -> str:
    """Как sender_name у аккаунта: имя выбирает отправитель, поэтому всегда с id."""
    s = m.get("from")
    if s is None or m.get("sender_chat"):
        c = m.get("sender_chat") or m["chat"]
        name, uname, sid = title_of(c), c.get("username"), c["id"]
    else:
        name = " ".join(filter(None, [s.get("first_name"), s.get("last_name")])) or "?"
        uname, sid = s.get("username"), s["id"]
    if uname:
        name += f" @{uname}"
    return f"{clean(name)} (id {sid})"


def fmt_bot(m: dict, chat: str | None = None) -> str:
    """Та же строка, что fmt() у аккаунта, из сообщения Bot API."""
    ts = datetime.fromtimestamp(m.get("edit_date") if m.get("edited") else m["date"]).astimezone()
    parts = [f"[{ts.strftime('%Y-%m-%d %H:%M')}]"]
    if chat:
        parts.append(f"«{clean(chat)}»")
    parts.append(f"#{m['message_id']}")
    parts.append(sender_of(m) + ":")
    reply = (m.get("reply_to_message") or {}).get("message_id")
    service = next((v for k, v in SERVICE_KINDS.items() if k in m), None)
    if service:
        if "pinned_message" in m:
            parts.append(f"→#{m['pinned_message'].get('message_id')}")
        parts.append(f"[служебное: {service}]")
        return " ".join(parts)
    if reply:
        parts.append(f"↩#{reply}")
    text = m.get("text") or m.get("caption") or ""
    found = attachment(m)
    if found:
        kind, item = found
        if kind == "voice":
            d = int(item.get("duration", 0))
            text = f"[Voice {d // 60}:{d % 60:02}] {text}"
        elif kind == "photo":
            text = f"[Photo] {text}"
        elif item.get("file_name"):
            text = f"[Document: {item['file_name']}] {text}"
        else:
            text = f"[Document] {text}"
        text = text.strip()
    elif not text:
        text = next((f"[{v}]" for k, v in CONTENT_KINDS.items() if k in m), "[служебное: Other]")
    if m.get("forward_origin") or m.get("forward_date"):
        text = f"[переслано] {text}"
    if m.get("edited"):
        text = f"[изменено] {text}"
    parts.append(clean(text))
    return " ".join(parts)


class BotDaemon(Daemon):
    """Демон аккаунта без Telethon: тот же сокет и протокол, источник — getUpdates бота."""

    def __init__(self) -> None:  # noqa: D107 — без Telethon: super().__init__ не зовём
        self.subs: set[Subscriber] = set()
        self.ready = asyncio.Event()
        self.fatal: str | None = None
        self.stop = asyncio.Event()
        self.started = time.time()
        self.db = open_inbox()
        self.bot: dict | None = None
        self.poll_error: str | None = None
        self.last_poll: float | None = None

    # ----- подключение и опрос -----

    async def connect(self) -> None:
        try:
            token = secret("bot_token")
            hook = await asyncio.to_thread(lambda: bot_api(token, "getWebhookInfo"))
            if hook.get("url"):
                # Не снимаем чужой webhook и не забираем его обновления молча.
                self.fatal = "у бота есть webhook — getUpdates займёт чужие обновления; сначала договорись о передаче"
            else:
                self.bot = await asyncio.to_thread(lambda: bot_api(token, "getMe"))
                log(f"bot-only mode: @{self.bot['username']}")
        except TgError as e:
            self.fatal = f"режим бота: {e}"
        if self.fatal:
            log("FATAL", self.fatal)
        self.ready.set()

    def allowed(self) -> set[str] | None:
        chats = config().get("chats")
        return {str(c) for c in chats} if chats else None

    async def poll_loop(self) -> None:
        await self.ready.wait()
        if self.fatal:
            return
        delay = 2
        while not self.stop.is_set():
            try:
                # Evaluate SQLite-backed arguments on the event-loop thread; only HTTP runs in a worker.
                updates = await asyncio.to_thread(bot_api,
                    secret("bot_token"), "getUpdates",
                    {"offset": get_meta(self.db, "offset", 0), "timeout": LONG_POLL, "allowed_updates": UPDATE_KINDS},
                    timeout=LONG_POLL + 15)
                stored = ingest(self.db, updates, self.allowed())
                self.last_poll = time.time()
                if self.poll_error:
                    self.poll_error = None
                    self.notify_all("tg-watch: опрос снова работает")
                delay = 2
                await self.push(stored)
            except Exception as e:  # noqa: BLE001 — опрос не должен умирать: сеть, 429, битая строка
                if not self.poll_error:
                    self.notify_all(f"tg-watch: ошибка опроса ({describe(e)}) — продолжаю пробовать")
                self.poll_error = describe(e)
                log("poll error", self.poll_error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    def notify_all(self, line: str) -> None:
        for s in self.subs:
            if not s.dead:
                send(s.w, out=line)

    async def push(self, stored: list[dict]) -> None:
        for m in stored:
            pid = m["chat"]["id"]
            for sub in [s for s in self.subs if pid in s.sent and s.live]:
                try:
                    if m["edited"]:  # правка уже показанного: строкой, метку не трогаем
                        if m["message_id"] <= sub.sent[pid]:
                            send(sub.w, out=fmt_bot(m, sub.titles[pid]))
                            await self.flush(sub)
                    else:
                        await self.deliver(sub, pid)
                except Exception as e:  # noqa: BLE001 — сбой одного подписчика не трогает других
                    log("push deliver error", pid, type(e).__name__, e)

    # ----- хранилище -----

    def latest(self, pid: int, *, after: int | None = None, limit: int) -> list[dict]:
        """Последняя версия каждого сообщения чата (с правками), по возрастанию id."""
        rows = self.db.execute(
            "SELECT data FROM messages WHERE update_id IN (SELECT MAX(update_id) FROM messages "
            "WHERE chat_id=? AND message_id>? GROUP BY message_id) ORDER BY message_id DESC LIMIT ?",
            (str(pid), after if after is not None else -1, limit)).fetchall()
        return [json.loads(d) for (d,) in reversed(rows)]

    def originals(self, pid: int, after: int) -> list[dict]:
        rows = self.db.execute(
            "SELECT data FROM messages WHERE chat_id=? AND message_id>? AND update_id IN "
            "(SELECT MIN(update_id) FROM messages WHERE chat_id=? GROUP BY message_id) "
            "ORDER BY message_id DESC LIMIT ?", (str(pid), after, str(pid), CATCH_UP_LIMIT)).fetchall()
        return [json.loads(d) for (d,) in reversed(rows)]

    def known_chats(self) -> dict[int, dict]:
        rows = self.db.execute("SELECT data FROM messages WHERE update_id IN "
                               "(SELECT MAX(update_id) FROM messages GROUP BY chat_id) ORDER BY update_id DESC")
        chats = {}
        for (d,) in rows:
            chat = json.loads(d)["chat"]
            chats[chat["id"]] = chat
        return chats

    def resolve_chat(self, query: str, *, fuzzy: bool) -> tuple[int, str]:
        """Как resolve() аккаунта, но по чатам, откуда бот получал сообщения. Число — как есть."""
        q, explicit = parse_query(query)
        chats = self.known_chats()
        if is_numeric_id(q):
            pid = int(q)
            return pid, title_of(chats[pid]) if pid in chats else str(pid)
        needle = q.lstrip("@").casefold()
        if explicit:
            found = [c for c in chats.values() if (c.get("username") or "").casefold() == needle]
        else:
            found = [c for c in chats.values() if title_of(c).casefold() == needle]
            if not found and fuzzy:
                found = [c for c in chats.values() if needle in title_of(c).casefold()]
        if len(found) == 1:
            return found[0]["id"], title_of(found[0])
        if not found:
            raise TgError(f"чат «{query}» не найден среди чатов, откуда бот получал сообщения — дай числовой id")
        lines = "\n".join(f"  {c['id']}  {title_of(c)}" for c in found[:15])
        raise TgError(f"«{query}» подходит к нескольким чатам, уточни id:\n{lines}")

    # ----- подписки -----

    async def deliver(self, sub: Subscriber, pid: int, prefix: str = "") -> None:
        async with sub.lock:
            if sub.dead or pid not in sub.sent:
                return
            msgs = self.originals(pid, sub.sent[pid])
            for m in msgs:
                send(sub.w, msg=prefix + fmt_bot(m, sub.titles[pid]), pid=pid, id=m["message_id"])
                sub.sent[pid] = m["message_id"]
                if "migrate_to_chat_id" in m:
                    send(sub.w, out=f"tg-watch: «{sub.titles[pid]}» стал супергруппой — сюда больше не пишут; "
                                     f"подпишись на {m['migrate_to_chat_id']}")
            if len(msgs) == CATCH_UP_LIMIT:
                send(sub.w, out=f"«{sub.titles[pid]}»: новых больше {CATCH_UP_LIMIT}, показаны "
                                 "последние — остальное: tg read")
            await self.flush(sub)

    async def watch(self, a, reader: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await self.wait_ready(None)
        since = {int(k): v for k, v in (a.since or {}).items()}
        sub = Subscriber(w)
        self.subs.add(sub)
        try:
            catch_up = []
            for q in a.chats:
                pid, title = self.resolve_chat(q, fuzzy=True)
                if pid in sub.titles:
                    continue
                sub.titles[pid] = title
                if since.get(pid) is None:  # первая подписка на чат — с текущего момента
                    last = self.latest(pid, limit=1)
                    sub.sent[pid] = last[0]["message_id"] if last else 0
                    send(w, init=pid, id=sub.sent[pid])
                else:
                    sub.sent[pid] = since[pid]
                    catch_up.append(pid)
            send(w, out="tg-watch started: " + ", ".join(f"«{t}» ({p})" for p, t in sub.titles.items())
                 + f"; бот @{self.bot['username']}, getUpdates (демон pid {os.getpid()})")
            if self.poll_error:
                send(w, out=f"tg-watch: ошибка опроса ({self.poll_error}) — продолжаю пробовать")
            await self.flush(sub)
            log("watch +", list(sub.titles))
            for pid in catch_up:
                await self.deliver(sub, pid, prefix="(пропущено) ")
            sub.live = True
            for pid in list(sub.sent):  # что пришло, пока шла догонка
                await self.deliver(sub, pid)
            while await reader.read(1024):
                pass
        finally:
            self.drop(sub)
            log("watch -", list(sub.titles))

    # ----- разовые команды -----

    async def op_whoami(self, a, out) -> None:
        out(f"аккаунт: нет ({NO_ACCOUNT})")
        out("бот: @" + self.bot["username"])

    async def op_chats(self, a, out) -> None:
        needle = (a.query or "").casefold()
        kinds = {"channel": "канал", "group": "группа", "supergroup": "группа", "private": "личка"}
        n = 0
        for pid, c in self.known_chats().items():
            if needle and needle not in title_of(c).casefold():
                continue
            out(f"{pid:>15}  {kinds.get(c.get('type'), '?'):6}  {title_of(c)}")
            n += 1
            if n >= a.limit:
                break
        if not n:
            out("чатов нет: бот видит только чаты, откуда получал сообщения, пока работает демон")

    async def op_read(self, a, out) -> None:
        pid, title = self.resolve_chat(a.chat, fuzzy=True)
        if a.after:
            msgs = self.latest(pid, after=a.after, limit=10**9)[:a.limit]
        else:
            msgs = self.latest(pid, limit=a.limit)
        out(f"=== {title} (id {pid}), сообщений: {len(msgs)} (режим бота: только полученное демоном)")
        for m in msgs:
            out(fmt_bot(m))
        if a.after and len(msgs) == a.limit:
            out(f"… показаны первые {a.limit} после #{a.after}; дальше: --after {msgs[-1]['message_id']}")

    def transport_up(self) -> bool:
        return self.ready.is_set() and not self.fatal and not self.poll_error

    async def op_send(self, a, out) -> None:
        ensure_writable("сообщение")
        if a.as_me:
            raise TgError(f"--as-me: {NO_ACCOUNT}")
        pid, title = self.resolve_chat(a.chat, fuzzy=False)
        file = getattr(a, "file", None)
        what = f" (файл {Path(file).name})" if file else ""
        payload = {"chat_id": pid}
        if a.text:
            payload["caption" if file else "text"] = a.text
        if a.reply_to:
            payload["reply_parameters"] = {"message_id": a.reply_to}
        token = secret("bot_token")
        if file:
            method, field = ("sendPhoto", "photo") if getattr(a, "photo", False) else ("sendDocument", "document")
            res = await asyncio.to_thread(lambda: bot_upload(token, method, payload, field, Path(file)))
        else:
            res = await asyncio.to_thread(lambda: bot_api(token, "sendMessage", payload))
        out(f"отправлено ботом в «{title}»: #{res['message_id']}{what}")

    async def op_react(self, a, out) -> None:
        ensure_writable("реакцию")
        if a.as_me:
            raise TgError(f"--as-me: {NO_ACCOUNT}")
        pid, title = self.resolve_chat(a.chat, fuzzy=False)
        payload = {"chat_id": pid, "message_id": a.msg_id, "reaction": [{"type": "emoji", "emoji": a.emoji}]}
        await asyncio.to_thread(lambda: bot_api(secret("bot_token"), "setMessageReaction", payload))
        out(f"реакция {a.emoji} на #{a.msg_id} в «{title}»")

    async def op_create_group(self, a, out) -> None:
        raise TgError(f"create-group: {NO_ACCOUNT}")

    async def op_download(self, a, out) -> None:
        pid, title = self.resolve_chat(a.chat, fuzzy=True)
        row = self.db.execute("SELECT data FROM messages WHERE chat_id=? AND message_id=? "
                              "ORDER BY update_id DESC LIMIT 1", (str(pid), a.msg_id)).fetchone()
        if not row:
            raise TgError(f"нет сообщения #{a.msg_id} в «{title}» среди полученных ботом")
        m = json.loads(row[0])
        found = attachment(m)
        if not found:
            raise TgError(f"в #{a.msg_id} нет файла для скачивания")
        kind, item = found
        if item.get("file_size", 0) > MAX_DOWNLOAD:
            raise TgError("файл больше 20 МБ — бот такой скачать не может (ограничение Bot API)")
        token = secret("bot_token")
        path = (await asyncio.to_thread(lambda: bot_api(token, "getFile", {"file_id": item["file_id"]})))["file_path"]
        data = await asyncio.to_thread(lambda: bot_file(token, path))
        dest = Path(a.out) / safe_filename(pid, a.msg_id, item.get("file_name"), kind, Path(path).suffix)
        part = dest.with_name(dest.name + ".part")
        try:
            part.write_bytes(data)
            part.chmod(0o600)
            part.replace(dest)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        out(str(dest))

    async def op_status(self, a, out) -> None:
        up = int(time.time() - self.started)
        if self.fatal:
            conn = "ОШИБКА: " + self.fatal
        elif not self.ready.is_set():
            conn = "подключается…"
        elif self.poll_error:
            conn = f"нет ({self.poll_error}; повторяю)"
        else:
            conn = "да"
        bot = f"@{self.bot['username']}" if self.bot else "?"
        out(f"демон: pid {os.getpid()}, работает {up // 3600}ч {up % 3600 // 60}м, режим бота ({bot}, без аккаунта), "
            f"опрос getUpdates: {conn}")
        stored = self.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        out(f"сообщений в хранилище: {stored} (до {KEEP})")
        if not self.subs:
            out("подписок нет")
        for s in self.subs:
            out("подписчик: " + ", ".join(f"«{s.titles[p]}» ({p}) до #{last}" for p, last in s.sent.items()))

    # ----- сервер -----

    async def run(self) -> None:
        SOCK.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self.handle, path=str(SOCK), limit=LINE_LIMIT)
        SOCK.chmod(0o600)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)
        log(f"started pid {os.getpid()}, socket {SOCK}, bot-only (no account session)")
        connector = asyncio.create_task(self.connect())
        poller = asyncio.create_task(self.poll_loop())
        await self.stop.wait()
        for t in (poller, connector):
            t.cancel()
        server.close()
        SOCK.unlink(missing_ok=True)
        self.db.close()
        log("stopped")


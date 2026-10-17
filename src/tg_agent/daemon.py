"""tgd — один процесс держит подключение к Telegram, остальные команды ходят к нему через unix-сокет.

Зачем: push-обновления Telegram приходят последнему подключению сессии. Если каждая команда
подключается сама, tg read / tg send перетягивают push на себя, и подписчик молча теряет сообщения.
С одним подключением push доходит; опрос раз в POLL секунд — страховка.

Протокол: клиент шлёт одну JSON-строку {"cmd": ..., "args": {...}}, демон отвечает JSON-строками
{"out": "<строка>"} и в конце {"rc": 0} или {"err": "...", "rc": 1}. Для watch поток не кончается,
пока клиент не отключится; сообщения идут как {"msg": "<строка>", "pid": ..., "id": ...}, а
{"init": pid, "id": ...} задаёт стартовую метку чата, на который подписались впервые.

Метки подписки хранит КЛИЕНТ (tg watch) и сдвигает их после того, как напечатал строку: при сбое
сообщение может прийти повторно, но не потеряется. Демон помнит только, что уже отправил каждому
подключённому подписчику.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import time
import traceback
from types import SimpleNamespace

from telethon import events, functions, utils

from .core import (
    CATCH_UP_LIMIT,
    DAEMON_LOCK,
    SOCK,
    TgError,
    bot_api,
    chat_title,
    ensure_state_dir,
    fmt,
    is_numeric_id,
    make_client,
    resolve,
    secret,
)

POLL = 60  # страховочный опрос, секунд: один GetPeerDialogs на все подписанные чаты
RPC_TIMEOUT = 30  # один запрос к Telegram дольше — считаем сбоем, не держим подписку
READY_TIMEOUT = 20  # сколько разовая команда ждёт подключения к Telegram
MAX_BUFFER = 1_000_000  # подписчик не читает и накопил столько — отключаем
LINE_LIMIT = 1_000_000  # максимальная длина JSON-строки запроса
EXIT_ALREADY_RUNNING = 3


def log(*a) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def send(w: asyncio.StreamWriter, **msg) -> None:
    w.write((json.dumps(msg, ensure_ascii=False) + "\n").encode())


class Subscriber:
    """Одно подключение tg watch: свои чаты, своё «уже отправлено», свой замок."""

    def __init__(self, w: asyncio.StreamWriter) -> None:
        self.w = w
        self.sent: dict[int, int] = {}
        self.titles: dict[int, str] = {}
        self.lock = asyncio.Lock()
        self.dead = False


class Daemon:
    def __init__(self) -> None:
        self.client = make_client(daemon=True)
        self.subs: set[Subscriber] = set()
        self.ready = asyncio.Event()
        self.fatal: str | None = None
        self.stop = asyncio.Event()
        self.started = time.time()

    # ----- подключение -----

    async def connect(self) -> None:
        try:
            await self.client.connect()  # без сети ждёт сколько нужно (connection_retries=-1)
            if not await self.client.is_user_authorized():
                self.fatal = "сессия недействительна — tg login"
            else:
                self.client.add_event_handler(self.on_new, events.NewMessage())
                log("connected to Telegram")
        except Exception as e:  # noqa: BLE001 — любая ошибка подключения = понятный ответ клиентам
            self.fatal = f"не удалось подключиться к Telegram: {type(e).__name__}: {e}"
        if self.fatal:
            log("FATAL", self.fatal)
        self.ready.set()

    async def wait_ready(self, timeout: float | None) -> None:
        if not self.ready.is_set():
            try:
                await asyncio.wait_for(self.ready.wait(), timeout)
            except TimeoutError as e:
                raise TgError("нет связи с Telegram — демон подключается; проверь сеть/VPN "
                              "и ~/.local/state/tg-agent/daemon.log") from e
        if self.fatal:
            raise TgError(self.fatal)

    # ----- подписки -----

    def drop(self, sub: Subscriber) -> None:
        sub.dead = True
        self.subs.discard(sub)

    async def deliver(self, sub: Subscriber, pid: int, prefix: str = "") -> None:
        """Отправить подписчику всё новее того, что он уже получил."""
        async with sub.lock:
            if sub.dead or pid not in sub.sent:
                return
            msgs = await asyncio.wait_for(self._fetch(pid, sub.sent[pid]), RPC_TIMEOUT)
            for m in sorted(msgs, key=lambda m: m.id):
                send(sub.w, msg=prefix + fmt(m, sub.titles[pid], compact=True), pid=pid, id=m.id)
                sub.sent[pid] = m.id
            if len(msgs) == CATCH_UP_LIMIT:
                send(sub.w, out=f"«{sub.titles[pid]}»: новых больше {CATCH_UP_LIMIT}, показаны "
                                 "последние — остальное: tg read")
            if sub.w.transport.get_write_buffer_size() > MAX_BUFFER:
                log("subscriber not reading — dropped")
                self.drop(sub)
                sub.w.close()
                return
            await sub.w.drain()

    async def _fetch(self, pid: int, after: int) -> list:
        return [m async for m in self.client.iter_messages(pid, min_id=after, limit=CATCH_UP_LIMIT)]

    async def on_new(self, event) -> None:
        pid = event.chat_id
        for sub in [s for s in self.subs if pid in s.sent]:
            try:
                await self.deliver(sub, pid)
            except Exception as e:  # noqa: BLE001 — push-обработчик не должен падать; догонит опрос
                log("push deliver error", pid, type(e).__name__, e)

    async def poll_loop(self) -> None:
        failing = False
        while True:
            await asyncio.sleep(POLL)
            subs = [s for s in self.subs if s.sent]
            pids = sorted({p for s in subs for p in s.sent})
            if not pids:
                continue
            try:
                peers = [await self.client.get_input_entity(p) for p in pids]
                res = await asyncio.wait_for(
                    self.client(functions.messages.GetPeerDialogsRequest(peers=peers)), RPC_TIMEOUT)
                top = {utils.get_peer_id(d.peer): d.top_message for d in res.dialogs}
                for s in subs:
                    for p, last in list(s.sent.items()):
                        if top.get(p, 0) > last:
                            log("poll caught", p, top[p])
                            await self.deliver(s, p)
                if failing:
                    for s in subs:
                        send(s.w, out="tg-watch: опрос снова работает")
                failing = False
            except Exception as e:  # noqa: BLE001 — опрос — страховка, он не имеет права умереть
                log("poll error", type(e).__name__, e)
                if not failing:
                    for s in subs:
                        send(s.w, out=f"tg-watch: ошибка опроса ({type(e).__name__}: {e}) — продолжаю пробовать")
                failing = True

    async def watch(self, a, reader: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        if not self.ready.is_set():
            send(w, out="tg-watch: жду подключения к Telegram…")
            await w.drain()
        await self.wait_ready(None)
        since = {int(k): v for k, v in (a.since or {}).items()}
        sub = Subscriber(w)
        catch_up = []
        for q in a.chats:
            e = await resolve(self.client, q, fuzzy=True)
            pid = utils.get_peer_id(e)
            sub.titles[pid] = chat_title(e)
            if since.get(pid) is None:  # первая подписка на чат — с текущего момента
                latest = await self.client.get_messages(pid, limit=1)
                sub.sent[pid] = latest[0].id if latest else 0
                send(w, init=pid, id=sub.sent[pid])
            else:
                sub.sent[pid] = since[pid]
                catch_up.append(pid)
        self.subs.add(sub)
        try:
            for pid in catch_up:
                await self.deliver(sub, pid, prefix="(пропущено) ")
            send(w, out="tg-watch started: " + ", ".join(f"«{t}» ({p})" for p, t in sub.titles.items())
                 + f"; push + страховочный опрос раз в {POLL}с (демон pid {os.getpid()})")
            await w.drain()
            log("watch +", list(sub.titles))
            while await reader.read(1024):  # до отключения клиента (Monitor истёк, TaskStop)
                pass
        finally:
            self.drop(sub)
            log("watch -", list(sub.titles))

    # ----- разовые команды -----

    async def op_whoami(self, a, out) -> None:
        me = await self.client.get_me()
        out(f"аккаунт: {utils.get_display_name(me)} (@{me.username}) id={me.id}")
        try:
            bot = await asyncio.to_thread(lambda: bot_api(secret("bot_token"), "getMe"))
            out("бот: @" + bot["username"])
        except TgError:
            out("бот: не настроен (tg setup-bot)")

    async def op_chats(self, a, out) -> None:
        needle = (a.query or "").casefold()
        n = 0
        async for d in self.client.iter_dialogs(limit=None if needle else a.limit):
            if needle and needle not in (d.name or "").casefold():
                continue
            kind = "канал" if d.is_channel and not d.is_group else "группа" if d.is_group else "личка"
            unread = f"  непрочит. {d.unread_count}" if d.unread_count else ""
            out(f"{d.id:>15}  {kind:6}  {d.name}{unread}")
            n += 1
            if n >= a.limit:
                break

    async def op_read(self, a, out) -> None:
        entity = await resolve(self.client, a.chat, fuzzy=True)
        if a.after:  # первые limit сообщений ПОСЛЕ id, по порядку
            msgs = [m async for m in self.client.iter_messages(entity, min_id=a.after, limit=a.limit, reverse=True)]
        else:  # последние limit
            msgs = list(reversed([m async for m in self.client.iter_messages(entity, limit=a.limit)]))
        out(f"=== {chat_title(entity)} (id {utils.get_peer_id(entity)}), сообщений: {len(msgs)}")
        for m in msgs:
            out(fmt(m))
        if a.after and len(msgs) == a.limit:
            out(f"… показаны первые {a.limit} после #{a.after}; дальше: --after {msgs[-1].id}")

    async def op_send(self, a, out) -> None:
        if a.as_me:
            entity = await resolve(self.client, a.chat, fuzzy=False)
            m = await self.client.send_message(entity, a.text, reply_to=a.reply_to)
            out(f"отправлено от аккаунта в «{chat_title(entity)}»: #{m.id}")
            return
        if is_numeric_id(a.chat):
            chat_id, title = int(a.chat), a.chat
        else:
            entity = await resolve(self.client, a.chat, fuzzy=False)
            chat_id, title = utils.get_peer_id(entity), chat_title(entity)
        payload = {"chat_id": chat_id, "text": a.text}
        if a.reply_to:
            # Номера сообщений у аккаунта и бота совпадают только в каналах и супергруппах (-100…);
            # в обычных группах и личке у каждого своя нумерация — ответ ушёл бы не туда.
            if not str(chat_id).startswith("-100"):
                raise TgError("--reply-to ботом — только в каналах и супергруппах (id -100…)")
            payload["reply_parameters"] = {"message_id": a.reply_to}
        res = await asyncio.to_thread(lambda: bot_api(secret("bot_token"), "sendMessage", payload))
        out(f"отправлено ботом в «{title}»: #{res['message_id']}")

    async def op_status(self, a, out) -> None:
        up = int(time.time() - self.started)
        if self.fatal:
            conn = "ОШИБКА: " + self.fatal
        elif not self.ready.is_set():
            conn = "подключается…"
        else:
            conn = "да" if self.client.is_connected() else "нет (переподключается)"
        out(f"демон: pid {os.getpid()}, работает {up // 3600}ч {up % 3600 // 60}м, подключён к Telegram: {conn}")
        if not self.subs:
            out("подписок нет")
        for s in self.subs:
            chats = ", ".join(f"«{s.titles[p]}» ({p}) до #{last}" for p, last in s.sent.items())
            out(f"подписчик: {chats}")

    async def op_shutdown(self, a, out) -> None:
        out(f"демон {os.getpid()} останавливается")
        self.stop.set()

    # ----- сервер -----

    async def handle(self, reader: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return  # проверка «демон жив?» — подключились и отключились
            req = json.loads(line)
            cmd, a = req["cmd"], SimpleNamespace(**req.get("args", {}))
            if cmd == "watch":
                await self.watch(a, reader, w)
                return
            op = getattr(self, f"op_{cmd}", None)
            if op is None:
                raise TgError(f"неизвестная команда {cmd}")
            if cmd not in ("status", "shutdown"):
                await self.wait_ready(READY_TIMEOUT)
            await op(a, lambda s: send(w, out=s))
            send(w, rc=0)
        except TgError as e:
            send(w, err=str(e), rc=1)
        except Exception as e:  # noqa: BLE001 — клиент должен получить ошибку, а не обрыв «демон упал»
            log("command error", type(e).__name__, e, "\n" + traceback.format_exc())
            send(w, err=f"{type(e).__name__}: {e}", rc=1)
        finally:
            try:
                await w.drain()
                w.close()
            except (ConnectionError, RuntimeError):
                pass

    async def run(self) -> None:
        # Сокет — СРАЗУ, до подключения к Telegram: без сети клиенты получат понятный ответ,
        # а tg stop/status работают всегда.
        SOCK.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self.handle, path=str(SOCK), limit=LINE_LIMIT)
        SOCK.chmod(0o600)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)
        log(f"started pid {os.getpid()}, socket {SOCK}")
        connector = asyncio.create_task(self.connect())
        poller = asyncio.create_task(self.poll_loop())
        await self.stop.wait()
        for t in (poller, connector):
            t.cancel()
        server.close()
        SOCK.unlink(missing_ok=True)
        await self.client.disconnect()
        log("stopped")


def run_daemon() -> None:
    ensure_state_dir()
    lock = open(DAEMON_LOCK, "w")  # noqa: SIM115 — держим открытым всю жизнь демона
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("другой демон уже работает — выхожу")
        raise SystemExit(EXIT_ALREADY_RUNNING) from None
    lock.write(str(os.getpid()))
    lock.flush()
    asyncio.run(Daemon().run())

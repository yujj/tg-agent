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

from telethon import events, functions, types, utils
from telethon.errors import FloodWaitError

from .core import (
    CATCH_UP_LIMIT,
    DAEMON_LOCK,
    DAEMON_LOG,
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
OP_TIMEOUT = 60  # разовая команда / старт подписки целиком: при обрыве связи Telethon ждёт бесконечно
DRAIN_TIMEOUT = 10  # подписчик не забрал данные за столько — он завис, отключаем
DRAIN_EVERY = 20  # при догонке сбрасывать буфер каждые N сообщений
MAX_BUFFER = 1_000_000  # подписчик не читает и накопил столько — отключаем
LINE_LIMIT = 1_000_000  # максимальная длина JSON-строки запроса
EXIT_ALREADY_RUNNING = 3


def log(*a) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def describe(e: BaseException) -> str:
    if isinstance(e, FloodWaitError):
        return f"Telegram просит подождать {e.seconds}с"
    if isinstance(e, TimeoutError):
        return "Telegram не ответил вовремя"
    return f"{type(e).__name__}: {e}"


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
        self.live = False  # до конца догонки push этому подписчику не шлём: догонка — с «(пропущено)»


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
                self.fatal = "сессия недействительна или Telegram не ответил — tg login, затем tg stop"
            else:
                self.client.add_event_handler(self.on_new, events.NewMessage())
                log("connected to Telegram")
                self.dc_task = asyncio.create_task(self.watch_disconnect())  # ссылка: иначе GC
        except Exception as e:  # noqa: BLE001 — любая ошибка подключения = понятный ответ клиентам
            self.fatal = f"не удалось подключиться к Telegram: {type(e).__name__}: {e} — исправь и tg stop"
        if self.fatal:
            log("FATAL", self.fatal)
        self.ready.set()

    async def watch_disconnect(self) -> None:
        """Telethon отключается насовсем при отзыве сессии / фатальной ошибке обновлений (не при обрыве
        сети — тут он переподключается сам). Без этого демон — живой труп с «переподключается»."""
        await self.client.disconnected
        if self.stop.is_set():
            return
        self.fatal = "Telegram отключил клиента (сессия отозвана?) — tg login, затем tg stop"
        log("FATAL", self.fatal)
        for s in list(self.subs):
            send(s.w, err=self.fatal, rc=1)
            self.kill(s, "fatal")

    async def wait_ready(self, timeout: float | None) -> None:
        if not self.ready.is_set():
            try:
                await asyncio.wait_for(self.ready.wait(), timeout)
            except TimeoutError as e:
                raise TgError(f"нет связи с Telegram — демон подключается; проверь сеть/VPN и {DAEMON_LOG}") from e
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
            if sub.w.transport.get_write_buffer_size() > MAX_BUFFER:  # прошлое так и не забрал
                self.kill(sub, "не читает")
                return
            msgs = await asyncio.wait_for(self._fetch(pid, sub.sent[pid]), RPC_TIMEOUT)
            for i, m in enumerate(sorted(msgs, key=lambda m: m.id), 1):
                send(sub.w, msg=prefix + fmt(m, sub.titles[pid]), pid=pid, id=m.id)
                sub.sent[pid] = m.id
                if isinstance(getattr(m, "action", None), types.MessageActionChatMigrateTo):
                    send(sub.w, out=f"tg-watch: «{sub.titles[pid]}» стал супергруппой — сюда больше не пишут; "
                                     f"подпишись на -100{m.action.channel_id}")
                if i % DRAIN_EVERY == 0 and not await self.flush(sub):
                    return
            if len(msgs) == CATCH_UP_LIMIT:
                send(sub.w, out=f"«{sub.titles[pid]}»: новых больше {CATCH_UP_LIMIT}, показаны "
                                 "последние — остальное: tg read")
            await self.flush(sub)

    async def flush(self, sub: Subscriber) -> bool:
        """drain с таймаутом: зависший подписчик не должен держать свой замок, а с ним — опрос."""
        try:
            await asyncio.wait_for(sub.w.drain(), DRAIN_TIMEOUT)
            return True
        except (TimeoutError, ConnectionError) as e:
            self.kill(sub, type(e).__name__)
            return False

    def kill(self, sub: Subscriber, why: str) -> None:
        log("subscriber dropped:", why)
        self.drop(sub)
        sub.w.close()

    async def _fetch(self, pid: int, after: int) -> list:
        return [m async for m in self.client.iter_messages(pid, min_id=after, limit=CATCH_UP_LIMIT)]

    async def on_new(self, event) -> None:
        pid = event.chat_id
        for sub in [s for s in self.subs if pid in s.sent and s.live]:
            try:
                await self.deliver(sub, pid)
            except Exception as e:  # noqa: BLE001 — push-обработчик не должен падать; догонит опрос
                log("push deliver error", pid, type(e).__name__, e)

    async def top_messages(self, pids: list[int]) -> dict[int, int | Exception]:
        """Последний id по каждому чату. Пакетно; не вышло — по одному, чтобы один сломанный чат
        (выгнали из канала) не выключал опрос для остальных."""
        try:
            peers = [await self.client.get_input_entity(p) for p in pids]
            res = await asyncio.wait_for(
                self.client(functions.messages.GetPeerDialogsRequest(peers=peers)), RPC_TIMEOUT)
            got = {utils.get_peer_id(d.peer): d.top_message for d in res.dialogs}
            # чата нет в ответе — вышли/выгнали/удалён: это ошибка, а не «ничего нового»
            return {p: got.get(p, LookupError("чата нет в ответе Telegram — вышли, выгнали или удалён"))
                    for p in pids}
        except Exception:  # noqa: BLE001 — разберёмся по одному
            out: dict[int, int | Exception] = {}
            for p in pids:
                try:
                    peer = await self.client.get_input_entity(p)
                    res = await asyncio.wait_for(
                        self.client(functions.messages.GetPeerDialogsRequest(peers=[peer])), RPC_TIMEOUT)
                    out[p] = res.dialogs[0].top_message if res.dialogs else \
                        LookupError("чата нет в ответе Telegram — вышли, выгнали или удалён")
                except Exception as e:  # noqa: BLE001
                    out[p] = e
            return out

    async def poll_loop(self) -> None:
        failing: set[int] = set()  # чаты, по которым подписчики уже знают об ошибке опроса
        while True:
            await asyncio.sleep(POLL)
            subs = [s for s in self.subs if s.sent and s.live]
            pids = sorted({p for s in subs for p in s.sent})
            if not pids:
                continue
            top = await self.top_messages(pids)
            for p in pids:
                ok = not isinstance(top.get(p), Exception)
                if not ok and p not in failing:
                    e = top[p]
                    log("poll error", p, type(e).__name__, e)
                    self.notify(p, f"tg-watch: ошибка опроса «{p}» ({describe(e)}) — продолжаю пробовать")
                    failing.add(p)
                elif ok and p in failing:
                    self.notify(p, "tg-watch: опрос снова работает")
                    failing.discard(p)
            for s in subs:
                for p, last in list(s.sent.items()):
                    t = top.get(p)
                    if isinstance(t, int) and t > last:
                        log("poll caught", p, t)
                        try:
                            await self.deliver(s, p)
                        except Exception as e:  # noqa: BLE001 — сбой одного подписчика не трогает других
                            log("poll deliver error", p, type(e).__name__, e)

    def notify(self, pid: int, line: str) -> None:
        for s in self.subs:
            if pid in s.sent and not s.dead:
                send(s.w, out=line)

    async def watch(self, a, reader: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        if not self.ready.is_set():
            send(w, out="tg-watch: жду подключения к Telegram…")
            await w.drain()
        await self.wait_ready(None)
        since = {int(k): v for k, v in (a.since or {}).items()}
        sub = Subscriber(w)
        self.subs.add(sub)  # сразу: push по уже готовому чату не должен проскочить, пока готовятся остальные
        try:
            catch_up = await asyncio.wait_for(self.watch_setup(sub, a.chats, since), OP_TIMEOUT)
            send(w, out="tg-watch started: " + ", ".join(f"«{t}» ({p})" for p, t in sub.titles.items())
                 + f"; push + страховочный опрос раз в {POLL}с (демон pid {os.getpid()})")
            await self.flush(sub)
            log("watch +", list(sub.titles))
            for pid in catch_up:  # сломанный чат (выгнали, флуд) не должен ронять подписку на остальные
                try:
                    await self.deliver(sub, pid, prefix="(пропущено) ")
                except Exception as e:  # noqa: BLE001
                    log("catch-up error", pid, type(e).__name__, e)
                    send(w, out=f"tg-watch: «{sub.titles[pid]}» пропускаю ({describe(e)}) — "
                                "остальные чаты под наблюдением")
                    sub.sent.pop(pid, None)
            sub.live = True
            for pid in list(sub.sent):  # что пришло, пока шла догонка
                await self.deliver(sub, pid)
            while await reader.read(1024):  # до отключения клиента (Monitor истёк, TaskStop)
                pass
        except TimeoutError as e:
            raise TgError(f"Telegram не ответил за {OP_TIMEOUT}с — нет связи? Перезапусти подписку") from e
        finally:
            self.drop(sub)
            log("watch -", list(sub.titles))

    async def watch_setup(self, sub: Subscriber, chats: list[str], since: dict[int, int]) -> list[int]:
        catch_up = []
        for q in chats:
            e = await resolve(self.client, q, fuzzy=True)
            pid = utils.get_peer_id(e)
            if pid in sub.titles:
                continue  # тот же чат, указанный по-разному
            sub.titles[pid] = chat_title(e)
            async with sub.lock:
                if since.get(pid) is None:  # первая подписка на чат — с текущего момента
                    latest = await self.client.get_messages(pid, limit=1)
                    sub.sent[pid] = latest[0].id if latest else 0
                    send(sub.w, init=pid, id=sub.sent[pid])
                else:
                    sub.sent[pid] = since[pid]
                    catch_up.append(pid)
        return catch_up

    # ----- разовые команды -----

    async def op_whoami(self, a, out) -> None:
        me = await self.client.get_me()
        uname = f" (@{me.username})" if me.username else ""
        out(f"аккаунт: {utils.get_display_name(me)}{uname} id={me.id}")
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

    def transport_up(self) -> bool:
        # is_connected() остаётся True и во время переподключения; транспорт — честнее.
        # _sender — приватный API Telethon 1.x.
        sender = getattr(self.client, "_sender", None)
        return sender._transport_connected() if sender else self.client.is_connected()

    async def op_send(self, a, out) -> None:
        if a.as_me and not self.transport_up():
            # Без связи запрос встал бы в очередь Telethon и ушёл после переподключения — даже если
            # команда уже отвалилась по таймауту и агент отправил повторно. Отказываем сразу.
            raise TgError("нет связи с Telegram — не отправляю (иначе сообщение может уйти позже вторым)")
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
            if utils.resolve_id(chat_id)[1] is not types.PeerChannel:
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
            conn = "да" if self.transport_up() else "нет (переподключается)"
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
            try:
                await asyncio.wait_for(op(a, lambda s: send(w, out=s)), OP_TIMEOUT)
            except TimeoutError as e:
                if cmd == "send":
                    raise TgError(f"Telegram не ответил за {OP_TIMEOUT}с — СТАТУС НЕИЗВЕСТЕН: сообщение могло "
                                  "уйти или уйдёт позже. Не повторяй — проверь tg read") from e
                raise TgError(f"Telegram не ответил за {OP_TIMEOUT}с — нет связи? (tg status)") from e
            except FloodWaitError as e:
                raise TgError(describe(e)) from e
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
    lock = open(DAEMON_LOCK, "a+")  # noqa: SIM115 — держим открытым всю жизнь демона; "a+" не стирает pid соседа
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("другой демон уже работает — выхожу")
        raise SystemExit(EXIT_ALREADY_RUNNING) from None
    lock.truncate(0)
    lock.write(str(os.getpid()))
    lock.flush()
    asyncio.run(Daemon().run())

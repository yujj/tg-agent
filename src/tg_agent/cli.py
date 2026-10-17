"""tg — Telegram для агента: читать чаты от аккаунта пользователя, писать от бота (или от его имени по явной просьбе).

Подключение к Telegram держит один демон (tg daemon), он запускается сам при первой команде и
работает, пока не сказать tg stop. Команды ходят к нему через unix-сокет.

Секреты — в системном keyring (сервис ``tg-agent``; на macOS — Связка ключей): в argv и в файлы не попадают.
Сессия аккаунта = полный доступ к Telegram. Отозвать: Telegram → Настройки → Устройства.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import getpass
import json
import socket
import subprocess
import sys
import time
from importlib import resources
from pathlib import Path

import keyring
from telethon import utils

from .core import (
    DAEMON_LOG,
    SERVICE,
    SOCK,
    SPAWN_LOCK,
    TgError,
    bot_api,
    ensure_state_dir,
    load_wm,
    make_client,
    save_wm,
    store,
    wm_path,
)

EXIT_ALREADY_RUNNING = 3  # = daemon.EXIT_ALREADY_RUNNING: другой демон уже стартует — ждём его сокет
MAX_TEXT = 4096  # лимит Telegram на одно сообщение

DAEMON_CMDS = {"whoami", "chats", "read", "send", "watch", "status"}


# ---------- связь с демоном ----------

def connect() -> socket.socket | None:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(str(SOCK))
        return s
    except (FileNotFoundError, ConnectionRefusedError):
        s.close()
        return None


def log_tail(n: int = 5) -> str:
    try:
        return "\n".join(DAEMON_LOG.read_text().splitlines()[-n:])
    except FileNotFoundError:
        return "(лога нет)"


def ensure_daemon() -> socket.socket:
    s = connect()
    if s:
        return s
    ensure_state_dir()
    with open(SPAWN_LOCK, "w") as lock:  # две команды разом не поднимут два демона
        fcntl.flock(lock, fcntl.LOCK_EX)
        s = connect()
        if s:
            return s
        if DAEMON_LOG.exists() and DAEMON_LOG.stat().st_size > 5_000_000:
            DAEMON_LOG.rename(DAEMON_LOG.with_suffix(".log.1"))
        with open(DAEMON_LOG, "a") as logf:
            proc = subprocess.Popen(
                [sys.executable, "-m", "tg_agent", "daemon"],
                stdin=subprocess.DEVNULL, stdout=logf, stderr=logf, start_new_session=True,
            )
        deadline = time.time() + 30
        while time.time() < deadline:
            s = connect()
            if s:
                return s
            if proc.poll() not in (None, EXIT_ALREADY_RUNNING):
                raise TgError(f"демон не запустился (код {proc.returncode}). {DAEMON_LOG}:\n{log_tail()}")
            time.sleep(0.2)
        raise TgError(f"демон не ответил за 30с. {DAEMON_LOG}:\n{log_tail()}")


def call(cmd: str, args: dict, *, sock: socket.socket | None = None, wm_file: Path | None = None) -> int:
    """Запрос к демону. Для watch (wm_file задан) — печатать сообщения и сдвигать метки ПОСЛЕ печати."""
    s = sock or ensure_daemon()
    err_out = sys.stdout if wm_file else sys.stderr  # для Monitor ошибка должна быть событием
    wm = load_wm(wm_file) if wm_file else {}
    if wm_file:
        args = {**args, "since": wm}
    s.sendall((json.dumps({"cmd": cmd, "args": args}, ensure_ascii=False) + "\n").encode())
    with s, s.makefile("r", encoding="utf-8") as f:
        for line in f:
            msg = json.loads(line)
            if "out" in msg:
                print(msg["out"], flush=True)
            if "msg" in msg:
                print(msg["msg"], flush=True)
                wm[str(msg["pid"])] = max(wm.get(str(msg["pid"]), 0), msg["id"])
                save_wm(wm_file, wm)
            if "init" in msg:
                wm.setdefault(str(msg["init"]), msg["id"])
                save_wm(wm_file, wm)
            if "err" in msg:
                print(f"tg: {msg['err']}", file=err_out, flush=True)
            if "rc" in msg:
                return msg["rc"]
    print("tg: демон закрыл соединение (остановлен или упал) — перезапусти команду, демон поднимется заново",
          file=err_out, flush=True)
    return 1


def cmd_stop(args) -> int:
    s = connect()
    if not s:
        print("демон не запущен")
        return 0
    return call("shutdown", {}, sock=s)


# ---------- настройка (локально, интерактивно) ----------

async def cmd_login(args) -> int:
    if not keyring.get_password(SERVICE, "api_id"):
        raise TgError("сначала: tg setup-api")
    client = make_client(session="")
    await client.start()  # спросит телефон, код и пароль 2FA в терминале
    me = await client.get_me()
    store("session", client.session.save())
    await client.disconnect()
    print(f"вошли как {utils.get_display_name(me)} (@{me.username}); сессия в Связке ключей ({SERVICE}/session)")
    cmd_stop(args)  # демон со старой сессией — остановить, следующая команда поднимет с новой
    return 0


def cmd_setup_api(args) -> int:
    print("api_id и api_hash: https://my.telegram.org → API development tools")
    api_id = input("api_id: ").strip()
    if not api_id.isdigit():
        raise TgError("api_id — число")
    store("api_id", api_id)
    store("api_hash", getpass.getpass("api_hash (не отображается): ").strip())
    print("сохранено в Связке ключей; дальше: tg login")
    return 0


def cmd_setup_bot(args) -> int:
    token = getpass.getpass("токен бота от @BotFather (не отображается): ").strip()
    name = bot_api(token, "getMe")["username"]  # ошибки сети/токена — TgError с понятным текстом
    store("bot_token", token)
    print(f"бот @{name} сохранён в Связке ключей")
    return 0


def cmd_install_skill(args) -> int:
    """Скопировать скилл Claude Code из пакета. После обновления tg — запустить снова."""
    target = Path(args.dir).expanduser() / "telegram"
    if target.is_symlink():
        print(f"{target} — ссылка (установка для разработки), не трогаю")
        return 0
    target.mkdir(parents=True, exist_ok=True)
    src = resources.files("tg_agent") / "skill" / "SKILL.md"
    (target / "SKILL.md").write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"скилл установлен: {target / 'SKILL.md'}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="tg", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup-api", help="сохранить api_id/api_hash (интерактивно)")
    sub.add_parser("login", help="войти аккаунтом (интерактивно: телефон, код, 2FA)")
    sub.add_parser("setup-bot", help="сохранить токен бота (интерактивно)")
    sub.add_parser("whoami", help="проверить аккаунт и бота")
    sub.add_parser("status", help="состояние демона и подписок")
    sub.add_parser("stop", help="остановить демон")
    p = sub.add_parser("install-skill", help="поставить скилл для Claude Code (~/.claude/skills/telegram)")
    p.add_argument("--dir", default="~/.claude/skills", help="каталог скиллов (по умолчанию ~/.claude/skills)")
    sub.add_parser("daemon", help="запустить демон на переднем плане (обычно не нужно — поднимается сам)")
    p = sub.add_parser("chats", help="список чатов")
    p.add_argument("query", nargs="?", help="подстрока названия")
    p.add_argument("-n", "--limit", type=int, default=30)
    p = sub.add_parser("read", help="последние сообщения чата")
    p.add_argument("chat", help="@username, t.me-ссылка, id или часть названия")
    p.add_argument("-n", "--limit", type=int, default=30)
    p.add_argument("--after", type=int, help="только сообщения с id больше этого")
    p = sub.add_parser("watch", help="новые сообщения, по строке (для Monitor)")
    p.add_argument("chats", nargs="+")
    p.add_argument("--key", default="default",
                   help="своя метка «прочитано» для параллельного подписчика (по умолчанию default)")
    p = sub.add_parser("send", help="написать (по умолчанию от бота)")
    p.add_argument("chat")
    p.add_argument("text", help="текст или - (из stdin)")
    p.add_argument("--as-me", action="store_true", help="от аккаунта пользователя — только по его явной просьбе")
    p.add_argument("--reply-to", type=int)
    args = ap.parse_args()

    try:
        if args.cmd == "daemon":
            from .daemon import run_daemon
            run_daemon()
            return
        if args.cmd in DAEMON_CMDS:
            payload = {k: v for k, v in vars(args).items() if k not in ("cmd", "key")}
            if args.cmd == "send":
                if args.text == "-":
                    payload["text"] = sys.stdin.read()
                if not payload["text"].strip():
                    raise TgError("пустой текст")
                if len(payload["text"]) > MAX_TEXT:
                    raise TgError(f"текст длиннее {MAX_TEXT} символов — раздели на несколько сообщений")
            rc = call(args.cmd, payload, wm_file=wm_path(args.key) if args.cmd == "watch" else None)
        else:
            handler = {"setup-api": cmd_setup_api, "setup-bot": cmd_setup_bot,
                       "login": cmd_login, "stop": cmd_stop,
                       "install-skill": cmd_install_skill}[args.cmd]
            res = handler(args)
            rc = asyncio.run(res) if asyncio.iscoroutine(res) else res
    except TgError as e:
        print(f"tg: {e}", file=sys.stdout if args.cmd == "watch" else sys.stderr)
        rc = 1
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)

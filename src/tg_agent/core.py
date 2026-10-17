"""Общее для CLI и демона: секреты, клиент Telethon, поиск чата, формат сообщений, Bot API, метки."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path

import keyring
from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.tl.custom import Message

SERVICE = "tg-agent"
DEFAULT_STATE_DIR = Path.home() / ".local/state/tg-agent"
STATE_DIR = Path(os.environ.get("TG_AGENT_STATE", DEFAULT_STATE_DIR)).expanduser()
SOCK = STATE_DIR / "tgd.sock"
DAEMON_LOG = STATE_DIR / "daemon.log"
DAEMON_LOCK = STATE_DIR / "tgd.lock"
SPAWN_LOCK = STATE_DIR / "spawn.lock"
CATCH_UP_LIMIT = 200  # больше пропущенного за раз не выдаём — иначе залп в чат агента


class TgError(Exception):
    pass


def ensure_state_dir() -> None:
    """Каталог состояния — только для владельца. chmod — только на свой каталог по умолчанию или на
    только что созданный: TG_AGENT_STATE=~ не должен закрыть домашний каталог."""
    created = not STATE_DIR.exists()
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    if created or STATE_DIR == DEFAULT_STATE_DIR:
        STATE_DIR.chmod(0o700)


# ---------- секреты ----------

def secret(name: str) -> str:
    value = keyring.get_password(SERVICE, name)
    if not value:
        hint = {"api_id": "tg setup-api", "api_hash": "tg setup-api",
                "session": "tg login", "bot_token": "tg setup-bot"}[name]
        raise TgError(f"нет секрета {name} в Связке ключей — сначала: {hint}")
    return value


def store(name: str, value: str) -> None:
    keyring.set_password(SERVICE, name, value)


# ---------- клиент и чаты ----------

def make_client(session: str | None = None, *, daemon: bool = False) -> TelegramClient:
    kw = {"connection_retries": -1, "retry_delay": 5, "auto_reconnect": True} if daemon else {}
    return TelegramClient(
        StringSession(session if session is not None else secret("session")),
        int(secret("api_id")),
        secret("api_hash"),
        catch_up=True,
        **kw,
    )


def chat_title(entity) -> str:
    return utils.get_display_name(entity) or str(utils.get_peer_id(entity))


def is_numeric_id(query: str) -> bool:
    return re.fullmatch(r"-?\d+", query.strip()) is not None


LINK = re.compile(r"^(?:https?://)?(?:www\.)?t(?:elegram)?\.me/(.+)$")


def parse_query(query: str) -> tuple[str, bool]:
    """→ (нормализованный запрос, явный ли он). Явный = @username, ссылка или id: для них
    промах — ошибка, а не поиск по подстроке (иначе опечатка молча выберет чужой чат)."""
    q = query.strip()
    m = LINK.match(q)
    if m:
        parts = m.group(1).split("/")
        if parts[0] in ("joinchat",) or parts[0].startswith("+"):
            raise TgError("инвайт-ссылки не поддерживаются — открой чат и дай его id (tg chats)")
        if parts[0] == "c" and len(parts) > 1 and parts[1].isdigit():
            return f"-100{parts[1]}", True
        if parts[0] == "s" and len(parts) > 1:
            parts = parts[1:]
        return "@" + parts[0], True
    return q, q.startswith("@") or is_numeric_id(q)


async def resolve(client: TelegramClient, query: str, *, fuzzy: bool):
    """Чат по @username, t.me-ссылке, числовому id или названию.

    fuzzy=True — название можно частью (чтение, подписка). fuzzy=False — только полное название
    (отправка). Явный запрос (@, ссылка, id) — только точное совпадение, всегда.
    """
    q, explicit = parse_query(query)
    if explicit:
        try:
            return await client.get_entity(int(q) if is_numeric_id(q) else q)
        except ValueError:
            pass  # нет в кэше сущностей — поищем среди своих диалогов, но только точно
    needle = q.lstrip("@").casefold() if q.startswith("@") else q.casefold()
    exact, partial = [], []
    async for d in client.iter_dialogs():
        title = (d.name or "").casefold()
        uname = (getattr(d.entity, "username", None) or "").casefold()
        if (explicit and (str(d.id) == q or (q.startswith("@") and uname == needle))) or \
                (not explicit and title == needle):
            exact.append(d)
        elif not explicit and fuzzy and needle in title:
            partial.append(d)
    found = exact or partial
    if len(found) == 1:
        return found[0].entity
    if not found:
        how = " (для отправки — полное название или id)" if not explicit and not fuzzy else ""
        raise TgError(f"чат «{query}» не найден{how} — поискать: tg chats <часть названия>")
    lines = "\n".join(f"  {d.id}  {d.name}" for d in found[:15])
    raise TgError(f"«{query}» подходит к нескольким чатам, уточни id:\n{lines}")


def sender_name(msg: Message) -> str:
    s = msg.sender
    if s is None:
        return chat_title(msg.chat) if msg.chat else "?"
    name = utils.get_display_name(s)
    uname = getattr(s, "username", None)
    return f"{name} (@{uname})" if uname else name


def fmt(msg: Message, chat: str | None = None, compact: bool = False) -> str:
    ts = msg.date.astimezone().strftime("%Y-%m-%d %H:%M")
    parts = [f"[{ts}]"]
    if chat:
        parts.append(f"«{chat}»")
    parts.append(f"#{msg.id}")
    parts.append(("я" if msg.out else sender_name(msg)) + ":")
    if msg.reply_to_msg_id:
        parts.append(f"↩#{msg.reply_to_msg_id}")
    text = msg.message or ""
    if msg.media:
        kind = type(msg.media).__name__.removeprefix("MessageMedia")
        text = f"[{kind}] {text}".strip()
    if compact:
        text = text.replace("\n", " ⏎ ")
    parts.append(text)
    return " ".join(parts)


# ---------- Bot API ----------

def bot_api(token: str, method: str, payload: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        body = e.read()
    except urllib.error.URLError as e:
        raise TgError(f"Bot API {method}: нет связи с api.telegram.org ({e.reason})") from e
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise TgError(f"Bot API {method}: неожиданный ответ ({body[:200]!r})") from e
    if not data.get("ok"):
        raise TgError(f"Bot API {method}: {data.get('description')}")
    return data["result"]


# ---------- метки подписки ----------

def wm_path(key: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", key):
        raise TgError("--key: только латиница, цифры, _ . -")
    return STATE_DIR / ("watermarks.json" if key == "default" else f"watermarks-{key}.json")


def load_wm(path: Path) -> dict[str, int]:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_wm(path: Path, wm: dict[str, int]) -> None:
    ensure_state_dir()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(wm, indent=1))
    tmp.replace(path)

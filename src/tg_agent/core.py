"""Общее для CLI и демона: секреты, клиент Telethon, поиск чата, формат сообщений, Bot API, метки."""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

import keyring
from telethon import TelegramClient, utils
from telethon.errors import FloodWaitError, RPCError
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


def check_sock_path() -> None:
    # sockaddr_un.sun_path: 104 байта на macOS, 108 на Linux
    if len(str(SOCK).encode()) > 100:
        raise TgError(f"путь к сокету слишком длинный ({SOCK}) — задай короче TG_AGENT_STATE (свой каталог, не /tmp)")


def ensure_state_dir() -> None:
    """Каталог состояния — только для владельца. chmod — только на свой каталог по умолчанию или на
    только что созданный: TG_AGENT_STATE=~ не должен закрыть домашний каталог. Чужой, общий или
    симлинк — отказ: там сокет, лог и метки, а в общем каталоге (/tmp) их подсунет другой пользователь."""
    created = not STATE_DIR.exists()
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    if created or STATE_DIR == DEFAULT_STATE_DIR:
        STATE_DIR.chmod(0o700)
    st = os.lstat(STATE_DIR)
    if stat.S_ISLNK(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise TgError(f"каталог {STATE_DIR} — симлинк, чужой или доступен другим (нужен свой, 700); "
                      "TG_AGENT_STATE — только в свой каталог (на macOS подойдёт $TMPDIR)")


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
    # Демон: переподключаться бесконечно; на FloodWait не спать молча дольше 5с — сказать клиенту.
    kw = {"connection_retries": -1, "retry_delay": 5, "auto_reconnect": True,
          "flood_sleep_threshold": 5} if daemon else {}
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
        parts = re.split(r"[?#]", m.group(1), maxsplit=1)[0].split("/")
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
            # get_input_entity берёт из кэша; get_entity("@x") каждый раз ходит в ResolveUsername,
            # а он у Telegram жёстко ограничен по частоте
            inp = await client.get_input_entity(int(q) if is_numeric_id(q) else q)
            return await client.get_entity(inp)
        except FloodWaitError:
            raise  # не «не найден»: Telegram просит подождать — пусть это и увидят
        except (ValueError, RPCError):
            pass  # нет в кэше / нет такого username — поищем среди своих диалогов, но только точно
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


# Всё, что пишут в чат (текст, имена, названия), контролирует посторонний, а вывод читает LLM-агент.
# Поэтому: одно сообщение = одна строка (иначе текст рисует поддельные строки «#id я: …» или
# «tg-watch started»), управляющие и bidi-символы — видимыми escape-последовательностями
# (терминал, ANSI, подмена порядка), а «своё» сообщение помечается так, как текстом не подделать.
LINE_BREAKS = re.compile(r"\r\n|[\r\n\u2028\u2029\u0085\v\f]")
UNSAFE = re.compile(r"[\x00-\x08\x0e-\x1f\x7f-\x9f\u200b-\u200f\u202a-\u202e\u2060-\u2069\ufeff]")


def clean(s: str | None) -> str:
    s = LINE_BREAKS.sub(" ⏎ ", s or "")
    return UNSAFE.sub(lambda m: f"\\u{ord(m.group()):04x}", s)


def sender_name(msg: Message) -> str:
    """Имя — не доказательство: его выбирает сам отправитель. Поэтому всегда с id."""
    s = msg.sender
    sid = msg.sender_id
    if s is None:
        name = chat_title(msg.chat) if msg.chat else "?"
    else:
        name = utils.get_display_name(s)
        uname = getattr(s, "username", None)
        if uname:
            name += f" @{uname}"
    return f"{clean(name)} (id {sid})"


def fmt(msg: Message, chat: str | None = None) -> str:
    ts = msg.date.astimezone().strftime("%Y-%m-%d %H:%M")
    parts = [f"[{ts}]"]
    if chat:
        parts.append(f"«{clean(chat)}»")
    parts.append(f"#{msg.id}")
    # [я] ставит код по msg.out; текст и имя чужого сообщения в квадратные скобки сюда не попадут
    parts.append(("[я]" if msg.out else sender_name(msg)) + ":")
    if msg.reply_to_msg_id:
        parts.append(f"↩#{msg.reply_to_msg_id}")
    if getattr(msg, "action", None) is not None:  # вступление, закреп, смена названия, миграция…
        parts[-1] = parts[-1].replace("↩", "→")  # у закрепа reply_to — закреплённое сообщение
        kind = type(msg.action).__name__.removeprefix("MessageAction")
        parts.append(f"[служебное: {kind}]")
        return " ".join(parts)
    text = msg.message or ""
    if msg.media:
        kind = type(msg.media).__name__.removeprefix("MessageMedia")
        text = f"[{kind}] {text}".strip()
    if getattr(msg, "fwd_from", None):
        text = f"[переслано] {text}"
    parts.append(clean(text))
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
        extra = data.get("parameters") or {}
        hint = ""
        if "retry_after" in extra:
            hint = f" — Telegram просит подождать {extra['retry_after']}с"
        if "migrate_to_chat_id" in extra:
            hint = f" — группа стала супергруппой, новый id {extra['migrate_to_chat_id']}"
        raise TgError(f"Bot API {method}: {data.get('description')}{hint}")
    return data["result"]


# ---------- метки подписки ----------

def wm_path(key: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", key):
        raise TgError("--key: только латиница, цифры, _ . -")
    return STATE_DIR / ("watermarks.json" if key == "default" else f"watermarks-{key}.json")


def load_wm(path: Path) -> dict[str, int]:
    """Метки подписчика. Повреждённый файл — TgError: молча начинать «с текущего момента» нельзя,
    это потеря всего пропущенного."""
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as e:
        raise TgError(f"файл меток {path} повреждён ({e})") from e
    if not isinstance(data, dict) or not all(is_numeric_id(k) and isinstance(v, int) for k, v in data.items()):
        raise TgError(f"файл меток {path} повреждён (неожиданный формат)")
    return data


def save_wm(path: Path, updates: dict[str, int]) -> None:
    """Слить метки в файл: под блокировкой перечитать, взять максимум по каждому чату, записать
    атомарно. Несколько tg watch с одним --key не затирают метки друг друга."""
    ensure_state_dir()
    with open(path.with_name(path.name + ".lock"), "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            cur = load_wm(path)
        except TgError:
            cur = {}  # повреждённый перезаписываем свежими данными
        for k, v in updates.items():
            cur[k] = max(cur.get(k, v), v)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(cur, f, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

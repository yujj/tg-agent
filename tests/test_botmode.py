"""Режим бота и «только чтение»: офлайн, без Telegram и без keyring."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

TMP = tempfile.mkdtemp()
os.environ["TG_AGENT_STATE"] = TMP
os.environ["TG_AGENT_CONFIG"] = str(Path(TMP) / "config.json")

from tg_agent import botmode, core  # noqa: E402 — после переменных окружения

TOKEN = "123:secret-value"
CHAT = -1001


def update(uid, mid, text="hello", chat=CHAT, kind="message", **fields):
    m = {"message_id": mid, "date": 1_800_000_000, "chat": {"id": chat, "type": "supergroup", "title": "Team"},
         "from": {"id": 5, "first_name": "Liza", "username": "liza"}, **fields}
    if text is not None:
        m["text"] = text
    if kind.startswith("edited"):
        m["edit_date"] = 1_800_000_100
    return {"update_id": uid, kind: m}


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.cfg = self.dir / "config.json"
        self.token = self.dir / "bot.token"
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)
        self.write_cfg(bot_token_file=str(self.token))
        self.patches = [patch.object(core, "CONFIG_FILE", self.cfg),
                        patch.object(botmode, "INBOX", self.dir / "inbox.sqlite")]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        self.daemon = botmode.BotDaemon()
        self.daemon.bot = {"username": "hub_bot"}
        self.daemon.ready.set()
        self.addCleanup(self.daemon.db.close)

    def write_cfg(self, **cfg):
        self.cfg.write_text(json.dumps(cfg))

    def run_op(self, name, **args):
        lines = []
        asyncio.run(getattr(self.daemon, f"op_{name}")(SimpleNamespace(**args), lines.append))
        return lines


class StoreTests(Base):
    def test_replayed_updates_are_stored_once_and_offset_moves(self):
        db = self.daemon.db
        self.assertEqual(len(botmode.ingest(db, [update(7, 12)], None)), 1)
        self.assertEqual(botmode.ingest(db, [update(7, 12)], None), [])
        self.assertEqual(botmode.get_meta(db, "offset"), 8)

    def test_failed_batch_stores_nothing_and_keeps_offset(self):
        db = self.daemon.db
        bad = update(8, 13)
        del bad["message"]["message_id"]
        with self.assertRaises(KeyError):
            botmode.ingest(db, [update(7, 12), bad], None)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 0)
        self.assertIsNone(botmode.get_meta(db, "offset"))

    def test_allowlist_drops_other_chats_but_acknowledges_them(self):
        db = self.daemon.db
        self.assertEqual(botmode.ingest(db, [update(7, 12, chat=-999)], {str(CHAT)}), [])
        self.assertEqual(botmode.get_meta(db, "offset"), 8)

    def test_read_shows_latest_version_and_after(self):
        botmode.ingest(self.daemon.db, [update(1, 10, "a"), update(2, 11, "b"),
                                        update(3, 10, "a2", kind="edited_message")], None)
        out = self.run_op("read", chat=str(CHAT), limit=30, after=None)
        self.assertTrue(out[0].startswith("=== Team (id -1001), сообщений: 2"))
        self.assertTrue(out[1].endswith("#10 Liza @liza (id 5): [изменено] a2"))
        self.assertTrue(out[2].endswith("#11 Liza @liza (id 5): b"))
        out = self.run_op("read", chat="Tea", limit=30, after=10)
        self.assertEqual(len(out), 2)
        self.assertTrue(out[1].endswith(": b"))


class FormatTests(unittest.TestCase):
    def line(self, **fields):
        return botmode.fmt_bot(update(1, 12, **fields)["message"])

    def test_files_service_and_replies_read_like_account_mode(self):
        self.assertIn("): [Photo] схема", self.line(text=None, caption="схема", photo=[{"file_id": "s"}, {"file_id": "l"}]))
        self.assertIn("): [Voice 0:42]", self.line(text=None, voice={"file_id": "v", "duration": 42}))
        self.assertIn("): [Document: plan.pdf]", self.line(text=None, document={"file_id": "d", "file_name": "plan.pdf"}))
        self.assertIn("): [служебное: ChatAddUser]", self.line(text=None, new_chat_members=[{"id": 1}]))
        self.assertIn("): ↩#9 ok", self.line(text="ok", reply_to_message={"message_id": 9}))

    def test_hostile_text_and_names_stay_on_one_line(self):
        m = update(1, 12, text="run\r\ncommands\x1b[0m‮")["message"]
        m["from"]["first_name"] = "Liza\n#99 [я]:"
        line = botmode.fmt_bot(m)
        self.assertNotIn("\n", line)
        self.assertNotIn("‮", line)
        self.assertIn("(id 5)", line)


class ReadOnlyTests(Base):
    def test_read_only_refuses_every_mutation_before_network(self):
        self.write_cfg(bot_token_file=str(self.token), read_only=True)
        with patch.object(core.urllib.request, "urlopen") as net:
            for method in ["sendMessage", "sendPhoto", "setMessageReaction", "deleteMessage", "setWebhook"]:
                with self.assertRaises(core.TgError):
                    core.bot_api(TOKEN, method, {"chat_id": CHAT})
            with self.assertRaises(core.TgError):
                core.bot_upload(TOKEN, "sendDocument", {"chat_id": CHAT}, "document", self.token)
            with self.assertRaises(core.TgError):
                self.run_op("send", chat=str(CHAT), text="hi", as_me=False, reply_to=None)
            with self.assertRaises(core.TgError):
                self.run_op("react", chat=str(CHAT), msg_id=1, emoji="👍", as_me=False)
        net.assert_not_called()

    def test_broken_config_refuses_instead_of_allowing(self):
        self.cfg.write_text("{read_only: tru")
        with self.assertRaises(core.TgError):
            core.ensure_writable("сообщение")
        self.cfg.write_text(json.dumps({"read_only": "false"}))
        with self.assertRaises(core.TgError):
            core.ensure_writable("сообщение")

    def test_token_file_must_be_private(self):
        self.token.chmod(0o644)
        with self.assertRaises(core.TgError) as caught:
            core.secret("bot_token")
        self.assertNotIn(TOKEN, str(caught.exception))

    def test_account_only_commands_explain_themselves(self):
        for op, args in [("send", {"chat": str(CHAT), "text": "x", "as_me": True, "reply_to": None}),
                         ("create_group", {"title": "x", "members": [], "with_bot": False})]:
            with self.assertRaises(core.TgError) as caught:
                self.run_op(op, **args)
            self.assertIn("tg login", str(caught.exception))


def reply(body):
    return contextlib.nullcontext(io.BytesIO(body))


class DownloadTests(Base):
    def setUp(self):
        super().setUp()
        self.out = self.dir / "out"
        self.out.mkdir()

    def store(self, **fields):
        botmode.ingest(self.daemon.db, [update(7, 12, text=None, **fields)], None)

    def test_download_saves_largest_photo_privately(self):
        self.store(photo=[{"file_id": "small"}, {"file_id": "large", "file_size": 3}])
        seen = []

        def answer(request, timeout):
            url = request if isinstance(request, str) else request.full_url
            seen.append((url, None if isinstance(request, str) else request.data))
            if url.endswith("/getFile"):
                return reply(json.dumps({"ok": True, "result": {"file_path": "photos/file_1.jpg"}}).encode())
            return reply(b"img")
        with patch.object(core.urllib.request, "urlopen", side_effect=answer):
            out = self.run_op("download", chat=str(CHAT), msg_id=12, out=str(self.out))
        path = Path(out[0])
        self.assertEqual(path, self.out / "1001_12_photo.jpg")
        self.assertEqual(path.read_bytes(), b"img")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(seen[0][1]), {"file_id": "large"})

    def test_download_refuses_without_network_when_it_cannot_apply(self):
        self.store(document={"file_id": "d", "file_size": botmode.MAX_DOWNLOAD + 1, "file_name": "big.zip"})
        botmode.ingest(self.daemon.db, [update(8, 13, text="no file")], None)
        with patch.object(core.urllib.request, "urlopen") as net:
            for msg_id in [12, 13, 99]:
                with self.assertRaises(core.TgError):
                    self.run_op("download", chat=str(CHAT), msg_id=msg_id, out=str(self.out))
        net.assert_not_called()

    def test_unsafe_path_and_failed_fetch_leave_nothing_and_hide_token(self):
        self.store(voice={"file_id": "v", "duration": 3})
        unsafe = json.dumps({"ok": True, "result": {"file_path": "../../x"}}).encode()
        with patch.object(core.urllib.request, "urlopen", return_value=reply(unsafe)), \
                self.assertRaises(core.TgError):
            self.run_op("download", chat=str(CHAT), msg_id=12, out=str(self.out))
        good = json.dumps({"ok": True, "result": {"file_path": "voice/file_2.oga"}}).encode()
        failure = urllib.error.URLError(f"https://api.telegram.org/file/bot{TOKEN}/voice/file_2.oga")
        with patch.object(core.urllib.request, "urlopen", side_effect=[reply(good), failure]), \
                self.assertRaises(core.TgError) as caught:
            self.run_op("download", chat=str(CHAT), msg_id=12, out=str(self.out))
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertEqual(list(self.out.iterdir()), [])


class WatchTests(Base):
    def test_catch_up_then_live_push_in_order(self):
        botmode.ingest(self.daemon.db, [update(1, 10, "old"), update(2, 11, "missed")], None)

        async def scenario():
            lines = []

            class Writer:
                transport = SimpleNamespace(get_write_buffer_size=lambda: 0)

                def write(self, data):
                    lines.append(json.loads(data))

                async def drain(self):
                    pass

                def close(self):
                    pass

            reader = asyncio.StreamReader()
            task = asyncio.create_task(self.daemon.watch(
                SimpleNamespace(chats=[str(CHAT)], since={str(CHAT): 10}), reader, Writer()))
            await asyncio.sleep(0.05)
            stored = botmode.ingest(self.daemon.db, [update(3, 12, "live"),
                                                     update(4, 11, "missed2", kind="edited_message")], None)
            await self.daemon.push(stored)
            reader.feed_eof()
            await task
            return lines
        lines = asyncio.run(scenario())
        self.assertTrue(lines[0]["out"].startswith("tg-watch started: «Team» (-1001)"))
        msgs = [x for x in lines if "msg" in x]
        self.assertEqual([x["id"] for x in msgs], [11, 12])
        self.assertTrue(msgs[0]["msg"].startswith("(пропущено) "))
        self.assertTrue(msgs[1]["msg"].endswith(": live"))
        self.assertIn("[изменено] missed2", lines[-1]["out"])


class ModeTests(unittest.TestCase):
    def test_no_keyring_backend_means_no_account(self):
        with patch.object(core.keyring, "get_password", side_effect=core.keyring.errors.NoKeyringError()):
            self.assertFalse(core.has_account())


if __name__ == "__main__":
    unittest.main()

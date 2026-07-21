import datetime as dt
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import app
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session


class TTLDictTests(unittest.TestCase):
    def test_expired_entry_is_never_returned(self):
        cache = app.TTLDict(maxsize=2, ttl=0.01)
        cache["hash"] = "handle"

        time.sleep(0.02)

        self.assertNotIn("hash", cache)
        self.assertEqual(cache.get("hash"), None)
        self.assertEqual(len(cache), 0)

    def test_delete_removes_entry_immediately(self):
        cache = app.TTLDict(maxsize=2, ttl=60)
        cache["hash"] = "handle"

        del cache["hash"]

        self.assertNotIn("hash", cache)

    def test_capacity_is_reported_without_replacing_entries(self):
        cache = app.TTLDict(maxsize=1, ttl=60)

        self.assertTrue(cache.add_if_absent("first", None))
        self.assertFalse(cache.add_if_absent("second", None))
        self.assertFalse(cache.add_if_absent("first", "replacement"))
        self.assertEqual(cache.keys(), ["first"])

    def test_put_can_replace_reserved_entry_at_capacity(self):
        cache = app.TTLDict(maxsize=1, ttl=60)
        cache.add_if_absent("hash", None)

        self.assertTrue(cache.put("hash", "handle"))
        self.assertEqual(cache["hash"], "handle")


class HelperTests(unittest.TestCase):
    def test_env_int_uses_default_and_parses_override(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(app.env_int("TEST_INTEGER", 10), 10)

        with patch.dict(os.environ, {"TEST_INTEGER": "42"}):
            self.assertEqual(app.env_int("TEST_INTEGER", 10), 42)

    def test_env_float_parses_override(self):
        with patch.dict(os.environ, {"TEST_FLOAT": "0.25"}):
            self.assertEqual(app.env_float("TEST_FLOAT", 1.0), 0.25)

    def test_env_list_splits_and_ignores_empty_items(self):
        with patch.dict(os.environ, {"TEST_LIST": "one, two, ,three"}):
            self.assertEqual(
                app.env_list("TEST_LIST", ["default"]),
                ["one", "two", "three"]
            )

    def test_invalid_numeric_environment_variable_is_rejected(self):
        with patch.dict(os.environ, {"TEST_INTEGER": "invalid"}):
            with self.assertRaisesRegex(ValueError, "TEST_INTEGER"):
                app.env_int("TEST_INTEGER", 10)

    def test_utc_now_is_naive_utc(self):
        before = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        value = app.utc_now()
        after = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

        self.assertIsNone(value.tzinfo)
        self.assertLessEqual(before, value)
        self.assertLessEqual(value, after)

    def test_resume_path_stays_inside_save_directory(self):
        with tempfile.TemporaryDirectory() as save_path:
            options = SimpleNamespace(save_path=save_path)
            with patch.object(app, "OPTIONS", options, create=True):
                path = app.safe_resume_path("../nested\\torrent", "fallback")

            self.assertEqual(os.path.dirname(path), save_path)
            self.assertEqual(
                os.path.basename(path),
                "_nested_torrent.fastresume"
            )

    def test_handle_hash_prefers_v1(self):
        hashes = SimpleNamespace(
            has_v1=lambda: True,
            has_v2=lambda: True,
            v1="v1-hash",
            v2="v2-hash"
        )
        handle = SimpleNamespace(
            is_valid=lambda: True,
            status=lambda: SimpleNamespace(info_hashes=hashes)
        )

        self.assertEqual(app.get_handle_info_hash(handle), "v1-hash")

    def test_invalid_handle_returns_none(self):
        handle = SimpleNamespace(is_valid=lambda: False)

        self.assertIsNone(app.get_handle_info_hash(handle))

    def test_alert_mask_excludes_unrelated_debug_categories(self):
        options = SimpleNamespace(
            enable_dht=1,
            listen_interface="0.0.0.0",
            port=6881,
            max_download_rate=-1,
            max_upload_rate=-1,
            outgoing_interface="",
            proxy_host=""
        )
        with patch.object(app, "OPTIONS", options, create=True):
            settings = app.init_settings()

        self.assertNotEqual(
            settings["alert_mask"],
            app.lt.alert.category_t.all_categories
        )
        self.assertEqual(settings["active_limit"], app.MAX_TORRENTS)
        self.assertEqual(settings["connections_limit"], 200)

    def test_alert_log_is_printed_only_once(self):
        output = []
        options = SimpleNamespace(print_alerts_log=True)
        console = SimpleNamespace(write=output.append)
        with (
            patch.object(app, "OPTIONS", options, create=True),
            patch.object(app, "CONSOLE", console, create=True),
            patch.object(
                app,
                "ALERTS_LOG",
                app.deque(["message"], maxlen=20),
                create=True
            )
        ):
            app.print_alerts_log()
            app.print_alerts_log()

        self.assertEqual(output, ["message\n"])

    def test_add_torrent_does_not_bypass_capacity(self):
        cache = app.TTLDict(maxsize=1, ttl=60)
        cache.add_if_absent("a" * 40, None)
        session = SimpleNamespace(async_add_torrent=lambda params: self.fail(
            "torrent should not be submitted when capacity is full"
        ))
        options = SimpleNamespace(download_mode=False)

        with (
            patch.object(app, "TORRENTS", cache, create=True),
            patch.object(app, "SESSION", session, create=True),
            patch.object(app, "OPTIONS", options, create=True),
            patch.object(app, "TRACKERS", [], create=True)
        ):
            added = app.add_torrent("b" * 40)

        self.assertFalse(added)


class TorrentAssemblyTests(unittest.TestCase):
    @staticmethod
    def make_torrent_info():
        hashes = SimpleNamespace(
            has_v1=lambda: True,
            v1="hash-v1",
            v2="hash-v2"
        )
        file_entry = SimpleNamespace(
            size=3 * 1024 * 1024 + 512,
            path="file.bin"
        )
        return SimpleNamespace(
            info_hashes=lambda: hashes,
            name=lambda: "torrent",
            total_size=lambda: file_entry.size,
            priv=lambda: False,
            creation_date=lambda: 0,
            files=lambda: [file_entry],
            comment=lambda: "",
            creator=lambda: ""
        )

    def test_file_size_preserves_existing_mib_contract(self):
        torrent_info = self.make_torrent_info()

        torrent = app.assemble_torrent_db_object(torrent_info)

        self.assertEqual(torrent.files[0].file_size, 3)

    def test_local_save_is_idempotent(self):
        engine = create_engine("sqlite://")
        app.Base.metadata.create_all(engine)
        options = SimpleNamespace(use_local_storage=True)

        with (
            patch.object(app, "ENGINE", engine),
            patch.object(app, "OPTIONS", options, create=True)
        ):
            app.save_torrent_db_object(self.make_torrent_info())
            app.save_torrent_db_object(self.make_torrent_info())

        with Session(engine) as session:
            count = session.scalar(select(func.count()).select_from(app.Torrent))

        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()

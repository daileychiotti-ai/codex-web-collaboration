from __future__ import annotations

import importlib.util
import inspect
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "网页会话池" / "会话池.py"
SPEC = importlib.util.spec_from_file_location("chat_pool", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
chat_pool = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = chat_pool
SPEC.loader.exec_module(chat_pool)


def create_legacy_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE chat_slots (
            thread_id TEXT PRIMARY KEY,
            label TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL CHECK (kind = 'chatgpt'),
            declared_model TEXT,
            model_verified INTEGER NOT NULL DEFAULT 0 CHECK (model_verified IN (0, 1)),
            memory_mode TEXT NOT NULL DEFAULT 'user_declared',
            state TEXT NOT NULL DEFAULT 'ready'
                CHECK (state IN ('ready', 'leased', 'retired', 'failed')),
            lease_job_id TEXT,
            leased_at TEXT,
            registered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            note TEXT,
            CHECK (
                (state = 'leased' AND lease_job_id IS NOT NULL AND leased_at IS NOT NULL)
                OR
                (state != 'leased' AND lease_job_id IS NULL AND leased_at IS NULL)
            )
        );
        CREATE TABLE pool_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id TEXT NOT NULL REFERENCES chat_slots(thread_id),
            event_type TEXT NOT NULL,
            job_id TEXT,
            occurred_at TEXT NOT NULL,
            details_json TEXT NOT NULL
        );
        INSERT INTO chat_slots(
            thread_id, label, kind, declared_model, model_verified,
            memory_mode, state, registered_at, updated_at, note
        ) VALUES (
            'legacy-thread', '旧会话', 'chatgpt', NULL, 0,
            'not_verified', 'ready', '2026-08-01T00:00:00+00:00',
            '2026-08-01T00:00:00+00:00', 'legacy'
        );
        INSERT INTO pool_events(
            thread_id, event_type, job_id, occurred_at, details_json
        ) VALUES (
            'legacy-thread', 'registered', NULL,
            '2026-08-01T00:00:00+00:00', '{"label":"旧会话"}'
        );
        """
    )
    connection.commit()
    connection.close()


def create_account_aware_database_with_historic_lease(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE account_profiles (
            profile_id TEXT PRIMARY KEY,
            alias TEXT NOT NULL UNIQUE,
            sentinel_thread_id TEXT NOT NULL UNIQUE,
            sentinel_marker TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            note TEXT
        );
        CREATE TABLE chat_slots (
            thread_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL REFERENCES account_profiles(profile_id),
            label TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind = 'chatgpt'),
            declared_model TEXT,
            model_verified INTEGER NOT NULL DEFAULT 0 CHECK (model_verified IN (0, 1)),
            memory_mode TEXT NOT NULL DEFAULT 'user_declared',
            state TEXT NOT NULL DEFAULT 'ready'
                CHECK (state IN ('ready', 'leased', 'retired', 'failed')),
            lease_job_id TEXT,
            leased_at TEXT,
            registered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            note TEXT,
            UNIQUE(profile_id, label),
            CHECK (
                (state = 'leased' AND lease_job_id IS NOT NULL AND leased_at IS NOT NULL)
                OR
                (state != 'leased' AND lease_job_id IS NULL AND leased_at IS NULL)
            )
        );
        CREATE TABLE pool_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id TEXT NOT NULL REFERENCES chat_slots(thread_id),
            event_type TEXT NOT NULL,
            job_id TEXT,
            occurred_at TEXT NOT NULL,
            details_json TEXT NOT NULL
        );
        INSERT INTO account_profiles VALUES (
            'account-a', '账号 A', 'sentinel-a', 'MARKER-A', 'enabled',
            '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:00+00:00', NULL
        );
        INSERT INTO chat_slots(
            thread_id, profile_id, label, kind, declared_model, model_verified,
            memory_mode, state, registered_at, updated_at, note
        ) VALUES
            ('historic-thread', 'account-a', '历史已用', 'chatgpt', NULL, 0,
             'not_verified', 'ready', '2026-08-01T00:00:00+00:00',
             '2026-08-01T00:00:00+00:00', NULL),
            ('clean-thread', 'account-a', '干净会话', 'chatgpt', NULL, 0,
             'not_verified', 'ready', '2026-08-01T00:00:00+00:00',
             '2026-08-01T00:00:00+00:00', NULL);
        INSERT INTO pool_events(
            thread_id, event_type, job_id, occurred_at, details_json
        ) VALUES
            ('historic-thread', 'baseline_recorded', 'ignored-job',
             '2026-07-31T00:00:00.000+00:00', '{}'),
            ('historic-thread', 'leased', 'historic-job',
             '2026-08-01T00:00:00.000+00:00', '{}'),
            ('historic-thread', 'dispatch_accepted', 'later-job',
             '2026-08-02T00:00:00.000+00:00', '{}');
        """
    )
    connection.commit()
    connection.close()


class ChatPoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.pool = chat_pool.ChatPool(
            Path(self.temporary_directory.name) / "chat-pool.sqlite3"
        )

    def register_profile(
        self,
        profile_id: str = "account-a",
        alias: str = "账号 A",
        sentinel_thread_id: str = "sentinel-a",
        sentinel_marker: str = "ACCOUNT-A-7K4M",
    ):
        return self.pool.register_profile(
            profile_id=profile_id,
            alias=alias,
            sentinel_thread_id=sentinel_thread_id,
            sentinel_marker=sentinel_marker,
            note="test",
        )

    def register(self, thread_id: str, label: str):
        if not self.pool.list_profiles():
            self.register_profile()
        return self.pool.register(
            profile_id="account-a",
            thread_id=thread_id,
            label=label,
            declared_model="gpt-5.6-sol",
            memory_mode="user_declared_off",
            note="test",
        )

    def lease(self, job_id: str, label: str | None = None):
        observation = self.pool.detect_profile(
            job_id=job_id,
            probes=[{"thread_id": "sentinel-a", "marker": "ACCOUNT-A-7K4M"}],
        )
        return self.pool.lease(
            job_id=job_id,
            observation_id=observation.observation_id,
            label=label,
        )

    def test_register_batch_generates_internal_labels_and_preserves_input_order(self) -> None:
        self.register_profile()

        slots = self.pool.register_batch(
            profile_id="account-a",
            thread_ids=["thread-one", "thread-two"],
            declared_model=None,
            memory_mode="not_verified",
            note="正式登记",
        )

        self.assertEqual([slot.thread_id for slot in slots], ["thread-one", "thread-two"])
        self.assertEqual(
            [slot.label for slot in slots],
            ["网页会话-thread-one", "网页会话-thread-two"],
        )
        self.assertTrue(all(slot.state == "ready" for slot in slots))

    def test_register_batch_is_idempotent_and_preserves_existing_slot_fields(self) -> None:
        self.register_profile()
        existing = self.register("thread-one", "人工保留标签")

        slots = self.pool.register_batch(
            profile_id="account-a",
            thread_ids=["thread-one", "thread-two"],
            declared_model=None,
            memory_mode="not_verified",
            note="批量登记",
        )

        self.assertEqual(slots[0].label, existing.label)
        self.assertEqual(slots[0].note, existing.note)
        self.assertEqual(slots[1].label, "网页会话-thread-two")
        self.assertEqual(
            [
                event["event_type"]
                for event in self.pool.events()
                if event["thread_id"] == "thread-one"
            ],
            ["registered"],
        )

    def test_register_batch_rejects_cross_profile_thread_without_partial_insert(self) -> None:
        self.register_profile()
        self.register_profile(
            profile_id="account-b",
            alias="账号 B",
            sentinel_thread_id="sentinel-b",
            sentinel_marker="ACCOUNT-B-9Q2P",
        )
        self.register("thread-owned", "已有槽位")

        with self.assertRaisesRegex(chat_pool.ChatPoolError, "已有会话不能通过批量登记更换账号归属"):
            self.pool.register_batch(
                profile_id="account-b",
                thread_ids=["thread-new", "thread-owned"],
                declared_model=None,
                memory_mode="not_verified",
                note=None,
            )

        self.assertEqual(self.pool.list_slots(profile_id="account-b"), [])

    def test_register_batch_rejects_duplicate_or_blank_thread_ids(self) -> None:
        self.register_profile()
        invalid_batches = [["thread-one", "thread-one"], ["thread-one", ""]]

        for thread_ids in invalid_batches:
            with self.subTest(thread_ids=thread_ids):
                with self.assertRaises(chat_pool.ChatPoolError):
                    self.pool.register_batch(
                        profile_id="account-a",
                        thread_ids=thread_ids,
                        declared_model=None,
                        memory_mode="not_verified",
                        note=None,
                    )

        self.assertEqual(self.pool.list_slots(profile_id="account-a"), [])

    def test_register_and_list_preserve_unverified_model_claim(self) -> None:
        slot = self.register("thread-one", "一")

        self.assertEqual(slot.state, "ready")
        self.assertEqual(slot.profile_id, "account-a")
        self.assertEqual(slot.declared_model, "gpt-5.6-sol")
        self.assertFalse(slot.model_verified)
        self.assertEqual(self.pool.list_slots(), [slot])

    def test_register_and_list_account_profile(self) -> None:
        self.assertTrue(
            hasattr(self.pool, "register_profile"),
            "ChatPool 应提供账号档案登记能力",
        )
        profile = self.register_profile()

        self.assertEqual(profile.profile_id, "account-a")
        self.assertEqual(profile.alias, "账号 A")
        self.assertEqual(profile.sentinel_thread_id, "sentinel-a")
        self.assertEqual(profile.sentinel_marker, "ACCOUNT-A-7K4M")
        self.assertEqual(profile.state, "enabled")
        self.assertEqual(self.pool.list_profiles(), [profile])

    def test_lease_and_complete_require_matching_job(self) -> None:
        self.register("thread-one", "一")
        leased = self.lease(job_id="probe-one", label="一")

        self.assertEqual(leased.state, "leased")
        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.complete(
                thread_id="thread-one",
                job_id="wrong-job",
                outcome="passed",
                return_state="ready",
                request_marker="A",
                response_marker="A",
            )

        completed = self.pool.complete(
            thread_id="thread-one",
            job_id="probe-one",
            outcome="cancelled_before_dispatch",
            return_state="ready",
            request_marker=None,
            response_marker=None,
        )
        self.assertEqual(completed.state, "ready")
        self.assertIsNone(completed.lease_job_id)

    def test_dispatched_slot_finishes_used_and_cannot_lease_for_another_job(self) -> None:
        self.register("thread-one", "一")
        leased = self.lease(job_id="job-one", label="一")
        self.pool.record_job_event(
            thread_id=leased.thread_id,
            job_id="job-one",
            event_type="dispatch_accepted",
            details={"request_marker": "REQ-1"},
        )
        completed = self.pool.complete(
            thread_id=leased.thread_id,
            job_id="job-one",
            outcome="done",
            return_state="used",
            request_marker="REQ-1",
            response_marker="REQ-1",
        )

        self.assertEqual(completed.state, "used")
        self.assertEqual(completed.first_dispatched_job_id, "job-one")
        observation = self.pool.detect_profile(
            job_id="job-two",
            probes=[{"thread_id": "sentinel-a", "marker": "ACCOUNT-A-7K4M"}],
        )
        with self.assertRaisesRegex(chat_pool.ChatPoolError, "没有可领取"):
            self.pool.lease(job_id="job-two", observation_id=observation.observation_id)

    def test_cancel_before_dispatch_can_return_ready(self) -> None:
        self.register("thread-one", "一")
        leased = self.lease(job_id="job-one", label="一")

        completed = self.pool.complete(
            thread_id=leased.thread_id,
            job_id="job-one",
            outcome="cancelled_before_dispatch",
            return_state="ready",
            request_marker=None,
            response_marker=None,
        )

        self.assertEqual(completed.state, "ready")
        self.assertIsNone(completed.first_dispatched_at)

    def test_ready_return_requires_explicit_pre_dispatch_cancellation(self) -> None:
        self.register("thread-one", "一")
        leased = self.lease(job_id="job-one", label="一")

        with self.assertRaisesRegex(chat_pool.ChatPoolError, "明确取消"):
            self.pool.complete(
                thread_id=leased.thread_id,
                job_id="job-one",
                outcome="done",
                return_state="ready",
                request_marker=None,
                response_marker=None,
            )

    def test_dispatched_slot_rejects_ready_return(self) -> None:
        self.register("thread-one", "一")
        leased = self.lease(job_id="job-one", label="一")
        self.pool.record_job_event(
            thread_id=leased.thread_id,
            job_id="job-one",
            event_type="dispatch_accepted",
            details={},
        )

        with self.assertRaisesRegex(chat_pool.ChatPoolError, "已承接业务任务"):
            self.pool.complete(
                thread_id=leased.thread_id,
                job_id="job-one",
                outcome="done",
                return_state="ready",
                request_marker=None,
                response_marker=None,
            )

    def test_one_slot_cannot_be_leased_twice(self) -> None:
        self.register("thread-one", "一")
        self.lease(job_id="probe-one")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.lease(job_id="probe-two")

    def test_duplicate_label_cannot_point_to_another_thread(self) -> None:
        self.register("thread-one", "一")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.register("thread-two", "一")

    def test_record_job_event_requires_matching_active_lease(self) -> None:
        self.register("thread-one", "一")
        self.lease(job_id="probe-one", label="一")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.record_job_event(
                thread_id="thread-one",
                job_id="wrong-job",
                event_type="dispatch_accepted",
                details={"marker": "A"},
            )

        self.pool.record_job_event(
            thread_id="thread-one",
            job_id="probe-one",
            event_type="dispatch_accepted",
            details={"marker": "A"},
        )

        event = self.pool.events()[-1]
        self.assertEqual(event["event_type"], "dispatch_accepted")
        self.assertEqual(event["job_id"], "probe-one")
        self.assertEqual(event["details"], {"marker": "A"})

    def test_record_job_event_rejects_unknown_event_type(self) -> None:
        self.register("thread-one", "一")
        self.lease(job_id="probe-one", label="一")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.record_job_event(
                thread_id="thread-one",
                job_id="probe-one",
                event_type="anything-goes",
                details={},
            )


class ProfileObservationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.pool = chat_pool.ChatPool(
            Path(self.temporary_directory.name) / "chat-pool.sqlite3"
        )
        self.pool.register_profile(
            profile_id="account-a",
            alias="账号 A",
            sentinel_thread_id="sentinel-a",
            sentinel_marker="MARKER-A",
            note=None,
        )
        self.pool.register_profile(
            profile_id="account-b",
            alias="账号 B",
            sentinel_thread_id="sentinel-b",
            sentinel_marker="MARKER-B",
            note=None,
        )

    def test_exact_sentinel_probe_creates_job_bound_observation(self) -> None:
        self.assertTrue(
            hasattr(self.pool, "detect_profile"),
            "ChatPool 应提供哨兵识别能力",
        )

        observation = self.pool.detect_profile(
            job_id="job-a",
            probes=[{"thread_id": "sentinel-a", "marker": "MARKER-A"}],
        )

        self.assertEqual(observation.job_id, "job-a")
        self.assertEqual(observation.profile_id, "account-a")
        self.assertEqual(observation.source, "automatic")
        self.assertEqual(observation.status, "matched")
        self.assertIsNone(observation.consumed_at)
        self.assertEqual(self.pool.list_observations(), [observation])

    def test_wrong_marker_rejects_and_records_audit_row(self) -> None:
        self.assertTrue(hasattr(self.pool, "detect_profile"))

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.detect_profile(
                job_id="job-wrong",
                probes=[{"thread_id": "sentinel-a", "marker": "WRONG"}],
            )

        observation = self.pool.list_observations()[-1]
        self.assertEqual(observation.job_id, "job-wrong")
        self.assertIsNone(observation.profile_id)
        self.assertEqual(observation.source, "automatic")
        self.assertEqual(observation.status, "rejected")
        self.assertEqual(observation.details["matched_profile_ids"], [])

    def test_probes_matching_two_profiles_fail_closed(self) -> None:
        self.assertTrue(hasattr(self.pool, "detect_profile"))

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.detect_profile(
                job_id="job-ambiguous",
                probes=[
                    {"thread_id": "sentinel-a", "marker": "MARKER-A"},
                    {"thread_id": "sentinel-b", "marker": "MARKER-B"},
                ],
            )

        observation = self.pool.list_observations()[-1]
        self.assertEqual(observation.status, "rejected")
        self.assertEqual(
            observation.details["matched_profile_ids"],
            ["account-a", "account-b"],
        )

    def test_disabled_profile_does_not_match(self) -> None:
        self.assertTrue(hasattr(self.pool, "detect_profile"))
        with self.pool.connect() as connection:
            connection.execute(
                "UPDATE account_profiles SET state = 'disabled' WHERE profile_id = 'account-a'"
            )
            connection.commit()

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.detect_profile(
                job_id="job-disabled",
                probes=[{"thread_id": "sentinel-a", "marker": "MARKER-A"}],
            )

        self.assertEqual(self.pool.list_observations()[-1].status, "rejected")

    def test_manual_selection_requires_reason_and_is_job_bound(self) -> None:
        self.assertTrue(
            hasattr(self.pool, "select_profile_manual"),
            "ChatPool 应提供一次性人工账号选择",
        )
        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.select_profile_manual(
                job_id="job-manual",
                profile_id="account-b",
                reason=" ",
            )

        observation = self.pool.select_profile_manual(
            job_id="job-manual",
            profile_id="account-b",
            reason="用户确认当前登录账号 B",
        )

        self.assertEqual(observation.job_id, "job-manual")
        self.assertEqual(observation.profile_id, "account-b")
        self.assertEqual(observation.source, "manual")
        self.assertEqual(observation.status, "matched")
        self.assertEqual(observation.details["reason"], "用户确认当前登录账号 B")


class ProfileScopedLeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.pool = chat_pool.ChatPool(
            Path(self.temporary_directory.name) / "chat-pool.sqlite3"
        )
        self.pool.register_profile(
            profile_id="account-a",
            alias="账号 A",
            sentinel_thread_id="sentinel-a",
            sentinel_marker="MARKER-A",
            note=None,
        )
        self.pool.register_profile(
            profile_id="account-b",
            alias="账号 B",
            sentinel_thread_id="sentinel-b",
            sentinel_marker="MARKER-B",
            note=None,
        )

    def assert_account_aware_lease_api(self) -> None:
        parameters = inspect.signature(self.pool.lease).parameters
        self.assertIn(
            "observation_id",
            parameters,
            "lease 必须要求一次性账号 observation",
        )

    def assert_profile_filter_api(self) -> None:
        parameters = inspect.signature(self.pool.list_slots).parameters
        self.assertIn(
            "profile_id",
            parameters,
            "list_slots 应支持按账号档案过滤",
        )

    def register_slot(
        self,
        profile_id: str,
        thread_id: str,
        label: str,
    ):
        return self.pool.register(
            profile_id=profile_id,
            thread_id=thread_id,
            label=label,
            declared_model=None,
            memory_mode="not_verified",
            note=None,
        )

    def detect(self, job_id: str, profile_id: str):
        suffix = profile_id[-1]
        return self.pool.detect_profile(
            job_id=job_id,
            probes=[
                {
                    "thread_id": f"sentinel-{suffix}",
                    "marker": f"MARKER-{suffix.upper()}",
                }
            ],
        )

    def test_same_label_can_exist_in_different_profiles(self) -> None:
        self.assert_profile_filter_api()
        first = self.register_slot("account-a", "thread-a", "图片生成-01")
        second = self.register_slot("account-b", "thread-b", "图片生成-01")

        self.assertEqual(first.label, second.label)
        self.assertEqual(
            [slot.thread_id for slot in self.pool.list_slots(profile_id="account-a")],
            ["thread-a"],
        )
        self.assertEqual(
            [slot.thread_id for slot in self.pool.list_slots(profile_id="account-b")],
            ["thread-b"],
        )

    def test_observation_leases_only_its_profile_and_is_consumed(self) -> None:
        self.assert_account_aware_lease_api()
        self.register_slot("account-a", "thread-a", "图片生成-01")
        self.register_slot("account-b", "thread-b", "图片生成-01")
        observation = self.detect("job-a", "account-a")

        slot = self.pool.lease(
            job_id="job-a",
            observation_id=observation.observation_id,
            label="图片生成-01",
        )

        self.assertEqual(slot.thread_id, "thread-a")
        stored = self.pool.list_observations()[-1]
        self.assertEqual(stored.status, "consumed")
        self.assertIsNotNone(stored.consumed_at)

    def test_observation_cannot_be_used_by_another_job(self) -> None:
        self.assert_account_aware_lease_api()
        self.register_slot("account-a", "thread-a", "一")
        observation = self.detect("job-a", "account-a")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.lease(
                job_id="job-b",
                observation_id=observation.observation_id,
            )

        self.assertEqual(self.pool.list_observations()[-1].status, "matched")

    def test_expired_observation_is_rejected(self) -> None:
        self.assert_account_aware_lease_api()
        self.register_slot("account-a", "thread-a", "一")
        observation = self.detect("job-expired", "account-a")
        with self.pool.connect() as connection:
            connection.execute(
                """
                UPDATE profile_observations
                SET expires_at = '2000-01-01T00:00:00.000+00:00'
                WHERE observation_id = ?
                """,
                (observation.observation_id,),
            )
            connection.commit()

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.lease(
                job_id="job-expired",
                observation_id=observation.observation_id,
            )

        self.assertEqual(self.pool.list_observations()[-1].status, "matched")

    def test_failed_slot_selection_does_not_consume_observation(self) -> None:
        self.assert_account_aware_lease_api()
        observation = self.detect("job-empty", "account-a")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.lease(
                job_id="job-empty",
                observation_id=observation.observation_id,
                label="不存在",
            )

        self.assertEqual(self.pool.list_observations()[-1].status, "matched")

    def test_consumed_observation_cannot_lease_twice(self) -> None:
        self.assert_account_aware_lease_api()
        self.register_slot("account-a", "thread-a", "一")
        self.register_slot("account-a", "thread-a2", "二")
        observation = self.detect("job-once", "account-a")
        self.pool.lease(
            job_id="job-once",
            observation_id=observation.observation_id,
            label="一",
        )

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.lease(
                job_id="job-once",
                observation_id=observation.observation_id,
                label="二",
            )

    def test_leased_slot_cannot_be_reassigned(self) -> None:
        self.assert_account_aware_lease_api()
        self.assertTrue(
            hasattr(self.pool, "assign_slot"),
            "ChatPool 应提供显式槽位归属调整",
        )
        self.register_slot("account-a", "thread-a", "一")
        observation = self.detect("job-leased", "account-a")
        self.pool.lease(
            job_id="job-leased",
            observation_id=observation.observation_id,
        )

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.assign_slot(thread_id="thread-a", profile_id="account-b")


class CollaborationBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.pool = chat_pool.ChatPool(
            Path(self.temporary_directory.name) / "chat-pool.sqlite3"
        )
        self.pool.register_profile(
            profile_id="account-a",
            alias="账号 A",
            sentinel_thread_id="sentinel-a",
            sentinel_marker="MARKER-A",
            note=None,
        )
        self.pool.register_profile(
            profile_id="account-b",
            alias="账号 B",
            sentinel_thread_id="sentinel-b",
            sentinel_marker="MARKER-B",
            note=None,
        )
        self._register_slot("account-a", "thread-a-1", "A-1")
        self._register_slot("account-a", "thread-a-2", "A-2")
        self._register_slot("account-b", "thread-b-1", "B-1")

    def _register_slot(self, profile_id: str, thread_id: str, label: str):
        return self.pool.register(
            profile_id=profile_id,
            thread_id=thread_id,
            label=label,
            declared_model=None,
            memory_mode="not_verified",
            note=None,
        )

    def _matched_observation(self, job_id: str, profile_id: str = "account-a"):
        suffix = profile_id[-1]
        return self.pool.detect_profile(
            job_id=job_id,
            probes=[
                {
                    "thread_id": f"sentinel-{suffix}",
                    "marker": f"MARKER-{suffix.upper()}",
                }
            ],
        )

    def _open_collaboration(
        self,
        *,
        topic_id: str,
        role: str,
        profile_id: str = "account-a",
        job_id: str | None = None,
        label: str | None = None,
    ):
        stable_job_id = job_id or f"collab-{topic_id}-{role}-{profile_id}"
        observation = self._matched_observation(stable_job_id, profile_id)
        return self.pool.open_collaboration(
            topic_id=topic_id,
            role=role,
            job_id=stable_job_id,
            observation_id=observation.observation_id,
            label=label,
        )

    def test_open_collaboration_leases_one_unused_slot_and_creates_active_binding(self) -> None:
        observation = self._matched_observation(job_id="collab-topic-1")
        opened = self.pool.open_collaboration(
            topic_id="topic-1",
            role="primary",
            job_id="collab-topic-1",
            observation_id=observation.observation_id,
        )

        self.assertTrue(opened.newly_leased)
        self.assertEqual(opened.binding.state, "active")
        self.assertEqual(opened.binding.topic_id, "topic-1")
        self.assertEqual(opened.binding.role, "primary")
        self.assertEqual(opened.slot.state, "leased")
        self.assertEqual(opened.binding.thread_id, opened.slot.thread_id)

    def test_same_topic_role_and_profile_resumes_original_thread(self) -> None:
        first = self._open_collaboration(topic_id="topic-1", role="primary")
        self.pool.pause_collaboration(
            binding_id=first.binding.binding_id,
            job_id=first.binding.lease_job_id,
        )
        observation = self._matched_observation(job_id=first.binding.lease_job_id)
        resumed = self.pool.open_collaboration(
            topic_id="topic-1",
            role="primary",
            job_id=first.binding.lease_job_id,
            observation_id=observation.observation_id,
        )

        self.assertFalse(resumed.newly_leased)
        self.assertEqual(resumed.binding.thread_id, first.binding.thread_id)
        self.assertEqual(resumed.binding.state, "active")
        self.assertEqual(resumed.slot.state, "leased")

    def test_second_role_uses_a_different_unused_thread(self) -> None:
        primary = self._open_collaboration(topic_id="topic-1", role="primary")
        critic = self._open_collaboration(topic_id="topic-1", role="critic")

        self.assertNotEqual(primary.binding.thread_id, critic.binding.thread_id)

    def test_same_topic_role_can_have_separate_bindings_per_profile(self) -> None:
        account_a = self._open_collaboration(topic_id="topic-1", role="primary")
        account_b = self._open_collaboration(
            topic_id="topic-1", role="primary", profile_id="account-b"
        )

        self.assertNotEqual(account_a.binding.profile_id, account_b.binding.profile_id)
        self.assertNotEqual(account_a.binding.thread_id, account_b.binding.thread_id)

    def test_close_after_dispatch_consumes_slot_and_preserves_binding(self) -> None:
        opened = self._open_collaboration(topic_id="topic-1", role="primary")
        self.pool.record_job_event(
            thread_id=opened.slot.thread_id,
            job_id=opened.binding.lease_job_id,
            event_type="dispatch_accepted",
            details={"channel": "collaboration"},
        )
        closed = self.pool.close_collaboration(
            binding_id=opened.binding.binding_id,
            job_id=opened.binding.lease_job_id,
            outcome="discussion_finished",
        )

        self.assertEqual(closed.binding.state, "closed")
        self.assertEqual(closed.slot.state, "used")
        listed = self.pool.list_collaborations(topic_id="topic-1", state="closed")
        self.assertEqual([binding.binding_id for binding in listed], [opened.binding.binding_id])

    def test_cancel_before_dispatch_closes_binding_and_returns_slot_ready(self) -> None:
        opened = self._open_collaboration(topic_id="topic-1", role="primary")

        closed = self.pool.close_collaboration(
            binding_id=opened.binding.binding_id,
            job_id=opened.binding.lease_job_id,
            outcome="cancelled_before_dispatch",
        )

        self.assertEqual(closed.binding.state, "closed")
        self.assertEqual(closed.slot.state, "ready")
        self.assertIsNone(closed.slot.first_dispatched_at)

    def test_other_profile_observation_cannot_resume_binding_and_rolls_back(self) -> None:
        first = self._open_collaboration(topic_id="topic-1", role="primary")
        self.pool.pause_collaboration(
            binding_id=first.binding.binding_id,
            job_id=first.binding.lease_job_id,
        )
        other_profile = self._matched_observation(
            job_id=first.binding.lease_job_id,
            profile_id="account-b",
        )

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.open_collaboration(
                topic_id="topic-1",
                role="primary",
                job_id=first.binding.lease_job_id,
                observation_id=other_profile.observation_id,
            )

        listed = self.pool.list_collaborations(topic_id="topic-1")
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0].profile_id, "account-a")
        self.assertEqual(listed[0].state, "paused")
        stored = [
            item
            for item in self.pool.list_observations()
            if item.observation_id == other_profile.observation_id
        ][0]
        self.assertEqual(stored.status, "matched")

    def test_resume_requires_same_stable_job_and_rolls_back_observation(self) -> None:
        first = self._open_collaboration(topic_id="topic-1", role="primary")
        self.pool.pause_collaboration(
            binding_id=first.binding.binding_id,
            job_id=first.binding.lease_job_id,
        )
        different_job = self._matched_observation(job_id="different-job")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.open_collaboration(
                topic_id="topic-1",
                role="primary",
                job_id="different-job",
                observation_id=different_job.observation_id,
            )

        stored = [
            item
            for item in self.pool.list_observations()
            if item.observation_id == different_job.observation_id
        ][0]
        self.assertEqual(stored.status, "matched")
        binding = self.pool.list_collaborations(topic_id="topic-1")[0]
        self.assertEqual(binding.lease_job_id, first.binding.lease_job_id)
        self.assertEqual(binding.state, "paused")

    def test_blank_identifier_rejection_does_not_consume_observation(self) -> None:
        observation = self._matched_observation(job_id="blank-topic-job")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.open_collaboration(
                topic_id=" ",
                role="primary",
                job_id="blank-topic-job",
                observation_id=observation.observation_id,
            )

        stored = [
            item
            for item in self.pool.list_observations()
            if item.observation_id == observation.observation_id
        ][0]
        self.assertEqual(stored.status, "matched")

    def test_record_collaboration_return_records_artifact_without_closing(self) -> None:
        opened = self._open_collaboration(topic_id="topic-1", role="primary")

        returned = self.pool.record_collaboration_return(
            binding_id=opened.binding.binding_id,
            job_id=opened.binding.lease_job_id,
            artifact="handoff.md",
        )

        self.assertEqual(returned.state, "active")
        self.assertEqual(returned.return_artifact, "handoff.md")
        self.assertIsNotNone(returned.return_received_at)

    def test_closed_job_cannot_be_rebound_and_failed_open_changes_nothing(self) -> None:
        self._assert_closed_job_cannot_rebind("account-b")

    def test_closed_job_cannot_start_another_binding_in_same_account(self) -> None:
        self._assert_closed_job_cannot_rebind("account-a")

    def _assert_closed_job_cannot_rebind(self, profile: str) -> None:
        opened = self._open_collaboration(topic_id="original", role="primary")
        job = opened.binding.lease_job_id
        self.pool.close_collaboration(binding_id=opened.binding.binding_id,
                                      job_id=job, outcome="discussion_finished")
        observation = self._matched_observation(job, profile)
        before_slots = self.pool.list_slots()
        before_events = self.pool.events()
        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.open_collaboration(topic_id="different", role="critic", job_id=job,
                                         observation_id=observation.observation_id)
        self.assertEqual(self.pool.list_slots(), before_slots)
        self.assertEqual(self.pool.events(), before_events)
        observed = next(o for o in self.pool.list_observations()
                        if o.observation_id == observation.observation_id)
        self.assertEqual(observed.status, "matched")
        self.assertEqual(len(self.pool.list_collaborations()), 1)

    def test_automatic_complete_rejects_active_collaboration_binding(self) -> None:
        opened = self._open_collaboration(topic_id="topic-1", role="primary")

        with self.assertRaises(chat_pool.ChatPoolError):
            self.pool.complete(
                thread_id=opened.slot.thread_id,
                job_id=opened.binding.lease_job_id,
                outcome="cancelled_before_dispatch",
                return_state="ready",
                request_marker=None,
                response_marker=None,
            )

        binding = self.pool.list_collaborations(topic_id="topic-1")[0]
        slot = [
            item
            for item in self.pool.list_slots()
            if item.thread_id == opened.slot.thread_id
        ][0]
        self.assertEqual(binding.state, "active")
        self.assertEqual(slot.state, "leased")


class LegacyMigrationTests(unittest.TestCase):
    def test_corrupt_migration_rolls_back_schema_and_data(self) -> None:
        for create_database in (
            create_legacy_database,
            create_account_aware_database_with_historic_lease,
        ):
            with self.subTest(schema=create_database.__name__):
                with tempfile.TemporaryDirectory() as directory:
                    database_path = Path(directory) / "corrupt.sqlite3"
                    create_database(database_path)
                    with closing(sqlite3.connect(database_path)) as connection:
                        connection.execute(
                            "INSERT INTO pool_events(thread_id,event_type,job_id,occurred_at,details_json) "
                            "VALUES ('missing-thread','registered',NULL,'2026-01-01','{}')"
                        )
                        connection.commit()
                        before = list(connection.iterdump())
                    with self.assertRaises(chat_pool.ChatPoolError):
                        chat_pool.ChatPool(database_path)
                    with closing(sqlite3.connect(database_path)) as connection:
                        self.assertEqual(list(connection.iterdump()), before)

    def test_account_aware_database_migrates_historic_leased_slot_to_used(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "chat-pool.sqlite3"
            create_account_aware_database_with_historic_lease(database_path)

            pool = chat_pool.ChatPool(database_path)
            slots = {slot.thread_id: slot for slot in pool.list_slots()}

            self.assertEqual(slots["historic-thread"].state, "used")
            self.assertEqual(
                slots["historic-thread"].first_dispatched_job_id,
                "historic-job",
            )
            self.assertEqual(
                slots["historic-thread"].first_dispatched_at,
                "2026-08-01T00:00:00.000+00:00",
            )
            self.assertEqual(slots["clean-thread"].state, "ready")
    def test_legacy_database_migrates_without_losing_slots_or_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "chat-pool.sqlite3"
            create_legacy_database(database_path)

            pool = chat_pool.ChatPool(database_path)

            self.assertTrue(
                hasattr(pool, "list_profiles"),
                "ChatPool 应在旧库迁移后提供账号档案查询能力",
            )
            profiles = pool.list_profiles()
            self.assertEqual([profile.profile_id for profile in profiles], ["legacy-unassigned"])
            self.assertEqual(profiles[0].state, "disabled")
            slots = pool.list_slots()
            self.assertEqual(len(slots), 1)
            self.assertEqual(slots[0].thread_id, "legacy-thread")
            self.assertEqual(slots[0].profile_id, "legacy-unassigned")
            events = pool.events()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_type"], "registered")
            self.assertEqual(events[0]["details"], {"label": "旧会话"})

    def test_legacy_slot_cannot_lease_until_explicit_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "chat-pool.sqlite3"
            create_legacy_database(database_path)
            pool = chat_pool.ChatPool(database_path)
            pool.register_profile(
                profile_id="account-a",
                alias="账号 A",
                sentinel_thread_id="sentinel-a",
                sentinel_marker="MARKER-A",
                note=None,
            )
            observation = pool.detect_profile(
                job_id="legacy-job",
                probes=[{"thread_id": "sentinel-a", "marker": "MARKER-A"}],
            )

            with self.assertRaises(chat_pool.ChatPoolError):
                pool.lease(
                    job_id="legacy-job",
                    observation_id=observation.observation_id,
                    label="旧会话",
                )

            self.assertEqual(pool.list_slots()[0].profile_id, "legacy-unassigned")
            assigned = pool.assign_slot(
                thread_id="legacy-thread",
                profile_id="account-a",
            )
            self.assertEqual(assigned.profile_id, "account-a")
            leased = pool.lease(
                job_id="legacy-job",
                observation_id=observation.observation_id,
                label="旧会话",
            )
            self.assertEqual(leased.thread_id, "legacy-thread")


class ChatPoolCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database_path = Path(self.temporary_directory.name) / "chat-pool.sqlite3"

    def run_cli(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(MODULE_PATH),
                "--db",
                str(self.database_path),
                *arguments,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )

    def assert_cli_success(self, *arguments: str):
        result = self.run_cli(*arguments)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def register_profile_and_slot(self) -> None:
        self.assert_cli_success(
            "profile-register",
            "--profile-id",
            "account-a",
            "--alias",
            "账号 A",
            "--sentinel-thread-id",
            "sentinel-a",
            "--sentinel-marker",
            "MARKER-A",
        )
        self.assert_cli_success(
            "register",
            "--profile-id",
            "account-a",
            "--thread-id",
            "thread-a",
            "--label",
            "一",
        )

    def test_cli_register_batch_accepts_many_threads_without_manual_labels(self) -> None:
        self.assert_cli_success(
            "profile-register",
            "--profile-id",
            "account-a",
            "--alias",
            "账号 A",
            "--sentinel-thread-id",
            "sentinel-a",
            "--sentinel-marker",
            "MARKER-A",
        )

        slots = self.assert_cli_success(
            "register-batch",
            "--profile-id",
            "account-a",
            "--thread-id",
            "thread-one",
            "--thread-id",
            "thread-two",
        )

        self.assertEqual([slot["thread_id"] for slot in slots], ["thread-one", "thread-two"])
        self.assertEqual(
            [slot["label"] for slot in slots],
            ["网页会话-thread-one", "网页会话-thread-two"],
        )

    def test_cli_completes_profile_aware_lease_round_trip(self) -> None:
        self.register_profile_and_slot()
        observation = self.assert_cli_success(
            "profile-detect",
            "--job-id",
            "job-cli",
            "--probe-json",
            '{"thread_id":"sentinel-a","marker":"MARKER-A"}',
        )

        leased = self.assert_cli_success(
            "lease",
            "--job-id",
            "job-cli",
            "--observation-id",
            observation["observation_id"],
            "--label",
            "一",
        )
        self.assertEqual(leased["thread_id"], "thread-a")
        self.assertEqual(leased["profile_id"], "account-a")

        self.assert_cli_success(
            "record",
            "--thread-id",
            "thread-a",
            "--job-id",
            "job-cli",
            "--event-type",
            "dispatch_accepted",
        )

        completed = self.assert_cli_success(
            "complete",
            "--thread-id",
            "thread-a",
            "--job-id",
            "job-cli",
            "--outcome",
            "passed",
            "--return-state",
            "used",
            "--request-marker",
            "DONE",
            "--response-marker",
            "DONE",
        )
        self.assertEqual(completed["state"], "used")

        slots = self.assert_cli_success("list", "--profile-id", "account-a")
        self.assertEqual([slot["thread_id"] for slot in slots], ["thread-a"])

        audit = self.assert_cli_success("events")
        self.assertEqual(len(audit["profile_observations"]), 1)
        self.assertEqual(audit["profile_observations"][0]["status"], "consumed")
        self.assertEqual(audit["pool_events"][-1]["event_type"], "completed")

    def test_cli_manual_selection_creates_one_job_observation(self) -> None:
        self.register_profile_and_slot()

        observation = self.assert_cli_success(
            "profile-select-manual",
            "--job-id",
            "job-manual-cli",
            "--profile-id",
            "account-a",
            "--reason",
            "用户确认当前账号",
        )

        self.assertEqual(observation["source"], "manual")
        self.assertEqual(observation["job_id"], "job-manual-cli")
        self.assertEqual(observation["profile_id"], "account-a")

    def test_cli_lease_without_observation_fails_closed(self) -> None:
        self.register_profile_and_slot()

        result = self.run_cli("lease", "--job-id", "job-no-observation")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--observation-id", result.stderr)

    def test_cli_collaboration_open_resume_return_and_close(self) -> None:
        self.register_profile_and_slot()
        self.assert_cli_success(
            "register",
            "--profile-id",
            "account-a",
            "--thread-id",
            "thread-b",
            "--label",
            "二",
        )
        first_observation = self.assert_cli_success(
            "profile-detect",
            "--job-id",
            "collab-cli-job",
            "--probe-json",
            '{"thread_id":"sentinel-a","marker":"MARKER-A"}',
        )

        opened = self.assert_cli_success(
            "collaboration-open",
            "--topic-id",
            "topic-cli",
            "--role",
            "primary",
            "--job-id",
            "collab-cli-job",
            "--observation-id",
            first_observation["observation_id"],
        )
        self.assertEqual(opened["binding"]["topic_id"], "topic-cli")
        self.assertEqual(opened["binding"]["role"], "primary")
        self.assertTrue(opened["newly_leased"])

        paused = self.assert_cli_success(
            "collaboration-pause",
            "--binding-id",
            opened["binding"]["binding_id"],
            "--job-id",
            "collab-cli-job",
        )
        self.assertEqual(paused["state"], "paused")

        second_observation = self.assert_cli_success(
            "profile-detect",
            "--job-id",
            "collab-cli-job",
            "--probe-json",
            '{"thread_id":"sentinel-a","marker":"MARKER-A"}',
        )
        resumed = self.assert_cli_success(
            "collaboration-open",
            "--topic-id",
            "topic-cli",
            "--role",
            "primary",
            "--job-id",
            "collab-cli-job",
            "--observation-id",
            second_observation["observation_id"],
        )
        self.assertFalse(resumed["newly_leased"])
        self.assertEqual(
            resumed["slot"]["thread_id"],
            opened["slot"]["thread_id"],
        )

        returned = self.assert_cli_success(
            "collaboration-return",
            "--binding-id",
            opened["binding"]["binding_id"],
            "--job-id",
            "collab-cli-job",
            "--artifact",
            "handoff.md",
        )
        self.assertEqual(returned["return_artifact"], "handoff.md")
        self.assertIsNotNone(returned["return_received_at"])

        listed = self.assert_cli_success(
            "collaboration-list",
            "--topic-id",
            "topic-cli",
            "--profile-id",
            "account-a",
            "--state",
            "active",
        )
        self.assertEqual([item["binding_id"] for item in listed], [opened["binding"]["binding_id"]])

        closed = self.assert_cli_success(
            "collaboration-close",
            "--binding-id",
            opened["binding"]["binding_id"],
            "--job-id",
            "collab-cli-job",
            "--outcome",
            "discussion_finished",
        )
        self.assertEqual(closed["binding"]["state"], "closed")
        self.assertEqual(closed["slot"]["state"], "used")

    def test_cli_collaboration_list_state_rejects_unknown_value(self) -> None:
        result = self.run_cli(
            "collaboration-list",
            "--state",
            "not-a-state",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--state", result.stderr)
        self.assertIn("invalid choice: 'not-a-state'", result.stderr)
        self.assertIn("active", result.stderr)
        self.assertIn("paused", result.stderr)
        self.assertIn("closed", result.stderr)

    def test_collaboration_business_error_is_machine_readable_json(self) -> None:
        result = self.run_cli("collaboration-pause", "--binding-id", "missing-binding", "--job-id", "job-test")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertTrue(result.stderr.strip().startswith("{"), result.stderr)
        error = json.loads(result.stderr)
        self.assertEqual(error["status"], "error")
        self.assertIn("missing-binding", error["error"])


if __name__ == "__main__":
    unittest.main()

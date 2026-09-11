from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator


STATES = ("ready", "leased", "used", "retired", "failed")
RETURN_STATES = ("ready", "used", "retired", "failed")
PROFILE_STATES = ("enabled", "disabled")
COLLABORATION_STATES = ("active", "paused", "closed")
JOB_EVENT_TYPES = (
    "baseline_recorded",
    "dispatch_accepted",
    "result_observed",
    "validation_failed",
    "timed_out",
    "cancel_requested",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class AccountProfile:
    profile_id: str
    alias: str
    sentinel_thread_id: str
    sentinel_marker: str
    state: str
    created_at: str
    updated_at: str
    note: str | None


@dataclass(frozen=True)
class ProfileObservation:
    observation_id: str
    job_id: str
    profile_id: str | None
    source: str
    status: str
    created_at: str
    expires_at: str
    consumed_at: str | None
    details: dict[str, object]


@dataclass(frozen=True)
class ChatSlot:
    thread_id: str
    profile_id: str
    label: str
    kind: str
    declared_model: str | None
    model_verified: bool
    memory_mode: str
    state: str
    lease_job_id: str | None
    leased_at: str | None
    first_dispatched_at: str | None
    first_dispatched_job_id: str | None
    registered_at: str
    updated_at: str
    note: str | None


@dataclass(frozen=True)
class CollaborationBinding:
    binding_id: str
    topic_id: str
    role: str
    thread_id: str
    profile_id: str
    lease_job_id: str
    state: str
    created_at: str
    updated_at: str
    return_received_at: str | None
    return_artifact: str | None
    note: str | None


@dataclass(frozen=True)
class CollaborationOpenResult:
    binding: CollaborationBinding
    slot: ChatSlot
    newly_leased: bool


class ChatPoolError(RuntimeError):
    pass


class ChatPool:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            slot_table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'chat_slots'"
            ).fetchone()
            if slot_table is not None:
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(chat_slots)")
                }
                if "profile_id" not in columns:
                    self._migrate_legacy_schema(connection)
                    return
                if "first_dispatched_at" not in columns:
                    self._migrate_account_aware_schema(connection)
                    return
            self._create_account_aware_schema(connection)
            connection.commit()

    @staticmethod
    def _create_account_aware_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS account_profiles (
                profile_id TEXT PRIMARY KEY,
                alias TEXT NOT NULL UNIQUE,
                sentinel_thread_id TEXT NOT NULL UNIQUE,
                sentinel_marker TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                note TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_slots (
                thread_id TEXT PRIMARY KEY,
                profile_id TEXT NOT NULL REFERENCES account_profiles(profile_id),
                label TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind = 'chatgpt'),
                declared_model TEXT,
                model_verified INTEGER NOT NULL DEFAULT 0 CHECK (model_verified IN (0, 1)),
                memory_mode TEXT NOT NULL DEFAULT 'user_declared',
                state TEXT NOT NULL DEFAULT 'ready'
                    CHECK (state IN ('ready', 'leased', 'used', 'retired', 'failed')),
                lease_job_id TEXT,
                leased_at TEXT,
                first_dispatched_at TEXT,
                first_dispatched_job_id TEXT,
                registered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                note TEXT,
                UNIQUE(profile_id, label),
                CHECK (
                    (state = 'leased' AND lease_job_id IS NOT NULL AND leased_at IS NOT NULL)
                    OR
                    (state != 'leased' AND lease_job_id IS NULL AND leased_at IS NULL)
                )
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pool_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL REFERENCES chat_slots(thread_id),
                event_type TEXT NOT NULL,
                job_id TEXT,
                occurred_at TEXT NOT NULL,
                details_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS profile_observations (
                observation_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                profile_id TEXT REFERENCES account_profiles(profile_id),
                source TEXT NOT NULL CHECK (source IN ('automatic', 'manual')),
                status TEXT NOT NULL CHECK (status IN ('matched', 'rejected', 'consumed')),
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT,
                details_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS collaboration_bindings (
                binding_id TEXT PRIMARY KEY,
                topic_id TEXT NOT NULL,
                role TEXT NOT NULL,
                thread_id TEXT NOT NULL REFERENCES chat_slots(thread_id),
                profile_id TEXT NOT NULL REFERENCES account_profiles(profile_id),
                lease_job_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('active', 'paused', 'closed')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                return_received_at TEXT,
                return_artifact TEXT,
                note TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS collaboration_open_topic_role_profile
            ON collaboration_bindings(topic_id, role, profile_id)
            WHERE state IN ('active', 'paused')
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS collaboration_open_thread
            ON collaboration_bindings(thread_id)
            WHERE state IN ('active', 'paused')
            """
        )

    @staticmethod
    def _historic_use_sql(alias: str = "pool_events_legacy") -> str:
        return f"""
            event_type IN (
                'leased', 'dispatch_accepted', 'result_observed',
                'validation_failed', 'timed_out', 'cancel_requested', 'completed'
            )
        """

    def _migrate_account_aware_schema(self, connection: sqlite3.Connection) -> None:
        connection.commit()
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("ALTER TABLE pool_events RENAME TO pool_events_legacy")
            connection.execute("ALTER TABLE chat_slots RENAME TO chat_slots_legacy")
            self._create_account_aware_schema(connection)
            historic_use = self._historic_use_sql()
            connection.execute(
                f"""
                INSERT INTO chat_slots(
                    thread_id, profile_id, label, kind, declared_model,
                    model_verified, memory_mode, state, lease_job_id,
                    leased_at, first_dispatched_at, first_dispatched_job_id,
                    registered_at, updated_at, note
                )
                SELECT
                    slot.thread_id, slot.profile_id, slot.label, slot.kind,
                    slot.declared_model, slot.model_verified, slot.memory_mode,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND {historic_use}
                    ) THEN 'used' ELSE slot.state END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND {historic_use}
                    ) THEN NULL ELSE slot.lease_job_id END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND {historic_use}
                    ) THEN NULL ELSE slot.leased_at END,
                    (
                        SELECT event.occurred_at FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND {historic_use}
                        ORDER BY event.occurred_at, event.id LIMIT 1
                    ),
                    (
                        SELECT event.job_id FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND {historic_use}
                        ORDER BY event.occurred_at, event.id LIMIT 1
                    ),
                    slot.registered_at, slot.updated_at, slot.note
                FROM chat_slots_legacy slot
                """
            )
            connection.execute(
                """
                INSERT INTO pool_events(
                    id, thread_id, event_type, job_id, occurred_at, details_json
                )
                SELECT id, thread_id, event_type, job_id, occurred_at, details_json
                FROM pool_events_legacy
                """
            )
            connection.execute("DROP TABLE pool_events_legacy")
            connection.execute("DROP TABLE chat_slots_legacy")
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise ChatPoolError("账号感知数据库迁移后外键校验失败")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")

    def _migrate_legacy_schema(self, connection: sqlite3.Connection) -> None:
        connection.commit()
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("ALTER TABLE pool_events RENAME TO pool_events_legacy")
            connection.execute("ALTER TABLE chat_slots RENAME TO chat_slots_legacy")
            self._create_account_aware_schema(connection)
            now = utc_now()
            connection.execute(
                """
                INSERT INTO account_profiles(
                    profile_id, alias, sentinel_thread_id, sentinel_marker,
                    state, created_at, updated_at, note
                ) VALUES (
                    'legacy-unassigned', '旧会话待归属',
                    'legacy-unassigned-sentinel', 'LEGACY-UNASSIGNED',
                    'disabled', ?, ?, '旧数据库自动迁移；人工确认前禁止领取'
                )
                """,
                (now, now),
            )
            historic_use = self._historic_use_sql()
            connection.execute(
                """
                INSERT INTO chat_slots(
                    thread_id, profile_id, label, kind, declared_model,
                    model_verified, memory_mode, state, lease_job_id,
                    leased_at, first_dispatched_at, first_dispatched_job_id,
                    registered_at, updated_at, note
                )
                SELECT
                    slot.thread_id, 'legacy-unassigned', slot.label, slot.kind,
                    slot.declared_model, slot.model_verified, slot.memory_mode,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND """
                + historic_use
                + """
                    ) THEN 'used' ELSE slot.state END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND """
                + historic_use
                + """
                    ) THEN NULL ELSE slot.lease_job_id END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND """
                + historic_use
                + """
                    ) THEN NULL ELSE slot.leased_at END,
                    (
                        SELECT event.occurred_at FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND """
                + historic_use
                + """
                        ORDER BY event.occurred_at, event.id LIMIT 1
                    ),
                    (
                        SELECT event.job_id FROM pool_events_legacy event
                        WHERE event.thread_id = slot.thread_id AND """
                + historic_use
                + """
                        ORDER BY event.occurred_at, event.id LIMIT 1
                    ),
                    slot.registered_at, slot.updated_at, slot.note
                FROM chat_slots_legacy slot
                """
            )
            connection.execute(
                """
                INSERT INTO pool_events(
                    id, thread_id, event_type, job_id, occurred_at, details_json
                )
                SELECT id, thread_id, event_type, job_id, occurred_at, details_json
                FROM pool_events_legacy
                """
            )
            connection.execute("DROP TABLE pool_events_legacy")
            connection.execute("DROP TABLE chat_slots_legacy")
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise ChatPoolError("旧数据库迁移后外键校验失败")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")

    @staticmethod
    def _slot(row: sqlite3.Row) -> ChatSlot:
        values = dict(row)
        values["model_verified"] = bool(values["model_verified"])
        return ChatSlot(**values)

    @staticmethod
    def _profile(row: sqlite3.Row) -> AccountProfile:
        return AccountProfile(**dict(row))

    @staticmethod
    def _observation(row: sqlite3.Row) -> ProfileObservation:
        values = dict(row)
        values["details"] = json.loads(values.pop("details_json"))
        return ProfileObservation(**values)

    @staticmethod
    def _binding(row: sqlite3.Row) -> CollaborationBinding:
        return CollaborationBinding(**dict(row))

    @staticmethod
    def _observation_times() -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        return (
            now.isoformat(timespec="milliseconds"),
            (now + timedelta(minutes=5)).isoformat(timespec="milliseconds"),
        )

    @staticmethod
    def _insert_observation(
        connection: sqlite3.Connection,
        *,
        observation_id: str,
        job_id: str,
        profile_id: str | None,
        source: str,
        status: str,
        created_at: str,
        expires_at: str,
        details: dict[str, object],
    ) -> None:
        connection.execute(
            """
            INSERT INTO profile_observations(
                observation_id, job_id, profile_id, source, status,
                created_at, expires_at, consumed_at, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                observation_id,
                job_id,
                profile_id,
                source,
                status,
                created_at,
                expires_at,
                json.dumps(details, ensure_ascii=False, sort_keys=True),
            ),
        )

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        thread_id: str,
        event_type: str,
        *,
        job_id: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO pool_events(thread_id, event_type, job_id, occurred_at, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                event_type,
                job_id,
                utc_now(),
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
            ),
        )

    def _consume_matched_observation(
        self,
        connection: sqlite3.Connection,
        *,
        job_id: str,
        observation_id: str,
        consumed_at: str,
    ) -> str:
        observation = connection.execute(
            "SELECT * FROM profile_observations WHERE observation_id = ?",
            (observation_id,),
        ).fetchone()
        if observation is None:
            raise ChatPoolError(f"未知账号 observation：{observation_id}")
        if observation["status"] != "matched":
            raise ChatPoolError("账号 observation 已失效或已使用")
        if observation["job_id"] != job_id:
            raise ChatPoolError("账号 observation 与 job_id 不匹配")
        if observation["expires_at"] <= consumed_at:
            raise ChatPoolError("账号 observation 已过期")
        profile_id = observation["profile_id"]
        if profile_id is None:
            raise ChatPoolError("账号 observation 没有唯一匹配档案")
        profile = connection.execute(
            "SELECT state FROM account_profiles WHERE profile_id = ?",
            (profile_id,),
        ).fetchone()
        if profile is None or profile["state"] != "enabled":
            raise ChatPoolError(f"账号档案未启用：{profile_id}")
        cursor = connection.execute(
            """
            UPDATE profile_observations
            SET status = 'consumed', consumed_at = ?
            WHERE observation_id = ? AND status = 'matched'
            """,
            (consumed_at, observation_id),
        )
        if cursor.rowcount != 1:
            raise ChatPoolError("账号 observation 已失效或已使用")
        return profile_id

    def register_profile(
        self,
        *,
        profile_id: str,
        alias: str,
        sentinel_thread_id: str,
        sentinel_marker: str,
        note: str | None,
    ) -> AccountProfile:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", profile_id):
            raise ChatPoolError("profile_id 只能包含小写字母、数字和连字符")
        if profile_id == "legacy-unassigned":
            raise ChatPoolError("不能修改系统保留档案 legacy-unassigned")
        if not alias.strip() or not sentinel_thread_id.strip() or not sentinel_marker.strip():
            raise ChatPoolError("账号别名、哨兵 thread ID 和识别码不能为空")
        now = utc_now()
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT * FROM account_profiles WHERE profile_id = ?", (profile_id,)
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO account_profiles(
                            profile_id, alias, sentinel_thread_id, sentinel_marker,
                            state, created_at, updated_at, note
                        ) VALUES (?, ?, ?, ?, 'enabled', ?, ?, ?)
                        """,
                        (
                            profile_id,
                            alias,
                            sentinel_thread_id,
                            sentinel_marker,
                            now,
                            now,
                            note,
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE account_profiles
                        SET alias = ?, sentinel_thread_id = ?, sentinel_marker = ?,
                            updated_at = ?, note = ?
                        WHERE profile_id = ?
                        """,
                        (
                            alias,
                            sentinel_thread_id,
                            sentinel_marker,
                            now,
                            note,
                            profile_id,
                        ),
                    )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ChatPoolError(f"账号档案登记失败：{exc}") from exc
            row = connection.execute(
                "SELECT * FROM account_profiles WHERE profile_id = ?", (profile_id,)
            ).fetchone()
            assert row is not None
            return self._profile(row)

    def list_profiles(self, state: str | None = None) -> list[AccountProfile]:
        with self.connect() as connection:
            if state is None:
                rows = connection.execute(
                    "SELECT * FROM account_profiles ORDER BY created_at, profile_id"
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM account_profiles
                    WHERE state = ? ORDER BY created_at, profile_id
                    """,
                    (state,),
                ).fetchall()
        return [self._profile(row) for row in rows]

    def detect_profile(
        self,
        *,
        job_id: str,
        probes: list[dict[str, str]],
    ) -> ProfileObservation:
        if not job_id.strip():
            raise ChatPoolError("job_id 不能为空")
        normalized_probes: set[tuple[str, str]] = set()
        for probe in probes:
            thread_id = probe.get("thread_id", "").strip()
            marker = probe.get("marker", "").strip()
            if not thread_id or not marker:
                raise ChatPoolError("每条哨兵观测都必须包含 thread_id 和 marker")
            normalized_probes.add((thread_id, marker))

        observation_id = str(uuid.uuid4())
        created_at, expires_at = self._observation_times()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            profiles = connection.execute(
                """
                SELECT * FROM account_profiles
                WHERE state = 'enabled' ORDER BY profile_id
                """
            ).fetchall()
            matched_profile_ids = [
                row["profile_id"]
                for row in profiles
                if (row["sentinel_thread_id"], row["sentinel_marker"])
                in normalized_probes
            ]
            details: dict[str, object] = {
                "matched_profile_ids": matched_profile_ids,
                "probe_count": len(normalized_probes),
            }
            if len(matched_profile_ids) != 1:
                self._insert_observation(
                    connection,
                    observation_id=observation_id,
                    job_id=job_id,
                    profile_id=None,
                    source="automatic",
                    status="rejected",
                    created_at=created_at,
                    expires_at=expires_at,
                    details=details,
                )
                connection.commit()
                raise ChatPoolError(
                    f"哨兵识别必须唯一匹配，当前匹配数：{len(matched_profile_ids)}"
                )
            profile_id = matched_profile_ids[0]
            self._insert_observation(
                connection,
                observation_id=observation_id,
                job_id=job_id,
                profile_id=profile_id,
                source="automatic",
                status="matched",
                created_at=created_at,
                expires_at=expires_at,
                details=details,
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM profile_observations WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
            assert row is not None
            return self._observation(row)

    def select_profile_manual(
        self,
        *,
        job_id: str,
        profile_id: str,
        reason: str,
    ) -> ProfileObservation:
        if not job_id.strip():
            raise ChatPoolError("job_id 不能为空")
        if not reason.strip():
            raise ChatPoolError("人工选择必须记录理由")
        observation_id = str(uuid.uuid4())
        created_at, expires_at = self._observation_times()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            profile = connection.execute(
                "SELECT state FROM account_profiles WHERE profile_id = ?", (profile_id,)
            ).fetchone()
            if profile is None:
                connection.rollback()
                raise ChatPoolError(f"未知账号档案：{profile_id}")
            if profile["state"] != "enabled":
                connection.rollback()
                raise ChatPoolError(f"账号档案未启用：{profile_id}")
            self._insert_observation(
                connection,
                observation_id=observation_id,
                job_id=job_id,
                profile_id=profile_id,
                source="manual",
                status="matched",
                created_at=created_at,
                expires_at=expires_at,
                details={"reason": reason.strip()},
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM profile_observations WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
            assert row is not None
            return self._observation(row)

    def list_observations(self) -> list[ProfileObservation]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM profile_observations ORDER BY rowid"
            ).fetchall()
        return [self._observation(row) for row in rows]

    def register(
        self,
        *,
        profile_id: str,
        thread_id: str,
        label: str,
        declared_model: str | None,
        memory_mode: str,
        note: str | None,
    ) -> ChatSlot:
        now = utc_now()
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                profile = connection.execute(
                    "SELECT state FROM account_profiles WHERE profile_id = ?", (profile_id,)
                ).fetchone()
                if profile is None:
                    raise ChatPoolError(f"未知账号档案：{profile_id}")
                if profile["state"] != "enabled":
                    raise ChatPoolError(f"账号档案未启用：{profile_id}")
                existing = connection.execute(
                    "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO chat_slots(
                            thread_id, profile_id, label, kind, declared_model, model_verified,
                            memory_mode, state, registered_at, updated_at, note
                        ) VALUES (?, ?, ?, 'chatgpt', ?, 0, ?, 'ready', ?, ?, ?)
                        """,
                        (
                            thread_id,
                            profile_id,
                            label,
                            declared_model,
                            memory_mode,
                            now,
                            now,
                            note,
                        ),
                    )
                    self._event(
                        connection,
                        thread_id,
                        "registered",
                        details={
                            "profile_id": profile_id,
                            "label": label,
                            "declared_model": declared_model,
                        },
                    )
                else:
                    if existing["profile_id"] != profile_id:
                        raise ChatPoolError("已有会话不能通过登记命令更换账号归属")
                    connection.execute(
                        """
                        UPDATE chat_slots
                        SET label = ?, declared_model = ?, memory_mode = ?, note = ?, updated_at = ?
                        WHERE thread_id = ?
                        """,
                        (label, declared_model, memory_mode, note, now, thread_id),
                    )
                    self._event(
                        connection,
                        thread_id,
                        "registration_refreshed",
                        details={
                            "profile_id": profile_id,
                            "label": label,
                            "declared_model": declared_model,
                        },
                    )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ChatPoolError(f"登记失败：{exc}") from exc
            row = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            assert row is not None
            return self._slot(row)

    def register_batch(
        self,
        *,
        profile_id: str,
        thread_ids: list[str],
        declared_model: str | None,
        memory_mode: str,
        note: str | None,
    ) -> list[ChatSlot]:
        normalized = [thread_id.strip() for thread_id in thread_ids]
        if not normalized or any(not thread_id for thread_id in normalized):
            raise ChatPoolError("批量登记至少需要一个非空 thread ID")
        if len(set(normalized)) != len(normalized):
            raise ChatPoolError("批量登记不能包含重复 thread ID")

        now = utc_now()
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                profile = connection.execute(
                    "SELECT state FROM account_profiles WHERE profile_id = ?",
                    (profile_id,),
                ).fetchone()
                if profile is None:
                    raise ChatPoolError(f"未知账号档案：{profile_id}")
                if profile["state"] != "enabled":
                    raise ChatPoolError(f"账号档案未启用：{profile_id}")

                placeholders = ",".join("?" for _ in normalized)
                existing_rows = connection.execute(
                    f"SELECT * FROM chat_slots WHERE thread_id IN ({placeholders})",
                    normalized,
                ).fetchall()
                existing_by_id = {row["thread_id"]: row for row in existing_rows}
                for thread_id, row in existing_by_id.items():
                    if row["profile_id"] != profile_id:
                        raise ChatPoolError(
                            f"已有会话不能通过批量登记更换账号归属：{thread_id}"
                        )

                for thread_id in normalized:
                    if thread_id in existing_by_id:
                        continue
                    label = f"网页会话-{thread_id}"
                    connection.execute(
                        """
                        INSERT INTO chat_slots(
                            thread_id, profile_id, label, kind, declared_model, model_verified,
                            memory_mode, state, registered_at, updated_at, note
                        ) VALUES (?, ?, ?, 'chatgpt', ?, 0, ?, 'ready', ?, ?, ?)
                        """,
                        (
                            thread_id,
                            profile_id,
                            label,
                            declared_model,
                            memory_mode,
                            now,
                            now,
                            note,
                        ),
                    )
                    self._event(
                        connection,
                        thread_id,
                        "registered",
                        details={
                            "profile_id": profile_id,
                            "label": label,
                            "declared_model": declared_model,
                            "source": "register_batch",
                        },
                    )

                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ChatPoolError(f"批量登记失败：{exc}") from exc
            except Exception:
                connection.rollback()
                raise

            rows_by_id = {
                row["thread_id"]: row
                for row in connection.execute(
                    f"SELECT * FROM chat_slots WHERE thread_id IN ({placeholders})",
                    normalized,
                ).fetchall()
            }
            return [self._slot(rows_by_id[thread_id]) for thread_id in normalized]

    def list_slots(
        self,
        state: str | None = None,
        profile_id: str | None = None,
    ) -> list[ChatSlot]:
        with self.connect() as connection:
            conditions: list[str] = []
            parameters: list[str] = []
            if state is not None:
                conditions.append("state = ?")
                parameters.append(state)
            if profile_id is not None:
                conditions.append("profile_id = ?")
                parameters.append(profile_id)
            where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
            rows = connection.execute(
                f"SELECT * FROM chat_slots{where} ORDER BY registered_at, label",
                parameters,
            ).fetchall()
        return [self._slot(row) for row in rows]

    def assign_slot(self, *, thread_id: str, profile_id: str) -> ChatSlot:
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                profile = connection.execute(
                    "SELECT state FROM account_profiles WHERE profile_id = ?", (profile_id,)
                ).fetchone()
                if profile is None:
                    raise ChatPoolError(f"未知账号档案：{profile_id}")
                if profile["state"] != "enabled":
                    raise ChatPoolError(f"账号档案未启用：{profile_id}")
                slot = connection.execute(
                    "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
                ).fetchone()
                if slot is None:
                    raise ChatPoolError(f"未知会话：{thread_id}")
                if slot["state"] == "leased":
                    raise ChatPoolError("已租用会话不能更换账号归属")
                previous_profile_id = slot["profile_id"]
                now = utc_now()
                connection.execute(
                    """
                    UPDATE chat_slots
                    SET profile_id = ?, updated_at = ?
                    WHERE thread_id = ?
                    """,
                    (profile_id, now, thread_id),
                )
                self._event(
                    connection,
                    thread_id,
                    "profile_assigned",
                    details={
                        "previous_profile_id": previous_profile_id,
                        "profile_id": profile_id,
                    },
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ChatPoolError(f"槽位归属调整失败：{exc}") from exc
            except ChatPoolError:
                connection.rollback()
                raise
            row = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            assert row is not None
            return self._slot(row)

    def lease(
        self,
        *,
        job_id: str,
        observation_id: str,
        label: str | None = None,
    ) -> ChatSlot:
        if not job_id.strip():
            raise ChatPoolError("job_id 不能为空")
        if not observation_id.strip():
            raise ChatPoolError("observation_id 不能为空")
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                if connection.execute(
                    "SELECT 1 FROM chat_slots WHERE lease_job_id = ?", (job_id,)
                ).fetchone():
                    raise ChatPoolError(f"job_id 已有租约：{job_id}")
                now = utc_now()
                profile_id = self._consume_matched_observation(
                    connection,
                    job_id=job_id,
                    observation_id=observation_id,
                    consumed_at=now,
                )
                if label is None:
                    row = connection.execute(
                        """
                        SELECT * FROM chat_slots
                        WHERE state = 'ready'
                          AND first_dispatched_at IS NULL
                          AND profile_id = ?
                        ORDER BY registered_at LIMIT 1
                        """,
                        (profile_id,),
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT * FROM chat_slots
                        WHERE state = 'ready'
                          AND first_dispatched_at IS NULL
                          AND profile_id = ? AND label = ?
                        """,
                        (profile_id, label),
                    ).fetchone()
                if row is None:
                    target = f"标签 {label}" if label else "任意会话"
                    raise ChatPoolError(f"账号 {profile_id} 没有可领取的{target}")
                thread_id = row["thread_id"]
                cursor = connection.execute(
                    """
                    UPDATE chat_slots
                    SET state = 'leased', lease_job_id = ?, leased_at = ?, updated_at = ?
                    WHERE thread_id = ? AND state = 'ready' AND first_dispatched_at IS NULL
                    """,
                    (job_id, now, now, thread_id),
                )
                if cursor.rowcount != 1:
                    raise ChatPoolError("会话领取竞争失败，请重试")
                self._event(
                    connection,
                    thread_id,
                    "leased",
                    job_id=job_id,
                    details={
                        "profile_id": profile_id,
                        "observation_id": observation_id,
                    },
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            leased = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            assert leased is not None
            return self._slot(leased)

    def open_collaboration(
        self,
        *,
        topic_id: str,
        role: str,
        job_id: str,
        observation_id: str,
        label: str | None = None,
    ) -> CollaborationOpenResult:
        if not topic_id.strip():
            raise ChatPoolError("topic_id 不能为空")
        if not role.strip():
            raise ChatPoolError("role 不能为空")
        if not job_id.strip():
            raise ChatPoolError("job_id 不能为空")
        if not observation_id.strip():
            raise ChatPoolError("observation_id 不能为空")
        if label is not None and not label.strip():
            raise ChatPoolError("label 不能为空")

        topic_id = topic_id.strip()
        role = role.strip()
        job_id = job_id.strip()
        observation_id = observation_id.strip()
        label = label.strip() if label is not None else None
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                now = utc_now()
                profile_id = self._consume_matched_observation(
                    connection,
                    job_id=job_id,
                    observation_id=observation_id,
                    consumed_at=now,
                )
                binding_row = connection.execute(
                    """
                    SELECT * FROM collaboration_bindings
                    WHERE topic_id = ? AND role = ? AND profile_id = ?
                      AND state IN ('active', 'paused')
                    """,
                    (topic_id, role, profile_id),
                ).fetchone()
                if binding_row is not None:
                    if binding_row["lease_job_id"] != job_id:
                        raise ChatPoolError("协作绑定必须使用原稳定 job_id 恢复")
                    slot_row = connection.execute(
                        "SELECT * FROM chat_slots WHERE thread_id = ?",
                        (binding_row["thread_id"],),
                    ).fetchone()
                    if slot_row is None:
                        raise ChatPoolError("协作绑定引用的会话不存在")
                    if (
                        slot_row["profile_id"] != profile_id
                        or slot_row["state"] != "leased"
                        or slot_row["lease_job_id"] != job_id
                    ):
                        raise ChatPoolError("协作绑定与稳定租约不一致")
                    connection.execute(
                        """
                        UPDATE collaboration_bindings
                        SET state = 'active', updated_at = ?
                        WHERE binding_id = ?
                        """,
                        (now, binding_row["binding_id"]),
                    )
                    self._event(
                        connection,
                        slot_row["thread_id"],
                        "collaboration_resumed",
                        job_id=job_id,
                        details={
                            "binding_id": binding_row["binding_id"],
                            "topic_id": topic_id,
                            "role": role,
                            "profile_id": profile_id,
                            "observation_id": observation_id,
                        },
                    )
                    connection.commit()
                    binding_row = connection.execute(
                        "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                        (binding_row["binding_id"],),
                    ).fetchone()
                    slot_row = connection.execute(
                        "SELECT * FROM chat_slots WHERE thread_id = ?",
                        (slot_row["thread_id"],),
                    ).fetchone()
                    assert binding_row is not None and slot_row is not None
                    return CollaborationOpenResult(
                        binding=self._binding(binding_row),
                        slot=self._slot(slot_row),
                        newly_leased=False,
                    )

                existing_job_binding = connection.execute(
                    """
                    SELECT * FROM collaboration_bindings
                    WHERE lease_job_id = ?
                    """,
                    (job_id,),
                ).fetchone()
                if existing_job_binding is not None:
                    raise ChatPoolError("稳定 job_id 已关联协作历史；新协作须使用新的 job_id")
                existing_job_slot = connection.execute(
                    "SELECT * FROM chat_slots WHERE lease_job_id = ?",
                    (job_id,),
                ).fetchone()
                if existing_job_slot is not None:
                    raise ChatPoolError(f"job_id 已有租约：{job_id}")

                if label is None:
                    slot_row = connection.execute(
                        """
                        SELECT * FROM chat_slots
                        WHERE state = 'ready'
                          AND first_dispatched_at IS NULL
                          AND profile_id = ?
                        ORDER BY registered_at LIMIT 1
                        """,
                        (profile_id,),
                    ).fetchone()
                else:
                    slot_row = connection.execute(
                        """
                        SELECT * FROM chat_slots
                        WHERE state = 'ready'
                          AND first_dispatched_at IS NULL
                          AND profile_id = ? AND label = ?
                        """,
                        (profile_id, label),
                    ).fetchone()
                if slot_row is None:
                    target = f"标签 {label}" if label else "任意会话"
                    raise ChatPoolError(f"账号 {profile_id} 没有可领取的{target}")
                thread_id = slot_row["thread_id"]
                cursor = connection.execute(
                    """
                    UPDATE chat_slots
                    SET state = 'leased', lease_job_id = ?, leased_at = ?, updated_at = ?
                    WHERE thread_id = ? AND state = 'ready' AND first_dispatched_at IS NULL
                    """,
                    (job_id, now, now, thread_id),
                )
                if cursor.rowcount != 1:
                    raise ChatPoolError("会话领取竞争失败，请重试")
                binding_id = str(uuid.uuid4())
                connection.execute(
                    """
                    INSERT INTO collaboration_bindings(
                        binding_id, topic_id, role, thread_id, profile_id,
                        lease_job_id, state, created_at, updated_at,
                        return_received_at, return_artifact, note
                    ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL, NULL, NULL)
                    """,
                    (
                        binding_id,
                        topic_id,
                        role,
                        thread_id,
                        profile_id,
                        job_id,
                        now,
                        now,
                    ),
                )
                self._event(
                    connection,
                    thread_id,
                    "collaboration_opened",
                    job_id=job_id,
                    details={
                        "binding_id": binding_id,
                        "topic_id": topic_id,
                        "role": role,
                        "profile_id": profile_id,
                        "observation_id": observation_id,
                    },
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ChatPoolError(f"协作绑定创建失败：{exc}") from exc
            except Exception:
                connection.rollback()
                raise
            binding_row = connection.execute(
                "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                (binding_id,),
            ).fetchone()
            slot_row = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            assert binding_row is not None and slot_row is not None
            return CollaborationOpenResult(
                binding=self._binding(binding_row),
                slot=self._slot(slot_row),
                newly_leased=True,
            )

    def pause_collaboration(
        self,
        *,
        binding_id: str,
        job_id: str,
    ) -> CollaborationBinding:
        if not binding_id.strip() or not job_id.strip():
            raise ChatPoolError("binding_id 和 job_id 不能为空")
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                binding = connection.execute(
                    "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()
                if binding is None:
                    raise ChatPoolError(f"未知协作绑定：{binding_id}")
                if binding["lease_job_id"] != job_id:
                    raise ChatPoolError("协作操作与稳定 job_id 不匹配")
                if binding["state"] == "closed":
                    raise ChatPoolError("已关闭协作绑定不能暂停")
                slot = connection.execute(
                    "SELECT * FROM chat_slots WHERE thread_id = ?",
                    (binding["thread_id"],),
                ).fetchone()
                if slot is None or slot["state"] != "leased" or slot["lease_job_id"] != job_id:
                    raise ChatPoolError("协作绑定与稳定租约不一致")
                now = utc_now()
                connection.execute(
                    "UPDATE collaboration_bindings SET state = 'paused', updated_at = ? WHERE binding_id = ?",
                    (now, binding_id),
                )
                self._event(
                    connection,
                    binding["thread_id"],
                    "collaboration_paused",
                    job_id=job_id,
                    details={"binding_id": binding_id},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            row = connection.execute(
                "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                (binding_id,),
            ).fetchone()
            assert row is not None
            return self._binding(row)

    def record_collaboration_return(
        self,
        *,
        binding_id: str,
        job_id: str,
        artifact: str,
    ) -> CollaborationBinding:
        if not binding_id.strip() or not job_id.strip():
            raise ChatPoolError("binding_id 和 job_id 不能为空")
        if not artifact.strip():
            raise ChatPoolError("artifact 不能为空")
        artifact = artifact.strip()
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                binding = connection.execute(
                    "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()
                if binding is None:
                    raise ChatPoolError(f"未知协作绑定：{binding_id}")
                if binding["lease_job_id"] != job_id:
                    raise ChatPoolError("协作操作与稳定 job_id 不匹配")
                if binding["state"] == "closed":
                    raise ChatPoolError("已关闭协作绑定不能记录返回")
                slot = connection.execute(
                    "SELECT * FROM chat_slots WHERE thread_id = ?",
                    (binding["thread_id"],),
                ).fetchone()
                if slot is None or slot["state"] != "leased" or slot["lease_job_id"] != job_id:
                    raise ChatPoolError("协作绑定与稳定租约不一致")
                now = utc_now()
                connection.execute(
                    """
                    UPDATE collaboration_bindings
                    SET return_received_at = ?, return_artifact = ?, updated_at = ?
                    WHERE binding_id = ?
                    """,
                    (now, artifact, now, binding_id),
                )
                self._event(
                    connection,
                    binding["thread_id"],
                    "collaboration_return_received",
                    job_id=job_id,
                    details={"binding_id": binding_id, "artifact": artifact},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            row = connection.execute(
                "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                (binding_id,),
            ).fetchone()
            assert row is not None
            return self._binding(row)

    def close_collaboration(
        self,
        *,
        binding_id: str,
        job_id: str,
        outcome: str,
    ) -> CollaborationOpenResult:
        if not binding_id.strip() or not job_id.strip() or not outcome.strip():
            raise ChatPoolError("binding_id、job_id 和 outcome 不能为空")
        outcome = outcome.strip()
        with self.connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                binding = connection.execute(
                    "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()
                if binding is None:
                    raise ChatPoolError(f"未知协作绑定：{binding_id}")
                if binding["lease_job_id"] != job_id:
                    raise ChatPoolError("协作操作与稳定 job_id 不匹配")
                if binding["state"] == "closed":
                    raise ChatPoolError("协作绑定已经关闭")
                slot = connection.execute(
                    "SELECT * FROM chat_slots WHERE thread_id = ?",
                    (binding["thread_id"],),
                ).fetchone()
                if slot is None or slot["state"] != "leased" or slot["lease_job_id"] != job_id:
                    raise ChatPoolError("协作绑定与稳定租约不一致")
                return_state = (
                    "ready"
                    if slot["first_dispatched_at"] is None
                    and outcome == "cancelled_before_dispatch"
                    else "used"
                )
                now = utc_now()
                connection.execute(
                    """
                    UPDATE collaboration_bindings
                    SET state = 'closed', updated_at = ?
                    WHERE binding_id = ?
                    """,
                    (now, binding_id),
                )
                connection.execute(
                    """
                    UPDATE chat_slots
                    SET state = ?, lease_job_id = NULL, leased_at = NULL, updated_at = ?
                    WHERE thread_id = ?
                    """,
                    (return_state, now, binding["thread_id"]),
                )
                self._event(
                    connection,
                    binding["thread_id"],
                    "collaboration_closed",
                    job_id=job_id,
                    details={
                        "binding_id": binding_id,
                        "outcome": outcome,
                        "return_state": return_state,
                    },
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            binding_row = connection.execute(
                "SELECT * FROM collaboration_bindings WHERE binding_id = ?",
                (binding_id,),
            ).fetchone()
            slot_row = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?",
                (binding["thread_id"],),
            ).fetchone()
            assert binding_row is not None and slot_row is not None
            return CollaborationOpenResult(
                binding=self._binding(binding_row),
                slot=self._slot(slot_row),
                newly_leased=False,
            )

    def list_collaborations(
        self,
        *,
        topic_id: str | None = None,
        profile_id: str | None = None,
        state: str | None = None,
    ) -> list[CollaborationBinding]:
        if state is not None and state not in COLLABORATION_STATES:
            raise ChatPoolError(f"非法协作状态：{state}")
        with self.connect() as connection:
            conditions: list[str] = []
            parameters: list[str] = []
            if topic_id is not None:
                conditions.append("topic_id = ?")
                parameters.append(topic_id)
            if profile_id is not None:
                conditions.append("profile_id = ?")
                parameters.append(profile_id)
            if state is not None:
                conditions.append("state = ?")
                parameters.append(state)
            where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
            rows = connection.execute(
                f"SELECT * FROM collaboration_bindings{where} ORDER BY created_at, binding_id",
                parameters,
            ).fetchall()
        return [self._binding(row) for row in rows]

    def complete(
        self,
        *,
        thread_id: str,
        job_id: str,
        outcome: str,
        return_state: str,
        request_marker: str | None,
        response_marker: str | None,
    ) -> ChatSlot:
        if return_state not in RETURN_STATES:
            raise ChatPoolError(f"非法回收状态：{return_state}")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            if row is None:
                connection.rollback()
                raise ChatPoolError(f"未知会话：{thread_id}")
            binding = connection.execute(
                """
                SELECT binding_id FROM collaboration_bindings
                WHERE thread_id = ? AND state IN ('active', 'paused')
                """,
                (thread_id,),
            ).fetchone()
            if binding is not None:
                connection.rollback()
                raise ChatPoolError("长期协作绑定必须通过 close_collaboration 关闭")
            if row["state"] != "leased" or row["lease_job_id"] != job_id:
                connection.rollback()
                raise ChatPoolError("完成请求与当前租约不匹配")
            if return_state == "ready" and row["first_dispatched_at"] is not None:
                connection.rollback()
                raise ChatPoolError("已承接业务任务的会话不能回收为 ready")
            if return_state == "ready" and outcome != "cancelled_before_dispatch":
                connection.rollback()
                raise ChatPoolError("回收为 ready 必须是投递前明确取消")
            now = utc_now()
            connection.execute(
                """
                UPDATE chat_slots
                SET state = ?, lease_job_id = NULL, leased_at = NULL, updated_at = ?
                WHERE thread_id = ?
                """,
                (return_state, now, thread_id),
            )
            self._event(
                connection,
                thread_id,
                "completed",
                job_id=job_id,
                details={
                    "outcome": outcome,
                    "return_state": return_state,
                    "request_marker": request_marker,
                    "response_marker": response_marker,
                    "markers_match": request_marker == response_marker,
                },
            )
            connection.commit()
            completed = connection.execute(
                "SELECT * FROM chat_slots WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            assert completed is not None
            return self._slot(completed)

    def record_job_event(
        self,
        *,
        thread_id: str,
        job_id: str,
        event_type: str,
        details: dict[str, object],
    ) -> None:
        if event_type not in JOB_EVENT_TYPES:
            raise ChatPoolError(f"非法任务事件：{event_type}")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state, lease_job_id FROM chat_slots WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise ChatPoolError(f"未知会话：{thread_id}")
            if row["state"] != "leased" or row["lease_job_id"] != job_id:
                connection.rollback()
                raise ChatPoolError("任务事件与当前租约不匹配")
            if event_type == "dispatch_accepted":
                now = utc_now()
                connection.execute(
                    """
                    UPDATE chat_slots
                    SET first_dispatched_at = COALESCE(first_dispatched_at, ?),
                        first_dispatched_job_id = COALESCE(first_dispatched_job_id, ?)
                    WHERE thread_id = ?
                    """,
                    (now, job_id, thread_id),
                )
            self._event(
                connection,
                thread_id,
                event_type,
                job_id=job_id,
                details=details,
            )
            connection.commit()

    def events(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM pool_events ORDER BY id"
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item.pop("details_json"))
            result.append(item)
        return result


def print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    command_parser = argparse.ArgumentParser(description="普通 Chat 会话池登记器")
    command_parser.add_argument(
        "--db", type=Path, default=root / "运行" / "chat-pool.sqlite3"
    )
    subparsers = command_parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init")

    profile_register = subparsers.add_parser("profile-register")
    profile_register.add_argument("--profile-id", required=True)
    profile_register.add_argument("--alias", required=True)
    profile_register.add_argument("--sentinel-thread-id", required=True)
    profile_register.add_argument("--sentinel-marker", required=True)
    profile_register.add_argument("--note")

    profile_list = subparsers.add_parser("profile-list")
    profile_list.add_argument("--state", choices=PROFILE_STATES)

    profile_detect = subparsers.add_parser("profile-detect")
    profile_detect.add_argument("--job-id", required=True)
    profile_detect.add_argument("--probe-json", action="append", required=True)

    profile_select_manual = subparsers.add_parser("profile-select-manual")
    profile_select_manual.add_argument("--job-id", required=True)
    profile_select_manual.add_argument("--profile-id", required=True)
    profile_select_manual.add_argument("--reason", required=True)

    slot_assign = subparsers.add_parser("slot-assign")
    slot_assign.add_argument("--thread-id", required=True)
    slot_assign.add_argument("--profile-id", required=True)

    register = subparsers.add_parser("register")
    register.add_argument("--profile-id", required=True)
    register.add_argument("--thread-id", required=True)
    register.add_argument("--label", required=True)
    register.add_argument("--declared-model")
    register.add_argument("--memory-mode", default="user_declared")
    register.add_argument("--note")

    register_batch = subparsers.add_parser("register-batch")
    register_batch.add_argument("--profile-id", required=True)
    register_batch.add_argument("--thread-id", action="append", required=True)
    register_batch.add_argument("--declared-model")
    register_batch.add_argument("--memory-mode", default="not_verified")
    register_batch.add_argument("--note")

    list_command = subparsers.add_parser("list")
    list_command.add_argument("--state", choices=STATES)
    list_command.add_argument("--profile-id")

    lease = subparsers.add_parser("lease")
    lease.add_argument("--job-id", required=True)
    lease.add_argument("--observation-id", required=True)
    lease.add_argument("--label")

    complete = subparsers.add_parser("complete")
    complete.add_argument("--thread-id", required=True)
    complete.add_argument("--job-id", required=True)
    complete.add_argument("--outcome", required=True)
    complete.add_argument("--return-state", choices=RETURN_STATES, required=True)
    complete.add_argument("--request-marker")
    complete.add_argument("--response-marker")

    record = subparsers.add_parser("record")
    record.add_argument("--thread-id", required=True)
    record.add_argument("--job-id", required=True)
    record.add_argument("--event-type", choices=JOB_EVENT_TYPES, required=True)
    record.add_argument("--details-json", default="{}")

    collaboration_open = subparsers.add_parser("collaboration-open")
    collaboration_open.add_argument("--topic-id", required=True)
    collaboration_open.add_argument("--role", required=True)
    collaboration_open.add_argument("--job-id", required=True)
    collaboration_open.add_argument("--observation-id", required=True)
    collaboration_open.add_argument("--label")

    collaboration_list = subparsers.add_parser("collaboration-list")
    collaboration_list.add_argument("--topic-id")
    collaboration_list.add_argument("--profile-id")
    collaboration_list.add_argument("--state", choices=COLLABORATION_STATES)

    collaboration_pause = subparsers.add_parser("collaboration-pause")
    collaboration_pause.add_argument("--binding-id", required=True)
    collaboration_pause.add_argument("--job-id", required=True)

    collaboration_return = subparsers.add_parser("collaboration-return")
    collaboration_return.add_argument("--binding-id", required=True)
    collaboration_return.add_argument("--job-id", required=True)
    collaboration_return.add_argument("--artifact", required=True)

    collaboration_close = subparsers.add_parser("collaboration-close")
    collaboration_close.add_argument("--binding-id", required=True)
    collaboration_close.add_argument("--job-id", required=True)
    collaboration_close.add_argument("--outcome", required=True)

    subparsers.add_parser("events")
    return command_parser


def main() -> int:
    args = parser().parse_args()
    pool = ChatPool(args.db.resolve())
    try:
        if args.command == "init":
            print_json({"db": str(pool.db_path), "status": "initialized"})
        elif args.command == "profile-register":
            print_json(
                asdict(
                    pool.register_profile(
                        profile_id=args.profile_id,
                        alias=args.alias,
                        sentinel_thread_id=args.sentinel_thread_id,
                        sentinel_marker=args.sentinel_marker,
                        note=args.note,
                    )
                )
            )
        elif args.command == "profile-list":
            print_json([asdict(profile) for profile in pool.list_profiles(args.state)])
        elif args.command == "profile-detect":
            probes: list[dict[str, str]] = []
            for raw_probe in args.probe_json:
                try:
                    probe = json.loads(raw_probe)
                except json.JSONDecodeError as exc:
                    raise ChatPoolError(f"probe-json 不是合法 JSON：{exc}") from exc
                if not isinstance(probe, dict):
                    raise ChatPoolError("probe-json 必须是 JSON 对象")
                thread_id = probe.get("thread_id")
                marker = probe.get("marker")
                if not isinstance(thread_id, str) or not isinstance(marker, str):
                    raise ChatPoolError("probe-json 必须包含字符串 thread_id 和 marker")
                probes.append({"thread_id": thread_id, "marker": marker})
            print_json(
                asdict(pool.detect_profile(job_id=args.job_id, probes=probes))
            )
        elif args.command == "profile-select-manual":
            print_json(
                asdict(
                    pool.select_profile_manual(
                        job_id=args.job_id,
                        profile_id=args.profile_id,
                        reason=args.reason,
                    )
                )
            )
        elif args.command == "slot-assign":
            print_json(
                asdict(
                    pool.assign_slot(
                        thread_id=args.thread_id,
                        profile_id=args.profile_id,
                    )
                )
            )
        elif args.command == "register":
            print_json(
                asdict(
                    pool.register(
                        profile_id=args.profile_id,
                        thread_id=args.thread_id,
                        label=args.label,
                        declared_model=args.declared_model,
                        memory_mode=args.memory_mode,
                        note=args.note,
                    )
                )
            )
        elif args.command == "register-batch":
            print_json(
                [
                    asdict(slot)
                    for slot in pool.register_batch(
                        profile_id=args.profile_id,
                        thread_ids=args.thread_id,
                        declared_model=args.declared_model,
                        memory_mode=args.memory_mode,
                        note=args.note,
                    )
                ]
            )
        elif args.command == "list":
            print_json(
                [
                    asdict(slot)
                    for slot in pool.list_slots(
                        state=args.state,
                        profile_id=args.profile_id,
                    )
                ]
            )
        elif args.command == "lease":
            print_json(
                asdict(
                    pool.lease(
                        job_id=args.job_id,
                        observation_id=args.observation_id,
                        label=args.label,
                    )
                )
            )
        elif args.command == "complete":
            print_json(
                asdict(
                    pool.complete(
                        thread_id=args.thread_id,
                        job_id=args.job_id,
                        outcome=args.outcome,
                        return_state=args.return_state,
                        request_marker=args.request_marker,
                        response_marker=args.response_marker,
                    )
                )
            )
        elif args.command == "record":
            try:
                details = json.loads(args.details_json)
            except json.JSONDecodeError as exc:
                raise ChatPoolError(f"details-json 不是合法 JSON：{exc}") from exc
            if not isinstance(details, dict):
                raise ChatPoolError("details-json 必须是 JSON 对象")
            pool.record_job_event(
                thread_id=args.thread_id,
                job_id=args.job_id,
                event_type=args.event_type,
                details=details,
            )
            print_json(
                {
                    "thread_id": args.thread_id,
                    "job_id": args.job_id,
                    "event_type": args.event_type,
                    "status": "recorded",
                }
            )
        elif args.command == "collaboration-open":
            result = pool.open_collaboration(
                topic_id=args.topic_id,
                role=args.role,
                job_id=args.job_id,
                observation_id=args.observation_id,
                label=args.label,
            )
            print_json(
                {
                    "binding": asdict(result.binding),
                    "slot": asdict(result.slot),
                    "newly_leased": result.newly_leased,
                }
            )
        elif args.command == "collaboration-list":
            print_json(
                [
                    asdict(binding)
                    for binding in pool.list_collaborations(
                        topic_id=args.topic_id,
                        profile_id=args.profile_id,
                        state=args.state,
                    )
                ]
            )
        elif args.command == "collaboration-pause":
            print_json(
                asdict(
                    pool.pause_collaboration(
                        binding_id=args.binding_id,
                        job_id=args.job_id,
                    )
                )
            )
        elif args.command == "collaboration-return":
            print_json(
                asdict(
                    pool.record_collaboration_return(
                        binding_id=args.binding_id,
                        job_id=args.job_id,
                        artifact=args.artifact,
                    )
                )
            )
        elif args.command == "collaboration-close":
            result = pool.close_collaboration(
                binding_id=args.binding_id,
                job_id=args.job_id,
                outcome=args.outcome,
            )
            print_json(
                {
                    "binding": asdict(result.binding),
                    "slot": asdict(result.slot),
                    "newly_leased": result.newly_leased,
                }
            )
        elif args.command == "events":
            print_json(
                {
                    "pool_events": pool.events(),
                    "profile_observations": [
                        asdict(observation)
                        for observation in pool.list_observations()
                    ],
                }
            )
    except ChatPoolError as exc:
        if args.command.startswith("collaboration-"):
            print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
            return 1
        command_parser = parser()
        command_parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

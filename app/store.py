"""口岸联合指挥的 SQLite 持久化层。"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL,
    zone TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    passenger_count INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    load_ratio REAL NOT NULL,
    risk_level TEXT NOT NULL,
    alert_id TEXT,
    backfilled INTEGER NOT NULL DEFAULT 0,
    stale INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL,
    arrival_seq INTEGER NOT NULL,
    UNIQUE (station_id, captured_at)
);
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL,
    version INTEGER NOT NULL,
    opened_event_at TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    last_decision_at TEXT NOT NULL,
    handler TEXT,
    handler_role TEXT,
    confirmed_by TEXT,
    confirmed_at TEXT,
    closed_by TEXT,
    closed_at TEXT,
    resolution TEXT
);
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    active INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    actor TEXT,
    at TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    UNIQUE (alert_id, seq)
);
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


class Store:
    """围绕单个 SQLite 连接的表操作集合；事务边界由调用方控制。"""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        try:
            yield
        except Exception:
            self._conn.rollback()
            raise
        else:
            self._conn.commit()

    @contextmanager
    def savepoint(self, name: str) -> Iterator[None]:
        self._conn.execute("SAVEPOINT " + name)
        try:
            yield
        except Exception:
            self._conn.execute("ROLLBACK TO " + name)
            self._conn.execute("RELEASE " + name)
            raise
        else:
            self._conn.execute("RELEASE " + name)

    @staticmethod
    def _dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        return dict(row) if row is not None else None

    # -- 计数器 ----------------------------------------------------------
    def next_value(self, name: str) -> int:
        self._conn.execute("INSERT OR IGNORE INTO counters(name, value) VALUES (?, 0)", (name,))
        self._conn.execute("UPDATE counters SET value = value + 1 WHERE name = ?", (name,))
        row = self._conn.execute("SELECT value FROM counters WHERE name = ?", (name,)).fetchone()
        return int(row["value"])

    # -- 请求幂等 --------------------------------------------------------
    def get_request_result(self, request_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT result_json FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        return json.loads(row["result_json"]) if row is not None else None

    def save_request(
        self,
        request_id: str,
        actor: str,
        action: str,
        payload_json: str,
        result: dict[str, Any],
        created_at: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO requests(request_id, actor, action, payload_json, result_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (request_id, actor, action, payload_json, json.dumps(result, ensure_ascii=False), created_at),
        )

    # -- 客流快照 --------------------------------------------------------
    def get_snapshot(self, snapshot_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        return self._dict(row)

    def find_snapshot(self, station_id: str, captured_at: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM snapshots WHERE station_id = ? AND captured_at = ?",
            (station_id, captured_at),
        ).fetchone()
        return self._dict(row)

    def insert_snapshot(self, row: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO snapshots(snapshot_id, station_id, zone, captured_at, passenger_count,"
            " capacity, load_ratio, risk_level, alert_id, backfilled, stale, received_at, arrival_seq)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["snapshot_id"], row["station_id"], row["zone"], row["captured_at"],
                row["passenger_count"], row["capacity"], row["load_ratio"], row["risk_level"],
                row["alert_id"], row["backfilled"], row["stale"], row["received_at"], row["arrival_seq"],
            ),
        )

    def snapshots_for_alert(self, alert_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM snapshots WHERE alert_id = ? ORDER BY captured_at, arrival_seq",
            (alert_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    # -- 预警 ------------------------------------------------------------
    def insert_alert(self, row: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO alerts(alert_id, zone, status, severity, version, opened_event_at, opened_at,"
            " last_decision_at, handler, handler_role, confirmed_by, confirmed_at,"
            " closed_by, closed_at, resolution)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["alert_id"], row["zone"], row["status"], row["severity"], row["version"],
                row["opened_event_at"], row["opened_at"], row["last_decision_at"],
                row["handler"], row["handler_role"], row["confirmed_by"], row["confirmed_at"],
                row["closed_by"], row["closed_at"], row["resolution"],
            ),
        )

    def get_alert(self, alert_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM alerts WHERE alert_id = ?", (alert_id,)
        ).fetchone()
        return self._dict(row)

    def alerts_for_zone(self, zone: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM alerts WHERE zone = ? ORDER BY opened_event_at", (zone,)
        ).fetchall()
        return [dict(row) for row in rows]

    def update_alert(self, alert_id: str, fields: dict[str, Any]) -> None:
        clauses = ", ".join(f"{key} = ?" for key in fields)
        self._conn.execute(
            f"UPDATE alerts SET {clauses} WHERE alert_id = ?",
            (*fields.values(), alert_id),
        )

    def list_alerts(self, status: Optional[str] = None, zone: Optional[str] = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM alerts"
        conditions, params = [], []
        if status is not None:
            conditions.append("status = ?")
            params.append(status)
        if zone is not None:
            conditions.append("zone = ?")
            params.append(zone)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY opened_at DESC, alert_id DESC"
        rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # -- 分流方案 --------------------------------------------------------
    def next_plan_version(self, alert_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM plans WHERE alert_id = ?",
            (alert_id,),
        ).fetchone()
        return int(row["next_version"])

    def deactivate_plans(self, alert_id: str) -> None:
        self._conn.execute("UPDATE plans SET active = 0 WHERE alert_id = ?", (alert_id,))

    def insert_plan(self, row: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO plans(plan_id, alert_id, version, content_json, created_by, created_at, active)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["plan_id"], row["alert_id"], row["version"], row["content_json"],
                row["created_by"], row["created_at"], row["active"],
            ),
        )

    def plans_for_alert(self, alert_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM plans WHERE alert_id = ? ORDER BY version", (alert_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # -- 事件留痕 --------------------------------------------------------
    def append_event(
        self, alert_id: str, event_type: str, actor: Optional[str], at: str, detail: dict[str, Any]
    ) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE alert_id = ?",
            (alert_id,),
        ).fetchone()
        seq = int(row["seq"])
        self._conn.execute(
            "INSERT INTO events(alert_id, seq, event_type, actor, at, detail_json)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (alert_id, seq, event_type, actor, at, json.dumps(detail, ensure_ascii=False)),
        )
        return seq

    def events_for_alert(self, alert_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE alert_id = ? ORDER BY seq", (alert_id,)
        ).fetchall()
        return [dict(row) for row in rows]

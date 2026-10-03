"""SQLite 存储层。

两级幂等：
  * requests.request_id      —— 同一调用（含命令与上报）重放，返回原结果；
  * snapshots.snapshot_id    —— 同一张站点快照再次到达，归入原记录，不产生新事件。

事件流 alert_events 只追加（INSERT），处置决定永不 UPDATE/DELETE；
预警当前状态用 alerts 表单独维护，积压数据补入只新增证据事件，不改写决定。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    zone       TEXT NOT NULL,
    name       TEXT NOT NULL,
    capacity   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    station_id  TEXT NOT NULL,
    zone        TEXT NOT NULL,
    occupancy   INTEGER NOT NULL,
    capacity    INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    alert_id    TEXT
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id      TEXT PRIMARY KEY,
    zone          TEXT NOT NULL,
    state         TEXT NOT NULL,
    risk_level    TEXT NOT NULL,
    opened_at     TEXT NOT NULL,
    closed_at     TEXT,
    owner_actor   TEXT,
    owner_role    TEXT,
    diversion_plan TEXT
);
-- 每个区域同时至多一条未关闭预警，由服务层在锁内保证。

CREATE TABLE IF NOT EXISTS alert_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id    TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    event_time  TEXT NOT NULL,
    actor       TEXT NOT NULL,
    actor_role  TEXT NOT NULL,
    snapshot_id TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (alert_id, seq)
);

CREATE TABLE IF NOT EXISTS requests (
    request_id  TEXT PRIMARY KEY,
    result_json TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
"""


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


class Storage:
    def __init__(self, path: Optional[str] = None) -> None:
        # 默认内存库；传入文件路径即可持久化，数据在进程重启后仍可恢复。
        self._conn = sqlite3.connect(path or ":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()

    # ---- 站点 ----------------------------------------------------------

    def upsert_station(self, station_id: str, zone: str, name: str, capacity: int) -> None:
        self._conn.execute(
            "INSERT INTO stations(station_id, zone, name, capacity) VALUES (?,?,?,?) "
            "ON CONFLICT(station_id) DO UPDATE SET "
            "zone=excluded.zone, name=excluded.name, capacity=excluded.capacity",
            (station_id, zone, name, capacity),
        )

    # ---- 快照 ----------------------------------------------------------

    def find_snapshot(self, snapshot_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
        ).fetchone()

    def insert_snapshot(
        self,
        snapshot_id: str,
        station_id: str,
        zone: str,
        occupancy: int,
        capacity: int,
        observed_at: str,
        ingested_at: str,
        alert_id: Optional[str],
    ) -> None:
        self._conn.execute(
            "INSERT INTO snapshots(snapshot_id, station_id, zone, occupancy, capacity, "
            "observed_at, ingested_at, alert_id) VALUES (?,?,?,?,?,?,?,?)",
            (snapshot_id, station_id, zone, occupancy, capacity,
             observed_at, ingested_at, alert_id),
        )

    def attach_snapshot(self, snapshot_id: str, alert_id: str) -> None:
        self._conn.execute(
            "UPDATE snapshots SET alert_id=? WHERE snapshot_id=?",
            (alert_id, snapshot_id),
        )

    def list_snapshots(self, alert_id: str) -> list[sqlite3.Row]:
        return list(self._conn.execute(
            "SELECT s.*, st.name AS station_name FROM snapshots s "
            "JOIN stations st ON st.station_id = s.station_id "
            "WHERE s.alert_id=? ORDER BY s.observed_at, s.snapshot_id",
            (alert_id,),
        ))

    # ---- 预警 ----------------------------------------------------------

    def find_open_alert(self, zone: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM alerts WHERE zone=? AND state != 'closed' "
            "ORDER BY opened_at DESC LIMIT 1",
            (zone,),
        ).fetchone()

    def find_alert_for_evidence(self, zone: str, observed_at: str) -> Optional[sqlite3.Row]:
        """积压快照归属：优先未关闭预警；否则取开窗时间不晚于该快照的最近一条。"""
        row = self.find_open_alert(zone)
        if row is not None:
            return row
        return self._conn.execute(
            "SELECT * FROM alerts WHERE zone=? AND opened_at <= ? "
            "ORDER BY opened_at DESC LIMIT 1",
            (zone, observed_at),
        ).fetchone()

    def get_alert(self, alert_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()

    def insert_alert(
        self, alert_id: str, zone: str, risk_level: str,
        opened_at: str, owner: Optional[tuple[str, str]] = None,
    ) -> None:
        owner_actor = owner[0] if owner else None
        owner_role = owner[1] if owner else None
        self._conn.execute(
            "INSERT INTO alerts(alert_id, zone, state, risk_level, opened_at, "
            "owner_actor, owner_role) VALUES (?,?, 'open', ?, ?, ?, ?)",
            (alert_id, zone, risk_level, opened_at, owner_actor, owner_role),
        )

    def update_alert(self, alert_id: str, **fields: Any) -> None:
        if not fields:
            return
        columns = ", ".join(f"{k}=?" for k in fields)
        self._conn.execute(
            f"UPDATE alerts SET {columns} WHERE alert_id=?",
            (*fields.values(), alert_id),
        )

    # ---- 事件流（只追加） ----------------------------------------------

    def next_seq(self, alert_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM alert_events WHERE alert_id=?",
            (alert_id,),
        ).fetchone()
        return int(row["next"])

    def append_event(
        self,
        alert_id: str,
        event_type: str,
        event_time: str,
        actor: str,
        actor_role: str,
        snapshot_id: Optional[str] = None,
        detail: Optional[dict[str, Any]] = None,
    ) -> int:
        seq = self.next_seq(alert_id)
        self._conn.execute(
            "INSERT INTO alert_events(alert_id, seq, event_type, event_time, actor, "
            "actor_role, snapshot_id, detail_json) VALUES (?,?,?,?,?,?,?,?)",
            (alert_id, seq, event_type, event_time, actor, actor_role,
             snapshot_id, json.dumps(detail or {}, ensure_ascii=False)),
        )
        return seq

    def list_events(self, alert_id: str) -> list[sqlite3.Row]:
        # 逻辑顺序按事件发生时间（积压补入的旧事件落在正确位置），
        # 同一时刻用追加序号 seq 兜底，保证决定不会被“倒写”。
        return list(self._conn.execute(
            "SELECT * FROM alert_events WHERE alert_id=? "
            "ORDER BY event_time, seq",
            (alert_id,),
        ))

    def list_open_alerts(self) -> list[sqlite3.Row]:
        return list(self._conn.execute(
            "SELECT * FROM alerts WHERE state != 'closed' ORDER BY opened_at"
        ))

    # ---- 请求幂等 ------------------------------------------------------

    def get_request(self, request_id: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT result_json FROM requests WHERE request_id=?", (request_id,)
        ).fetchone()
        return row["result_json"] if row else None

    def save_request(self, request_id: str, result_json: str, created_at: str) -> None:
        self._conn.execute(
            "INSERT INTO requests(request_id, result_json, created_at) VALUES (?,?,?)",
            (request_id, result_json, created_at),
        )

    def commit(self) -> None:
        self._conn.commit()

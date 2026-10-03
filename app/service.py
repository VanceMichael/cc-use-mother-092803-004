"""口岸联合指挥服务。

职责：
- 接收各站点带时间戳的客流快照，按容量占用率识别风险并生成预警；
- 同一区域在时间窗口内的多部门上报并入同一条预警，避免同一拥堵事件被重复升级；
- 不同职责的值班人员围绕同一预警确认、调整、关闭分流方案，动作按职责鉴权；
- 相同快照再次到达归入原记录；断网积压数据恢复后按事件时间顺序补入，
  迟到数据只能作为证据归档，不能倒写已经确认的决定；
- 任一预警可回看其采用的站点数据、当前处置人，以及从产生到关闭的完整经过。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Callable, Optional

from .contracts import Request, Result, validate_request
from .models import (
    ACTION_ROLES,
    ALERT_CLOSED,
    ALERT_CONFIRMED,
    ALERT_OPEN,
    InvalidPayload,
    RISK_ORDER,
    SnapshotInput,
    evaluate_risk,
    iso,
    parse_instant,
    parse_snapshot,
    utcnow,
)
from .store import Store

# 同一区域先后两次拥堵事件归并的容忍窗口
MERGE_TOLERANCE = timedelta(minutes=30)
# 快照到达晚于采集时间超过该阈值，视为断网期间的积压补传
LATE_ARRIVAL = timedelta(minutes=10)


class BorderCommandService:
    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        merge_tolerance: timedelta = MERGE_TOLERANCE,
        late_arrival: timedelta = LATE_ARRIVAL,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._store = Store(db_path)
        self._merge_tolerance = merge_tolerance
        self._late_arrival = late_arrival
        self._now = now
        self._lock = RLock()

    def close(self) -> None:
        self._store.close()

    # ------------------------------------------------------------------ 入口
    def handle(self, request: Request) -> Result:
        validate_request(request)
        handler = self._handlers().get(request.action)
        if handler is None:
            return Result(False, "rejected", f"未知动作: {request.action}")
        with self._lock:
            replay = self._store.get_request_result(request.request_id)
            if replay is not None:
                return Result(replay["accepted"], replay["state"], replay["message"], replay["data"])
            try:
                with self._store.transaction():
                    result = handler(request)
                    self._store.save_request(
                        request.request_id,
                        request.actor,
                        request.action,
                        json.dumps(request.payload, ensure_ascii=False),
                        self._result_dict(result),
                        iso(request.created_at),
                    )
            except InvalidPayload as exc:
                result = Result(False, "rejected", str(exc))
            return result

    def _handlers(self) -> dict[str, Callable[[Request], Result]]:
        return {
            "ingest_snapshot": self._ingest_snapshot,
            "ingest_batch": self._ingest_batch,
            "confirm_alert": self._confirm_alert,
            "adjust_plan": self._adjust_plan,
            "assign_alert": self._assign_alert,
            "close_alert": self._close_alert,
            "get_alert": self._get_alert,
            "list_alerts": self._list_alerts,
        }

    @staticmethod
    def _result_dict(result: Result) -> dict[str, Any]:
        return {
            "accepted": result.accepted,
            "state": result.state,
            "message": result.message,
            "data": result.data,
        }

    # -------------------------------------------------------------- 快照接入
    def _ingest_snapshot(self, request: Request) -> Result:
        return self._apply_snapshot(parse_snapshot(request.payload), request.actor)

    def _ingest_batch(self, request: Request) -> Result:
        raw = request.payload.get("snapshots")
        if not isinstance(raw, list) or not raw:
            raise InvalidPayload("ingest_batch 需要非空的 snapshots 列表")
        parsed: list[tuple[int, SnapshotInput]] = []
        items: list[dict[str, Any]] = []
        rejected = 0
        for index, entry in enumerate(raw):
            try:
                if not isinstance(entry, dict):
                    raise InvalidPayload("快照必须是对象")
                parsed.append((index, parse_snapshot(entry)))
            except InvalidPayload as exc:
                rejected += 1
                items.append({"index": index, "accepted": False, "state": "rejected", "message": str(exc)})
        # 断网积压数据恢复后，按事件时间顺序补入
        parsed.sort(key=lambda pair: (pair[1].captured_at, pair[1].snapshot_id))
        applied = duplicates = conflicts = 0
        for index, snap in parsed:
            with self._store.savepoint(f"batch_{index}"):
                result = self._apply_snapshot(snap, request.actor)
            item = {"index": index, "accepted": result.accepted, "state": result.state,
                    "message": result.message, **result.data}
            items.append(item)
            if result.state == "duplicate":
                duplicates += 1
            elif result.state == "conflict":
                conflicts += 1
            elif result.accepted:
                applied += 1
        return Result(
            True,
            "applied",
            f"已按事件顺序补入 {applied} 条快照",
            {"items": items, "applied": applied, "duplicates": duplicates,
             "conflicts": conflicts, "rejected": rejected},
        )

    def _apply_snapshot(self, snap: SnapshotInput, actor: str) -> Result:
        now = self._now()
        # 相同快照再次到达：归入原记录，不产生新数据
        existing = self._store.get_snapshot(snap.snapshot_id)
        if existing is not None:
            return Result(True, "duplicate", "相同快照已归入原记录", {
                "snapshot_id": existing["snapshot_id"],
                "alert_id": existing["alert_id"],
                "duplicate": True,
            })
        twin = self._store.find_snapshot(snap.station_id, iso(snap.captured_at))
        if twin is not None:
            if twin["passenger_count"] == snap.passenger_count and twin["capacity"] == snap.capacity:
                return Result(True, "duplicate", "相同快照已归入原记录", {
                    "snapshot_id": twin["snapshot_id"],
                    "alert_id": twin["alert_id"],
                    "duplicate": True,
                })
            return Result(False, "conflict", "同一站点同一时刻已存在不同数据，未覆盖原记录", {
                "snapshot_id": twin["snapshot_id"],
            })

        ratio, level = evaluate_risk(snap.passenger_count, snap.capacity)
        late = (now - snap.captured_at) > self._late_arrival
        alert = self._match_alert(snap.zone, snap.captured_at)

        if alert is None:
            alert_id = None
            if level != "green":
                alert_id = self._raise_alert(snap, level, ratio, now, actor)
            self._record_snapshot(snap, ratio, level, alert_id, late, False, now)
            if alert_id is None:
                return Result(True, "recorded", "客流处于容量安全范围，已记录", {
                    "snapshot_id": snap.snapshot_id,
                    "risk_level": level,
                    "load_ratio": round(ratio, 4),
                })
            return Result(True, "alert_raised", "识别到容量风险，已生成预警", {
                "snapshot_id": snap.snapshot_id,
                "alert_id": alert_id,
                "severity": level,
            })

        alert_id = alert["alert_id"]
        if alert["status"] == ALERT_CLOSED:
            # 补入已关闭的预警：只存档留痕，不倒写已确认的决定
            self._record_snapshot(snap, ratio, level, alert_id, True, True, now)
            self._append_event(alert_id, "snapshot_attached", actor, now, {
                "snapshot_id": snap.snapshot_id,
                "station_id": snap.station_id,
                "captured_at": iso(snap.captured_at),
                "backfilled": True,
                "note": "断网积压数据补入已关闭预警，仅存档，不改变已确认的决定",
            })
            return Result(True, "attached", "快照已补入原预警记录，已确认的决定不受影响", {
                "snapshot_id": snap.snapshot_id,
                "alert_id": alert_id,
                "backfilled": True,
            })

        # 事件时间早于最近一次决定的快照是迟到数据：只作证据，不再驱动状态变化
        stale = snap.captured_at < parse_instant(alert["last_decision_at"])
        backfilled = stale or late
        escalated = False
        if not stale and RISK_ORDER[level] > RISK_ORDER[alert["severity"]]:
            self._store.update_alert(alert_id, {"severity": level, "version": alert["version"] + 1})
            escalated = True
        self._record_snapshot(snap, ratio, level, alert_id, backfilled, stale, now)
        self._append_event(alert_id, "snapshot_attached", actor, now, {
            "snapshot_id": snap.snapshot_id,
            "station_id": snap.station_id,
            "captured_at": iso(snap.captured_at),
            "backfilled": backfilled,
            "stale": stale,
        })
        if escalated:
            self._append_event(alert_id, "severity_escalated", actor, now, {
                "from": alert["severity"],
                "to": level,
                "snapshot_id": snap.snapshot_id,
            })
        return Result(True, "attached", "快照已并入进行中的预警", {
            "snapshot_id": snap.snapshot_id,
            "alert_id": alert_id,
            "backfilled": backfilled,
            "stale": stale,
            "escalated": escalated,
        })

    def _match_alert(self, zone: str, captured_at: datetime) -> Optional[dict[str, Any]]:
        """为快照找到应归入的预警：进行中的预警按时间窗口归并；
        已关闭的预警只吸收其事件时段内的迟到数据。"""
        best: Optional[dict[str, Any]] = None
        for alert in self._store.alerts_for_zone(zone):
            opened = parse_instant(alert["opened_event_at"])
            if captured_at < opened - self._merge_tolerance:
                continue
            if alert["status"] == ALERT_CLOSED and captured_at > parse_instant(alert["closed_at"]):
                continue
            if best is None or opened > parse_instant(best["opened_event_at"]):
                best = alert
        return best

    def _raise_alert(
        self, snap: SnapshotInput, level: str, ratio: float, now: datetime, actor: str
    ) -> str:
        alert_id = f"ALT-{self._store.next_value('alert_seq'):06d}"
        self._store.insert_alert({
            "alert_id": alert_id,
            "zone": snap.zone,
            "status": ALERT_OPEN,
            "severity": level,
            "version": 1,
            "opened_event_at": iso(snap.captured_at),
            "opened_at": iso(now),
            "last_decision_at": iso(snap.captured_at),
            "handler": None,
            "handler_role": None,
            "confirmed_by": None,
            "confirmed_at": None,
            "closed_by": None,
            "closed_at": None,
            "resolution": None,
        })
        self._append_event(alert_id, "alert_raised", actor, now, {
            "snapshot_id": snap.snapshot_id,
            "station_id": snap.station_id,
            "zone": snap.zone,
            "captured_at": iso(snap.captured_at),
            "severity": level,
            "load_ratio": round(ratio, 4),
        })
        return alert_id

    def _record_snapshot(
        self,
        snap: SnapshotInput,
        ratio: float,
        level: str,
        alert_id: Optional[str],
        backfilled: bool,
        stale: bool,
        now: datetime,
    ) -> None:
        self._store.insert_snapshot({
            "snapshot_id": snap.snapshot_id,
            "station_id": snap.station_id,
            "zone": snap.zone,
            "captured_at": iso(snap.captured_at),
            "passenger_count": snap.passenger_count,
            "capacity": snap.capacity,
            "load_ratio": ratio,
            "risk_level": level,
            "alert_id": alert_id,
            "backfilled": int(backfilled),
            "stale": int(stale),
            "received_at": iso(now),
            "arrival_seq": self._store.next_value("arrival_seq"),
        })

    # -------------------------------------------------------------- 协同处置
    def _check_role(self, request: Request) -> Optional[Result]:
        allowed = ACTION_ROLES.get(request.action, frozenset())
        if request.role not in allowed:
            return Result(False, "rejected", f"职责「{request.role or '未声明'}」无权执行 {request.action}")
        return None

    def _must_alert(self, payload: dict[str, Any]) -> tuple[Optional[dict[str, Any]], Optional[Result]]:
        alert_id = payload.get("alert_id")
        if not isinstance(alert_id, str) or not alert_id.strip():
            raise InvalidPayload("缺少 alert_id")
        alert = self._store.get_alert(alert_id.strip())
        if alert is None:
            return None, Result(False, "rejected", f"预警不存在: {alert_id}")
        return alert, None

    def _confirm_alert(self, request: Request) -> Result:
        denied = self._check_role(request)
        if denied is not None:
            return denied
        alert, missing = self._must_alert(request.payload)
        if missing is not None:
            return missing
        if alert["status"] != ALERT_OPEN:
            return Result(False, "rejected", f"预警当前状态为 {alert['status']}，不能确认", {
                "alert_id": alert["alert_id"], "status": alert["status"],
            })
        now = self._now()
        note = str(request.payload.get("note") or "")
        self._store.update_alert(alert["alert_id"], {
            "status": ALERT_CONFIRMED,
            "handler": request.actor,
            "handler_role": request.role,
            "confirmed_by": request.actor,
            "confirmed_at": iso(now),
            "last_decision_at": iso(now),
            "version": alert["version"] + 1,
        })
        self._append_event(alert["alert_id"], "alert_confirmed", request.actor, now, {
            "role": request.role, "note": note,
        })
        return Result(True, "confirmed", "预警已确认，处置责任已落实到当班人员", {
            "alert_id": alert["alert_id"],
            "status": ALERT_CONFIRMED,
            "handler": request.actor,
            "version": alert["version"] + 1,
        })

    def _adjust_plan(self, request: Request) -> Result:
        denied = self._check_role(request)
        if denied is not None:
            return denied
        alert, missing = self._must_alert(request.payload)
        if missing is not None:
            return missing
        if alert["status"] not in (ALERT_OPEN, ALERT_CONFIRMED):
            return Result(False, "rejected", "预警已关闭，不能再调整分流方案", {
                "alert_id": alert["alert_id"], "status": alert["status"],
            })
        plan = request.payload.get("plan")
        if not isinstance(plan, dict):
            raise InvalidPayload("plan 必须是对象")
        summary = plan.get("summary")
        measures = plan.get("measures")
        if not isinstance(summary, str) or not summary.strip():
            raise InvalidPayload("plan.summary 不能为空")
        if (not isinstance(measures, list) or not measures
                or not all(isinstance(m, str) and m.strip() for m in measures)):
            raise InvalidPayload("plan.measures 必须是非空字符串列表")
        now = self._now()
        note = str(request.payload.get("note") or "")
        version = self._store.next_plan_version(alert["alert_id"])
        self._store.deactivate_plans(alert["alert_id"])
        self._store.insert_plan({
            "plan_id": f"{alert['alert_id']}-P{version}",
            "alert_id": alert["alert_id"],
            "version": version,
            "content_json": json.dumps(
                {"summary": summary.strip(), "measures": [m.strip() for m in measures]},
                ensure_ascii=False,
            ),
            "created_by": request.actor,
            "created_at": iso(now),
            "active": 1,
        })
        self._store.update_alert(alert["alert_id"], {
            "last_decision_at": iso(now),
            "version": alert["version"] + 1,
        })
        self._append_event(alert["alert_id"], "plan_adjusted", request.actor, now, {
            "plan_version": version, "summary": summary.strip(), "note": note,
        })
        return Result(True, "adjusted", f"分流方案已调整为第 {version} 版", {
            "alert_id": alert["alert_id"],
            "plan_version": version,
            "version": alert["version"] + 1,
        })

    def _assign_alert(self, request: Request) -> Result:
        denied = self._check_role(request)
        if denied is not None:
            return denied
        alert, missing = self._must_alert(request.payload)
        if missing is not None:
            return missing
        if alert["status"] == ALERT_CLOSED:
            return Result(False, "rejected", "预警已关闭，不能再指派", {
                "alert_id": alert["alert_id"], "status": alert["status"],
            })
        assignee = request.payload.get("assignee")
        if not isinstance(assignee, str) or not assignee.strip():
            raise InvalidPayload("assignee 不能为空")
        assignee = assignee.strip()
        assignee_role = str(request.payload.get("assignee_role") or "")
        now = self._now()
        note = str(request.payload.get("note") or "")
        self._store.update_alert(alert["alert_id"], {
            "handler": assignee,
            "handler_role": assignee_role,
            "version": alert["version"] + 1,
        })
        self._append_event(alert["alert_id"], "alert_assigned", request.actor, now, {
            "assignee": assignee, "assignee_role": assignee_role, "note": note,
        })
        return Result(True, "assigned", "预警已指派", {
            "alert_id": alert["alert_id"],
            "handler": assignee,
            "version": alert["version"] + 1,
        })

    def _close_alert(self, request: Request) -> Result:
        denied = self._check_role(request)
        if denied is not None:
            return denied
        alert, missing = self._must_alert(request.payload)
        if missing is not None:
            return missing
        if alert["status"] != ALERT_CONFIRMED:
            return Result(False, "rejected", f"预警当前状态为 {alert['status']}，须先确认再关闭", {
                "alert_id": alert["alert_id"], "status": alert["status"],
            })
        now = self._now()
        resolution = str(request.payload.get("resolution") or "")
        self._store.update_alert(alert["alert_id"], {
            "status": ALERT_CLOSED,
            "closed_by": request.actor,
            "closed_at": iso(now),
            "resolution": resolution,
            "last_decision_at": iso(now),
            "version": alert["version"] + 1,
        })
        self._append_event(alert["alert_id"], "alert_closed", request.actor, now, {
            "resolution": resolution,
        })
        return Result(True, "closed", "预警已关闭，分流处置结束", {
            "alert_id": alert["alert_id"],
            "status": ALERT_CLOSED,
            "version": alert["version"] + 1,
        })

    # -------------------------------------------------------------- 指挥查看
    def _get_alert(self, request: Request) -> Result:
        alert, missing = self._must_alert(request.payload)
        if missing is not None:
            return missing
        alert_id = alert["alert_id"]
        snapshots = self._store.snapshots_for_alert(alert_id)
        plans = self._store.plans_for_alert(alert_id)
        events = self._store.events_for_alert(alert_id)
        detail = {
            "alert_id": alert_id,
            "zone": alert["zone"],
            "status": alert["status"],
            "severity": alert["severity"],
            "version": alert["version"],
            "handler": (
                {"actor": alert["handler"], "role": alert["handler_role"]}
                if alert["handler"] else None
            ),
            "opened_event_at": alert["opened_event_at"],
            "opened_at": alert["opened_at"],
            "confirmed_by": alert["confirmed_by"],
            "confirmed_at": alert["confirmed_at"],
            "closed_by": alert["closed_by"],
            "closed_at": alert["closed_at"],
            "resolution": alert["resolution"],
            "stations": sorted({s["station_id"] for s in snapshots}),
            "snapshots": [self._public_snapshot(s) for s in snapshots],
            "plans": [
                {
                    "plan_id": p["plan_id"],
                    "version": p["version"],
                    "content": json.loads(p["content_json"]),
                    "created_by": p["created_by"],
                    "created_at": p["created_at"],
                    "active": bool(p["active"]),
                }
                for p in plans
            ],
            "timeline": [
                {
                    "seq": e["seq"],
                    "event_type": e["event_type"],
                    "actor": e["actor"],
                    "at": e["at"],
                    "detail": json.loads(e["detail_json"]),
                }
                for e in events
            ],
        }
        return Result(True, "detail", "预警详情", detail)

    def _list_alerts(self, request: Request) -> Result:
        status = request.payload.get("status")
        zone = request.payload.get("zone")
        if status is not None and status not in (ALERT_OPEN, ALERT_CONFIRMED, ALERT_CLOSED):
            raise InvalidPayload(f"未知预警状态: {status}")
        if zone is not None and not isinstance(zone, str):
            raise InvalidPayload("zone 必须是字符串")
        rows = self._store.list_alerts(status, zone)
        summaries = [
            {
                "alert_id": a["alert_id"],
                "zone": a["zone"],
                "status": a["status"],
                "severity": a["severity"],
                "handler": a["handler"],
                "opened_event_at": a["opened_event_at"],
                "version": a["version"],
            }
            for a in rows
        ]
        return Result(True, "list", f"共 {len(summaries)} 条预警", {"alerts": summaries})

    @staticmethod
    def _public_snapshot(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "snapshot_id": row["snapshot_id"],
            "station_id": row["station_id"],
            "zone": row["zone"],
            "captured_at": row["captured_at"],
            "passenger_count": row["passenger_count"],
            "capacity": row["capacity"],
            "load_ratio": round(row["load_ratio"], 4),
            "risk_level": row["risk_level"],
            "backfilled": bool(row["backfilled"]),
            "stale": bool(row["stale"]),
            "received_at": row["received_at"],
        }

    def _append_event(
        self, alert_id: str, event_type: str, actor: Optional[str], at: datetime, detail: dict[str, Any]
    ) -> None:
        self._store.append_event(alert_id, event_type, actor, iso(at), detail)

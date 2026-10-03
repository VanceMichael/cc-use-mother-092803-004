"""口岸联合指挥服务。

职责：
  * ingest_snapshot  接收各站点带时间戳的客流快照，识别容量风险并维护
                     “每个区域一条共享预警”，从根本上消除跨部门重复升级；
  * 值班人员围绕同一条预警完成 确认 / 启动分流 / 调整分流 / 关闭；
  * 快照与请求两级幂等：重复快照归入原记录，重复请求返回原结果；
  * 断网积压数据按事件时间补入只追加的事件流，已确认的决定永不回写；
  * get_alert 聚合“采用了哪些站点数据、当前由谁处理、完整变化时间线”。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any, Optional

from .contracts import Request, Result, utc_now_naive, validate_request
from .domain import can_transition, classify_load, is_final, risk_rank
from .storage import Storage, iso

# 占用率达到 warning 才开预警；watch 只作为证据留存。
ALERT_OPEN_LEVEL = "warning"


class BorderCommandService:
    def __init__(self, storage: Optional[Storage] = None) -> None:
        self._db = storage or Storage()
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ---- 入口 -----------------------------------------------------------

    def handle(self, request: Request) -> Result:
        validate_request(request)
        with self._lock:
            cached = self._db.get_request(request.request_id)
            if cached is not None:
                return self._deserialize(cached)
            try:
                result = self._dispatch(request)
            except Exception:
                self._db.conn.rollback()
                raise
            self._db.save_request(
                request.request_id,
                json.dumps(self._serialize(result), ensure_ascii=False),
                iso(request.created_at),
            )
            self._db.commit()
            return result

    def _dispatch(self, request: Request) -> Result:
        if request.action == "ingest_snapshot":
            return self._ingest(request)
        if request.action == "acknowledge":
            return self._decision(request, "acknowledge")
        if request.action == "start_diversion":
            return self._decision(request, "start_diversion", need_plan=True)
        if request.action == "adjust_diversion":
            return self._decision(request, "adjust_diversion", need_plan=True)
        if request.action == "close":
            return self._decision(request, "close")
        return Result(False, "rejected", f"未知动作: {request.action}")

    # ---- 快照上报 -------------------------------------------------------

    def _ingest(self, request: Request) -> Result:
        p = request.payload
        try:
            snapshot_id = str(p["snapshot_id"]).strip()
            station_id = str(p["station_id"]).strip()
            zone = str(p["zone"]).strip()
            name = str(p.get("station_name", station_id)).strip() or station_id
            occupancy = int(p["occupancy"])
            capacity = int(p["capacity"])
            observed_at = str(p["observed_at"]).strip()
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"快照字段不完整或格式错误: {exc}") from exc
        if not snapshot_id or not station_id or not zone or not observed_at:
            raise ValueError("快照缺少 snapshot_id/station_id/zone/observed_at")
        observed_dt = datetime.fromisoformat(observed_at)
        level = classify_load(occupancy, capacity)
        now = iso(utc_now_naive())

        # 第一级幂等：同一张快照再次到达，直接归入原记录，不产生任何新事件。
        duplicate = self._db.find_snapshot(snapshot_id)
        if duplicate is not None:
            alert_id = duplicate["alert_id"]
            message = "快照已存在，归入原记录"
            if alert_id:
                message += f"（预警 {alert_id}）"
                return Result(True, self._db.get_alert(alert_id)["state"], message,
                              {"alert_id": alert_id, "duplicate": True})
            return Result(True, "normal", message, {"duplicate": True})

        self._db.upsert_station(station_id, zone, name, capacity)

        alert = self._resolve_alert_for_snapshot(zone, observed_at, observed_dt, level)
        if alert is None:
            # 无覆盖预警（含早于当前预警开窗的陈旧积压）：仅留存，不打扰值班员。
            self._db.insert_snapshot(snapshot_id, station_id, zone, occupancy,
                                     capacity, observed_at, now, None)
            return Result(True, "normal", "快照已记录，未纳入任何预警",
                          {"risk_level": level})

        alert_id = alert["alert_id"]
        sealed = is_final(alert["state"])
        self._db.insert_snapshot(snapshot_id, station_id, zone, occupancy,
                                 capacity, observed_at, now, alert_id)
        self._db.append_event(
            alert_id, "snapshot_attached", observed_at,
            request.actor, request.role, snapshot_id,
            {"station_id": station_id, "station_name": name, "zone": zone,
             "occupancy": occupancy, "capacity": capacity,
             "ratio": round(occupancy / capacity, 3), "risk_level": level},
        )

        # 用该预警采纳的全部证据重算区域风险；只升不降、只追加事件，
        # 已关闭预警的决定与状态一律不动（积压证据只能补入，不能翻案）。
        if not sealed:
            computed = self._recompute_risk(alert_id)
            if risk_rank(computed) > risk_rank(alert["risk_level"]):
                self._db.update_alert(alert_id, risk_level=computed)
                self._db.append_event(
                    alert_id, "risk_escalated", observed_at,
                    request.actor, request.role, snapshot_id,
                    {"from_level": alert["risk_level"], "to_level": computed},
                )

        state = self._db.get_alert(alert_id)["state"]
        note = "证据补入已关闭预警，决定不变" if sealed else "快照已采纳"
        return Result(True, state, note, {
            "alert_id": alert_id, "risk_level": level, "backfilled": sealed,
        })

    def _resolve_alert_for_snapshot(
        self, zone: str, observed_at: str, observed_dt: datetime, level: str
    ):
        """决定快照归属哪条预警，或为实时风险新开一条，或不予采纳。"""
        open_alert = self._db.find_open_alert(zone)

        # 事件时间落在当前未关闭预警的开窗之后：直接归入。
        if open_alert is not None and observed_at >= open_alert["opened_at"]:
            return open_alert

        # 断网恢复的旧快照：优先归入其发生时段内仍开窗、现已关闭的预警。
        covering = self._db.conn.execute(
            "SELECT * FROM alerts WHERE zone=? AND opened_at<=? "
            "AND closed_at IS NOT NULL AND closed_at>=? "
            "ORDER BY opened_at DESC LIMIT 1",
            (zone, observed_at, observed_at),
        ).fetchone()
        if covering is not None:
            return covering

        # 早于当前预警开窗的陈旧数据，或风险未达门槛：留存但不采纳为证据，
        # 避免过期数据触发新升级、污染当前判断。
        if open_alert is not None:
            return None
        if risk_rank(level) < risk_rank(ALERT_OPEN_LEVEL):
            return None

        # 若已有更晚开窗的预警，说明这是更陈旧的积压数据，不再追溯新开。
        newer = self._db.conn.execute(
            "SELECT 1 FROM alerts WHERE zone=? AND opened_at>? LIMIT 1",
            (zone, observed_at),
        ).fetchone()
        if newer is not None:
            return None

        alert_id = f"A-{zone}-{observed_dt.strftime('%Y%m%d%H%M%S')}"
        self._db.insert_alert(alert_id, zone, level, observed_at)
        self._db.append_event(
            alert_id, "alert_opened", observed_at,
            "system", "commander", None,
            {"zone": zone, "trigger_risk_level": level},
        )
        return self._db.get_alert(alert_id)

    def _recompute_risk(self, alert_id: str) -> str:
        latest: dict[str, str] = {}
        for row in self._db.list_snapshots(alert_id):
            sid = row["station_id"]
            if sid not in latest or row["observed_at"] >= latest[sid]["observed_at"]:
                latest[sid] = row
        level = "normal"
        for row in latest.values():
            station_level = classify_load(row["occupancy"], row["capacity"])
            if risk_rank(station_level) > risk_rank(level):
                level = station_level
        return level

    # ---- 处置决定 -------------------------------------------------------

    def _decision(self, request: Request, action: str, need_plan: bool = False) -> Result:
        p = request.payload
        alert_id = str(p.get("alert_id", "")).strip()
        if not alert_id:
            raise ValueError("处置请求缺少 alert_id")
        alert = self._db.get_alert(alert_id)
        if alert is None:
            return Result(False, "rejected", f"预警不存在: {alert_id}")
        if not can_transition(alert["state"], action):
            return Result(False, alert["state"],
                          f"当前状态 {alert['state']} 不允许 {action}（该预警已由"
                          f"{alert['owner_actor'] or '其他值班员'}处置，请勿重复升级）",
                          {"alert_id": alert_id, "owner_actor": alert["owner_actor"]})

        plan = str(p.get("diversion_plan", "")).strip()
        if need_plan and not plan:
            return Result(False, alert["state"], "分流方案不能为空", {"alert_id": alert_id})

        # 处置命令同样允许携带事件时间 occurred_at，断网积压的决定按其
        # 实际作出时刻进入时间线；缺省用请求到达时刻。
        at = str(p.get("occurred_at", "")).strip() or iso(request.created_at)
        if at < alert["opened_at"]:
            return Result(False, alert["state"],
                          "处置时间早于预警产生时间，拒绝倒写", {"alert_id": alert_id})
        last_decision = self._db.conn.execute(
            "SELECT MAX(event_time) AS t FROM alert_events WHERE alert_id=? "
            "AND event_type IN ('acknowledged','diversion_started',"
            "'diversion_adjusted','closed')",
            (alert_id,),
        ).fetchone()["t"]
        if last_decision is not None and at < last_decision:
            return Result(False, alert["state"],
                          f"已有更早的处置决定（{last_decision}）生效，"
                          "迟到命令不得倒写已确认的决定", {"alert_id": alert_id})
        if action == "acknowledge":
            self._db.update_alert(
                alert_id, state="acknowledged",
                owner_actor=request.actor, owner_role=request.role,
            )
            self._db.append_event(alert_id, "acknowledged", at,
                                  request.actor, request.role,
                                  detail={"note": str(p.get("note", ""))})
            msg = "预警已确认并认领"

        elif action == "start_diversion":
            self._db.update_alert(alert_id, state="diverting", diversion_plan=plan)
            self._db.append_event(alert_id, "diversion_started", at,
                                  request.actor, request.role,
                                  detail={"diversion_plan": plan})
            msg = "分流方案已启动"

        elif action == "adjust_diversion":
            self._db.update_alert(alert_id, diversion_plan=plan)
            self._db.append_event(alert_id, "diversion_adjusted", at,
                                  request.actor, request.role,
                                  detail={"diversion_plan": plan,
                                          "reason": str(p.get("reason", ""))})
            msg = "分流方案已调整"

        else:  # close
            self._db.update_alert(alert_id, state="closed", closed_at=at)
            self._db.append_event(alert_id, "closed", at,
                                  request.actor, request.role,
                                  detail={"reason": str(p.get("reason", ""))})
            msg = "预警已关闭"

        return Result(True, self._db.get_alert(alert_id)["state"], msg,
                      {"alert_id": alert_id})

    # ---- 查询 -----------------------------------------------------------

    def list_open_alerts(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._brief(r) for r in self._db.list_open_alerts()]

    def get_alert(self, alert_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            alert = self._db.get_alert(alert_id)
            if alert is None:
                return None
            return self._detail(alert)

    def _brief(self, row) -> dict[str, Any]:
        return {
            "alert_id": row["alert_id"], "zone": row["zone"],
            "state": row["state"], "risk_level": row["risk_level"],
            "opened_at": row["opened_at"], "closed_at": row["closed_at"],
            "owner_actor": row["owner_actor"], "owner_role": row["owner_role"],
        }

    def _detail(self, alert) -> dict[str, Any]:
        snapshot_rows = self._db.list_snapshots(alert["alert_id"])

        # 每个被采纳站点取最新一张快照，作为当前判断依据。
        latest: dict[str, Any] = {}
        for row in snapshot_rows:
            sid = row["station_id"]
            if sid not in latest or row["observed_at"] >= latest[sid]["observed_at"]:
                latest[sid] = row
        stations = [
            {
                "station_id": row["station_id"],
                "station_name": row["station_name"],
                "occupancy": row["occupancy"],
                "capacity": row["capacity"],
                "ratio": round(row["occupancy"] / row["capacity"], 3),
                "risk_level": classify_load(row["occupancy"], row["capacity"]),
                "observed_at": row["observed_at"],
                "snapshot_id": row["snapshot_id"],
            }
            for row in sorted(latest.values(), key=lambda r: r["observed_at"])
        ]

        timeline = []
        for ev in self._db.list_events(alert["alert_id"]):
            timeline.append({
                "seq": ev["seq"],
                "type": ev["event_type"],
                "time": ev["event_time"],
                "actor": ev["actor"],
                "actor_role": ev["actor_role"],
                "snapshot_id": ev["snapshot_id"],
                "detail": json.loads(ev["detail_json"]),
            })

        return {
            **self._brief(alert),
            "diversion_plan": alert["diversion_plan"],
            "adopted_snapshot_count": len(snapshot_rows),
            "stations": stations,
            "timeline": timeline,
        }

    # ---- 序列化 ---------------------------------------------------------

    @staticmethod
    def _serialize(result: Result) -> dict[str, Any]:
        return {"accepted": result.accepted, "state": result.state,
                "message": result.message, "data": result.data}

    @staticmethod
    def _deserialize(raw: str) -> Result:
        d = json.loads(raw)
        return Result(d["accepted"], d["state"], d["message"], d.get("data", {}))

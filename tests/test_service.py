import tempfile
import unittest
from datetime import datetime, timedelta
from uuid import uuid4

from app.contracts import Request
from app.service import BorderCommandService

T0 = datetime(2026, 10, 3, 10, 0, 0)


class Clock:
    def __init__(self, start):
        self._t = start

    def __call__(self):
        return self._t

    def set(self, t):
        self._t = t


def make_request(action, payload, actor="ops", role="", request_id=None):
    return Request(
        actor=actor,
        action=action,
        payload=payload,
        request_id=request_id or f"req-{uuid4().hex[:12]}",
        role=role,
    )


def snapshot(sid, station="police-1", zone="arrival-hall", at="2026-10-03T10:00:00",
             count=800, capacity=1000):
    return {
        "snapshot_id": sid,
        "station_id": station,
        "zone": zone,
        "captured_at": at,
        "passenger_count": count,
        "capacity": capacity,
    }


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.clock = Clock(T0)
        self.service = BorderCommandService(now=self.clock)

    def tearDown(self):
        self.service.close()

    def ingest(self, snap, **kw):
        return self.service.handle(make_request("ingest_snapshot", snap, **kw))

    def get_alert(self, alert_id):
        result = self.service.handle(make_request("get_alert", {"alert_id": alert_id}, actor="cmd", role="commander"))
        self.assertTrue(result.accepted)
        return result.data

    def list_alerts(self, **filters):
        return self.service.handle(make_request("list_alerts", filters)).data["alerts"]


class IngestTest(ServiceTestBase):
    def test_green_snapshot_recorded_without_alert(self):
        result = self.ingest(snapshot("s-1", count=500))
        self.assertTrue(result.accepted)
        self.assertEqual(result.state, "recorded")
        self.assertEqual(result.data["risk_level"], "green")
        self.assertEqual(self.list_alerts(), [])

    def test_risky_snapshot_raises_alert(self):
        result = self.ingest(snapshot("s-1", count=950))
        self.assertTrue(result.accepted)
        self.assertEqual(result.state, "alert_raised")
        alert = self.get_alert(result.data["alert_id"])
        self.assertEqual(alert["status"], "open")
        self.assertEqual(alert["severity"], "red")
        self.assertIsNone(alert["handler"])

    def test_same_snapshot_merges_into_original_record(self):
        first = self.ingest(snapshot("s-1"))
        second = self.ingest(snapshot("s-1"))  # 相同快照再次到达
        self.assertEqual(second.state, "duplicate")
        self.assertEqual(second.data["alert_id"], first.data["alert_id"])
        # 同一站点同一时刻、内容一致的换号重报也归入原记录
        third = self.ingest(snapshot("s-1-retry"))
        self.assertEqual(third.state, "duplicate")
        self.assertEqual(third.data["snapshot_id"], "s-1")
        alert = self.get_alert(first.data["alert_id"])
        self.assertEqual(len(alert["snapshots"]), 1)
        self.assertEqual(len(self.list_alerts()), 1)

    def test_conflicting_snapshot_rejected(self):
        self.ingest(snapshot("s-1", count=800))
        conflict = self.ingest(snapshot("s-2", count=900))  # 同站同时刻不同数据
        self.assertFalse(conflict.accepted)
        self.assertEqual(conflict.state, "conflict")
        alert = self.get_alert(self.list_alerts()[0]["alert_id"])
        self.assertEqual(len(alert["snapshots"]), 1)
        self.assertEqual(alert["snapshots"][0]["passenger_count"], 800)

    def test_departments_converge_on_one_alert(self):
        raised = self.ingest(snapshot("p-1", station="police-1", count=900))
        alert_id = raised.data["alert_id"]
        customs = self.ingest(snapshot("c-1", station="customs-1", count=880))
        railway = self.ingest(snapshot("r-1", station="railway-1", count=860))
        self.assertEqual(customs.data["alert_id"], alert_id)
        self.assertEqual(railway.data["alert_id"], alert_id)
        self.assertEqual(len(self.list_alerts()), 1)  # 同一拥堵事件不被重复升级
        alert = self.get_alert(alert_id)
        self.assertEqual(alert["stations"], ["customs-1", "police-1", "railway-1"])
        self.assertEqual(len(alert["snapshots"]), 3)


class DisposalTest(ServiceTestBase):
    def setUp(self):
        super().setUp()
        self.alert_id = self.ingest(snapshot("s-1", count=900)).data["alert_id"]

    def test_confirm_adjust_close_flow(self):
        confirmed = self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id, "note": "铁路值班接手"},
            actor="railway.zhang", role="railway"))
        self.assertTrue(confirmed.accepted)
        self.assertEqual(confirmed.data["handler"], "railway.zhang")

        plan_v1 = self.service.handle(make_request(
            "adjust_plan",
            {"alert_id": self.alert_id,
             "plan": {"summary": "开启西侧通道分流", "measures": ["增开 2 条查验通道", "引导至西候车区"]}},
            actor="customs.li", role="customs"))
        self.assertEqual(plan_v1.data["plan_version"], 1)
        plan_v2 = self.service.handle(make_request(
            "adjust_plan",
            {"alert_id": self.alert_id,
             "plan": {"summary": "西侧分流加限流", "measures": ["增开 2 条查验通道", "入口限流 500 人/批"]}},
            actor="police.wang", role="police"))
        self.assertEqual(plan_v2.data["plan_version"], 2)

        closed = self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id, "resolution": "客流回落至容量内"},
            actor="cmd.zhao", role="commander"))
        self.assertTrue(closed.accepted)

        alert = self.get_alert(self.alert_id)
        self.assertEqual(alert["status"], "closed")
        self.assertEqual(alert["handler"], {"actor": "railway.zhang", "role": "railway"})
        self.assertEqual([p["version"] for p in alert["plans"]], [1, 2])
        self.assertFalse(alert["plans"][0]["active"])
        self.assertTrue(alert["plans"][1]["active"])
        self.assertEqual(
            [e["event_type"] for e in alert["timeline"]],
            ["alert_raised", "alert_confirmed", "plan_adjusted", "plan_adjusted", "alert_closed"],
        )
        self.assertEqual(alert["timeline"][-1]["actor"], "cmd.zhao")

    def test_role_enforcement(self):
        no_role = self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="someone"))
        self.assertFalse(no_role.accepted)

        self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="police.wang", role="police"))
        close_by_police = self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id}, actor="police.wang", role="police"))
        self.assertFalse(close_by_police.accepted)  # 关闭须由指挥员执行
        close_by_commander = self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id}, actor="cmd.zhao", role="commander"))
        self.assertTrue(close_by_commander.accepted)

    def test_assign_requires_commander(self):
        denied = self.service.handle(make_request(
            "assign_alert", {"alert_id": self.alert_id, "assignee": "customs.li"},
            actor="police.wang", role="police"))
        self.assertFalse(denied.accepted)
        assigned = self.service.handle(make_request(
            "assign_alert", {"alert_id": self.alert_id, "assignee": "customs.li", "assignee_role": "customs"},
            actor="cmd.zhao", role="commander"))
        self.assertTrue(assigned.accepted)
        self.assertEqual(self.get_alert(self.alert_id)["handler"]["actor"], "customs.li")

    def test_invalid_transitions(self):
        close_first = self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id}, actor="cmd.zhao", role="commander"))
        self.assertFalse(close_first.accepted)  # 未确认不能关闭

        self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="police.wang", role="police"))
        confirm_again = self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="customs.li", role="customs"))
        self.assertFalse(confirm_again.accepted)

        self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id}, actor="cmd.zhao", role="commander"))
        adjust_closed = self.service.handle(make_request(
            "adjust_plan",
            {"alert_id": self.alert_id, "plan": {"summary": "x", "measures": ["y"]}},
            actor="police.wang", role="police"))
        self.assertFalse(adjust_closed.accepted)  # 已关闭不能再调整


class BackfillTest(ServiceTestBase):
    def _close_alert(self):
        self.clock.set(datetime(2026, 10, 3, 10, 5))
        self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="police.wang", role="police"))
        self.clock.set(datetime(2026, 10, 3, 11, 0))
        self.service.handle(make_request(
            "close_alert", {"alert_id": self.alert_id, "resolution": "处置完毕"},
            actor="cmd.zhao", role="commander"))

    def setUp(self):
        super().setUp()
        self.alert_id = self.ingest(snapshot("s-1", count=800)).data["alert_id"]  # amber

    def test_backfill_after_outage_does_not_rewrite_decisions(self):
        self._close_alert()
        before = self.get_alert(self.alert_id)

        # 断网期间积压的快照恢复后到达：事件时间在已关闭预警的处置时段内
        self.clock.set(datetime(2026, 10, 3, 11, 30))
        late = self.ingest(snapshot("s-late", station="railway-1", at="2026-10-03T10:20:00", count=990))
        self.assertTrue(late.accepted)
        self.assertEqual(late.data["alert_id"], self.alert_id)  # 归入原记录
        self.assertTrue(late.data["backfilled"])

        after = self.get_alert(self.alert_id)
        self.assertEqual(after["status"], "closed")          # 不重开
        self.assertEqual(after["severity"], "amber")         # 不改写已确认的级别
        self.assertEqual(after["version"], before["version"])
        self.assertEqual(len(after["snapshots"]), 2)         # 但留痕可查
        self.assertEqual(after["timeline"][-1]["event_type"], "snapshot_attached")
        self.assertTrue(after["timeline"][-1]["detail"]["backfilled"])

    def test_stale_snapshot_does_not_escalate_confirmed_alert(self):
        self.clock.set(datetime(2026, 10, 3, 10, 5))
        self.service.handle(make_request(
            "confirm_alert", {"alert_id": self.alert_id}, actor="police.wang", role="police"))

        # 事件时间早于确认时刻的迟到数据：只作证据，不倒写决定
        self.clock.set(datetime(2026, 10, 3, 10, 6))
        stale = self.ingest(snapshot("s-stale", station="customs-1",
                                     at="2026-10-03T10:03:00", count=980))
        self.assertTrue(stale.data["stale"])
        self.assertEqual(self.get_alert(self.alert_id)["severity"], "amber")

        # 事件时间晚于确认时刻的新数据仍可驱动升级
        self.clock.set(datetime(2026, 10, 3, 10, 7))
        fresh = self.ingest(snapshot("s-fresh", station="customs-1",
                                     at="2026-10-03T10:06:30", count=980))
        self.assertFalse(fresh.data["stale"])
        self.assertTrue(fresh.data["escalated"])
        self.assertEqual(self.get_alert(self.alert_id)["severity"], "red")

    def test_batch_backfill_applies_in_event_order(self):
        self.clock.set(datetime(2026, 10, 3, 12, 0))
        batch = {"snapshots": [
            snapshot("b-3", station="railway-2", zone="departure-hall", at="2026-10-03T10:30:00", count=960),
            snapshot("b-1", station="police-2", zone="departure-hall", at="2026-10-03T10:00:00", count=800),
            snapshot("b-2", station="customs-2", zone="departure-hall", at="2026-10-03T10:15:00", count=820),
        ]}
        result = self.service.handle(make_request("ingest_batch", batch, actor="gateway"))
        self.assertTrue(result.accepted)
        self.assertEqual(result.data["applied"], 3)
        # 无论到达顺序如何，按事件时间补入
        self.assertEqual([i["snapshot_id"] for i in result.data["items"]], ["b-1", "b-2", "b-3"])

        alerts = [a for a in self.list_alerts() if a["alert_id"] != self.alert_id]
        self.assertEqual(len(alerts), 1)
        alert = self.get_alert(alerts[0]["alert_id"])
        self.assertEqual(alert["severity"], "red")  # 补入过程中按事件顺序升级
        raised = alert["timeline"][0]
        self.assertEqual(raised["event_type"], "alert_raised")
        self.assertEqual(raised["detail"]["snapshot_id"], "b-1")  # 由最早的快照触发
        self.assertEqual(
            [e["event_type"] for e in alert["timeline"]],
            ["alert_raised", "snapshot_attached", "snapshot_attached", "severity_escalated"],
        )
        self.assertTrue(all(s["backfilled"] for s in alert["snapshots"]))

    def test_new_congestion_after_close_opens_new_alert(self):
        self._close_alert()
        self.clock.set(datetime(2026, 10, 3, 13, 1))
        fresh = self.ingest(snapshot("s-new", at="2026-10-03T13:00:00", count=900))
        self.assertEqual(fresh.state, "alert_raised")
        self.assertNotEqual(fresh.data["alert_id"], self.alert_id)
        self.assertEqual(len(self.list_alerts()), 2)


class IdempotencyTest(ServiceTestBase):
    def test_request_replay_returns_same_result(self):
        request = make_request("ingest_snapshot", snapshot("s-1"), request_id="req-fixed-1")
        first = self.service.handle(request)
        second = self.service.handle(request)
        self.assertEqual(first, second)
        self.assertEqual(second.state, "alert_raised")  # 重放返回原结果而非 duplicate
        self.assertEqual(len(self.list_alerts()), 1)

    def test_decision_replay_has_no_double_effect(self):
        alert_id = self.ingest(snapshot("s-1")).data["alert_id"]
        confirm = make_request("confirm_alert", {"alert_id": alert_id},
                               actor="police.wang", role="police", request_id="req-confirm-1")
        first = self.service.handle(confirm)
        second = self.service.handle(confirm)
        self.assertEqual(first, second)
        alert = self.get_alert(alert_id)
        self.assertEqual(alert["version"], 2)
        self.assertEqual(
            [e["event_type"] for e in alert["timeline"]],
            ["alert_raised", "alert_confirmed"],
        )


class PersistenceTest(unittest.TestCase):
    def test_state_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = f"{tmp}/command.db"
            clock = Clock(T0)
            service = BorderCommandService(path, now=clock)
            alert_id = service.handle(make_request(
                "ingest_snapshot", snapshot("s-1"), request_id="req-ingest-1")).data["alert_id"]
            service.handle(make_request(
                "confirm_alert", {"alert_id": alert_id},
                actor="police.wang", role="police", request_id="req-confirm-1"))
            service.close()

            restarted = BorderCommandService(path, now=clock)
            try:
                alert = restarted.handle(make_request(
                    "get_alert", {"alert_id": alert_id})).data
                self.assertEqual(alert["status"], "confirmed")
                self.assertEqual(alert["handler"]["actor"], "police.wang")
                # 请求级幂等结果也持久化：重放返回原结果
                replay = restarted.handle(make_request(
                    "ingest_snapshot", snapshot("s-1"), request_id="req-ingest-1"))
                self.assertEqual(replay.state, "alert_raised")
                self.assertEqual(len(restarted.handle(
                    make_request("list_alerts", {})).data["alerts"]), 1)
            finally:
                restarted.close()


class ContractTest(ServiceTestBase):
    def test_unknown_action_rejected(self):
        result = self.service.handle(make_request("explode", {}))
        self.assertFalse(result.accepted)

    def test_envelope_validation(self):
        with self.assertRaises(ValueError):
            self.service.handle(make_request("list_alerts", {}, actor=""))

    def test_alert_detail_shows_sources_handler_and_history(self):
        alert_id = self.ingest(snapshot("p-1", station="police-1", count=900)).data["alert_id"]
        self.ingest(snapshot("r-1", station="railway-1", count=870))
        self.service.handle(make_request(
            "confirm_alert", {"alert_id": alert_id}, actor="railway.zhang", role="railway"))
        self.service.handle(make_request(
            "adjust_plan",
            {"alert_id": alert_id, "plan": {"summary": "限流", "measures": ["入口分批放行"]}},
            actor="railway.zhang", role="railway"))
        self.service.handle(make_request(
            "close_alert", {"alert_id": alert_id, "resolution": "已缓解"},
            actor="cmd.zhao", role="commander"))

        alert = self.get_alert(alert_id)
        # 采用了哪些站点数据
        self.assertEqual(alert["stations"], ["police-1", "railway-1"])
        self.assertEqual(
            {s["snapshot_id"] for s in alert["snapshots"]}, {"p-1", "r-1"})
        # 目前由谁处理
        self.assertEqual(alert["handler"], {"actor": "railway.zhang", "role": "railway"})
        # 从产生到关闭经历了什么变化
        self.assertEqual(
            [e["event_type"] for e in alert["timeline"]],
            ["alert_raised", "snapshot_attached", "alert_confirmed", "plan_adjusted", "alert_closed"],
        )
        self.assertEqual(alert["timeline"][0]["detail"]["station_id"], "police-1")
        self.assertEqual(alert["closed_by"], "cmd.zhao")
        self.assertEqual(alert["resolution"], "已缓解")


if __name__ == "__main__":
    unittest.main()

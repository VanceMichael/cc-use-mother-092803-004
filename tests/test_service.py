import os
import tempfile
import unittest
from datetime import datetime

from app.contracts import Request
from app.domain import classify_load
from app.service import BorderCommandService
from app.storage import Storage


def snapshot(req_id, actor, role, station, zone, occupancy, capacity,
             observed, snapshot_id=None, name=None):
    return Request(
        actor, "ingest_snapshot",
        {
            "snapshot_id": snapshot_id or f"{station}-{observed}",
            "station_id": station, "zone": zone,
            "station_name": name or station,
            "occupancy": occupancy, "capacity": capacity,
            "observed_at": observed,
        },
        req_id, role, datetime.fromisoformat(observed.replace("Z", "")),
    )


def command(req_id, actor, role, action, alert_id, at=None, **extra):
    payload = {"alert_id": alert_id, **extra}
    created = datetime.fromisoformat(at) if at else datetime.utcnow()
    return Request(actor, action, payload, req_id, role, created)


class RiskClassificationTest(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(classify_load(70, 100), "normal")
        self.assertEqual(classify_load(80, 100), "watch")
        self.assertEqual(classify_load(100, 100), "warning")
        self.assertEqual(classify_load(130, 100), "critical")


class SharedAlertTest(unittest.TestCase):
    def setUp(self):
        self.service = BorderCommandService()

    def tearDown(self):
        self.service.close()

    def test_three_departments_share_one_alert(self):
        # 警务率先报 warning，预警产生。
        r1 = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 105, 100,
            "2026-10-03T08:00:00", name="北广场警务口"))
        self.assertTrue(r1.accepted)
        alert_id = r1.data["alert_id"]

        # 海关、铁路随后上报同区域：必须挂到同一条预警，不能新开。
        r2 = self.service.handle(snapshot(
            "c-1", "李海关", "customs", "ST-C1", "north", 110, 100,
            "2026-10-03T08:02:00", name="北广场海关通道"))
        r3 = self.service.handle(snapshot(
            "r-1", "王铁路", "railway", "ST-R1", "north", 90, 100,
            "2026-10-03T08:03:00", name="北站台"))
        self.assertEqual(r2.data["alert_id"], alert_id)
        self.assertEqual(r3.data["alert_id"], alert_id)

        detail = self.service.get_alert(alert_id)
        self.assertEqual(len(detail["stations"]), 3)
        self.assertEqual({s["station_id"] for s in detail["stations"]},
                         {"ST-P1", "ST-C1", "ST-R1"})
        self.assertEqual(detail["adopted_snapshot_count"], 3)

    def test_watch_only_does_not_open_alert(self):
        r = self.service.handle(snapshot(
            "p-0", "张警官", "police", "ST-P1", "south", 85, 100,
            "2026-10-03T08:00:00"))
        self.assertTrue(r.accepted)
        self.assertNotIn("alert_id", r.data)
        self.assertEqual(self.service.list_open_alerts(), [])

    def test_risk_escalation_is_appended(self):
        r1 = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "east", 105, 100,
            "2026-10-03T08:00:00"))
        alert_id = r1.data["alert_id"]
        self.service.handle(snapshot(
            "p-2", "张警官", "police", "ST-P1", "east", 130, 100,
            "2026-10-03T08:10:00"))
        detail = self.service.get_alert(alert_id)
        self.assertEqual(detail["risk_level"], "critical")
        self.assertIn("risk_escalated", [e["type"] for e in detail["timeline"]])


class IdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.service = BorderCommandService()

    def tearDown(self):
        self.service.close()

    def test_same_snapshot_replays_onto_original_record(self):
        req = snapshot("p-1", "张警官", "police", "ST-P1", "north", 105, 100,
                       "2026-10-03T08:00:00", snapshot_id="snap-1")
        r1 = self.service.handle(req)
        # 断网重试会换新的请求ID，但 snapshot_id 相同：必须归入原记录。
        retry = snapshot("p-1-retry", "张警官", "police", "ST-P1", "north", 105, 100,
                         "2026-10-03T08:00:00", snapshot_id="snap-1")
        r2 = self.service.handle(retry)
        self.assertTrue(r2.data["duplicate"])
        self.assertEqual(r2.data["alert_id"], r1.data["alert_id"])
        detail = self.service.get_alert(r1.data["alert_id"])
        self.assertEqual(detail["adopted_snapshot_count"], 1)
        self.assertEqual(
            [e["type"] for e in detail["timeline"]].count("snapshot_attached"), 1)

    def test_same_command_request_returns_original_result(self):
        r1 = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 105, 100,
            "2026-10-03T08:00:00"))
        alert_id = r1.data["alert_id"]
        ack = command("cmd-1", "李海关", "customs", "acknowledge", alert_id,
                      "2026-10-03T08:05:00")
        first = self.service.handle(ack)
        second = self.service.handle(ack)
        self.assertEqual(first, second)
        detail = self.service.get_alert(alert_id)
        self.assertEqual(
            [e["type"] for e in detail["timeline"]].count("acknowledged"), 1)


class CollaborationStateMachineTest(unittest.TestCase):
    def setUp(self):
        self.service = BorderCommandService()
        r = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 110, 100,
            "2026-10-03T08:00:00"))
        self.alert_id = r.data["alert_id"]

    def tearDown(self):
        self.service.close()

    def test_full_lifecycle_and_owner_tracking(self):
        # 海关值班员确认并认领。
        ack = self.service.handle(command(
            "cmd-1", "李海关", "customs", "acknowledge", self.alert_id,
            "2026-10-03T08:05:00", note="已到场"))
        self.assertEqual(ack.state, "acknowledged")
        detail = self.service.get_alert(self.alert_id)
        self.assertEqual(detail["owner_actor"], "李海关")
        self.assertEqual(detail["owner_role"], "customs")

        # 铁路此时再点“确认/升级”必须被拒绝——预警已在处置。
        dup = self.service.handle(command(
            "cmd-2", "王铁路", "railway", "acknowledge", self.alert_id,
            "2026-10-03T08:06:00"))
        self.assertFalse(dup.accepted)
        self.assertEqual(dup.data["owner_actor"], "李海关")

        # 启动分流后可以多次调整，最后关闭。
        start = self.service.handle(command(
            "cmd-3", "李海关", "customs", "start_diversion", self.alert_id,
            "2026-10-03T08:10:00", diversion_plan="开启东侧临时通道，限流30%"))
        self.assertEqual(start.state, "diverting")

        plan_before = self.service.get_alert(self.alert_id)["diversion_plan"]
        self.service.handle(command(
            "cmd-4", "王铁路", "railway", "adjust_diversion", self.alert_id,
            "2026-10-03T08:15:00",
            diversion_plan="东侧临时通道+北站台只出不进", reason="站台持续承压"))
        plan_after = self.service.get_alert(self.alert_id)["diversion_plan"]
        self.assertNotEqual(plan_before, plan_after)

        # 未确认不能直接启动分流的守卫。
        other = self.service.handle(snapshot(
            "p-9", "张警官", "police", "ST-X", "west", 110, 100,
            "2026-10-03T08:00:00"))
        bad = self.service.handle(command(
            "cmd-5", "张警官", "police", "start_diversion",
            other.data["alert_id"], "2026-10-03T08:01:00",
            diversion_plan="x"))
        self.assertFalse(bad.accepted)

        closed = self.service.handle(command(
            "cmd-6", "指挥员", "commander", "close", self.alert_id,
            "2026-10-03T09:00:00", reason="客流回落"))
        self.assertEqual(closed.state, "closed")

        # 关闭是终态，任何处置都被拒绝。
        again = self.service.handle(command(
            "cmd-7", "李海关", "customs", "close", self.alert_id,
            "2026-10-03T09:05:00"))
        self.assertFalse(again.accepted)

    def test_diversion_plan_required(self):
        self.service.handle(command(
            "cmd-8", "李海关", "customs", "acknowledge", self.alert_id,
            "2026-10-03T08:05:00"))
        r = self.service.handle(command(
            "cmd-9", "李海关", "customs", "start_diversion", self.alert_id,
            "2026-10-03T08:06:00"))
        self.assertFalse(r.accepted)


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.service = BorderCommandService()

    def tearDown(self):
        self.service.close()

    def test_offline_backlog_inserts_in_event_order_without_rewriting(self):
        # 08:00 产生预警，08:05 确认，08:10 启动分流，08:20 关闭。
        r = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 110, 100,
            "2026-10-03T08:00:00"))
        alert_id = r.data["alert_id"]
        self.service.handle(command("c-1", "李海关", "customs", "acknowledge",
                                    alert_id, "2026-10-03T08:05:00"))
        self.service.handle(command(
            "c-2", "李海关", "customs", "start_diversion", alert_id,
            "2026-10-03T08:10:00", diversion_plan="东侧限流"))
        self.service.handle(command(
            "c-3", "指挥员", "commander", "close", alert_id,
            "2026-10-03T08:20:00", reason="恢复正常"))

        # 09:00 网络恢复，积压的 08:15 海关快照补入。
        backfill = self.service.handle(snapshot(
            "c-late", "李海关", "customs", "ST-C1", "north", 140, 100,
            "2026-10-03T08:15:00"))
        self.assertTrue(backfill.accepted)
        self.assertEqual(backfill.data["alert_id"], alert_id)
        self.assertTrue(backfill.data["backfilled"])

        detail = self.service.get_alert(alert_id)
        # 已关闭、已确认的决定不被倒写：状态仍是 closed，处理人不变。
        self.assertEqual(detail["state"], "closed")
        self.assertEqual(detail["owner_actor"], "李海关")
        self.assertEqual(detail["risk_level"], "warning")

        # 时间线按事件发生时间排列：08:15 的补入证据位于 08:10 与 08:20 之间。
        ordered = [(e["time"], e["type"]) for e in detail["timeline"]]
        self.assertEqual(ordered, [
            ("2026-10-03T08:00:00", "alert_opened"),
            ("2026-10-03T08:00:00", "snapshot_attached"),
            ("2026-10-03T08:05:00", "acknowledged"),
            ("2026-10-03T08:10:00", "diversion_started"),
            ("2026-10-03T08:15:00", "snapshot_attached"),
            ("2026-10-03T08:20:00", "closed"),
        ])
        self.assertEqual(detail["adopted_snapshot_count"], 2)


class StaleAndOrderingTest(unittest.TestCase):
    def setUp(self):
        self.service = BorderCommandService()

    def tearDown(self):
        self.service.close()

    def test_snapshot_older_than_open_is_not_adopted(self):
        # 08:00 开预警（warning）。
        r = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 105, 100,
            "2026-10-03T08:00:00"))
        alert_id = r.data["alert_id"]
        # 07:50 的陈旧积压（critical）在更晚才送达：必须留存但不升级当前预警。
        stale = self.service.handle(snapshot(
            "p-old", "张警官", "police", "ST-P0", "north", 140, 100,
            "2026-10-03T07:50:00"))
        self.assertTrue(stale.accepted)
        self.assertNotIn("alert_id", stale.data)
        detail = self.service.get_alert(alert_id)
        self.assertEqual(detail["risk_level"], "warning")
        self.assertEqual(detail["adopted_snapshot_count"], 1)

    def test_late_decision_cannot_rewrite_confirmed_order(self):
        r = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 110, 100,
            "2026-10-03T08:00:00"))
        alert_id = r.data["alert_id"]
        # 08:20 已启动分流。
        self.service.handle(command("c-1", "李海关", "customs", "acknowledge",
                                    alert_id, "2026-10-03T08:05:00"))
        self.service.handle(command(
            "c-2", "李海关", "customs", "start_diversion", alert_id,
            "2026-10-03T08:20:00", diversion_plan="东侧限流"))
        # 一条声称 08:10 作出、断网积压的调整命令迟到：不得插入到决定之前。
        late = self.service.handle(command(
            "c-late", "王铁路", "railway", "adjust_diversion", alert_id,
            "2026-10-03T09:00:00", occurred_at="2026-10-03T08:10:00",
            diversion_plan="北站限流", reason="积压重放"))
        self.assertFalse(late.accepted)
        self.assertIn("倒写", late.message)
        detail = self.service.get_alert(alert_id)
        self.assertEqual(detail["diversion_plan"], "东侧限流")
        self.assertNotIn("diversion_adjusted",
                         [e["type"] for e in detail["timeline"]])

    def test_command_occurred_at_places_event_on_timeline(self):
        r = self.service.handle(snapshot(
            "p-1", "张警官", "police", "ST-P1", "north", 110, 100,
            "2026-10-03T08:00:00"))
        alert_id = r.data["alert_id"]
        # 请求 09:00 才到达，但携带 occurred_at=08:05。
        req = Request(
            "李海关", "acknowledge", {"alert_id": alert_id,
                                      "occurred_at": "2026-10-03T08:05:00"},
            "c-1", "customs", datetime(2026, 10, 3, 9, 0, 0))
        self.service.handle(req)
        detail = self.service.get_alert(alert_id)
        ack = next(e for e in detail["timeline"] if e["type"] == "acknowledged")
        self.assertEqual(ack["time"], "2026-10-03T08:05:00")


class PersistenceTest(unittest.TestCase):
    def test_state_survives_restart_with_file_storage(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            storage = Storage(path)
            service = BorderCommandService(storage)
            r = service.handle(snapshot(
                "p-1", "张警官", "police", "ST-P1", "north", 110, 100,
                "2026-10-03T08:00:00"))
            alert_id = r.data["alert_id"]
            service.handle(command("c-1", "李海关", "customs", "acknowledge",
                                   alert_id, "2026-10-03T08:05:00"))
            service.close()

            # 进程重启后从同一文件恢复。
            service2 = BorderCommandService(Storage(path))
            detail = service2.get_alert(alert_id)
            self.assertIsNotNone(detail)
            self.assertEqual(detail["state"], "acknowledged")
            self.assertEqual(detail["owner_actor"], "李海关")
            self.assertEqual(len(detail["stations"]), 1)
            replay = service2.handle(command(
                "c-1", "李海关", "customs", "acknowledge", alert_id,
                "2026-10-03T08:05:00"))
            self.assertTrue(replay.accepted)  # 幂等结果一并恢复
            service2.close()
        finally:
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(path + suffix):
                    os.remove(path + suffix)


class CommanderViewTest(unittest.TestCase):
    def test_view_shows_evidence_owner_and_full_timeline(self):
        service = BorderCommandService()
        try:
            service.handle(snapshot(
                "p-1", "张警官", "police", "ST-P1", "north", 105, 100,
                "2026-10-03T08:00:00", name="北广场警务口"))
            alert_id = service.handle(snapshot(
                "c-1", "李海关", "customs", "ST-C1", "north", 105, 100,
                "2026-10-03T08:01:00", name="北广场海关通道")).data["alert_id"]
            # 同站更新快照：视图取最新一张，但两张都计入采纳证据。
            service.handle(snapshot(
                "c-2", "李海关", "customs", "ST-C1", "north", 90, 100,
                "2026-10-03T08:09:00"))
            service.handle(command("k-1", "王铁路", "railway", "acknowledge",
                                   alert_id, "2026-10-03T08:05:00"))

            detail = service.get_alert(alert_id)
            self.assertEqual(detail["zone"], "north")
            self.assertEqual(detail["state"], "acknowledged")
            self.assertEqual(detail["owner_actor"], "王铁路")
            self.assertEqual(detail["owner_role"], "railway")
            self.assertEqual(len(detail["stations"]), 2)
            customs = next(s for s in detail["stations"]
                           if s["station_id"] == "ST-C1")
            self.assertEqual(customs["occupancy"], 90)  # 最新值
            self.assertEqual(detail["adopted_snapshot_count"], 3)
            actors = {e["actor"] for e in detail["timeline"]}
            self.assertIn("张警官", actors)
            self.assertIn("李海关", actors)
            self.assertIn("王铁路", actors)
            self.assertIsNone(service.get_alert("A-nope"))
        finally:
            service.close()


if __name__ == "__main__":
    unittest.main()

"""口岸联合指挥的领域对象、风险判定与职责约定。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# 容量风险等级：green < amber < red
RISK_ORDER = {"green": 0, "amber": 1, "red": 2}
AMBER_THRESHOLD = 0.75
RED_THRESHOLD = 0.90

# 预警状态机：open -> confirmed -> closed
ALERT_OPEN = "open"
ALERT_CONFIRMED = "confirmed"
ALERT_CLOSED = "closed"

# 值班职责：commander 为指挥员，police/customs/railway 为各岗位值班员
ROLE_COMMANDER = "commander"
DUTY_ROLES = frozenset({ROLE_COMMANDER, "police", "customs", "railway"})

# 各处置动作允许的职责：确认与调整分流方案允许各岗位值班员，
# 人员指派与关闭预警须由指挥员执行
ACTION_ROLES = {
    "confirm_alert": DUTY_ROLES,
    "adjust_plan": DUTY_ROLES,
    "assign_alert": frozenset({ROLE_COMMANDER}),
    "close_alert": frozenset({ROLE_COMMANDER}),
}


class InvalidPayload(ValueError):
    """请求数据不符合约定。"""


@dataclass(frozen=True)
class SnapshotInput:
    """站点上报的一条带时间戳的客流快照。"""

    snapshot_id: str
    station_id: str
    zone: str
    captured_at: datetime
    passenger_count: int
    capacity: int


def evaluate_risk(passenger_count: int, capacity: int) -> tuple[float, str]:
    """按容量占用率判定风险等级。"""
    ratio = passenger_count / capacity
    if ratio >= RED_THRESHOLD:
        return ratio, "red"
    if ratio >= AMBER_THRESHOLD:
        return ratio, "amber"
    return ratio, "green"


def parse_instant(raw: object) -> datetime:
    """解析 ISO 8601 时间戳，统一为 UTC 朴素时间。"""
    if not isinstance(raw, str) or not raw.strip():
        raise InvalidPayload("时间戳必须是非空字符串")
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        instant = datetime.fromisoformat(text)
    except ValueError as exc:
        raise InvalidPayload(f"无法解析时间戳: {raw}") from exc
    if instant.tzinfo is not None:
        instant = instant.astimezone(timezone.utc).replace(tzinfo=None)
    return instant


def iso(instant: datetime) -> str:
    return instant.isoformat(timespec="seconds")


def utcnow() -> datetime:
    """当前 UTC 时间（朴素对象，与存储格式一致）。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_snapshot(payload: dict) -> SnapshotInput:
    """把请求数据解析为客流快照，字段不合法时抛出 InvalidPayload。"""
    def required_text(key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise InvalidPayload(f"快照缺少有效的 {key}")
        return value.strip()

    count = payload.get("passenger_count")
    capacity = payload.get("capacity")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise InvalidPayload("passenger_count 必须是非负整数")
    if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
        raise InvalidPayload("capacity 必须是正整数")
    return SnapshotInput(
        snapshot_id=required_text("snapshot_id"),
        station_id=required_text("station_id"),
        zone=required_text("zone"),
        captured_at=parse_instant(payload.get("captured_at")),
        passenger_count=count,
        capacity=capacity,
    )

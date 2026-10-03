"""纯领域规则：容量风险分级与预警状态机。

这一层不接触数据库与时钟，便于单独测试；所有判定都是纯函数。
"""
from __future__ import annotations

# 容量占用率阈值（占用人数 / 设计容量）。
# watch：开始关注；warning：达到分流预警门槛；critical：必须立即处置。
WATCH_AT = 0.80
WARNING_AT = 1.00
CRITICAL_AT = 1.20

# 状态机：当前状态 -> 允许执行的处置动作。
# 分流执行中可以反复调整方案；关闭是终态，终态拒绝一切处置。
_TRANSITIONS: dict[str, set[str]] = {
    "open": {"acknowledge"},
    "acknowledged": {"start_diversion", "close"},
    "diverting": {"adjust_diversion", "close"},
    "closed": set(),
}

# 预警一旦产生，占用率跌回阈值以下也不自动消失，必须由值班员关闭。
# 但风险等级随证据实时重算，用于决定是否升级。
_RISK_ORDER = {"normal": 0, "watch": 1, "warning": 2, "critical": 3}


def classify_load(occupancy: int, capacity: int) -> str:
    """按容量占用率返回风险等级。"""
    if capacity <= 0:
        raise ValueError("站点容量必须为正数")
    if occupancy < 0:
        raise ValueError("占用人数不能为负")
    ratio = occupancy / capacity
    if ratio >= CRITICAL_AT:
        return "critical"
    if ratio >= WARNING_AT:
        return "warning"
    if ratio >= WATCH_AT:
        return "watch"
    return "normal"


def risk_rank(level: str) -> int:
    return _RISK_ORDER[level]


def can_transition(state: str, action: str) -> bool:
    return action in _TRANSITIONS.get(state, set())


def is_final(state: str) -> bool:
    return state == "closed"

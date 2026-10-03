"""口岸联合指挥 的输入输出约定。"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_naive() -> datetime:
    """统一的朴素 UTC 时钟，与 ISO 字符串按字典序比较保持一致。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)

# 三类上报部门 / 值班角色。预警按区域共享，与来源部门解耦，
# 这样警务、海关、铁路围绕同一条预警协同，而不是各自升级。
ROLES = ("police", "customs", "railway", "commander")

# 风险等级（由站点容量占用率推导）。
RISK_LEVELS = ("normal", "watch", "warning", "critical")

# 预警状态机：
#   open         已产生，等待任一部门值班员确认
#   acknowledged 已确认（已认领，分流方案尚未启动）
#   diverting    分流方案执行中，可多次调整
#   closed       已关闭（终态，只读）
ALERT_STATES = ("open", "acknowledged", "diverting", "closed")

# 事件时间线上的事件类型。事件流只追加，任何已确认的决定都不可改写。
EVENT_TYPES = (
    "alert_opened",
    "risk_escalated",
    "snapshot_attached",
    "acknowledged",
    "diversion_started",
    "diversion_adjusted",
    "closed",
)


@dataclass(frozen=True)
class Request:
    actor: str
    action: str
    payload: dict[str, Any]
    request_id: str
    role: str = "commander"
    created_at: datetime = field(default_factory=utc_now_naive)


@dataclass
class Result:
    accepted: bool
    state: str
    message: str
    data: dict[str, Any] = field(default_factory=dict)


def validate_request(request: Request) -> None:
    if not request.actor or not request.action or not request.request_id:
        raise ValueError("请求缺少身份、动作或幂等键")
    if not isinstance(request.payload, dict):
        raise TypeError("请求数据必须是对象")
    if request.role not in ROLES:
        raise ValueError(f"未知角色: {request.role}")

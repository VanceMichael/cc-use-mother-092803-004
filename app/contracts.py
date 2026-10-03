"""口岸联合指挥 的输入输出约定。"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

@dataclass(frozen=True)
class Request:
    actor: str
    action: str
    payload: dict[str, Any]
    request_id: str
    created_at: datetime = field(default_factory=_utcnow)
    # 调用方职责：commander / police / customs / railway，处置类动作按此鉴权
    role: str = ""

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

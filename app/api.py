"""口岸联合指挥 的轻量本地调用入口。

用法：python3 -m app.api [数据库文件]
从标准输入读取一条 JSON 请求（actor/action/payload/request_id/role），
处理结果以 JSON 写到标准输出。数据库文件也可用环境变量 BORDER_COMMAND_DB 指定。
"""
import json
import os
import sys
from .contracts import Request
from .models import utcnow
from .service import BorderCommandService

def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        return 2
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("BORDER_COMMAND_DB", "border_command.db")
    item = json.loads(raw)
    request = Request(
        actor=str(item.get("actor", "")),
        action=str(item.get("action", "")),
        payload=dict(item.get("payload", {})),
        request_id=str(item.get("request_id", "")),
        created_at=utcnow(),
        role=str(item.get("role", "")),
    )
    service = BorderCommandService(db_path)
    try:
        result = service.handle(request)
    finally:
        service.close()
    print(json.dumps({"accepted": result.accepted, "state": result.state, "message": result.message, "data": result.data}, ensure_ascii=False))
    return 0 if result.accepted else 1

if __name__ == "__main__":
    raise SystemExit(main())

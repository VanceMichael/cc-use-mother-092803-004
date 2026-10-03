"""口岸联合指挥 的轻量本地调用入口。

从标准输入读取一个 JSON 请求，例如：

  # 站点客流快照（警务/海关/铁路）
  {"role": "police", "actor": "张警官", "action": "ingest_snapshot",
   "request_id": "p-1",
   "payload": {"snapshot_id": "snap-1", "station_id": "ST-P1", "zone": "north",
               "station_name": "北广场警务口", "occupancy": 110, "capacity": 100,
               "observed_at": "2026-10-03T08:00:00"}}

  # 查看预警（证据站点 / 当前处理人 / 完整时间线）
  {"actor": "指挥员", "action": "get_alert", "request_id": "q-1",
   "payload": {"alert_id": "A-north-20261003080000"}}

可用 --db 指定 SQLite 文件，默认使用内存库。
处置动作：acknowledge / start_diversion / adjust_diversion / close。
"""
from __future__ import annotations

import json
import sys

from .contracts import Request, utc_now_naive
from .service import BorderCommandService
from .storage import Storage


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    db_path = None
    if "--db" in argv:
        db_path = argv[argv.index("--db") + 1]

    service = BorderCommandService(Storage(db_path))
    raw = sys.stdin.read().strip()
    if not raw:
        return 2
    item = json.loads(raw)
    request = Request(
        actor=str(item.get("actor", "")),
        action=str(item.get("action", "")),
        payload=dict(item.get("payload", {})),
        request_id=str(item.get("request_id", "")),
        role=str(item.get("role", "commander")),
        created_at=utc_now_naive(),
    )

    if request.action == "get_alert":
        detail = service.get_alert(str(request.payload.get("alert_id", "")))
        if detail is None:
            print(json.dumps({"found": False}, ensure_ascii=False))
            return 1
        print(json.dumps({"found": True, "alert": detail}, ensure_ascii=False))
        return 0

    result = service.handle(request)
    print(json.dumps({"accepted": result.accepted, "state": result.state,
                      "message": result.message, "data": result.data},
                     ensure_ascii=False))
    return 0 if result.accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())

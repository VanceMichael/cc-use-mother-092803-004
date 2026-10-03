# 口岸联合指挥

面向黄金周等大客流场景的口岸联合指挥后端：警务、海关、铁路各站点上报带时间戳的客流快照，
服务按容量占用率识别风险，并为每个口岸区域维护**一条跨部门共享预警**，
让不同职责的值班人员围绕同一预警完成确认、分流方案调整与关闭，
避免“已经执行分流，却被另一部门当作未处置事件再次升级”。

## 领域模型

**风险分级**（占用人数 / 站点设计容量，见 `app/domain.py`）：

| 占用率 | 等级 | 含义 |
| --- | --- | --- |
| < 80% | `normal` | 正常 |
| ≥ 80% | `watch` | 关注（仅留存证据，不开预警） |
| ≥ 100% | `warning` | 达到分流预警门槛，**开预警** |
| ≥ 120% | `critical` | 必须立即处置，触发升级事件 |

**预警状态机**（每个区域同时至多一条未关闭预警）：

```
open ──acknowledge──▶ acknowledged ──start_diversion──▶ diverting
                        │                                   │
                        └──────────────close───────────────┴──▶ closed（终态，只读）
                                                            diverting 可反复 adjust_diversion
```

- `acknowledge`：任一部门值班员确认并**认领**（记录处理人），之后其他部门的重复确认/升级被拒绝并提示当前处理人。
- `start_diversion`：认领后启动分流方案（方案内容必填）。
- `adjust_diversion`：分流执行中可多次调整方案，每次调整作为事件留痕。
- `close`：关闭预警。关闭后只读，任何处置动作都被拒绝。

## 关键语义

- **同一快照归入原记录**：`snapshot_id` 是快照业务键，重复到达直接返回原结果，
  不重复挂证据、不重复升级；所有请求另有 `request_id` 幂等键，命令重放返回首次结果。
- **断网积压按事件顺序补入**：快照携带 `observed_at`（事件时间），处置命令可携带
  `occurred_at`。网络恢复后，旧快照归入其发生时段内开窗的预警；事件流按 `event_time`
  排序展示，旧证据/旧决定落在正确位置。
- **只追加、不回写**：事件流仅 INSERT。积压证据可以补入已关闭预警，但不会改变
  已确认的处理人、已启动/调整的方案和关闭状态；处置时间早于预警开窗、或晚到命令的
  事件时间早于已生效决定时直接拒绝。早于当前预警开窗的陈旧快照只留存、不采纳为证据，
  避免过期数据触发误升级。
- **指挥员视图** `get_alert` 直接给出：
  - 采用了哪些站点数据（站点、最新占用/容量/风险、采纳快照数）；
  - 当前由谁处理（`owner_actor` / `owner_role`）与当前分流方案；
  - 从产生到关闭的完整时间线（含每条证据来自哪个部门、每次决定由谁作出）。

## 目录

`app/contracts.py` 请求/结果约定与角色枚举；`app/domain.py` 风险分级与状态机纯函数；
`app/storage.py` SQLite 存储（幂等键、只追加事件流）；`app/service.py` 服务编排与视图；
`app/api.py` 本地 JSON 调用入口；`tests/` 行为测试。

## 测试

```bash
python3 -m unittest discover -s tests
python3 -m compileall app
```

## 使用

```bash
# 1) 警务站点上报一张超容快照 → 产生区域预警
echo '{"role":"police","actor":"张警官","action":"ingest_snapshot","request_id":"p-1",
"payload":{"snapshot_id":"snap-1","station_id":"ST-P1","zone":"north",
"station_name":"北广场警务口","occupancy":110,"capacity":100,
"observed_at":"2026-10-03T08:00:00"}}' | python3 -m app.api --db command.db

# 2) 海关值班员确认认领
echo '{"role":"customs","actor":"李海关","action":"acknowledge","request_id":"c-1",
"payload":{"alert_id":"A-north-20261003080000","note":"已到场"}}' | python3 -m app.api --db command.db

# 3) 启动并调整分流
echo '{"role":"customs","actor":"李海关","action":"start_diversion","request_id":"c-2",
"payload":{"alert_id":"A-north-20261003080000",
"diversion_plan":"开启东侧临时通道，限流30%"}}' | python3 -m app.api --db command.db

# 4) 指挥员查看同一条预警：证据站点 / 处理人 / 完整时间线
echo '{"actor":"指挥员","action":"get_alert","request_id":"q-1",
"payload":{"alert_id":"A-north-20261003080000"}}' | python3 -m app.api --db command.db
```

不传 `--db` 时使用内存库，适合测试与演示。

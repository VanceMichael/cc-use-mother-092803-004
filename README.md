# 口岸联合指挥

面向口岸联合演练场景的客流风险与分流预警协同处置后端服务。警务、海关、铁路等站点的客流快照统一接入，按容量占用率识别风险；不同职责的值班人员围绕同一条预警确认、调整、关闭分流方案，全程留痕，避免同一拥堵事件被多部门当作未处置事件重复升级。

## 能力

- **快照接入与风险识别**：`ingest_snapshot` / `ingest_batch` 接收带时间戳的客流快照，按占用率判定 green / amber / red（阈值 0.75 / 0.90），达到 amber 即生成预警。
- **同一事件归并**：同一区域（zone）在时间窗口（±30 分钟）内的多部门上报并入同一条预警，而不是各自升级。
- **协同处置**：`confirm_alert` / `adjust_plan` 允许指挥员与各岗位值班员（police / customs / railway）执行；`assign_alert` / `close_alert` 仅指挥员。预警状态机：`open → confirmed → closed`，关闭前须先确认。
- **幂等与去重**：
  - 请求级：`request_id` 重放返回首次处理的结果，不产生二次效果；
  - 数据级：相同快照（按 `snapshot_id`，或同站点同采集时刻且内容一致）再次到达归入原记录；同站同时刻但数据不一致视为冲突，拒绝且不覆盖原记录。
- **断网补传**：`ingest_batch` 把积压快照按采集时间（事件时间）排序后依次补入；迟到快照只能作为证据归入原预警——已关闭的预警不会被重开，事件时间早于最近一次决定的数据不会改写已确认的级别与方案。
- **全程留痕**：`get_alert` 返回预警采用的站点数据、当前处置人、分流方案版本历史，以及从产生到关闭的完整时间线。

## 动作目录

| action | 职责要求 | 说明 |
| --- | --- | --- |
| `ingest_snapshot` | 不限 | 接入单条快照，必要时生成或并入预警 |
| `ingest_batch` | 不限 | 积压快照按事件时间顺序补入 |
| `confirm_alert` | 各岗位值班员 / 指挥员 | 确认预警，处置责任落实到当班人员 |
| `adjust_plan` | 各岗位值班员 / 指挥员 | 调整分流方案，生成新版本并作废旧版本 |
| `assign_alert` | 指挥员 | 指派处置人 |
| `close_alert` | 指挥员 | 关闭预警（须先确认） |
| `get_alert` | 不限 | 查看预警详情：站点数据、处置人、方案版本、时间线 |
| `list_alerts` | 不限 | 按状态 / 区域列出预警 |

请求约定见 `app/contracts.py`：`actor`（操作人）、`action`、`payload`、`request_id`（幂等键）、`role`（职责）。

## 目录

`app/` 放置领域对象和服务入口，`tests/` 保存行为测试。

- `app/models.py` — 领域对象、风险阈值、职责矩阵
- `app/store.py` — SQLite 持久化（请求、快照、预警、方案、事件）
- `app/service.py` — 状态机与业务规则
- `app/api.py` — 标准输入输出的本地调用入口

## 测试

```bash
python3 -m unittest discover -s tests
```

## 构建检查

```bash
python3 -m compileall app
```

## 使用

服务以本地 Python 模块运行，数据默认保存在调用方提供的 SQLite 文件中：

```bash
echo '{"actor": "police-1-gw", "action": "ingest_snapshot", "request_id": "req-1", "payload": {"snapshot_id": "s-1", "station_id": "police-1", "zone": "arrival-hall", "captured_at": "2026-10-03T10:00:00", "passenger_count": 900, "capacity": 1000}}' \
  | python3 -m app.api command.db
```

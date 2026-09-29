# OTA Fleet Rollout（设备车队灰度 OTA 升级编排）

基于 Spring Boot 的内存模拟实现：设备注册/心跳/在线判定、按批次推进的灰度升级、
失败暂停与重试回滚、离线设备重连收敛。所有状态保存在本地内存，无外部依赖。

## 运行与测试

```bash
mvn test          # 运行全部测试（单测日志打印输入、判定依据与最终状态）
mvn spring-boot:run
```

## 状态流转

### 设备在线状态

```
心跳上报 ──> lastHeartbeatAt = max(已有, 新上报时间戳)   （大者生效，单调不减）
判定: now - lastHeartbeatAt <= ota.offline-threshold-ms  ?  ONLINE : OFFLINE
```

- 重复、乱序或并发的同设备上报只会收敛到最大时间戳，**不会互相覆盖或抖动**；
- 同一设备的所有写操作经 `ConcurrentHashMap.compute` 串行化；
- 在线判定是纯函数，任意入口在同一时刻得到一致结果；
- 重复注册幂等：保留首次注册的生效版本。

### 设备升级记录状态机（单设备在任务内）

```
PENDING --上报成功--> SUCCESS       （终态，重复上报忽略）
PENDING --上报失败--> FAILED        （终态，保留 failReason）
FAILED  --重试------> PENDING       （清空失败原因，attempts 递增）
FAILED  --回滚------> ROLLED_BACK   （终态，生效版本恢复为 previousVersion）
```

终态迁移只发生一次：命令重复投递或设备重复上报被识别为 `DUPLICATE_IGNORED`，
**不重复计数、不跳批**。

### 任务状态机

```
RUNNING --失败率超限--> PAUSED   （pauseReason 汇总各设备失败原因，可追溯）
PAUSED  --失败记录全部消解(重试/回滚)--> RUNNING （自动恢复后立即重新收尾判定）
RUNNING --全部批次达标且设备定性--> COMPLETED （终态，finishReason 记录收尾依据）
RUNNING --当前批全部定性但成功不足--> FAILED   （终态，finishReason 记录收尾依据）
```

`COMPLETED` 与 `FAILED` 均为**终态**：任务进入终态后状态不再变化，迟到的设备上报
被拒绝（`REJECTED_TASK_FINISHED`），显式推进接口返回 false，因此多设备并发上报、
重复上报或查询时看到的最终状态唯一且确定，不会出现“一会儿能过、一会儿过不了”的抖动。

## 批次推进规则

- 设备按 `batchSize` 顺序切批，从第 0 批开始；
- **放行门槛（生效阈值）= min(`successThreshold`, 当前批实际设备数)**：
  - 满批时门槛就是 `successThreshold`，与历史规则完全一致；
  - 设备数不是每批台数整数倍时，最后一个**收尾批次**可能只有少量设备（例如每批 3 台、
    阈值 2，投 4 台时收尾批只有 1 台）。若仍按原阈值 2 判定则永远不可能达标，任务会无限期
    卡在 RUNNING；因此收尾批的门槛按其实际台数收敛（上例中收敛为 1，该设备成功即完成）；
- 成功台数达到放行门槛才允许进入下一批（每次上报后自动检查，也可调用显式推进接口，判定相同）；
- 设备所在批次晚于当前批时，其上报被拒绝（`REJECTED_BATCH_NOT_STARTED`），防止跳批；
- 失败率 = `FAILED / (SUCCESS + FAILED)`，超过 `maxFailureRate` 任务立即暂停并保留原因；
- **收尾与完成语义**：每次上报以及失败设备被重试/回滚后，都在任务锁内执行同一次收尾判定——
  - 当前批成功台数达标：非最后一批则推进；最后一批且设备全部定性（无 PENDING/FAILED 未决）
    则任务 `COMPLETED`；
  - 当前批成功台数未达标，但设备**已全部定性**（成功的 SUCCESS、失败的已回滚 ROLLED_BACK，
    不存在还能改变结果的 PENDING/FAILED 记录）：这批结果已不可能再变化，任务明确收尾为
    `FAILED` 终态，并在 `finishReason` 中记录“实际成功台数 / 门槛 / 批大小”，查询时一眼可辨；
  - 仍有未决设备（PENDING 等待上报，或 FAILED 等待重试/回滚）时保持 RUNNING 继续等待。
- 终态只迁移一次：重试只针对 FAILED 记录（记录变 PENDING 后该批重新存在未决设备，
  自然不会被误判为收尾失败）；回滚把 FAILED 变为 ROLLED_BACK。所有判定均为幂等纯状态推导，
  配合任务级锁，并发/重复上报下批次能否放行、任务最终状态只有一个确定结果。

## 重试与回滚规则

- **重试**：`FAILED -> PENDING`，设备可重新上报；失败记录全部消解后任务自动恢复；
- **回滚**：`FAILED -> ROLLED_BACK`，设备生效版本恢复为升级前版本；
- 设备生效版本是**单一字段、任务锁内原子更新**，任何时刻唯一且确定，
  不会出现两个生效版本或长期中间态。

## 离线设备重连收敛

设备心跳上报后触发 `convergeOnReconnect`：

- 若设备处于 RUNNING 任务中、记录仍为 PENDING 且生效版本落后于任务目标版本，
  则重新下发升级命令（保持 PENDING 等待上报）；
- 版本已对齐则不重复下发；
- 由此保证离线设备重新上线后**收敛到任务当前目标版本**。

## 配置项（application.properties）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `ota.offline-threshold-ms` | 30000 | 离线阈值：超过该时长未收到心跳判定 OFFLINE |
| `ota.batch-success-threshold` | 2 | 批次成功阈值（台数），达到才允许进入下一批 |
| `ota.max-failure-rate` | 0.5 | 最大失败率（0~1），超过任务暂停 |
| `ota.batch-size` | 3 | 创建任务时默认每批设备数 |

## REST 入口

### 设备侧 `/api/devices`

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/{deviceId}/register?version=1.0.0` | 注册设备（幂等） |
| POST | `/{deviceId}/heartbeat[?at=ts]` | 心跳上报，触发重连收敛 |
| GET  | `/{deviceId}/status` | 查询在线状态与生效版本 |

### 任务侧 `/api/rollout`

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks/{taskId}?targetVersion=2.0.0&deviceIds=d1,d2,...[&batchSize=&successThreshold=&maxFailureRate=]` | 创建任务（幂等） |
| POST | `/tasks/{taskId}/report?deviceId=&success=&failReason=` | 上报升级结果 |
| POST | `/tasks/{taskId}/advance` | 显式推进批次 |
| POST | `/tasks/{taskId}/devices/{deviceId}/retry` | 重试失败设备 |
| POST | `/tasks/{taskId}/devices/{deviceId}/rollback` | 回滚失败设备 |
| POST | `/tasks/{taskId}/rollback-all-failed` | 回滚全部失败设备 |
| GET  | `/tasks/{taskId}` | 查询任务与各设备记录 |

## 测试覆盖

- `DeviceRegistryTest`：离线阈值边界、乱序/重复心跳不抖动、重复注册幂等、
  并发心跳收敛到最大时间戳、并发注册与心跳的一致性；
- `RolloutServiceTest`：批次阈值边界（不足不推进/达标自动推进）、防跳批、
  重复上报不重复计数、失败率超限暂停且原因可追溯、重试恢复、回滚后版本唯一确定、
  离线设备重连收敛、并发上报恰好计数一次、**收尾批次门槛按实际批大小收敛后正常完成**、
  **批内设备全部定性但成功台数不足时明确收尾为 FAILED（含“失败暂停后回滚”与
  “未暂停直接回滚”两条路径）**、收尾批并发上报只完成一次、满批老配置放行语义不变。

所有用例日志均打印**输入、判定依据与最终状态**，便于审计与排障。

## 本地复现与验证

### 自动化测试（推荐，稳定可重复）

```bash
mvn test                                  # 运行全部用例（共 19 个）
mvn test -Dtest=RolloutServiceTest       # 只跑编排相关用例
```

两个此前“结束不了”的场景对应的用例：

- `tailBatchSmallerThanThresholdConvergesAndCompletes`
  复现“默认配置（每批 3 台、阈值 2）投 4 台，收尾批仅 1 台”——修复前任务永久 RUNNING，
  修复后收尾批门槛收敛为 1，`d4` 上报成功后任务 `COMPLETED`；
- `batchFailsWhenAllDevicesDecidedButSuccessBelowThreshold`（批内定性但成功不足，经“失败→暂停→回滚”路径）
  与 `rollbackWhileRunningAlsoFinalizesWhenBatchExhausted`（同语义，经“未暂停直接回滚”路径）
  ——修复前批内已无待处理设备却永远 RUNNING，修复后明确落 `FAILED` 终态，
  `finishReason` 标明实际成功台数与门槛。

### 手工 HTTP 复现

```bash
mvn spring-boot:run
# 注册 4 台设备
for d in d1 d2 d3 d4; do
  curl -s -X POST "http://localhost:8080/api/devices/$d/register?version=1.0.0" >/dev/null
done
# 用默认配置（每批 3 台、阈值 2）创建任务
curl -s -X POST "http://localhost:8080/api/rollout/tasks/demo?targetVersion=2.0.0&deviceIds=d1,d2,d3,d4"
# 第 1 批 d1、d2 成功 -> 自动进入收尾批 [d4]
curl -s -X POST "http://localhost:8080/api/rollout/tasks/demo/report?deviceId=d1&success=true"
curl -s -X POST "http://localhost:8080/api/rollout/tasks/demo/report?deviceId=d2&success=true"
# 收尾批 d4 成功 -> status=COMPLETED（修复前这里会永远停在 RUNNING）
curl -s -X POST "http://localhost:8080/api/rollout/tasks/demo/report?deviceId=d4&success=true"
curl -s "http://localhost:8080/api/rollout/tasks/demo"   # 查询：status 与 finishReason 一目了然
```

“批内全部定性但成功不足”的手工复现（3 台/批、阈值 2）：d1 成功，d2、d3 报失败后调用
`/rollback-all-failed`；任务恢复后因批内为 1 SUCCESS + 2 ROLLED_BACK、成功台数 1 < 门槛 2，
`GET /api/rollout/tasks/{taskId}` 返回 `"status":"FAILED"`，`finishReason` 形如
`batch 0 exhausted: successCount=1 < requiredThreshold=2 (batchSize=3), all devices decided ...`。

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
PAUSED  --失败记录全部消解(重试/回滚)--> RUNNING （自动恢复，恢复瞬间重新收尾判定）
RUNNING --全部批次收尾--> COMPLETED
RUNNING --当前批设备已全部定性但成功台数不足--> FAILED   （终态，failureReason 可追溯）
```

- `COMPLETED` 与 `FAILED` 都是**终态**：之后的重复上报、重复回滚、显式推进都不会再翻转状态；
- 查询任务（`GET /api/rollout/tasks/{taskId}`）时 `status` 一眼可辨是否已结束，
  `failureReason` 说明是在第几批、成功几台、差多少而失败收尾。

## 批次推进规则

- 设备按 `batchSize` 顺序切批，从第 0 批开始；
- **仅当前批次成功台数 >= 成功阈值才允许进入下一批**（每次上报后自动检查，
  也可调用显式推进接口，判定相同）；
- 设备所在批次晚于当前批时，其上报被拒绝（`REJECTED_BATCH_NOT_STARTED`），防止跳批；
- 失败率 = `FAILED / (SUCCESS + FAILED)`，超过 `maxFailureRate` 任务立即暂停并保留原因。

### 收尾批次（设备数不是每批台数整数倍）

车队设备数往往不能被 `batchSize` 整除，最后一批（收尾批次）只有更少的设备，
可能永远凑不满配置的成功阈值。因此判定使用**有效成功阈值**：

```
有效阈值 = min(配置阈值 successThreshold, 本批实际设备数)
```

- 满员批次：有效阈值就是配置阈值，**行为与历史完全一致**（如批 3 台、阈值 2，仍需 2 台成功）；
- 收尾批次：有效阈值降为本批实际台数，即整批全部成功即放行并收尾。
  例如默认配置（每批 3 台、阈值 2）投 4 台：第 1 批 2 台成功放行，
  末批仅 1 台、该台成功即 `COMPLETED`，不再永远卡在“进行中”。

### 一批内设备都已定性但成功台数不够

当当前批次**所有设备都已有明确定性**——成功的为 `SUCCESS`，失败的已重试成功或回滚为
`ROLLED_BACK`，不存在 `PENDING`/`FAILED`——但成功台数仍低于有效阈值时，
已经没有任何设备还能补够成功台数，任务立即落入 **`FAILED` 终态**，
并在 `failureReason` 中记录：批次序号、成功台数、有效阈值/配置阈值/本批台数。

该判定在所有会改变设备定性的入口后统一执行（上报、重试、回滚、回滚全部失败设备、
显式推进），因此无论是“未暂停直接回滚”还是“先因失败率暂停、回滚后自动恢复”，
都能收敛到同一个终态，不会无限期停留在 `RUNNING`。

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
  离线设备重连收敛、并发上报恰好计数一次；
  **收尾与完成语义专项**：
  - `tailBatchCompletesWhenFleetSizeNotMultipleOfBatchSize`：默认配置（每批 3、阈值 2）
    投 4 台，末批仅 1 台也能正常判定并 `COMPLETED`（稳定复现修复前的卡死）；
  - `fullyDecidedBatchBelowThresholdFailsTaskWithoutPause`：3 台一批、阈值 2，
    1 台成功 + 2 台失败后全部回滚（失败率未超限、任务未暂停），任务落入 `FAILED` 终态；
  - `fullyDecidedBatchBelowThresholdFailsTaskAfterPauseAndRollback`：同一场景走默认失败率，
    先暂停、回滚恢复后同样收敛到 `FAILED`（稳定复现恢复后卡死）；
  - `tailBatchConcurrentReportsAndAdvanceSettleExactlyOnce`：收尾批次并发重复上报 +
    并发显式推进，成功只接受一次、收尾只发生一次，最终 `COMPLETED` 唯一且不翻转。

所有用例日志均打印**输入、判定依据与最终状态**，便于审计与排障。

## 本地复现与验证

```bash
mvn test          # 运行全部测试（含两类“结束不了”的回归用例）
mvn spring-boot:run
```

### 复现 1：设备数不是每批整数倍的收尾批次

默认配置（每批 3 台、阈值 2）投 4 台，修复前最后一台上报后任务永远停在 `RUNNING`：

```bash
curl -s 'http://localhost:8080/api/devices/m1/register?version=1.0.0' -X POST
curl -s 'http://localhost:8080/api/devices/m2/register?version=1.0.0' -X POST
curl -s 'http://localhost:8080/api/devices/m3/register?version=1.0.0' -X POST
curl -s 'http://localhost:8080/api/devices/m4/register?version=1.0.0' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-tail?targetVersion=2.0.0&deviceIds=m1,m2,m3,m4' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-tail/report?deviceId=m1&success=true' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-tail/report?deviceId=m2&success=true' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-tail/report?deviceId=m4&success=true' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-tail'   # 期望 status=COMPLETED
```

### 复现 2：一批内设备已全部定性但成功台数不够

3 台一批、阈值 2、失败率放宽到 1.0（不暂停）：1 台成功、2 台失败后全部回滚，
修复前任务一直 `RUNNING`：

```bash
for d in n1 n2 n3; do curl -s "http://localhost:8080/api/devices/$d/register?version=1.0.0" -X POST; done
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail?targetVersion=2.0.0&deviceIds=n1,n2,n3&batchSize=3&successThreshold=2&maxFailureRate=1.0' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail/report?deviceId=n1&success=true' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail/report?deviceId=n2&success=false&failReason=flash-error' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail/report?deviceId=n3&success=false&failReason=power-loss' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail/rollback-all-failed' -X POST
curl -s 'http://localhost:8080/api/rollout/tasks/demo-fail'   # 期望 status=FAILED, failureReason 非空
```

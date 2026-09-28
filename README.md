# OTA Fleet Rollout（设备车队灰度 OTA 升级编排）

基于 Spring Boot 的内存模拟实现：设备注册/心跳/在线判定、按批次推进的灰度升级、
失败暂停与重试回滚、离线设备重连收敛，并支持**批次按比例达标、整任务一键中止回滚、
设备重试次数上限自动回滚**。所有状态保存在本地内存，无外部依赖。

## 运行与测试

```bash
mvn test          # 运行全部测试（单测日志打印输入、判定依据与最终状态）
mvn spring-boot:run
# 若本机 8080 被占用，可换端口：mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=18090
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
PENDING --上报失败--> FAILED        （保留 failReason；attempts 已计本次）
FAILED  --重试------> PENDING       （清空失败原因，可重新上报；受重试上限约束）
FAILED  --回滚------> ROLLED_BACK   （终态，生效版本恢复为 previousVersion，记录 rollbackReason）
SUCCESS --整任务中止--> ROLLED_BACK （终态，已成功设备随任务回滚退回旧版本）
PENDING --整任务中止--> SKIPPED     （终态，尚未升级，版本保持原样、不再升级）
```

终态迁移只发生一次：命令重复投递或设备重复上报被识别为 `DUPLICATE_IGNORED`，
**不重复计数、不跳批**。

### 任务状态机

```
RUNNING --失败率超限--> PAUSED   （pauseReason 汇总各设备失败原因，可追溯）
PAUSED  --失败记录全部消解(重试/回滚)--> RUNNING （自动恢复）
RUNNING --全部批次完成--> COMPLETED      （终态）
RUNNING/PAUSED --一键中止--> ABORTING --全量回滚落库--> ABORTED   （终态，幂等）
```

`ABORTING` 是瞬态：中止回滚全程在任务级锁内原子完成，锁外查询不会观察到该状态
（查询接口同样在任务锁内快照读取），因此中止期间查询**不会看到新旧版本混杂或设备停在半路**。

## 批次推进规则

- 设备按 `batchSize` 顺序切批，从第 0 批开始；设备总数不是每批台数整数倍时，最后会有一个较小的“尾巴批次”；
- 达标判定有两种方式，可按任务（创建参数）或全局（`ota.batch-success-mode`）选择：
  - **`COUNT`（默认，历史行为）**：仅当前批次**成功台数 ≥ `successThreshold`** 才允许进入下一批；
  - **`RATIO`（新增，按比例）**：当前批次 `成功设备数 / 当前批设备总数 ≥ batchSuccessRatio` 即达标。
    分母是**当前批实际设备数**，所以尾巴批次即使只有 1～2 台，也能按成功比例正常判定通过、
    继续推进直至整个任务 COMPLETED，不会卡在尾巴批次结束不了；
  - 两种模式的自动推进/显式推进判定一致；比例不达标同样不放行（如 3 台批、阈值 0.7 时仅 2 台成功不会推进）；
- 设备所在批次晚于当前批时，其上报被拒绝（`REJECTED_BATCH_NOT_STARTED`），防止跳批；
- 失败率 = `FAILED / (SUCCESS + FAILED)`，超过 `maxFailureRate` 任务立即暂停并保留原因；
- 最后一批达标且全部设备到达终态（SUCCESS / ROLLED_BACK / SKIPPED）后任务完成。

> 兼容性：不配置 `batch-success-mode`、不传入新参数时即为 `COUNT` 模式，老配置行为完全不变。

## 重试与回滚规则

- **重试**：`FAILED -> PENDING`，设备可重新上报；失败记录全部消解后任务自动恢复；
- **回滚**：`FAILED -> ROLLED_BACK`，设备生效版本恢复为升级前版本；
- 设备生效版本是**单一字段、任务锁内原子更新**，任何时刻唯一且确定，
  不会出现两个生效版本或长期中间态。

### 重试次数上限（新增）

- 可为任务（`maxAttemptsPerDevice`）或全局（`ota.max-attempts-per-device`）配置每台设备的
  **最大尝试次数 = 首次上报 + 重试次数**；
- 设备失败时若 `attempts` 已达到上限：**不再允许重试，直接自动回滚**到升级前版本，
  记录状态 `ROLLED_BACK`，并在 `rollbackReason` 中写明 `retry-limit-exceeded: attempts=x/y, lastFailReason=...`，
  避免无限重试；
- 任务查询可见每台设备的 `attempts`（试了几次）、`failReason`（最后一次失败原因）、
  `rollbackReason`（为何变成回滚），便于事后排查；
- `maxAttemptsPerDevice=0`（默认/不配置）表示**不限制**重试次数，与历史行为一致。

## 整任务中止回滚（新增）

一键把整个任务停掉并收敛到确定终态，适用于发现新版本有问题的紧急场景：

- 入口：`POST /api/rollout/tasks/{taskId}/abort`（幂等，重复中止返回 `aborted=false`）；
- `RUNNING` / `PAUSED` 可中止；`COMPLETED` / 已 `ABORTED` 的任务不可再中止；
- 中止时对每台设备的处置：
  - **已升级成功（SUCCESS）**：状态 -> `ROLLED_BACK`，生效版本**退回各自升级前版本**；
  - **当前失败（FAILED）**：状态 -> `ROLLED_BACK`，生效版本恢复为升级前版本；
  - **尚未升级（PENDING）**：状态 -> `SKIPPED`，版本**保持原样**、不再升级；
- 任务最终到达明确终态 **`ABORTED`**；
- **状态可见性 / 并发正确性**：中止回滚全程在按 `taskId` 的任务锁内原子完成，
  与设备上报、重试、推进互斥；查询接口在同一把锁内快照读取，因此中止期间既看不到
  `ABORTING` 瞬态，也看不到“部分新版本/部分旧版本”或设备停在半路；
- **中止与上报同时发生时结果唯一确定**（两种顺序都收敛到同一结果，绝不重复回滚或版本错配）：
  - 中止先发生：上报被拒绝（`REJECTED_TASK_ABORTED`），设备保持旧版本并最终 `SKIPPED`；
  - 成功上报先发生：设备先到新版本，随后被中止回滚到旧版本（`ROLLED_BACK`）。

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
| `ota.batch-success-threshold` | 2 | 批次成功阈值（台数），COUNT 模式下达到才允许进入下一批 |
| `ota.max-failure-rate` | 0.5 | 最大失败率（0~1），超过任务暂停 |
| `ota.batch-size` | 3 | 创建任务时默认每批设备数 |
| `ota.batch-success-mode` | COUNT | 批次达标判定方式：`COUNT` 固定台数（历史默认）/ `RATIO` 成功比例 |
| `ota.batch-success-ratio` | 1.0 | RATIO 模式下当前批达标所需最小成功占比（0~1） |
| `ota.max-attempts-per-device` | 0 | 每设备最大尝试次数（首次+重试）；`0` 不限制（历史默认），达到上限失败自动回滚 |

> 三项新增配置均有兼容老配置的默认值（COUNT / 1.0 / 0），不配置时行为与上一版完全一致。
> 任务级参数优先级高于全局配置：创建任务时传入的 `batchSuccessMode`、`batchSuccessRatio`、
> `maxAttemptsPerDevice` 仅作用于该任务；不传则回落到全局配置。

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
| POST | `/tasks/{taskId}?targetVersion=2.0.0&deviceIds=d1,d2,...[&batchSize=&successThreshold=&maxFailureRate=&batchSuccessMode=RATIO&batchSuccessRatio=0.8&maxAttemptsPerDevice=3]` | 创建任务（幂等）；新参数全可选，不传用全局默认 |
| POST | `/tasks/{taskId}/report?deviceId=&success=&failReason=` | 上报升级结果（任务中止时返回 `REJECTED_TASK_ABORTED`） |
| POST | `/tasks/{taskId}/advance` | 显式推进批次 |
| POST | `/tasks/{taskId}/devices/{deviceId}/retry` | 重试失败设备（达上限被拒） |
| POST | `/tasks/{taskId}/devices/{deviceId}/rollback` | 回滚失败设备 |
| POST | `/tasks/{taskId}/rollback-all-failed` | 回滚全部失败设备 |
| POST | `/tasks/{taskId}/abort` | **一键中止整个任务**：成功设备回滚旧版本、未升级设备跳过，终态 ABORTED（幂等） |
| GET  | `/tasks/{taskId}` | 在任务锁内一致地查询任务与各设备记录（含 attempts/failReason/rollbackReason） |

## 本地验证

```bash
# 1) 自动化测试：覆盖新增行为且确认旧用例无回归
mvn test

# 2) 启动（8080 被占用时换端口）
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=18090
B=http://127.0.0.1:18090

# 3) 注册设备
for d in d1 d2 d3; do curl -s -X POST "$B/api/devices/$d/register?version=1.0.0" >/dev/null; done

# 4) 比例模式 + 尾巴批次：3 台 / 每批 2 台（尾巴 1 台），比例 1.0
curl -s -X POST "$B/api/rollout/tasks/t?targetVersion=2.0.0&deviceIds=d1,d2,d3&batchSize=2&successThreshold=2&batchSuccessMode=RATIO&batchSuccessRatio=1.0"
for d in d1 d2 d3; do curl -s -X POST "$B/api/rollout/tasks/t/report?deviceId=$d&success=true" >/dev/null; done
curl -s "$B/api/rollout/tasks/t" | grep -o '"status":"[A-Z]*"'   # 期望 "status":"COMPLETED"

# 5) 中止回滚（另起任务：成功设备回滚、未升级设备 SKIPPED、上报被拒）
#    curl -s -X POST "$B/api/rollout/tasks/t2/abort"

# 6) 重试上限：maxAttemptsPerDevice=2，连续两次失败后设备自动 ROLLED_BACK，重试被拒
```

> 若处于带代理的环境，curl 加 `--noproxy '*'` 并使用 `127.0.0.1` 直连。

## 测试覆盖

- `DeviceRegistryTest`：离线阈值边界、乱序/重复心跳不抖动、重复注册幂等、
  并发心跳收敛到最大时间戳、并发注册与心跳的一致性；
- `RolloutServiceTest`：批次阈值边界（不足不推进/达标自动推进）、防跳批、
  重复上报不重复计数、失败率超限暂停且原因可追溯、重试恢复、回滚后版本唯一确定、
  离线设备重连收敛、并发上报恰好计数一次；
  以及本次新增——
  **比例达标与尾巴批次收尾**（设备数不是每批整数倍时按比例通过并完成任务、
  比例不达标不放行、COUNT 老模式行为保持不变）、
  **整任务中止回滚**（成功设备回滚 / 未升级设备 SKIPPED / 暂停态也可中止 /
  中止幂等且上报被拒，以及**与设备上报同时发生的并发场景**下版本唯一确定、无中间态、不重复回滚）、
  **重试次数上限**（达上限自动回滚并记录 attempts 与 rollbackReason、之后重试被拒、
  `maxAttempts=0` 老配置不限制重试）。

所有用例日志均打印**输入、判定依据与最终状态**，便于审计与排障。

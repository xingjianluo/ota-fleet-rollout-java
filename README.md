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
PAUSED  --失败记录全部消解(重试/回滚)--> RUNNING （自动恢复）
RUNNING --全部批次完成--> COMPLETED
```

## 批次推进规则

- 设备按 `batchSize` 顺序切批，从第 0 批开始；
- **仅当前批次成功台数 >= `successThreshold` 才允许进入下一批**（每次上报后自动检查，
  也可调用显式推进接口，判定相同）；
- 设备所在批次晚于当前批时，其上报被拒绝（`REJECTED_BATCH_NOT_STARTED`），防止跳批；
- 失败率 = `FAILED / (SUCCESS + FAILED)`，超过 `maxFailureRate` 任务立即暂停并保留原因；
- 最后一批达标且全部设备到达终态（SUCCESS/ROLLED_BACK）后任务完成。

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
  离线设备重连收敛、并发上报恰好计数一次。

所有用例日志均打印**输入、判定依据与最终状态**，便于审计与排障。

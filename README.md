# OTA 车队灰度升级编排（本地内存版）

Spring Boot 3 / Java 21 实现，用本地内存模拟设备与升级任务，提供设备注册、心跳与在线判定、
按批次灰度推进、失败暂停与可追溯原因、失败重试、整体回滚以及离线设备重新上线后的版本收敛能力。

---

## 1. 设计原则：一致性从何而来

| 关注点 | 机制 |
|---|---|
| 同一设备并发心跳 | **设备级锁** `synchronized(device)`：同一设备的心跳串行，跨设备不互斥 |
| 同一任务并发操作 | **任务级锁** `synchronized(rollout)`：命令投递、结果上报、重试、回滚全部串行 |
| 锁序 | 固定为 **设备锁 → 任务锁**（仅心跳收敛路径），其余路径只持任务锁，不存在锁序反转 |
| 心跳时间 | 以**服务器接收时间**为准（时间源 `Clock` 可注入，便于测试），规避设备端时钟漂移/伪造 |
| 在线状态 | **不存为可变枚举**，由 `now - lastHeartbeatMillis <= 离线阈值` 动态推导，因此没有 ONLINE/OFFLINE 互相覆盖的抖动 |
| 心跳时间单调性 | `lastHeartbeat` 只增不减，乱序/重复/并发上报后该值确定（= 最后处理的服务器时间） |
| 结果重复上报 | 按去重键只计一次（见下表），重复/并发上报为 no-op，不重复计数、不跳批 |
| 命令重复投递 | 同一 `commandId` 为 no-op；省略 `commandId` 时使用确定性命令 ID（`设备+尝试序号`） |
| 版本确定性 | 升级成功只可能写入 `targetVersion`；回滚成功只可能写入 `baseVersion`，由服务端在结果处理时落版本 |

去重键：

| 场景 | 去重键 |
|---|---|
| 升级（含重试）结果 | `UPGRADE:{deviceId}:{attempt}`，首次下发 attempt=0，每重试一次 +1 |
| 回滚结果 | `ROLLBACK:{deviceId}`（一次任务只回滚一次） |

> 幂等检查优先于状态校验：即使任务已到终态 `COMPLETED/ROLLED_BACK`，迟到的重复结果上报仍按
> no-op 返回同一状态，而不是报错——保证“设备重复上报”在任何入口下结果一致。

---

## 2. 设备生命周期与在线判定

### 2.1 在线状态

```
注册时 lastHeartbeat = 服务器当前时间
心跳时 lastHeartbeat = max(旧值, 服务器接收时间)

ONLINE  ⇔  now - lastHeartbeatMillis <= ota.offline-threshold-millis
OFFLINE ⇔  now - lastHeartbeatMillis >  ota.offline-threshold-millis
```

- 状态是时间的纯函数，不需要后台扫描，任何时刻查询结果都可解释；
- 重复注册幂等（保留原设备与初始版本）；
- 心跳可携带 `currentVersion`；上线（心跳）是离线设备版本收敛的触发点（见 §5）。

### 2.2 设备在任务中的阶段 `DevicePhase`

`IDLE → UPGRADING → SUCCESS / FAILED`，`FAILED → UPGRADING`（重试），
任意已下发阶段 → `ROLLING_BACK → ROLLED_BACK`。

- `IDLE`：已在当前批次但尚未下发命令，或属于尚未打开的批次；
- `UPGRADING`：升级/重试命令在途，等待结果；
- `SUCCESS/FAILED`：当前批次内的确定结果；
- `ROLLING_BACK`：回滚命令在途（**中间态**，不应长期停留，见 §5）；
- `ROLLED_BACK`：已回到基线版本。

---

## 3. 升级任务状态流转 `RolloutState`

```
                 create                start
  （无） ───────────────▶ PENDING ───────────────▶ RUNNING
                                                    │  │
                              批次达标且有下一批 ◀────┘  │ 失败率超限 / 批次结束成功率不足
                              （仍 RUNNING）            ▼
                                                     PAUSED
                                                   │   │  │
                              重试+resume（重新评估）┘   │  └──── rollback
                              （回到 RUNNING）          │
                                                         ▼
                                                    ROLLING_BACK ──全部回滚成功──▶ ROLLED_BACK

  最后一批达标 ───────────────────────────────────────────────▶ COMPLETED
  COMPLETED 也可手动 rollback（已发布版本仍可整体撤回）────────▶ ROLLING_BACK
```

- **PENDING**：仅创建未开始；
- **RUNNING**：某一批次已打开，正在下发/收集结果；
- **PAUSED**：推进停止，**保留 `failureReason` 与 `failureDetail`**，只接受重试或回滚；
- **ROLLING_BACK / ROLLED_BACK**：回滚中 / 回滚完成；
- **COMPLETED**：全部批次达标。

非法状态上的操作返回 `409 Conflict` 并带可解释的错误信息。

---

## 4. 批次推进与失败规则

### 4.1 批次推进

任务创建时给定有序的二维设备列表 `batches`（每批至少 1 台设备）。
`start` 打开第 0 批；之后只有**当前批次**满足条件才允许进入下一批：

1. **失败率熔断（提前暂停）**：收集过程中，一旦

   ```
   failed / batchTotal > failureThreshold（严格大于）
   ```

   立即 `PAUSED`，无需等批次结束；
2. **批次结束判定**：当 `succeeded + failed == batchTotal`：
   - `succeeded / batchTotal >= successThreshold`（大于等于）→ 进入下一批；最后一批则 `COMPLETED`；
   - 否则 `PAUSED`。
3. **暂停不自动解除**：暂停后迟到/重试的结果仍然入账（计数、设备阶段都会更新），
   但不会自动推进；必须对失败设备重试后显式调用 `resume`，由系统重新评估当前批次。

### 4.2 计数与重试

- 成功/失败只按去重键计一次；重试成功时会把该设备此前计入的 `failed` 修正回 `succeeded`；
- 重试次数受 `maxAttempts` 约束：首次下发 attempt=0，之后最多 `maxAttempts-1` 次重试；
- 重试只能针对当前批次 `FAILED` 的设备，且任务处于 `PAUSED`；
- 已结束批次的迟到上报会被拒绝（设备批次号 ≠ 当前批次），避免“跳到未来批次计数”。

### 4.3 暂停原因（可追溯）

`failureReason` 取值：

- `FAILURE_RATE_EXCEEDED`：`failureDetail` 含批次号、实际与阈值比率、成功/失败计数；
- `MANUAL_ROLLBACK`：无前置失败时在运行中/完成后手动回滚。

若暂停后再回滚，原始失败原因与明细**保留不变**。

---

## 5. 回滚规则与离线收敛

### 5.1 回滚确定性

- 回滚只针对**已打开批次**中曾收到升级命令（非 IDLE）且未回滚的设备；
  尚未打开的后续批次不受影响（它们本就还在基线版本）；
- 回滚命令的目标版本唯一为 `baseVersion`；设备回滚成功后生效版本被服务端确定为 `baseVersion`，
  升级成功设备同样回退，因此 **`ROLLED_BACK` 时相关设备不可能同时存在两个生效版本**；
- 回滚结果去重键为 `ROLLBACK:{deviceId}`，重复上报不重复计数；
- 回滚失败不改变设备阶段（保持 `ROLLING_BACK`）并记录 `ROLLBACK_FAILED: ...`，允许重发回滚命令重试，
  全部成功后任务才置 `ROLLED_BACK`（避免长期停留中间态）。

### 5.2 离线设备重新上线收敛

每次心跳都作为“上线信号”，系统遍历该设备参与的任务：

| 任务状态 | 设备阶段 | 判定与动作 |
|---|---|---|
| RUNNING / PAUSED | `UPGRADING` | 若在途升级命令已丢失（命令记录不存在）→ 以**确定性命令 ID 重发**目标版本 `targetVersion` |
| RUNNING | 当前批次 `IDLE` | 尚未下发过命令 → 补发升级命令 |
| ROLLING_BACK | `ROLLING_BACK` | 在途回滚命令丢失 → 重发基线版本 `baseVersion` |
| 任意 | `SUCCESS` | 版本已锁定为目标版本，不动作 |
| 任意 | `ROLLED_BACK` | 版本已锁定为基线版本，不动作 |

因此离线设备错过命令、或在回滚窗口离线，恢复心跳后都会重新收到命令并收敛到**任务当前目标版本**，
不会出现“两个生效版本”或长期 `UPGRADING/ROLLING_BACK`。

---

## 6. 配置项（`application.properties`）

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `ota.offline-threshold-millis` | `30000` | 离线阈值（毫秒），距最后心跳超过该值判定 OFFLINE |
| `server.port` | `8080` | HTTP 端口 |
| `logging.level...otafleetrolloutjava` | `INFO` | 服务层日志级别 |

创建任务时可按任务覆盖的参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `successThreshold` | `0.8` | 批次成功阈值，`succeeded/total >= 该值` 才进入下一批 |
| `failureThreshold` | `0.2` | 批次失败熔断阈值，`failed/total > 该值` 立即暂停（严格大于） |
| `maxAttempts` | `3` | 每台设备升级的最大尝试次数（首次 + 重试合计） |

---

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/devices/register` | 注册设备（幂等），body: `deviceId, initialVersion` |
| POST | `/api/devices/heartbeat` | 心跳（上线/收敛触发点），body: `deviceId, currentVersion?` |
| GET  | `/api/devices/{deviceId}` | 查询设备：版本、最后心跳、动态在线状态 |
| POST | `/api/rollouts` | 创建任务：`rolloutId, targetVersion, baseVersion, batches, successThreshold?, failureThreshold?, maxAttempts?` |
| GET  | `/api/rollouts/{id}` | 查询任务（状态、当前批次、各批计数、设备阶段、失败原因） |
| POST | `/api/rollouts/{id}/start` | 打开第 0 批开始灰度 |
| POST | `/api/rollouts/{id}/commands/upgrade` | 投递升级命令（幂等），body: `commandId?, deviceId` |
| POST | `/api/rollouts/{id}/commands/retry` | PAUSED 下对失败设备重试，body: `commandId?, deviceId` |
| POST | `/api/rollouts/{id}/results` | 设备上报结果，body: `deviceId, success, version?, errorMessage?` |
| POST | `/api/rollouts/{id}/resume` | 重试后显式恢复推进 |
| POST | `/api/rollouts/{id}/rollback` | 回滚到 `baseVersion`（body 可空） |

### 快速示例

```bash
curl -X POST localhost:8080/api/devices/register -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1","initialVersion":"v1"}'
curl -X POST localhost:8080/api/rollouts -H 'Content-Type: application/json' \
  -d '{"rolloutId":"r1","targetVersion":"v2","baseVersion":"v1",
       "batches":[["d1"]],"successThreshold":1.0,"failureThreshold":0.0,"maxAttempts":3}'
curl -X POST localhost:8080/api/rollouts/r1/start
curl -X POST localhost:8080/api/rollouts/r1/commands/upgrade -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1"}'
curl -X POST localhost:8080/api/rollouts/r1/results -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1","success":true,"version":"v2"}'
```

---

## 8. 测试

```bash
mvn test
```

测试不依赖 Spring 容器启动，使用可注入的 `MutableClock` 精确控制时间。覆盖：

- `DeviceHeartbeatConcurrencyTest`：重复注册、离线阈值边界、并发交错心跳、并发重复心跳；
- `BatchProgressionTest`：成功阈值边界、重复投递/重复上报幂等、失败率熔断与原因保留、80 次并发重复上报；
- `RollbackAndReconnectTest`：失败重试→resume、回滚后版本唯一、离线命令丢失后上线收敛、
  回滚窗口离线重连收敛、手动回滚原因、重试上限。

每个用例的日志均打印 **测试输入 → 判定依据 → 最终状态**（服务层日志前缀
`[register]/[heartbeat]/[report]/[pause]/[advance]/[rollback]/[converge]/[version]`）。

## 9. 局限

- 数据存于内存，重启丢失；生产应替换 `InMemoryStore` 为持久化存储（保持锁与幂等键语义即可）；
- 时间源与命令通道为模拟实现；真实设备命令确认可复用同一幂等键模型。

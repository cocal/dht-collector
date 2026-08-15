# DHT 分布式部署方案

本文定义 DHT 采集器在多节点部署时的事件、统计和监控页面架构。

## 目标

- 每个 DHT 节点可以独立运行和扩缩容。
- 不启用监控页面时，节点不依赖 Redis 或 dashboard。
- 监控统计不扫描 PostgreSQL 明细表。
- 节点重启、网络重试和聚合器故障不会造成重复计数。
- 页面可以同时展示全局统计、节点统计和节点在线状态。

## 组件

```text
DHT node A ─┐
DHT node B ─┼──> Redis bridge / aggregator
DHT node C ─┘              |
                           v
                    dht:summary
                    dht:node:{id}
                           |
                           v
                    dashboard SSE
                           |
                           v
                         browser
```

### DHT 主服务

主服务只负责 DHT、metadata 和 PostgreSQL 业务数据。它输出带身份的
`monitor.v1` 事件到 stdout/journal；Redis bridge 是可选旁路，不能成为
DHT 采集链路的硬依赖。

没有监控需求时，只部署：

```text
dht-passive-collector.service
```

### Redis bridge / aggregator

可选的 bridge 从 journal 或事件输出读取 `monitor.v1`，使用 event_id 去重后直接
维护全局和节点维度的 summary，并通过 Pub/Sub 通知 Dashboard。

### Dashboard

dashboard 首次读取 Redis summary，之后通过 SSE 接收实时变更。PostgreSQL
只用于搜索、历史详情和统计重建，不参与首页实时计数。

## 事件身份

每个事件必须包含稳定的生产者身份：

```json
{
  "schema": "monitor.v1",
  "event_id": "uuid",
  "node_id": "dht-a",
  "boot_id": "uuid-for-process-start",
  "sequence": 123456,
  "metric": "dht.resource_discovered",
  "value": 1,
  "occurred_at": "2026-08-15T00:00:00Z"
}
```

- `node_id`：稳定的部署节点名，例如 `dht-a`。
- `boot_id`：每次进程启动生成的新 UUID，用于区分重启前后的 sequence。
- `sequence`：节点进程内单调递增序号。
- `event_id`：全局唯一事件 ID，用于幂等去重。

只有 `event_id`、`node_id`、`boot_id` 和 `sequence` 都可追踪，聚合器才能
区分重试、重复消费和节点重启。

## Redis 数据结构

```text
dht:summary                      全局计数 Hash
dht:node:{node_id}               节点计数和最近状态 Hash
dht:dedupe:{event_id}            幂等去重键，带 TTL
dht:node:{node_id}:heartbeat     节点心跳，带过期时间
```

bridge 聚合事件时执行：

```text
SET dht:dedupe:{event_id} 1 NX EX 86400
HINCRBY dht:summary dht.resource_discovered 1
HINCRBY dht:node:dht-a dht.resource_discovered 1
HSET dht:node:dht-a last_event_at <timestamp>
```

只有 `SET ... NX` 成功时才允许增加计数。

PostgreSQL 仍是业务事实的长期存储，Redis 丢失后可以从 PostgreSQL 和 journal
重新构建 summary。

## 节点心跳

每个节点定期刷新：

```text
SET dht:node:dht-a:heartbeat <unix-seconds> EX 30
```

页面按心跳判断在线状态，而不是按最近是否有业务事件判断。没有流量的节点
也可能是健康的，不能把“没有事件”当作“节点离线”。

## 页面实时更新

页面打开时：

1. 读取 `dht:summary` 和所有节点状态。
2. 通过 SSE 长连接接收后续聚合变更。
3. 连接断开后重新读取 summary。

页面不直接消费每个 DHT 节点的 journal，也不自己累加业务事件。

## 部署模式

### 最小模式（无监控）

```text
dht-passive-collector.service
PostgreSQL
```

### 单机监控模式

```text
dht-passive-collector.service
Redis
dht-monitor-redis-bridge.service
dht-search-dashboard.service
PostgreSQL
```

当前仓库提供可选的 `deploy/dht-monitor-redis-bridge.service`。在 systemd 主机
上启用监控旁路：

```bash
dnf install keydb python3-redis
systemctl enable --now keydb.service
systemctl enable --now dht-monitor-redis-bridge.service
```

bridge 的环境变量位于 `/etc/dht-search/monitor-redis.env`，至少应为每个
采集节点设置唯一的 `DHT_NODE_ID`。停用监控时只需停止并禁用 bridge 和
dashboard，`dht-passive-collector.service` 不需要修改或重启。

Redis 本身的健康检查由可选的 `dht-redis-monitor.service` 提供。它每 30 秒
输出以下 `monitor.v1` 指标到 journal：

```text
redis.up
redis.error
redis.latency_ms
redis.used_memory_bytes
redis.connected_clients
```

Redis 不可用时，检查服务仍会记录 `redis.error`；它不会依赖 Redis 写回自身，
因此外部 monitor-center 可以从 `dht-redis-monitor.service` journal 发现故障。

### 多节点模式

```text
DHT node 1..N
        |
        +--> shared Redis / Redis Cluster
        +--> shared PostgreSQL
        +--> monitor aggregator group
        +--> dashboard instances
```

所有节点使用不同的 `node_id`，共享同一个 Redis 实例。每个 bridge 使用事件
ID 幂等更新全局和节点 summary；dashboard 可以部署多个实例，但只读 summary，
不会重复修改计数。

## 一致性和故障处理

- bridge 使用带 TTL 的 `SET ... NX` 做事件幂等去重。
- dashboard 断线不影响 DHT 采集，重连时重新读取 summary。
- Redis 不可用时，DHT 主服务继续采集并保留 journal；bridge 恢复后继续处理
  新的 journal 事件。
- summary 定期与 PostgreSQL 业务事实做校准，发现偏差时允许重建。
- Redis 不是 PostgreSQL 的替代品，不保存唯一的业务事实。

## 规模选择

- 几台到几十台节点：Redis summary + aggregator + SSE。
- 数百台节点或每天数十亿事件：Kafka/Kafka Streams 或 ClickHouse 增量物化
  视图，再由 Redis/SSE 提供页面实时状态。
- 当前数据规模优先使用 Redis 方案，避免直接引入 Kafka、Pinot 或 Druid。

## 当前实现总结（2026-08）

当前监控链路已经与 DHT 主服务解耦：

```text
dht-passive-collector.service
        | journal monitor.v1
        v
dht-monitor-redis-bridge.service
        | summary + node hash / Pub/Sub
        v
KeyDB/Redis ----> dht-search-dashboard.service ----> SSE browser
        ^
dht-redis-monitor.service（健康指标）
```

Redis 只保存实时监控状态，不保存 content、file_entry 等业务事实：

```text
dht:summary                         全局统计 Hash
dht:node:<node_id>                  节点统计 Hash
dht:dedupe:<event_id>               幂等去重 key，带 TTL
dht:node:<node_id>:heartbeat        节点心跳，TTL 30 秒
```

`dht:events` Stream 已移除。当前没有 Stream 消费者，Dashboard 的实时更新使用
`dht:summary:update` Pub/Sub；PostgreSQL 和 journal 作为长期事实与故障恢复来源。

内存治理原则：去重 key 必须设置 TTL，Redis 不设置无限增长的事件缓存；KeyDB
使用内存上限和监控告警保护主机，summary 字段保持有限规模。

## 104 节点试部署

已验证的第二节点通过环境外部配置注入公网地址，WireGuard 地址使用独立的
私有网段。双方在现有接口上增加了直连 peer，DHT UDP 端口为
`51413-51415`。

第二节点运行：

```text
dht-remote-collector.service
dht-remote-monitor-redis-bridge.service
```

第二节点通过 WireGuard 直接访问共享 PostgreSQL（`10.77.0.1:5432`）和本机
KeyDB（`10.78.0.166:6379`），不依赖 SSH 反向隧道。collector 使用独立的
`dht-remote-104` 数据库 ApplicationName 和节点 ID，便于观察连接与节点统计。

任务分发不依赖 Redis：所有 collector 连接同一个 `metadata_job` 表，使用
`FOR UPDATE SKIP LOCKED` 原子认领任务。这样多个节点可以并行处理同一队列，
同一个 info-hash 不会被两个 worker 同时执行。当前远端连接池限制为 1，原因是
现有 PostgreSQL 角色连接额度较小；扩容前应先提高数据库角色连接上限并重新规划
各服务 pool size。

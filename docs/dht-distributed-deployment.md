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

只有 `SET ... NX` 成功时才允许增加计数。Redis Stream 使用 Consumer Group，
处理成功后再 `XACK`；未确认消息保留在 pending list，可由其他 aggregator
接管。

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

所有节点使用不同的 `node_id`，共享同一个 Redis Stream 和聚合 Consumer
Group。dashboard 可以部署多个实例，但不能让每个实例重复消费并修改全局
计数；只有 aggregator 写 summary，dashboard 只读。

## 一致性和故障处理

- Redis Stream 消费采用至少一次语义，必须依赖幂等去重。
- aggregator 在计数成功后再 `XACK`；崩溃会导致重试，但不会重复计数。
- dashboard 断线不影响 DHT 采集，重连时重新读取 summary。
- Redis 不可用时，DHT 主服务继续采集并保留 journal；bridge 恢复后从可用
  的 journal 起点继续消费。
- summary 定期与 PostgreSQL 业务事实做校准，发现偏差时允许重建。
- Redis 不是 PostgreSQL 的替代品，不保存唯一的业务事实。

## 规模选择

- 几台到几十台节点：Redis Streams + aggregator + SSE。
- 数百台节点或每天数十亿事件：Kafka/Kafka Streams 或 ClickHouse 增量物化
  视图，再由 Redis/SSE 提供页面实时状态。
- 当前数据规模优先使用 Redis 方案，避免直接引入 Kafka、Pinot 或 Druid。

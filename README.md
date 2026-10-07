# Cluster Resource Dashboard

实时集群资源看板：从可本地复现的数据源接入资源快照与变更事件，对外提供
一致的资源列表视图与 SSE 实时订阅，不依赖真实集群或外部网络。

## 运行

```bash
uv sync
uv run python main.py          # http://127.0.0.1:8000
uv run pytest                  # 全部测试
```

## API

| 端点 | 说明 |
| --- | --- |
| `GET /health` | 健康检查，返回当前资源版本 |
| `GET /resources` | 列表，支持 `namespace` / `kind` 过滤、`sort_by`（name/namespace/kind/status/id）、`offset` / `limit` 分页；响应带 `total` 与 `version` |
| `GET /resources/{id}` | 单个资源；不可见资源返回 404（与不存在不可区分） |
| `GET /watch?since=<version>` | SSE 流。首条消息为 `sync`（全量对齐）或从 `since` 之后的增量事件 |

调用方授权范围通过请求头传入（真实部署中由认证层注入）：
`X-Namespaces: prod,staging`、`X-Kinds: Pod,Deployment`，缺省表示全部可见。

## 视图一致性语义

- **资源版本**：数据源为每个事件分配全局单调递增的 `seq`。快照与每次
  列表响应都携带版本号，`/watch?since=` 可从任意未过期版本续传。
- **幂等与乱序容忍**：每个资源只应用见过的最大 `seq`，重复与乱序事件
  被丢弃；删除留有墓碑（tombstone），旧事件无法复活已删资源。因此任意
  顺序、任意重复地应用同一事件日志，视图都收敛到与按序重放完全一致
  （有测试覆盖）。
- **list→watch 无断层**：`/watch` 在同一把锁内完成"注册订阅 + 计算
   backlog"，从列表拿到的版本切换到监听不会丢事件也不会重复。
- **版本过期**：事件缓冲区有界（`event_buffer_size`）。`since` 过期时
  不返回不一致的增量流，而是下发一条 `sync` 消息让客户端用全量快照
  重新对齐。
- **慢订阅者隔离**：每个订阅者有独立的有界队列（`max_queue_size`），
  发布是非阻塞的；队列打满的订阅者收到重同步标记并重新对齐，不影响
  其他订阅者，也不会反压数据源。
- **父子一致性**：有子资源的父资源状态始终派生自子资源的最差状态
  （failed > degraded > unknown > healthy），派生在每次变更内部同步
  完成，任意时刻读到的父子状态不自相矛盾。
- **越权不可旁路**：授权过滤发生在分页与聚合之前，`total`、分页边界、
  单资源查询与 watch 流都不会泄露被隐藏的资源。
- **可复现**：相同 seed 的数据源产生逐字节相同的快照与事件流；相同
  快照与相同查询参数下，列表结果逐条一致。

## 可配置项

| 参数 | 位置 | 默认 | 说明 |
| --- | --- | --- | --- |
| `seed` | `ReplayableSource` | 42 | 数据生成种子，相同 seed 结果完全相同 |
| `num_resources` / `num_events` | `ReplayableSource` | 50 / 200 | 快照规模与事件日志长度 |
| `retention` | `ReplayableSource` | 1000 | 源端事件保留窗口，模拟版本过期 |
| `max_queue_size` | `DashboardService` | 100 | 每个订阅者的队列容量，打满即重同步 |
| `event_buffer_size` | `DashboardService` | 1000 | 服务端事件缓冲，决定可续传的最旧版本 |
| `pump_interval` | `create_app` | None | 后台从数据源拉取事件的间隔（秒），None 表示手动 `pump()`（测试用） |

## 结构

```
app/
  models.py   # Resource / ResourceEvent / Scope / Snapshot 等类型
  source.py   # 确定性本地数据源（快照 + 有界事件日志）
  store.py    # 幂等、乱序容忍的资源视图，父状态派生
  authz.py    # 授权范围过滤
  service.py  # 列表 / 订阅编排，订阅者隔离，版本过期重对齐
  api.py      # FastAPI 路由（REST + SSE）
tests/        # 越权、乱序重复、版本过期、慢订阅者、重连等场景
```

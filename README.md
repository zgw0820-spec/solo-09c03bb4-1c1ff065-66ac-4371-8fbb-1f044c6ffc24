# 故障日志服务：事件入仓归并 + legacy_summary 列安全退役

FastAPI + PostgreSQL 16，双实例共享一个数据库。在原有“旧版检索/新版检索/旧列退役”
能力之上，新增**事件入仓 API**：值班排查故障风暴时，同一服务的相同错误在
60 秒窗口内归并为一条日志，并显示累计次数与末次时间。保证：

- 按 `service + severity + message` 归并，从首条事件起 60 秒内累计次数、刷新末次时间，
  超窗新建日志；窗口内摘要沿用首条；
- 同一 `event_id` 重试（内容一致）不重复计数；换内容复用 `event_id` 拒绝（409）；
- 双实例并发入仓由数据库事务级咨询锁串行化，同一窗口不会被拆成两条、计数不丢；
- 新旧检索都返回 `event_count` / `last_seen_at`，原有日志按一次显示；
- `legacy_summary` 退役期间入仓照常可用且继续写旧列，退役完成后不再写旧列；
  实例重启后入仓与窗口计数延续，已退役库的旧列**永不重建**；
- 退役受理后两个实例立即拒绝新发起的旧版检索（409，完成后 410）；
- 已接纳的旧查询读完后才删列（事务级咨询锁，跨实例生效）；
- 退役任何阶段新版检索与事件入仓都不受影响；
- 删列失败或实例重启时保留 `retiring` 待退役状态，旧版不能重新接单，重启后自动续跑；
- 重试与并发退役不误报完成、不重复删列（状态机 + 行锁 + `DROP COLUMN IF EXISTS` 与置位同事务）。

## 事件入仓与窗口归并

```
POST /api/events   {event_id, severity, service, message, summary}
        │
        ├─ event_id 已登记，内容一致 ──────────────► 200 deduplicated=true（计数不变）
        ├─ event_id 已登记，内容不同 ──────────────► 409（不写入）
        └─ 新 event_id
              ├─ 同 service/severity/message 存在首条事件 < 60s 的日志
              │      └─► 201：event_count+1、last_seen_at=本次时间、摘要保持首条
              └─ 否则 ─► 201：新建日志（count=1，summary=本次摘要，窗口重新计时）
```

- **窗口边界**：`created_at > 本次事件时刻 - 60s`，时间取数据库 `clock_timestamp()`，
  两个实例共享同一时钟；恰好 60 秒的事件视为超窗，新建日志。
- **并发安全**：先按 `event_id` 取事务级咨询锁（去重判定），再按归并键取锁
  （窗口判定 + 计数更新），最后与去重登记同事务提交。锁 key 使用
  `(classid, hashtext(text))` 两参数形式，与退役任务的单参数 key 分属不同命名空间。
- **幂等/去重**：`event_ingestions(event_id PRIMARY KEY, content_hash, fault_log_id)`；
  `content_hash` 覆盖 severity/service/message/summary 的全部内容。同事务提交意味着
  崩溃时“计数 + 去重登记”一起回滚，重试只会产生一次计数。
- **旧列写入**：`retirement_state.status != 'retired'` 时，新建窗口的日志会写
  `legacy_summary`（由事件生成的 `LEGACY|级别|服务|事件号|摘要` 文本）；归并更新
  不触碰旧列。响应里的 `used_legacy_column` 如实表示本次请求是否写入了旧列。
  若入仓事务读到 `retiring` 后旧列恰好被并发删除，捕获 `undefined_column` 后
  自动降级重试一次（不写旧列），不失败、不重复计数。

## 退役协调协议

```
                 POST /api/retirement
   active  ───────────────────────►  retiring  ──(在途旧查询全部读完)──►  retired
                                      ▲   │                                  │
                                      │   ├─ 删列失败：记录 last_error，      │ 旧版 410
   旧版 200                            │   │   保持 retiring，可重试          │ 新版 200
   新版 200                            │   └─ 实例重启：启动时发现 retiring    │ 入仓 201
                                      │       自动续跑退役任务                │
                                      └──── 重试/并发调用（幂等）
```

1. **`retirement_state` 单行表**（两实例共享）：`active → retiring → retired`。
   受理退役时**先提交 `retiring`** 再启动后台删列任务，因此两个实例的旧版检索
   都能立即看到状态并拒单（快速预检 + 锁内权威复检双重检查，消除竞态）。
2. **PostgreSQL 事务级咨询锁**（key 为常量，两实例相同）：
   - 旧版检索：事务内 `pg_advisory_xact_lock_shared(KEY)` → 复检状态 → 读 `legacy_summary`；
   - 退役任务：事务内 `pg_advisory_xact_lock(KEY)`（排他）→ 等所有在途旧查询读完
     → `ALTER TABLE fault_logs DROP COLUMN IF EXISTS legacy_summary`
     → 同事务把状态置为 `retired`。
   PG 锁队列的公平性保证排他请求排队后，新共享锁请求不会插队。
3. **删列与置位同一事务**：崩溃则整体回滚；`IF EXISTS` + 条件 `UPDATE` 使重试、
   并发调用安全，不会重复删列，也不会把未完成误报为完成（响应永远反映库中真实状态）。
4. **入仓与退役互不阻塞**：入仓只读写 `fault_logs` 的 `summary`/`event_count`/
   `last_seen_at`（新建窗口在旧列未删时额外写 `legacy_summary`），不取退役的排他锁；
   旧列删除瞬间的竞态由上述降级重试兜底。

## 目录结构

```
app/main.py          FastAPI 应用：检索/入仓/退役/状态 API、启动时续跑与兼容迁移
app/ingest.py        事件入仓：窗口归并、去重、事件号复用拒绝、并发锁
app/schema.py        启动时幂等 schema 兼容迁移（绝不重建已退役旧列）
app/retirement.py    退役后台任务（咨询锁 + 删列 + 状态置位）
db/init/01_init.sql  建表、预置 14 条故障日志、初始化状态机与去重表（可重入）
docker-compose.yml   postgres:16 + app1(:8001) + app2(:8002)
Dockerfile           应用镜像
scripts/e2e.py       端到端场景库（可对 compose 起的服务运行）
scripts/local_e2e.py 本地编排器：内嵌 PG16 + 双进程实例，含崩溃重启场景
```

## 快速开始（Docker Compose）

```bash
docker compose up --build -d
```

- app1 Swagger: http://localhost:8001/docs
- app2 Swagger: http://localhost:8002/docs
- 数据库已预置 14 条故障日志（`summary` 与 `legacy_summary` 两列均有数据）。

> 本地进程方式（非 compose）同样有 Swagger：`INSTANCE_ID=app1 DATABASE_URL=... \
> uvicorn app.main:app --port 8001` → http://127.0.0.1:8001/docs。

## Swagger 操作手册

**0. 事件入仓与窗口归并**：`POST /api/events`（任一实例），请求体：
```json
{"event_id": "evt-1001", "severity": "ERROR", "service": "checkout-api",
 "message": "payment gateway timeout", "summary": "支付网关超时，切换到备用通道"}
```
- 首次 → **201**、`event_count=1`；60 秒内换一个 `event_id` 发同样
  `severity/service/message`（摘要可不同）→ **201**、`event_count=2`，摘要仍是首条；
- 用**同一** `event_id` 重发 → **200**、`deduplicated=true`，计数不变；
- 同一 `event_id` 改任何内容 → **409**，库中数据不变；
- 对 app1、app2 交替并发发同一组事件 → 只有一条窗口日志、`event_count` 等于成功次数；
- `GET /api/v2/fault-logs` 每行附 `event_count`、`last_seen_at`（预置历史日志为 1 次、
  末次时间等于首条时间）。

**1. 正常检索**：对 app1、app2 分别执行 `GET /api/v1/legacy/fault-logs`
（返回 `legacy_summary`）和 `GET /api/v2/fault-logs`（返回 `summary`、`event_count`、
`last_seen_at`）。

**2. 长查询与退役交错**：
- app1：`GET /api/v1/legacy/fault-logs?slow_ms=8000`（`slow_ms` 为演示参数，模拟长查询）；
- 查询在途时，app2：`POST /api/retirement` → 返回 **202 / status=retiring**；
- 立即对两个实例再发旧版检索 → **409**；新版检索 → **200**；
- `GET /api/retirement` 观察：长查询读完前 `legacy_column_present=true`，
  读完后状态变为 `retired`、`legacy_column_present=false`；
- 此后旧版检索 → **410**。

**3. 删列失败与重试**（演示钩子，compose 中已开启 `ENABLE_TEST_HOOKS=true`）：
- app1：`POST /api/test-hooks/fail-next-drops?times=1`；
- app1：`POST /api/retirement` → 202；
- `GET /api/retirement` → `status=retiring`、`failed_attempts=1`、`last_error` 有记录，
  旧列仍在，两实例旧版均 409；
- app2：`POST /api/retirement`（重试）→ 稍候 `GET /api/retirement` → `retired`。

**4. 退役途中实例重启**：
- app2：`GET /api/v1/legacy/fault-logs?slow_ms=9000`（保持在途）；
- app1：`POST /api/retirement` → 202 后立即 `docker compose restart app1`；
- 期间 app2：旧版 409、新版 200、`GET /api/retirement` 保持 `retiring`；
- app1 重启完成后自动续跑：长查询读完 → 状态变 `retired`。

**5. 并发退役**：同时（两个浏览器标签/脚本）对 app1、app2 发 `POST /api/retirement`
→ 两者都返回 202 且 `status=retiring`、同一 `request_id`，不会重复删列、不误报完成。

**6. 退役期间的入仓**：
- retiring 期间 `POST /api/events` 仍返回 201；新建窗口的响应 `used_legacy_column=true`
  （旧列未删，继续写）；归并更新的响应 `used_legacy_column=false`（不触碰旧列）；
- 状态变 `retired` 后再入仓 → 201 且 `used_legacy_column=false`，计数延续；
- `docker compose restart app1` 后重复投递同一 `event_id` → 200 去重，原窗口计数不变。

## API 一览

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/events` | 事件入仓（按服务/级别/消息归并，60 秒窗口）；201 新计入、200 重试去重、409 事件号复用 |
| `GET /api/v1/legacy/fault-logs` | 旧版检索，读 `legacy_summary`；retiring→409，retired→410 |
| `GET /api/v2/fault-logs` | 新版检索，读 `summary`，含 `event_count`/`last_seen_at`；退役各阶段均可用 |
| `POST /api/retirement` | 受理退役（幂等/可重试）；202=进行中，200=已完成 |
| `GET /api/retirement` | 退役状态（含 `failed_attempts`、`last_error`、`legacy_column_present`） |
| `POST /api/test-hooks/fail-next-drops` | [仅测试] 模拟删列失败，需 `ENABLE_TEST_HOOKS=true` |
| `GET /healthz` | 健康检查 |

## 自测

无 Docker 的本机环境（内嵌 PostgreSQL 16.2 + 两个 uvicorn 进程）：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python scripts/local_e2e.py     # 9 个场景、115 条断言
```

对 compose 起的服务运行（重启场景除外）：

```bash
python3 scripts/e2e.py --app1 http://localhost:8001 --app2 http://localhost:8002 \
    --pg-dsn postgresql://ops:ops_password@localhost:5432/faultdb
```

场景覆盖：① 双实例基线检索；② 删列失败→保留 retiring 拒单→跨实例重试成功；
③ 长查询交错 + 双实例并发退役；④ 入仓窗口归并/摘要沿用首条/重试去重/事件号复用 409；
⑤ 超 60 秒开新窗口；⑥ 双实例并发入仓窗口不分裂、计数不丢、同 event_id 并发重试只计一次；
⑦ 退役前/retiring/退役后入仓的旧列写入行为；⑧ 旧列在入仓瞬间被并发删除的降级重试；
⑨ 退役途中 kill -9 实例→状态保留→重启自动续跑，重启前后入仓/去重/计数延续。

## 验证状态（据实）

- **已验证**（真实 PostgreSQL 16.2，双实例共享库，115/115 断言通过）：上述全部场景，
  含窗口归并/超窗新日志、跨实例并发入仓不分裂、重试去重与事件号复用拒绝、
  retiring 与 retired 阶段的旧列写入行为、旧列并发删除降级、
  长查询读完才删列、失败保留 retiring、重启自动续跑、并发/重试幂等、
  Swagger `/docs` 与 `/openapi.json` 可用（含 `/api/events`）、测试钩子默认关闭（404）。
- **额外验证**：用旧版 schema（无 `event_count`/`last_seen_at`/`event_ingestions`）和
  已退役 schema（无 `legacy_summary`）分别启动应用，启动迁移幂等且不重建旧列。
- **未在本环境执行**：`docker compose up` 本身（本环境无 Docker）。compose 文件已做
  YAML 静态校验；应用逻辑在与 compose 相同的主版本（PG 16）上完整验证过。
  首次在有 Docker 的环境启动时如遇到问题，请优先检查 `db` 健康检查与端口占用。

## 设计取舍说明

- `slow_ms` 参数与 fail-next-drops 钩子是为演示/自测长查询与失败路径而设，
  后者默认关闭；生产部署请移除或保持关闭。
- 退役为单向操作，不提供"回退到 active"的 API（列删除不可逆，状态机如实反映）。
- 删列瞬间 `ALTER TABLE` 需短暂获取表级 ACCESS EXCLUSIVE 锁；因在途旧查询已被
  咨询锁排空、新版查询都很短，实际阻塞窗口极小。
- 窗口归并的**起点是该归并键的首条事件**（固定 60 秒），不是滚动窗口；这是为了
  让“故障风暴”的次数按固定周期可比较，也避免持续刷新的窗口永不封口。
- 归并键包含 `message` 原始文本：同一根因若消息里带动态变量，会被视为不同错误。
  需要更强归并时应在调用方规范化 `message`，或后续增加显式指纹字段。
- 窗口判定依赖 `fault_logs.(service, severity, message, created_at)` 索引；预置的
  历史日志如果消息恰好与某事件相同且落在窗口内，首次入仓会并入该历史日志
  （这是“同一服务相同错误”的一致行为，但会让那条历史日志的次数变为 2）。
- `event_ingestions` 随业务量单调增长；当前未设 TTL/清理任务，长期运行需按保留策略归档。
- `legacy_summary` 退役完成后，入仓只能写 `summary`；新旧检索都返回
  `event_count`/`last_seen_at`，因此退役不影响故障风暴的排查能力。

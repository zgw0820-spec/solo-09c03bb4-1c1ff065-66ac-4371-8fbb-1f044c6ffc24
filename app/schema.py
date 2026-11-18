"""启动时的幂等 schema 兼容迁移。

`db/init/01_init.sql` 只在 PostgreSQL 数据卷首次初始化时执行。已有部署升级到
本版本、或实例重启时，需要在启动阶段补齐事件入仓所需的列/表：

- 只做“增补”，**绝不重建 legacy_summary**——退役完成后旧列必须保持删除状态；
- 全部语句幂等（IF NOT EXISTS / 条件回填），两个实例同时启动也安全；
- 预置/历史日志没有入仓事件：``event_count`` 默认 1，``last_seen_at`` 回填为首条时间。
"""
from __future__ import annotations

import logging

import asyncpg

log = logging.getLogger("schema")

# 与入仓/退役的咨询锁分属不同命名空间（classid 固定，objid 区分）。
_SCHEMA_LOCK_CLASS = 2
_SCHEMA_LOCK_KEY = 202_611_802

_ADD_EVENT_COUNT = """
ALTER TABLE fault_logs
    ADD COLUMN IF NOT EXISTS event_count integer NOT NULL DEFAULT 1
"""

_ADD_LAST_SEEN = """
ALTER TABLE fault_logs
    ADD COLUMN IF NOT EXISTS last_seen_at timestamptz
"""

_BACKFILL_LAST_SEEN = """
UPDATE fault_logs
   SET last_seen_at = created_at
 WHERE last_seen_at IS NULL
"""

_TIGHTEN_LAST_SEEN = """
ALTER TABLE fault_logs
    ALTER COLUMN last_seen_at SET DEFAULT now(),
    ALTER COLUMN last_seen_at SET NOT NULL
"""

_CREATE_WINDOW_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fault_logs_window
    ON fault_logs (service, severity, message, created_at DESC)
"""

_CREATE_EVENT_INGESTIONS = """
CREATE TABLE IF NOT EXISTS event_ingestions (
    event_id     text PRIMARY KEY,
    content_hash text NOT NULL,
    fault_log_id bigint NOT NULL REFERENCES fault_logs (id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now()
)
"""


async def ensure_ingest_schema(conn: asyncpg.Connection) -> None:
    """在单条连接上执行兼容迁移。

    - 用咨询锁串行化**多实例同时启动**的 DDL（避免 `tuple concurrently updated`）；
    - 事务包裹：迁移要么整体生效，要么整体回滚，失败时下次启动重试；
    - 先查系统目录，只在确实缺失/不满足时执行对应 DDL，避免每次重启都对大表
      拿 ACCESS EXCLUSIVE 锁做重复的 SET NOT NULL。
    """
    exists = await conn.fetchval("SELECT to_regclass('public.fault_logs') IS NOT NULL")
    if not exists:
        # 全新数据库：由 db/init/01_init.sql 建表（compose 中先于服务启动执行）。
        log.warning("fault_logs not found; skipping ingest schema migration")
        return
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1, $2)", _SCHEMA_LOCK_CLASS, _SCHEMA_LOCK_KEY
        )
        row = await conn.fetchrow(
            """
            SELECT count(*) FILTER (WHERE column_name = 'event_count')    AS has_count,
                   count(*) FILTER (WHERE column_name = 'last_seen_at')   AS has_seen,
                   bool_or(column_name = 'last_seen_at' AND is_nullable = 'NO') AS seen_not_null
              FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = 'fault_logs'
            """
        )
        if not row["has_count"]:
            await conn.execute(_ADD_EVENT_COUNT)
        if not row["has_seen"]:
            await conn.execute(_ADD_LAST_SEEN)
        # 有新列（或历史 NULL）时回填；回填完才收紧 NOT NULL。
        backfilled = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM fault_logs WHERE last_seen_at IS NULL)"
        )
        if backfilled:
            await conn.execute(_BACKFILL_LAST_SEEN)
        if not row["seen_not_null"]:
            await conn.execute(_TIGHTEN_LAST_SEEN)
        await conn.execute(_CREATE_WINDOW_INDEX)
        await conn.execute(_CREATE_EVENT_INGESTIONS)
    log.info("ingest schema ensured")

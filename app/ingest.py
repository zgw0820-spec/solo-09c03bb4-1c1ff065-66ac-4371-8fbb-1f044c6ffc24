"""事件入仓：按 (service, severity, message) 归并到 60 秒窗口内的一条故障日志。

归并规则
--------
- 同一 ``service + severity + message`` 在**首条事件发生后 60 秒内**的事件，
  累加到同一条 ``fault_logs`` 记录：``event_count`` 加一，``last_seen_at`` 更新；
- 超过窗口（事件时刻 >= 首条事件 + 60 秒）则新建一条日志，从该事件重新计时；
- ``summary`` 永远保留窗口内首条事件的摘要，后续事件不覆盖；
- 同一 ``event_id`` 的重复投递（内容一致）不增加计数，直接返回原日志；
- 同一 ``event_id`` 携带不同内容（severity/service/message/summary 任一不同）
  视为事件号被复用，拒绝并返回 409。

并发与事务
----------
- 以 ``event_id`` 和归并键分别取 PostgreSQL 事务级咨询锁，保证两个实例
  （以及同一实例的多个并发请求）不会同时创建两条窗口日志，也不会重复计数；
- 去重表 ``event_ingestions`` 与日志计数在同一事务中提交：实例崩溃时二者一起
  回滚，重试只会产生一次计数；
- ``legacy_summary`` 退役期间（``status != 'retired'``）继续写旧列；退役完成
  后不写。若落库瞬间旧列恰好被删（事务状态尚未读到 retired），捕获
  ``undefined_column`` 后重试一次并改写“无旧列”版本。

窗口边界使用数据库的 ``clock_timestamp()``，两个实例共享同一时钟。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any

import asyncpg

log = logging.getLogger("ingest")

WINDOW_SECONDS = 60
# 咨询锁使用 (classid, objid) 两参数形式。PostgreSQL 的两参数锁空间与退役任务用的
# 单参数 64 位锁空间互不重叠；入仓内部再用 classid 区分事件号锁与归并键锁。
_LOCK_CLASS = 1
_EVENT_LOCK_NAMESPACE = "faultlog:event:"
_GROUP_LOCK_NAMESPACE = "faultlog:group:"


class EventContentConflict(Exception):
    """event_id 已被另一份内容占用。"""


@dataclass(frozen=True)
class EventInput:
    event_id: str
    severity: str
    service: str
    message: str
    summary: str

    def content_hash(self) -> str:
        # 规范化后的全部业务内容共同构成“同一份内容”的判据。
        canonical = json.dumps(
            [self.severity, self.service, self.message, self.summary],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def legacy_summary(self) -> str:
        # 旧列是给旧版检索读取的管道分隔文本；入仓事件没有历史解析结果，
        # 用事件本身生成一条确定、可追溯的旧格式摘要。
        return (
            f"LEGACY|{self.severity}|{self.service}|"
            f"{self.event_id}|{self.summary}"
        )


def _group_key(event: EventInput) -> str:
    return json.dumps(
        [event.service, event.severity, event.message],
        ensure_ascii=False,
        separators=(",", ":"),
    )


async def ingest_event(pool: asyncpg.Pool, event: EventInput) -> dict[str, Any]:
    """在独立事务中入仓一条事件；失败（含并发删列竞态）时安全重试一次。

    返回响应字段（含 ``deduplicated`` / ``used_legacy_column``）。
    若 event_id 内容不一致，抛 ``EventContentConflict``，数据库无任何改动。
    """
    content_hash = event.content_hash()
    # 第一次按“旧列可能还在”处理；若恰好与 DROP COLUMN 竞态，第二次强制不写旧列。
    force_without_legacy = False
    last_error: asyncpg.PostgresError | None = None

    for attempt in range(2):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await _ingest_tx(
                        conn,
                        event,
                        content_hash,
                        force_without_legacy=force_without_legacy,
                    )
        except asyncpg.PostgresError as exc:
            # 42703 = undefined_column：读到 retiring 后旧列被并发删掉。
            if exc.sqlstate == "42703" and not force_without_legacy:
                last_error = exc
                force_without_legacy = True
                log.warning("legacy_summary dropped during ingest; retrying without it")
                await asyncio.sleep(0)  # 让事件循环有机会调度其他请求
                continue
            raise
    raise last_error  # pragma: no cover - 仅当重试仍失败时


async def _ingest_tx(
    conn: asyncpg.Connection,
    event: EventInput,
    content_hash: str,
    *,
    force_without_legacy: bool,
) -> dict[str, Any]:
    # 1) 事件号级锁：同一 event_id 的并发投递串行化，先做去重判定。
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        _LOCK_CLASS,
        _EVENT_LOCK_NAMESPACE + event.event_id,
    )

    existing = await conn.fetchrow(
        """
        SELECT content_hash, fault_log_id
          FROM event_ingestions
         WHERE event_id = $1
        """,
        event.event_id,
    )
    if existing is not None:
        if existing["content_hash"] != content_hash:
            raise EventContentConflict(event.event_id)
        row = await conn.fetchrow(
            """
            SELECT id, created_at, severity, service, message, summary,
                   event_count, last_seen_at
              FROM fault_logs
             WHERE id = $1
            """,
            existing["fault_log_id"],
        )
        if row is None:  # 理论不可达：外键保证日志存在
            raise RuntimeError("event registry points to a missing fault log")
        return _payload(event, row, deduplicated=True, used_legacy_column=False)

    # 2) 归并键级锁：同一服务/级别/消息的并发事件串行化，避免分裂窗口。
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        _LOCK_CLASS,
        _GROUP_LOCK_NAMESPACE + _group_key(event),
    )

    event_time = await conn.fetchval("SELECT clock_timestamp()")
    status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
    use_legacy = (status != "retired") and not force_without_legacy

    # 3) 找当前仍开放的窗口：首条事件发生在最近 60 秒内（含同一组的旧日志）。
    open_row = await conn.fetchrow(
        """
        SELECT id
          FROM fault_logs
         WHERE service = $1
           AND severity = $2
           AND message = $3
           AND created_at > $4::timestamptz - ($5::int * interval '1 second')
         ORDER BY created_at DESC, id DESC
         LIMIT 1
        """,
        event.service,
        event.severity,
        event.message,
        event_time,
        WINDOW_SECONDS,
    )

    wrote_legacy = False
    if open_row is not None:
        # 归并到既有日志：只更新次数/末次时间，不触碰 legacy_summary。
        row = await conn.fetchrow(
            """
            UPDATE fault_logs
               SET event_count = event_count + 1,
                   last_seen_at = GREATEST(last_seen_at, $2::timestamptz)
             WHERE id = $1
            RETURNING id, created_at, severity, service, message, summary,
                      event_count, last_seen_at
            """,
            open_row["id"],
            event_time,
        )
    elif use_legacy:
        row = await conn.fetchrow(
            """
            INSERT INTO fault_logs
                (created_at, severity, service, message, summary,
                 legacy_summary, event_count, last_seen_at)
            VALUES ($1, $2, $3, $4, $5, $6, 1, $1)
            RETURNING id, created_at, severity, service, message, summary,
                      event_count, last_seen_at
            """,
            event_time,
            event.severity,
            event.service,
            event.message,
            event.summary,
            event.legacy_summary(),
        )
        wrote_legacy = True
    else:
        row = await conn.fetchrow(
            """
            INSERT INTO fault_logs
                (created_at, severity, service, message, summary,
                 event_count, last_seen_at)
            VALUES ($1, $2, $3, $4, $5, 1, $1)
            RETURNING id, created_at, severity, service, message, summary,
                      event_count, last_seen_at
            """,
            event_time,
            event.severity,
            event.service,
            event.message,
            event.summary,
        )

    # 4) 去重记录与计数同事务提交；重复投递以后只读这里，不再加计数。
    await conn.execute(
        """
        INSERT INTO event_ingestions (event_id, content_hash, fault_log_id)
        VALUES ($1, $2, $3)
        """,
        event.event_id,
        content_hash,
        row["id"],
    )
    # used_legacy_column 如实反映本次请求是否写入旧列：归并更新从不写它。
    return _payload(event, row, deduplicated=False, used_legacy_column=wrote_legacy)


def _payload(
    event: EventInput,
    row: asyncpg.Record,
    *,
    deduplicated: bool,
    used_legacy_column: bool,
) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "fault_log_id": row["id"],
        "created_at": row["created_at"],
        "last_seen_at": row["last_seen_at"],
        "event_count": row["event_count"],
        "severity": row["severity"],
        "service": row["service"],
        "message": row["message"],
        "summary": row["summary"],
        "deduplicated": deduplicated,
        "used_legacy_column": used_legacy_column,
    }

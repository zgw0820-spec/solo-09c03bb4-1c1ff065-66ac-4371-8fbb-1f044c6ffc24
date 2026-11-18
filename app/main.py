"""故障日志服务：事件入仓归并 + 旧版/新版检索 + legacy_summary 列退役协调。

部署形态：两个本服务实例（app1/app2）共享同一个 PostgreSQL 16 数据库。
"""
from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Literal

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .ingest import EventContentConflict, EventInput, WINDOW_SECONDS, ingest_event
from .retirement import ADVISORY_LOCK_KEY, ensure_retirement_worker
from .schema import ensure_ingest_schema

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ops:ops_password@localhost:5432/faultdb"
)
INSTANCE_ID = os.environ.get("INSTANCE_ID", "local")
ENABLE_TEST_HOOKS = os.environ.get("ENABLE_TEST_HOOKS", "").lower() in {"1", "true", "yes"}
MAX_SLOW_MS = 120_000  # slow_ms 演示参数上限


class EventIn(BaseModel):
    """入仓事件：event_id + 级别 + 服务 + 消息 + 摘要。"""

    event_id: str = Field(..., min_length=1, max_length=200, description="事件号，全局唯一；重试需完全一致")
    severity: Literal["INFO", "WARN", "ERROR", "CRITICAL"] = Field(..., description="级别")
    service: str = Field(..., min_length=1, max_length=200, description="服务名")
    message: str = Field(..., min_length=1, description="原始错误消息")
    summary: str = Field(..., min_length=1, description="摘要；仅窗口内首条事件的摘要会展示")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
    app.state.fail_drops_remaining = 0  # 测试钩子计数器（仅 ENABLE_TEST_HOOKS 时可被武装）
    async with app.state.pool.acquire() as conn:
        # 已有库升级/实例重启时补齐入仓列与去重表；绝不重建已退役的旧列。
        await ensure_ingest_schema(conn)
    # 退役途中实例重启：发现库里仍是 retiring，自动续跑退役任务；
    # 重启期间及之后旧版检索依然被拒绝（状态在共享库中）。
    async with app.state.pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
    if status == "retiring":
        await ensure_retirement_worker(app)
    yield
    await app.state.pool.close()


app = FastAPI(
    title="Fault Log Service",
    version="1.1.0",
    description=(
        "故障日志服务：`POST /api/events` 事件入仓（按服务/级别/消息在 60 秒窗口内归并）；"
        "旧版检索读 `legacy_summary`，新版检索读 `summary`；"
        "`POST /api/retirement` 受理 legacy_summary 列退役，两个实例通过共享数据库协调。"
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------- helpers

def _reject_if_closed(status: str) -> None:
    """退役受理后旧版检索立即拒单（retiring=409 临时关闭，retired=410 永久关闭）。"""
    if status == "retiring":
        raise HTTPException(
            status_code=409,
            detail="legacy_summary 退役已受理，旧版检索不再接受新请求（新版 GET /api/v2/fault-logs 仍可用）",
        )
    if status == "retired":
        raise HTTPException(
            status_code=410,
            detail="legacy_summary 已退役删除，旧版检索永久关闭（请使用 GET /api/v2/fault-logs）",
        )


async def _state_payload(conn) -> dict[str, Any]:
    row = await conn.fetchrow("SELECT * FROM retirement_state WHERE id = 1")
    col_present = await conn.fetchval(
        """SELECT EXISTS (
                 SELECT 1 FROM information_schema.columns
                  WHERE table_schema = current_schema()
                    AND table_name = 'fault_logs'
                    AND column_name = 'legacy_summary')"""
    )
    return {
        "status": row["status"],
        "request_id": str(row["request_id"]) if row["request_id"] else None,
        "requested_at": row["requested_at"],
        "updated_at": row["updated_at"],
        "failed_attempts": row["failed_attempts"],
        "last_error": row["last_error"],
        "legacy_column_present": col_present,
        "served_by": INSTANCE_ID,
    }


# ---------------------------------------------------------------- endpoints

@app.get("/healthz", tags=["meta"], summary="健康检查")
async def healthz():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/api/v1/legacy/fault-logs", tags=["legacy"], summary="旧版检索（读 legacy_summary）")
async def legacy_search(
    request: Request,
    severity: str | None = Query(None, description="按严重级别过滤：INFO/WARN/ERROR/CRITICAL"),
    limit: int = Query(50, ge=1, le=500),
    slow_ms: int = Query(
        0, ge=0, le=MAX_SLOW_MS,
        description="演示/自测用：让本次读取慢 slow_ms 毫秒，模拟长查询",
    ),
):
    """读取 `legacy_summary` 列。退役受理（retiring）后返回 409，退役完成（retired）后返回 410。

    已接纳的请求受共享咨询锁保护：退役任务会等它们读完才删列。
    """
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        # 快速预检：绝大多数情况下立即拒绝，不必排队等锁。
        status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
        _reject_if_closed(status)
        async with conn.transaction():
            # 共享咨询锁：与退役任务的排他锁互斥；锁内权威复检状态，消除竞态。
            await conn.execute("SELECT pg_advisory_xact_lock_shared($1)", ADVISORY_LOCK_KEY)
            status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
            _reject_if_closed(status)
            if slow_ms:
                await conn.execute("SELECT pg_sleep($1)", slow_ms / 1000.0)
            rows = await conn.fetch(
                """
                SELECT id, created_at, severity, service, message, legacy_summary,
                       event_count, last_seen_at
                  FROM fault_logs
                 WHERE ($1::text IS NULL OR severity = $1)
                 ORDER BY id
                 LIMIT $2
                """,
                severity,
                limit,
            )
    return {
        "instance": INSTANCE_ID,
        "source_column": "legacy_summary",
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }


@app.get("/api/v2/fault-logs", tags=["v2"], summary="新版检索（读 summary，含归并次数与末次时间）")
async def v2_search(
    request: Request,
    severity: str | None = Query(None, description="按严重级别过滤：INFO/WARN/ERROR/CRITICAL"),
    limit: int = Query(50, ge=1, le=500),
):
    """读取 `summary` 列。退役的任何阶段都可用。

    每行附带 `event_count`（60 秒窗口内累计次数）与 `last_seen_at`（末次事件时间）；
    预置的历史日志没有入仓事件，按一次显示（`event_count=1`，`last_seen_at=created_at`）。
    """
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, created_at, severity, service, message, summary,
                   event_count, last_seen_at
              FROM fault_logs
             WHERE ($1::text IS NULL OR severity = $1)
             ORDER BY id
             LIMIT $2
            """,
            severity,
            limit,
        )
    return {
        "instance": INSTANCE_ID,
        "source_column": "summary",
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }


@app.post(
    "/api/events",
    tags=["ingest"],
    summary="事件入仓（按服务/级别/消息在 60 秒窗口内归并）",
    status_code=201,
    responses={
        200: {"description": "同一 event_id 的重复投递：不重复计数，返回原日志"},
        201: {"description": "已计入窗口：新建日志或累加次数"},
        409: {"description": "event_id 已存在但内容不同（事件号复用被拒绝）"},
    },
)
async def create_event(event: EventIn, request: Request):
    """接收一条事件并归并到 60 秒窗口。

    - 首次出现的 `service + severity + message` 组合从该事件起计时 60 秒；
    - 窗口内同组合事件累加 `event_count`、刷新 `last_seen_at`，摘要沿用首条；
    - 超窗新建一条日志；
    - `event_id` 重试（内容一致）幂等：返回 200、`deduplicated=true`，计数不变；
    - `event_id` 换内容复用：409，不写入任何数据；
    - 两个实例并发入仓由数据库咨询锁串行化，同一窗口不会被拆成两条。
    """
    pool = request.app.state.pool
    try:
        payload = await ingest_event(
            pool,
            EventInput(
                event_id=event.event_id,
                severity=event.severity,
                service=event.service,
                message=event.message,
                summary=event.summary,
            ),
        )
    except EventContentConflict:
        raise HTTPException(
            status_code=409,
            detail=f"event_id {event.event_id!r} 已用于另一份内容，拒绝复用事件号",
        )
    payload["instance"] = INSTANCE_ID
    payload["window_seconds"] = WINDOW_SECONDS
    return JSONResponse(
        jsonable_encoder(payload),
        status_code=200 if payload["deduplicated"] else 201,
    )


@app.post(
    "/api/retirement",
    tags=["retirement"],
    summary="受理 legacy_summary 列退役（幂等，可重试）",
    responses={
        202: {"description": "退役已受理/正在进行（status=retiring）"},
        200: {"description": "退役此前已完成（status=retired）"},
    },
)
async def request_retirement(request: Request):
    """把状态机推进到 `retiring` 并确保本实例有后台任务在执行删列。

    - 并发调用：数据库行锁串行化，只有第一个请求做状态迁移，其余看到既有状态；
    - 重试：已处于 retiring 时再次调用会（在本实例）补起一个后台任务，不会重复删列，
      也不会把未完成误报为完成——响应始终反映数据库里的真实状态。
    """
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT status FROM retirement_state WHERE id = 1 FOR UPDATE"
            )
            if row["status"] == "active":
                await conn.execute(
                    """
                    UPDATE retirement_state
                       SET status = 'retiring', request_id = $1,
                           requested_at = now(), updated_at = now(), last_error = NULL
                     WHERE id = 1
                    """,
                    uuid.uuid4(),
                )
        payload = await _state_payload(conn)
    if payload["status"] == "retiring":
        await ensure_retirement_worker(request.app)
        return JSONResponse(jsonable_encoder(payload), status_code=202)
    return JSONResponse(jsonable_encoder(payload), status_code=200)


@app.get("/api/retirement", tags=["retirement"], summary="查询退役状态")
async def get_retirement(request: Request):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await _state_payload(conn)


# ---------------------------------------------------------------- test hooks

if ENABLE_TEST_HOOKS:

    @app.post(
        "/api/test-hooks/fail-next-drops",
        tags=["test-hooks"],
        summary="[仅测试] 让本实例接下来 times 次删列尝试失败",
    )
    async def fail_next_drops(request: Request, times: int = Query(1, ge=1, le=10)):
        """用于演示/自测“删列失败保留 retiring 状态、可重试”的路径。仅在
        环境变量 ENABLE_TEST_HOOKS=true 时注册。"""
        request.app.state.fail_drops_remaining = times
        return {"armed_failures": times, "instance": INSTANCE_ID}

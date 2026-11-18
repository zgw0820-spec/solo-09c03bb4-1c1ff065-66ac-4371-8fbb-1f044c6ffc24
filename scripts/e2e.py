#!/usr/bin/env python3
"""端到端自测场景库：对两个运行中的服务实例执行（compose 或本地进程均可）。

用法：
    python3 scripts/e2e.py --app1 http://localhost:8001 --app2 http://localhost:8002 \
        --pg-dsn postgresql://ops:ops_password@localhost:5432/faultdb

--pg-dsn 用于在场景之间重置数据库（重新执行 db/init/01_init.sql）。
被 local_e2e.py 导入复用，也可独立对 docker compose 起的服务运行。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
import time

import asyncpg
import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INIT_SQL = os.path.join(ROOT, "db", "init", "01_init.sql")

TIMEOUT = httpx.Timeout(30.0, connect=5.0)

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


class Check:
    """断言收集器：记录每条断言，最后汇总，失败即非零退出。"""

    def __init__(self) -> None:
        self.failures: list[str] = []
        self.count = 0

    def __call__(self, cond: bool, label: str, extra: str = "") -> None:
        self.count += 1
        if cond:
            print(f"  [{PASS}] {label}")
        else:
            msg = f"{label}  {extra}"
            self.failures.append(msg)
            print(f"  [{FAIL}] {label}  {extra}")


def get(url: str, **kw) -> httpx.Response:
    return httpx.get(url, timeout=TIMEOUT, **kw)


def post(url: str, **kw) -> httpx.Response:
    return httpx.post(url, timeout=TIMEOUT, **kw)


def wait_until(fn, timeout: float, interval: float = 0.4, desc: str = "") -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  .. wait_until 超时: {desc}")
    return False


def reset_db(dsn: str) -> None:
    """重新执行初始化 SQL，把库恢复到 active 初始状态。"""
    with open(INIT_SQL, encoding="utf-8") as f:
        sql = f.read()

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(sql)  # 无参数 => 简单查询协议，可多语句
        finally:
            await conn.close()

    asyncio.run(_run())


def retirement_state(base: str) -> dict:
    return get(f"{base}/api/retirement").json()


# ---------------------------------------------------------------- scenarios

def scenario_basic(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 1：初始状态，两实例旧版/新版检索均可用 ==")
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs")
        body = r.json()
        ck(r.status_code == 200, f"{name} 旧版检索 200", f"got {r.status_code}")
        ck(body["count"] > 0 and "legacy_summary" in body["items"][0],
           f"{name} 旧版返回 legacy_summary 数据")
        ck(body.get("source_column") == "legacy_summary", f"{name} 旧版来源列正确")

        r = get(f"{base}/api/v2/fault-logs")
        body = r.json()
        ck(r.status_code == 200, f"{name} 新版检索 200", f"got {r.status_code}")
        ck(body["count"] > 0 and "summary" in body["items"][0],
           f"{name} 新版返回 summary 数据")

        r = get(f"{base}/api/v1/legacy/fault-logs", params={"severity": "CRITICAL"})
        ck(r.status_code == 200 and all(i["severity"] == "CRITICAL" for i in r.json()["items"]),
           f"{name} 旧版按 severity 过滤")

        st = retirement_state(base)
        ck(st["status"] == "active" and st["legacy_column_present"],
           f"{name} 退役状态为 active 且旧列存在")


def scenario_failure_and_retry(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 2：删列失败 -> 保留 retiring 拒单 -> 跨实例重试成功 ==")
    # 武装 app1：下一次删列必失败（测试钩子）
    r = post(f"{app1}/api/test-hooks/fail-next-drops", params={"times": 1})
    ck(r.status_code == 200, "app1 测试钩子已武装", f"got {r.status_code}")

    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 202 and r.json()["status"] == "retiring",
       "app1 受理退役返回 202/retiring", f"got {r.status_code} {r.text[:120]}")

    # 等失败被记录
    ok = wait_until(lambda: retirement_state(app1)["failed_attempts"] >= 1, 15,
                    desc="failed_attempts>=1")
    ck(ok, "删列失败被记录（failed_attempts>=1）")
    st = retirement_state(app2)
    ck(st["status"] == "retiring", "失败后状态仍为 retiring（未误报完成）")
    ck(st["last_error"] and "simulated" in st["last_error"], "last_error 记录了失败原因",
       f"last_error={st['last_error']!r}")
    ck(st["legacy_column_present"], "失败后旧列仍在")

    # 失败保留 retiring 期间：两实例旧版都拒单，新版可用
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs")
        ck(r.status_code == 409, f"{name} retiring 期间旧版拒单(409)", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200, f"{name} retiring 期间新版可用(200)", f"got {r.status_code}")

    # 从另一个实例重试（其未武装失败钩子）
    r = post(f"{app2}/api/retirement")
    ck(r.status_code == 202, "app2 重试退役被受理(202)", f"got {r.status_code} {r.text[:120]}")
    ok = wait_until(lambda: retirement_state(app2)["status"] == "retired", 20,
                    desc="status==retired")
    ck(ok, "重试后退役完成(retired)")
    st = retirement_state(app1)
    ck(not st["legacy_column_present"], "旧列已删除（两实例视图一致）")
    ck(st["failed_attempts"] >= 1, "失败历史保留可查")

    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs")
        ck(r.status_code == 410, f"{name} 退役完成后旧版 410", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200 and r.json()["count"] > 0, f"{name} 退役完成后新版仍可用")

    # 完成后再调用退役：幂等，不得重复删列/报错
    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 200 and r.json()["status"] == "retired",
       "完成后重复调用退役：幂等 200/retired", f"got {r.status_code} {r.text[:120]}")


def scenario_interleave_and_concurrent(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 3：长查询与退役交错 + 双实例并发退役 ==")
    results: dict[str, httpx.Response] = {}

    def slow_legacy() -> None:
        results["slow"] = get(f"{app1}/api/v1/legacy/fault-logs",
                              params={"slow_ms": 6000})

    t = threading.Thread(target=slow_legacy)
    t.start()
    time.sleep(1.0)  # 确保慢查询已被接纳（持共享锁）

    posts: dict[str, httpx.Response] = {}

    def do_post(key: str, base: str) -> None:
        posts[key] = post(f"{base}/api/retirement")

    t1 = threading.Thread(target=do_post, args=("p1", app1))
    t2 = threading.Thread(target=do_post, args=("p2", app2))
    t1.start(); t2.start(); t1.join(); t2.join()

    ck(posts["p1"].status_code == 202 and posts["p2"].status_code == 202,
       "并发退役两个请求均被受理(202)",
       f"p1={posts['p1'].status_code} p2={posts['p2'].status_code}")
    ck(posts["p1"].json()["status"] == "retiring" and posts["p2"].json()["status"] == "retiring",
       "并发退役均如实返回 retiring（无误报完成）",
       f"p1={posts['p1'].json()['status']} p2={posts['p2'].json()['status']}")
    ck(posts["p1"].json()["request_id"] == posts["p2"].json()["request_id"],
       "并发退役共享同一 request_id（只迁移一次状态）")

    # 退役受理后立即：两实例旧版拒单、新版可用、列仍在（慢查询还没读完）
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs")
        ck(r.status_code == 409, f"{name} 退役受理后立即拒单(409)", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200, f"{name} 退役受理后新版仍可用(200)")
    st = retirement_state(app1)
    ck(st["status"] == "retiring" and st["legacy_column_present"],
       "慢查询在途期间：状态 retiring 且旧列尚未删除")

    # 已接纳的慢查询必须完整读完
    t.join(timeout=30)
    slow = results.get("slow")
    ck(slow is not None and slow.status_code == 200,
       "已接纳的长查询正常读完(200)", f"got {None if slow is None else slow.status_code}")
    if slow is not None and slow.status_code == 200:
        ck(slow.json()["count"] > 0 and "legacy_summary" in slow.json()["items"][0],
           "长查询返回完整 legacy_summary 数据")

    # 慢查询读完后删列才完成
    ok = wait_until(lambda: retirement_state(app2)["status"] == "retired", 25,
                    desc="status==retired")
    ck(ok, "长查询读完后退役完成(retired)")
    st = retirement_state(app1)
    ck(not st["legacy_column_present"], "旧列已删除")
    ck(st["failed_attempts"] == 0, "并发退役零失败（未重复删列）",
       f"failed_attempts={st['failed_attempts']}")


def _event(event_id: str, *, severity: str = "ERROR", service: str = "checkout-api",
           message: str = "payment gateway timeout", summary: str = "支付网关超时") -> dict:
    return {
        "event_id": event_id,
        "severity": severity,
        "service": service,
        "message": message,
        "summary": summary,
    }


def post_event(base: str, event: dict) -> httpx.Response:
    return post(f"{base}/api/events", json=event)


def v2_item(base: str, message: str) -> dict | None:
    body = get(f"{base}/api/v2/fault-logs", params={"limit": 500}).json()
    for item in body["items"]:
        if item["message"] == message:
            return item
    return None


def scenario_ingest_merge(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 4：事件入仓——窗口归并、摘要沿用首条、重试去重、复用拒绝 ==")
    msg = "ingest-window-merge-check"
    e1 = _event("evt-merge-1", message=msg, summary="首条摘要")
    r = post_event(app1, e1)
    body = r.json()
    ck(r.status_code == 201, "首条事件入仓 201", f"got {r.status_code} {r.text[:160]}")
    ck(body["event_count"] == 1 and not body["deduplicated"], "首条事件计数为 1")
    ck(body["summary"] == "首条摘要", "首条事件摘要已保存")
    log_id = body["fault_log_id"]
    first_seen = body["created_at"]
    ck(body["last_seen_at"] == first_seen, "首条事件首末时间一致")

    # 同 event_id 重试（内容一致）：200 + deduplicated，计数不变
    r = post_event(app2, e1)
    ck(r.status_code == 200 and r.json()["deduplicated"] and r.json()["event_count"] == 1,
       "同 event_id 跨实例重试不重复计数（200/deduplicated）",
       f"got {r.status_code} {r.text[:160]}")
    ck(r.json()["fault_log_id"] == log_id, "重试返回原日志 id")

    # 窗口内第二条（不同 event_id、同 service/severity/message）：计数+1，摘要沿用首条
    e2 = _event("evt-merge-2", message=msg, summary="第二条摘要")
    r = post_event(app2, e2)
    body = r.json()
    ck(r.status_code == 201 and body["event_count"] == 2,
       "窗口内第二条事件累加到同一条日志（count=2）", f"got {r.status_code} {r.text[:160]}")
    ck(body["fault_log_id"] == log_id and body["summary"] == "首条摘要",
       "窗口内第二条复用首条日志与首条摘要")
    ck(body["last_seen_at"] > first_seen, "末次时间随新事件推进")

    # 不同级别/服务/消息各自独立计数（归并键正确）
    r = post_event(app1, _event("evt-merge-3", severity="WARN", message=msg))
    ck(r.status_code == 201 and r.json()["event_count"] == 1
       and r.json()["fault_log_id"] != log_id,
       "不同级别的事件独立开窗（不误并入）")
    r = post_event(app1, _event("evt-merge-4", message=msg + "-other"))
    ck(r.status_code == 201 and r.json()["event_count"] == 1,
       "不同消息的事件独立开窗（不误并入）")

    # event_id 复用但内容不同：409，且不影响原有计数
    r = post_event(app1, _event("evt-merge-1", message=msg, summary="被篡改的摘要"))
    ck(r.status_code == 409, "event_id 换内容复用被拒绝(409)",
       f"got {r.status_code} {r.text[:160]}")
    r = post_event(app1, _event("evt-merge-2", severity="CRITICAL", message=msg))
    ck(r.status_code == 409, "event_id 复用改级别也被拒绝(409)", f"got {r.status_code}")
    item = v2_item(app2, msg)
    ck(item is not None and item["event_count"] == 2, "被拒绝的复用请求未改动计数",
       f"event_count={None if item is None else item['event_count']}")

    # 新版检索展示次数与末次时间；旧日志按一次显示
    ck(item is not None and item["last_seen_at"] is not None,
       "新版检索返回 last_seen_at")
    legacy_item = next(
        (i for i in get(f"{app1}/api/v2/fault-logs", params={"limit": 500}).json()["items"]
         if i["service"] == "auth-gateway"), None)
    ck(legacy_item is not None and legacy_item["event_count"] == 1
       and legacy_item["last_seen_at"] == legacy_item["created_at"],
       "原有日志按一次显示（event_count=1，last_seen=created_at）")
    # 入仓事件在旧版检索里也能看到（退役前）
    legacy_rows = get(f"{app1}/api/v1/legacy/fault-logs", params={"limit": 500}).json()["items"]
    ingested = [i for i in legacy_rows if i["message"] == msg]
    ck(any(i["event_count"] == 2 for i in ingested), "退役前旧版检索可见入仓日志的计数")


def scenario_ingest_window_expiry(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 5：60 秒窗口到期后新建日志 ==")
    msg = "ingest-window-expiry-check"

    async def backdate(log_id: int, seconds: int) -> None:
        conn = await asyncpg.connect(dsn)
        try:
            # 把首条事件时间整体前移，模拟窗口已过期（不影响其他场景的数据）。
            await conn.execute(
                """
                UPDATE fault_logs
                   SET created_at = created_at - ($2 * interval '1 second'),
                       last_seen_at = last_seen_at - ($2 * interval '1 second')
                 WHERE id = $1
                """,
                log_id,
                seconds,
            )
        finally:
            await conn.close()

    r = post_event(app1, _event("evt-expiry-1", message=msg, summary="窗口一摘要"))
    ck(r.status_code == 201 and r.json()["event_count"] == 1, "窗口一：首条事件 201/count=1")
    id1 = r.json()["fault_log_id"]
    r = post_event(app2, _event("evt-expiry-2", message=msg, summary="窗口一第二条"))
    ck(r.json()["fault_log_id"] == id1 and r.json()["event_count"] == 2,
       "窗口内第二条并入窗口一")

    asyncio.run(backdate(id1, 61))
    r = post_event(app1, _event("evt-expiry-3", message=msg, summary="窗口二摘要"))
    body = r.json()
    ck(r.status_code == 201 and body["fault_log_id"] != id1 and body["event_count"] == 1,
       "首条事件超过 60 秒后新建日志（新窗口 count=1）",
       f"got {r.status_code} {r.text[:160]}")
    ck(body["summary"] == "窗口二摘要", "新窗口的摘要取自新窗口首条事件")
    id2 = body["fault_log_id"]
    r = post_event(app2, _event("evt-expiry-4", message=msg, summary="窗口二第二条"))
    ck(r.json()["fault_log_id"] == id2 and r.json()["event_count"] == 2,
       "新窗口内继续归并到新日志")

    rows = get(f"{app1}/api/v2/fault-logs", params={"limit": 500}).json()["items"]
    windows = [i for i in rows if i["message"] == msg]
    ck(len(windows) == 2 and sorted(i["event_count"] for i in windows) == [2, 2],
       "同一归并键在数据中恰好两条窗口日志（各 2 次）",
       f"got {[(i['id'], i['event_count']) for i in windows]}")


def scenario_ingest_concurrency(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 6：双实例并发入仓不分裂窗口、不重复计数 ==")
    msg = "ingest-concurrent-check"
    rounds = 24
    results: list[httpx.Response] = []
    lock = threading.Lock()

    def fire(i: int, base: str) -> None:
        r = post_event(base, _event(f"evt-conc-{i}", message=msg, summary="并发摘要"))
        with lock:
            results.append(r)

    threads = [threading.Thread(target=fire, args=(i, app1 if i % 2 == 0 else app2))
               for i in range(rounds)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ids = {r.json().get("fault_log_id") for r in results if r.status_code == 201}
    ck(len(results) == rounds and all(r.status_code == 201 for r in results),
       f"{rounds} 条并发入仓全部成功",
       f"codes={sorted({r.status_code for r in results})}")
    ck(len(ids) == 1, "并发事件全部落入同一条窗口日志（未分裂）", f"fault_log_ids={sorted(ids)}")
    item = v2_item(app1, msg)
    ck(item is not None and item["event_count"] == rounds,
       f"并发计数无丢失（event_count={rounds}）",
       f"event_count={None if item is None else item['event_count']}")
    rows = get(f"{app2}/api/v2/fault-logs", params={"limit": 500}).json()["items"]
    ck(sum(1 for i in rows if i["message"] == msg) == 1, "库中只有一条窗口日志")

    # 同一 event_id 的高并发重试：只计一次
    retry_msg = "ingest-concurrent-retry-check"
    retry_event = _event("evt-conc-retry", message=retry_msg, summary="重试摘要")
    retry_results: list[httpx.Response] = []

    def retry(base: str) -> None:
        r = post_event(base, retry_event)
        with lock:
            retry_results.append(r)

    threads = [threading.Thread(target=retry, args=(app1 if i % 2 == 0 else app2,))
               for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    item = v2_item(app2, retry_msg)
    ck(all(r.status_code in (200, 201) for r in retry_results)
       and item is not None and item["event_count"] == 1,
       "同一 event_id 的并发重试只计一次",
       f"codes={sorted({r.status_code for r in retry_results})} "
       f"count={None if item is None else item['event_count']}")


def scenario_ingest_retirement_interleave(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 7：退役期间与退役后入仓（旧列写入行为） ==")
    msg = "ingest-retirement-check"
    r = post_event(app1, _event("evt-ret-1", message=msg, summary="退役前摘要"))
    ck(r.status_code == 201 and r.json()["used_legacy_column"],
       "退役前入仓写入旧列", f"got {r.status_code} {r.text[:160]}")
    id_before = r.json()["fault_log_id"]

    # 受理退役 -> retiring。用一条在途慢查询把退役任务挡在咨询锁外，
    # 确保“retiring 期间”的入仓断言不会被毫秒级的快速退役抢跑。
    slow_results: dict[str, httpx.Response] = {}

    def slow_legacy() -> None:
        slow_results["r"] = get(f"{app2}/api/v1/legacy/fault-logs",
                               params={"slow_ms": 6000})

    slow = threading.Thread(target=slow_legacy)
    slow.start()
    time.sleep(1.0)
    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 202 and r.json()["status"] == "retiring",
       "退役受理(202/retiring)")
    r = post_event(app2, _event("evt-ret-2", message=msg, summary="退役中摘要"))
    ck(r.status_code == 201 and not r.json()["used_legacy_column"],
       "retiring 期间可入仓（归并更新不写旧列，如实标注）",
       f"got {r.status_code} {r.text[:160]}")
    ck(r.json()["fault_log_id"] == id_before and r.json()["event_count"] == 2,
       "retiring 期间同一窗口继续归并")
    r = get(f"{app2}/api/v1/legacy/fault-logs", params={"limit": 500})
    ck(r.status_code == 409, "retiring 期间旧版检索仍拒单(409)", f"got {r.status_code}")
    r = get(f"{app2}/api/v2/fault-logs", params={"limit": 500})
    ck(r.status_code == 200, "retiring 期间新版检索可用")
    ck(retirement_state(app1)["status"] == "retiring", "慢查询在途时退役保持 retiring")

    # retiring 期间新建窗口仍写旧列（旧列此时还没删）
    r = post_event(app2, _event("evt-ret-2b", message=msg + "-retiring-new",
                                summary="退役中新建窗口摘要"))
    ck(r.status_code == 201 and r.json()["used_legacy_column"],
       "retiring 期间新建窗口仍写入旧列",
       f"got {r.status_code} {r.text[:160]}")

    slow.join(timeout=30)
    ck(slow_results.get("r") is not None and slow_results["r"].status_code == 200
       and any(i["message"] == msg and i["event_count"] == 2
               for i in slow_results["r"].json()["items"]),
       "已接纳的慢查询读到 retiring 期间入仓的累计次数")

    ok = wait_until(lambda: retirement_state(app1)["status"] == "retired", 25,
                    desc="退役完成")
    ck(ok, "退役完成(retired)")
    ck(not retirement_state(app2)["legacy_column_present"], "旧列已删除")

    # 退役后：入仓仍可用，但不写旧列
    r = post_event(app1, _event("evt-ret-3", message=msg, summary="退役后摘要"))
    ck(r.status_code == 201 and not r.json()["used_legacy_column"],
       "退役完成后入仓不再写旧列", f"got {r.status_code} {r.text[:160]}")
    ck(r.json()["fault_log_id"] == id_before and r.json()["event_count"] == 3,
       "退役后同一窗口继续归并")

    # 退役后新建窗口也不写旧列，且旧版检索 410、新版可用
    r = post_event(app2, _event("evt-ret-4", message=msg + "-new", summary="退役后新窗"))
    ck(r.status_code == 201 and not r.json()["used_legacy_column"],
       "退役后新窗口入仓不写旧列")
    async def columns() -> set[str]:
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                """SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'fault_logs'""")
            return {row["column_name"] for row in rows}
        finally:
            await conn.close()

    cols = asyncio.run(columns())
    ck("legacy_summary" not in cols, "退役后库中确实没有 legacy_summary 列")
    r = get(f"{app1}/api/v1/legacy/fault-logs")
    ck(r.status_code == 410, "退役后旧版检索 410")
    item = next((i for i in get(f"{app2}/api/v2/fault-logs", params={"limit": 500}).json()["items"]
                 if i["message"] == msg), None)
    ck(item is not None and item["event_count"] == 3 and item["summary"] == "退役前摘要",
       "退役后新版检索仍显示累计次数与首条摘要",
       f"item={item}")


def scenario_ingest_drop_race(app1: str, app2: str, dsn: str, ck: Check) -> None:
    print("\n== 场景 8：入仓瞬间旧列被删 -> 自动降级为不写旧列（不失败、不重复计数） ==")
    msg = "ingest-drop-race-check"

    async def drop_column() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            # 模拟“入仓读到 retiring 之后、落库之前，退役任务提交了 DROP COLUMN”。
            # 状态仍为 active，确保应用层会先尝试写旧列，从而走到 42703 降级重试路径。
            await conn.execute("ALTER TABLE fault_logs DROP COLUMN legacy_summary")
        finally:
            await conn.close()

    asyncio.run(drop_column())
    r = post_event(app1, _event("evt-race-1", message=msg, summary="降级摘要"))
    ck(r.status_code == 201, "旧列消失后入仓仍成功(201)",
       f"got {r.status_code} {r.text[:160]}")
    ck(not r.json()["used_legacy_column"], "响应如实标注未写旧列")
    ck(r.json()["event_count"] == 1, "降级重试没有重复计数",
       f"event_count={r.json().get('event_count')}")
    log_id = r.json()["fault_log_id"]

    async def registry_count() -> int:
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetchval(
                "SELECT count(*) FROM event_ingestions WHERE event_id = 'evt-race-1'")
        finally:
            await conn.close()

    ck(asyncio.run(registry_count()) == 1, "去重登记恰好一条（重试未产生脏数据）")

    r = post_event(app2, _event("evt-race-2", message=msg, summary="降级第二条"))
    ck(r.status_code == 201 and r.json()["fault_log_id"] == log_id
       and r.json()["event_count"] == 2 and not r.json()["used_legacy_column"],
       "后续事件正常归并（窗口未分裂）", f"got {r.status_code} {r.text[:160]}")


ALL_SCENARIOS = [
    ("basic", scenario_basic),
    ("failure_and_retry", scenario_failure_and_retry),
    ("interleave_and_concurrent", scenario_interleave_and_concurrent),
    ("ingest_merge", scenario_ingest_merge),
    ("ingest_window_expiry", scenario_ingest_window_expiry),
    ("ingest_concurrency", scenario_ingest_concurrency),
    ("ingest_retirement_interleave", scenario_ingest_retirement_interleave),
    ("ingest_drop_race", scenario_ingest_drop_race),
]


def run_scenarios(app1: str, app2: str, dsn: str, names: list[str], ck: Check) -> None:
    selected = [s for s in ALL_SCENARIOS if s[0] in names] if names else ALL_SCENARIOS
    for name, fn in selected:
        print(f"  .. 重置数据库（场景 {name}）")
        reset_db(dsn)
        fn(app1, app2, dsn, ck)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app1", default="http://localhost:8001")
    ap.add_argument("--app2", default="http://localhost:8002")
    ap.add_argument("--pg-dsn", default="postgresql://ops:ops_password@localhost:5432/faultdb")
    ap.add_argument("--scenario", action="append", default=[],
                    help="只跑指定场景（可多次给出）；默认全部")
    args = ap.parse_args()

    ck = Check()
    run_scenarios(args.app1, args.app2, args.pg_dsn, args.scenario, ck)
    print(f"\n==== 断言 {ck.count} 条，失败 {len(ck.failures)} 条 ====")
    if ck.failures:
        for f in ck.failures:
            print(f"  失败: {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

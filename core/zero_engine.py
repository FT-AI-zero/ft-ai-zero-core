# core/zero_engine.py
import os, json, time, math, sqlite3, threading, datetime, traceback
from decimal import Decimal

from core.okx_trader import OKXTrader
from utils.config import SIGNAL_POOL_DB, TRADES_DB, ZERO_LOG, HEALTH_LOG, STRATEGY_POOL_DB
from utils.allowlist import is_gid_allowed

FAIL = '\033[91m'; OK = '\033[92m'; END = '\033[0m'
HEARTBEAT_INTERVAL = 60

# === 预算参数（可调） ===
RISK_FRACTION = 0.05   # 总余额的 5% 用于本轮下单预算池
MIN_BUDGET    = 10.0   # 单信号最小预算
MAX_BUDGET    = 200.0  # 单信号最大预算
ALLOWLIST_REFRESH_SEC = 60  # 白名单&权重刷新周期
MODE = os.getenv("FT_MODE", "paper").strip().lower()

def _assert_gid_not_null(gid):
    if gid is None:
        raise ValueError("gid is None: refuse to place order; check distributor/allowlist gate.")

def log(line):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f"[{ts}] {line}"
    print(msg)
    try:
        with open(ZERO_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except:
        pass

def health(status="OK", msg=""):
    try:
        with open(HEALTH_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] zero_engine.py {status} {msg}\n")
    except:
        pass

def ensure_tables():
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instId TEXT,
        interval TEXT,
        period   TEXT,
        ts INTEGER,
        close REAL,
        vol REAL,
        signal_type TEXT,
        status TEXT,
        detected_at TEXT,
        meta TEXT,
        priority INTEGER DEFAULT 3,
        promotion_level INTEGER DEFAULT 0,
        expire_ts INTEGER DEFAULT 0
    )""")
    conn.commit(); conn.close()

    conn2 = sqlite3.connect(TRADES_DB)
    conn2.execute("""
    CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instId TEXT,
        action TEXT,
        price REAL,
        vol REAL,
        status TEXT,
        comment TEXT,
        ts TEXT
    )""")
    conn2.commit(); conn2.close()

def mark_signal_done(sid, status="DONE"):
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    conn.execute("UPDATE signals SET status=?, detected_at=? WHERE id=?",
                 (status, datetime.datetime.utcnow().isoformat(), sid))
    conn.commit(); conn.close()

def fetch_wait_live():
    now_ts = int(time.time())
    sql = """
    SELECT id, instId, COALESCE(period, interval), ts, close, vol, signal_type, meta,
           IFNULL(priority,3), IFNULL(promotion_level,0), IFNULL(expire_ts,0)
    FROM signals
    WHERE status='WAIT_LIVE'
      AND (expire_ts IS NULL OR expire_ts=0 OR expire_ts > ?)
    ORDER BY priority ASC, promotion_level DESC, ts ASC
    """
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    rows = conn.execute(sql, (now_ts,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        sid, inst, period, ts, close, vol, sigtype, meta, pri, promo, exp = r
        try:
            m = json.loads(meta) if meta else {}
        except:
            m = {}
        out.append({
            "id": sid, "instId": inst, "period": period or "",
            "ts": int(ts or 0), "price": float(close or 0), "vol": float(vol or 0),
            "signal_type": sigtype or "", "meta": m,
            "priority": pri, "promotion_level": promo, "expire_ts": exp
        })
    return out

def record_trade(instId, action, price, vol, status, comment):
    conn = sqlite3.connect(TRADES_DB)
    conn.execute("""INSERT INTO trades(instId,action,price,vol,status,comment,ts)
                    VALUES(?,?,?,?,?,?,?)""",
                 (instId, action, float(price or 0), float(vol or 0),
                  status, comment, datetime.datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

def tp_sl_sanity(side:str, price:float, tp:float|None, sl:float|None):
    if price <= 0:
        return (None if tp is None else tp, None if sl is None else sl)
    tp_ok, sl_ok = tp, sl
    if side == "buy":
        if tp is not None and tp <= price: tp_ok = None
        if sl is not None and sl >= price: sl_ok = None
    else:
        if tp is not None and tp >= price: tp_ok = None
        if sl is not None and sl <= price: sl_ok = None
    return tp_ok, sl_ok

def size_from_budget(t: OKXTrader, instId: str, usdt_budget: float, lev: int) -> str | None:
    """
    预算→合法张数（字符串）。不足返回 None。
    """
    try:
        tk = t.get_ticker(instId) or {}
        last = float(tk.get("last") or tk.get("lastPx") or 0)
        if last <= 0: return None
        effective_notional = float(usdt_budget) * max(1, int(lev))
        # 优先用 trader 自带方法（若存在）
        if hasattr(t, "sz_from_budget"):
            return t.sz_from_budget(instId, effective_notional, price=last, round_mode="down")
        # 兼容旧逻辑
        cont = t.contracts_from_usdt(instId, effective_notional, price=last, mode="down")
        if cont <= 0:
            need = float(t.min_notional_usdt(instId, price=last) or 0)
            if need and effective_notional < need:
                return None
            return None
        return t.stringify_sz(cont)
    except Exception:
        return None

def load_allow_weights():
    """
    从 strategy_pool.allowlist 读权重。
    返回 (weights_map, sum_weight, last_updated_iso)
    - 若 weight 为 NULL 则用 score 兜底
    - 过滤 <=0 的权重
    """
    conn = sqlite3.connect(STRATEGY_POOL_DB)
    rows = conn.execute("""
       SELECT param_group_id, IFNULL(weight, score) AS w
       FROM allowlist
    """).fetchall()
    conn.close()
    wmap = {}
    for gid, w in rows:
        try:
            val = float(w or 0)
            if val > 0:
                wmap[int(gid)] = val
        except:
            pass
    s = sum(wmap.values()) if wmap else 0.0
    return wmap, s, datetime.datetime.utcnow().isoformat()

def heartbeat():
    while True:
        try:
            health("OK")
        except Exception as e:
            log(f"[心跳异常] {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def main():
    ensure_tables()
    threading.Thread(target=heartbeat, daemon=True).start()
    log(f"Zero Engine 启动（MODE={MODE}，实盘仅放行白名单分组，按 allowlist.weight 分配预算）")
    t = OKXTrader()

    # 权重缓存
    weights_map, weights_sum, weights_updated = load_allow_weights()
    last_refresh = time.time()

    consecutive_fail = 0
    cooldown_until = 0

    while True:
        try:
            # 定期刷新白名单权重
            if time.time() - last_refresh > ALLOWLIST_REFRESH_SEC:
                weights_map, weights_sum, weights_updated = load_allow_weights()
                last_refresh = time.time()
                log(f"[allowlist] refreshed: {len(weights_map)} gids, sum={weights_sum:.4f}")

            if time.time() < cooldown_until:
                time.sleep(2); continue

            sigs = fetch_wait_live()
            if not sigs:
                time.sleep(2); continue

            # 计算本轮总可用预算池
            balance = float(t.get_available_balance("USDT") or 0)
            if balance <= 0:
                log(f"{FAIL}[资金不足] 可用USDT=0，全部跳过{END}")
                for s in sigs:
                    mark_signal_done(s["id"], "SKIP_NO_BAL")
                time.sleep(3)
                continue
            total_pool = max(MIN_BUDGET, balance * RISK_FRACTION)

            for sig in sigs:
                gid = (
                    sig["meta"].get("param_group_id")
                    or sig["meta"].get("gid")
                    or sig["meta"].get("group_id")
                    or sig["meta"].get("group")
                )
                if not gid:
                    log(f"{FAIL}[SKIP] 信号缺少分组ID: {sig}{END}")
                    mark_signal_done(sig["id"], "SKIP_NO_GID")
                    continue
                gid = int(gid)

                if not is_gid_allowed(gid):
                    log(f"[白名单拒绝] gid={gid} 非今日白名单，跳过 {sig['instId']}")
                    mark_signal_done(sig["id"], "SKIP_NOT_ALLOWED")
                    continue

                # 按权重分配单信号预算
                w = float(weights_map.get(gid, 0.0))
                if weights_sum <= 0 or w <= 0:
                    # 权重不可用：均分退化
                    usdt_budget = min(MAX_BUDGET, max(MIN_BUDGET, total_pool))
                    reason = "fallback_equal"
                else:
                    frac = w / weights_sum
                    usdt_budget = total_pool * frac
                    usdt_budget = min(MAX_BUDGET, max(MIN_BUDGET, usdt_budget))
                    reason = f"weight={w:.4f}/{weights_sum:.4f} -> frac={frac:.4%}"

                side = sig["meta"].get("side")
                if not side:
                    side = "buy" if str(sig["signal_type"]).upper().endswith("UP") else "sell"

                lev = int(sig["meta"].get("lev") or 10)
                sz = size_from_budget(t, sig["instId"], usdt_budget, lev)
                if not sz or float(sz) <= 0:
                    log(f"{FAIL}[SIZE_ZERO] gid={gid} 预算={usdt_budget:.2f} 无法覆盖合约最小门槛：{sig['instId']}{END}")
                    mark_signal_done(sig["id"], "SKIP_SIZE_ZERO")
                    continue

                raw_tp = sig["meta"].get("tp")
                raw_sl = sig["meta"].get("sl")
                tp, sl = tp_sl_sanity(side, sig["price"], raw_tp, raw_sl)
                if raw_tp and not tp: log(f"[TP过滤] {raw_tp} 不合法，已忽略")
                if raw_sl and not sl: log(f"[SL过滤] {raw_sl} 不合法，已忽略")

                try:
                    _assert_gid_not_null(gid)
                    resp = t.open_order(
                        instId=sig["instId"], side=side, sz=sz, lever=lev,
                        tdMode="cross", ordType="market",
                        tp=tp, sl=sl, reduceOnly=False, clOrdId=None
                    )
                    if str(resp.get("code")) != "0":
                        raise RuntimeError(resp)

                    ord_id = (resp.get("data") or [{}])[0].get("ordId")
                    done = t.wait_order_filled(sig["instId"], ordId=ord_id)
                    state = (done or {}).get("state")
                    log(f"[AI组 {gid}] 下单完成 state={state} ordId={ord_id} budget={usdt_budget:.2f} ({reason})")

                    record_trade(sig["instId"], side, sig["price"], float(sz),
                                 "OPEN", f"gid={gid}|budget={usdt_budget:.2f}|lev={lev}|w={w:.4f}")
                    mark_signal_done(sig["id"], "DONE")
                    consecutive_fail = 0

                except Exception as e:
                    consecutive_fail += 1
                    backoff = min(60, 2 ** min(6, consecutive_fail))
                    cooldown_until = time.time() + backoff
                    log(f"{FAIL}[下单异常] gid={gid} {sig['instId']} err={e}，{backoff}s 后重试{END}")
                    mark_signal_done(sig["id"], "ERROR")

        except Exception as e:
            log(f"{FAIL}[主循环异常] {e}\n{traceback.format_exc()}{END}")
            time.sleep(5)

if __name__ == "__main__":
    main()

# utils/risk.py
import json, os, time
from typing import Tuple, Optional
from utils.config import JSON_DIR

STATE_PATH = os.path.join(JSON_DIR, "risk_state.json")

# === 可根据需要微调的阈值（先给一套稳妥默认） ===
STALE_TICKER_MAX_AGE = 5         # ticker超过5秒视为陈旧
MAX_SLIPPAGE_RATE    = 0.002     # 20bp 允许的下单滑点
COOLDOWN_MAX_SEC     = 60        # 连续失败指数退避最大到60s
BASE_BUDGET_PERCENT  = 0.05      # 账户余额*5% 作为基准预算
MIN_USDT_BUDGET      = 10.0
MAX_USDT_BUDGET      = 200.0

def _load_state():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except: pass
    return {"consecutive_fail": 0, "cooldown_until": 0}

def _save_state(st):
    try:
        os.makedirs(JSON_DIR, exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False)
    except: pass

def backoff_on_fail():
    st = _load_state()
    st["consecutive_fail"] = int(st.get("consecutive_fail", 0)) + 1
    # 指数退避
    backoff = min(COOLDOWN_MAX_SEC, 2 ** min(6, st["consecutive_fail"]))
    st["cooldown_until"] = time.time() + backoff
    _save_state(st)
    return backoff

def clear_backoff():
    st = _load_state()
    st["consecutive_fail"] = 0
    st["cooldown_until"] = 0
    _save_state(st)

def is_in_cooldown() -> float:
    """返回距离冷却结束还剩多少秒（<=0 表示不在冷却）。"""
    st = _load_state()
    left = float(st.get("cooldown_until", 0) - time.time())
    return left

def tp_sl_sanity(side: str, price: float,
                 tp: Optional[float], sl: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """修正明显不合法的 TP/SL，避免51052/51053。"""
    if not price or price <= 0:
        return (None if tp is None else tp, None if sl is None else sl)
    tp_ok, sl_ok = tp, sl
    if side == "buy":
        if tp is not None and tp <= price: tp_ok = None
        if sl is not None and sl >= price: sl_ok = None
    else:
        if tp is not None and tp >= price: tp_ok = None
        if sl is not None and sl <= price: sl_ok = None
    return tp_ok, sl_ok

def pretrade_sanity_from_ticker(ticker: dict, intend_price: float) -> Tuple[bool, str]:
    """用ticker做下单前体检：是否陈旧、滑点是否超阈。"""
    try:
        last = float(ticker.get("last") or ticker.get("lastPx") or 0)
        ts   = float(ticker.get("ts") or 0) / 1000.0  # OKX返回ms
    except:
        return False, "ticker_parse_error"

    if last <= 0:
        return False, "ticker_last_leq_zero"

    now = time.time()
    if ts and now - ts > STALE_TICKER_MAX_AGE:
        return False, f"stale_ticker_{now - ts:.1f}s"

    # 滑点：按市价单的合理偏离做保护
    if intend_price and intend_price > 0:
        slip = abs(intend_price - last) / last
        if slip > MAX_SLIPPAGE_RATE:
            return False, f"slippage_{slip:.4f}"
    return True, "ok"

def budget_by_balance(balance_usdt: float) -> float:
    raw = max(MIN_USDT_BUDGET, min(balance_usdt * BASE_BUDGET_PERCENT, MAX_USDT_BUDGET))
    return float(raw)

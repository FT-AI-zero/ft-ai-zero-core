# core/position_guard.py
import time, json, datetime, sqlite3, traceback
from decimal import Decimal
from typing import Optional

from core.okx_trader import OKXTrader
from utils.config import POSITION_GUARD_LOG, HEALTH_LOG

OK = '\033[92m'; FAIL = '\033[91m'; END = '\033[0m'

# ==== 可调参数（先用固定值，后面再接DB/白名单）====
CHECK_INTERVAL_SEC = 10          # 扫描间隔
TP_RATE_DEFAULT    = 0.012       # +1.2% 止盈
SL_RATE_DEFAULT    = 0.006       # -0.6% 止损
TRAIL_RATIO_DEF    = 0.004       # 0.4% 追踪止盈回撤比例
TRAIL_MIN_PROFIT   = 0.008       # 浮盈 >0.8% 才挂追踪止盈
BREAKEVEN_TRIGGER  = 0.010       # 浮盈 >1.0% 时，把 SL 抬到开仓价（保护盈利）

def log(msg: str):
    line = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line)
    try:
        with open(POSITION_GUARD_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass

def health(status="OK", msg=""):
    try:
        with open(HEALTH_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] position_guard.py {status} {msg}\n")
    except:
        pass

def pct_change(entry: float, last: float, side: str) -> float:
    if not entry or not last:
        return 0.0
    r = (last - entry) / entry
    return float(r if side == "long" else -r)

def tp_sl_for(pos_side: str, entry: float):
    """
    基于默认参数返回 (tp_px, sl_px, trail_ratio)
    多单: tp=entry*(1+TP)  sl=entry*(1-SL)
    空单: tp=entry*(1-TP)  sl=entry*(1+SL)
    """
    tp = SL = None
    tp_rate = TP_RATE_DEFAULT
    sl_rate = SL_RATE_DEFAULT
    if pos_side == "long":
        tp = entry * (1 + tp_rate)
        SL = entry * (1 - sl_rate)
    else:
        tp = entry * (1 - tp_rate)
        SL = entry * (1 + sl_rate)
    return round(tp, 4), round(SL, 4), TRAIL_RATIO_DEF

def main():
    log("Position Guard 启动")
    t = OKXTrader()
    last_heartbeat = 0
    # 已经挂过保护的持仓 key: (instId,posSide) -> True
    protected = {}

    while True:
        try:
            now = time.time()
            if now - last_heartbeat > 60:
                health("OK")
                last_heartbeat = now

            poss = t.get_positions() or []
            if not poss:
                time.sleep(CHECK_INTERVAL_SEC)
                continue

            for p in poss:
                try:
                    instId = p.get("instId")
                    posSide = p.get("posSide") or ("long" if (p.get("pos") or "0").startswith("-") is False else "short")
                    pos = float(p.get("pos") or 0)
                    if pos <= 0:
                        continue

                    # 平均开仓价
                    entry = float(p.get("avgPx") or 0)
                    # 现价
                    tk = t.get_ticker(instId) or {}
                    last = float(tk.get("last") or tk.get("lastPx") or 0)
                    if last <= 0 or entry <= 0:
                        continue

                    # 当前盈利率
                    rr = pct_change(entry, last, posSide)

                    # 首次接管：挂 TP/SL + (必要时) 追踪止盈
                    key = (instId, posSide)
                    if not protected.get(key):
                        tp_px, sl_px, trail = tp_sl_for(posSide, entry)

                        # 若已有明显盈利，SL 抬到开仓价（略加一点点 tick）
                        if rr >= BREAKEVEN_TRIGGER:
                            sl_px = entry * (1.0002 if posSide == "long" else 0.9998)

                        trail_ratio = None
                        if rr >= TRAIL_MIN_PROFIT:
                            trail_ratio = trail

                        res = t.set_tp_sl(
                            instId=instId, sz=None, posSide=posSide,
                            tp=tp_px, sl=sl_px, trailing_ratio=trail_ratio,
                            tdMode="cross", trigger_px_type="last"
                        )
                        log(f"{OK}[GUARD] {instId}/{posSide} 挂保护 tp={tp_px} sl={sl_px} trail={trail_ratio}{END}")
                        protected[key] = True
                        continue

                    # 已受保护：如果盈利扩大而未到追踪条件，也可以轻微抬高SL到保本
                    if rr >= BREAKEVEN_TRIGGER and protected.get(key):
                        # 抬一次即可，不频繁
                        # 这里简单示例：再次调用 set_tp_sl 仅传 sl（OKX 会并存/覆盖，根据账户设置）
                        try:
                            be_sl = entry * (1.0002 if posSide == "long" else 0.9998)
                            t.set_tp_sl(instId=instId, sz=None, posSide=posSide, sl=be_sl, tdMode="cross")
                            log(f"[BE] {instId}/{posSide} SL 抬到保本 {round(be_sl,4)} (rr={round(rr*100,2)}%)")
                        except Exception as e:
                            log(f"{FAIL}[BE_ERR] {instId}/{posSide} {e}{END}")

                except Exception as e:
                    log(f"{FAIL}[LOOP_INNER_ERR] {e}{END}")

            time.sleep(CHECK_INTERVAL_SEC)

        except Exception as e:
            log(f"{FAIL}[MAIN_ERR] {e}\n{traceback.format_exc()}{END}")
            time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    main()

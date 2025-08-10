# -*- coding: utf-8 -*-
# jobs/pm_auto_tuner.py
import os, sys, time, json, sqlite3, datetime, math, random, traceback
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from utils.config import DB_DIR, LOG_DIR
from core.pm_runtime import load, save, path as cfg_path

REVIEW_DB = os.path.join(DB_DIR, "review.db")
LOG_FILE  = os.path.join(LOG_DIR, "pm_auto_tuner.log")

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except: pass

def score_recent(hours=24):
    """最近 hours 小时的真实回放收益（排除 no_kline）"""
    try:
        con = sqlite3.connect(REVIEW_DB)
        cur = con.execute("""
          SELECT SUM(pnl), COUNT(1), AVG(hold_sec)
          FROM pnl_by_trade
          WHERE exit_reason!='no_kline'
            AND datetime(close_ts) >= datetime('now', ?)
        """, (f'-{hours} hours',))
        s = cur.fetchone() or (0,0,0)
        con.close()
        pnl_sum, n, avg_hold = float(s[0] or 0), int(s[1] or 0), float(s[2] or 0)
        # 简单打分：净收益 - 时间罚项
        return pnl_sum - 0.0001*avg_hold, n
    except Exception as e:
        log(f"score_recent err={e}")
        return 0.0, 0

def tweak(cfg):
    """小幅探索：对关键阈值做±10% 微调，带边界"""
    def _jitter(x, lo, hi, pct=0.1):
        nx = x * (1 + random.uniform(-pct, pct))
        return max(lo, min(hi, nx))
    cfg["PYRAMID_STEP_PCT"] = _jitter(cfg["PYRAMID_STEP_PCT"], 0.001, 0.02)
    cfg["REDUCE_STEP_PCT"]  = _jitter(cfg["REDUCE_STEP_PCT"],  0.002, 0.03)
    cfg["TARGET_MOVE"]      = _jitter(cfg["TARGET_MOVE"],      0.003, 0.05)
    cfg["BASE_BUDGET"]      = _jitter(cfg["BASE_BUDGET"],      5.0,   50.0)
    cfg["MAX_LAYERS"]       = int(max(1, min(6, round(cfg["MAX_LAYERS"] + random.choice([-1,0,1])))))
    return cfg

def main():
    best = None
    while True:
        try:
            cfg   = load()
            baseS, n = score_recent(hours=24)
            if best is None or baseS > best:
                best = baseS
            # ε-贪婪：多数时间沿着更优方向微调，少数时间探索
            explore = random.random() < 0.25
            new_cfg = dict(cfg)
            if explore or n < 5:
                new_cfg = tweak(new_cfg)
                log(f"[explore] n={n} -> tweak params")
            else:
                # exploit：朝当前最优分位再推一小步
                new_cfg = tweak(new_cfg)
                log(f"[exploit] base_score={baseS:.2f} best={best:.2f} -> tweak small")

            save(new_cfg)
            log(f"[saved] {cfg_path()} | BASE_BUDGET={new_cfg['BASE_BUDGET']:.2f} "
                f"STEP_IN={new_cfg['PYRAMID_STEP_PCT']:.3%} STEP_OUT={new_cfg['REDUCE_STEP_PCT']:.3%} "
                f"TARGET_MOVE={new_cfg['TARGET_MOVE']:.2%} MAX_LAYERS={new_cfg['MAX_LAYERS']}")
        except Exception as e:
            log(f"[ERROR] {e}\n{traceback.format_exc()}")
        time.sleep(900)  # 15 min

if __name__ == "__main__":
    main()

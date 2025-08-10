# -*- coding: utf-8 -*-
# core/position_policy.py
import time, math, datetime

# ===== 默认参数（可用环境变量覆盖）=====
MIN_LEV = float(__import__("os").getenv("PM_MIN_LEV", "2"))
MAX_LEV = float(__import__("os").getenv("PM_MAX_LEV", "50"))
TARGET_MOVE = float(__import__("os").getenv("PM_TARGET_MOVE", "0.01"))  # 目标单根波动承受 ~1%
PYRAMID_STEP = float(__import__("os").getenv("PM_PYRAMID_STEP_PCT", "0.005"))  # 回撤/上浮 0.5% 触发一层
REDUCE_STEP  = float(__import__("os").getenv("PM_REDUCE_STEP_PCT",  "0.007"))  # 浮盈 0.7% 减一层
MAX_LAYERS   = int(__import__("os").getenv("PM_MAX_LAYERS", "3"))
BASE_BUDGET  = float(__import__("os").getenv("PM_BASE_BUDGET", "10"))  # 单层预算(USDT)
MIN_DELTA_USDT = float(__import__("os").getenv("PM_MIN_DELTA_USDT", "3"))  # 小于这个就不下单避免噪音

def _atr_pct_from_klines(kls, period=14):
    """kls: [(ts,o,h,l,c,vol)] 升序；返回ATR/收盘 的近似百分比"""
    if not kls: return 0.01
    tr = []
    prev_close = kls[0][4]
    for _,o,h,l,c,_ in kls:
        tr.append(max(h-l, abs(h-prev_close), abs(l-prev_close)))
        prev_close = c
    n = max(1, min(period, len(tr)))
    atr = sum(tr[-n:]) / n
    base = kls[-1][4]
    return (atr / base) if base else 0.01

def leverage_from_vol(atr_pct, min_lev=MIN_LEV, max_lev=MAX_LEV, target_move=TARGET_MOVE):
    """
    简单自适应：杠杆 ≈ 目标承受波动 / 当前ATR%
    """
    if atr_pct <= 0: return min_lev
    lev = target_move / atr_pct
    return max(min_lev, min(max_lev, lev))

def decide_scale_action(side:str, avg_px:float, last_px:float) -> str|None:
    """
    返回 'scale_in'（加一层），'reduce'（减一层），或 None
    long: 价格↓PYRAMID_STEP 加仓；价格↑REDUCE_STEP 减仓
    short: 反过来
    """
    if avg_px <= 0 or last_px <= 0: return None
    chg = (last_px / avg_px) - 1.0
    if side == "long":
        if chg <= -PYRAMID_STEP: return "scale_in"
        if chg >=  REDUCE_STEP:  return "reduce"
    else:
        if chg >=  PYRAMID_STEP: return "scale_in"  # 空头盈利加仓（顺势）
        if chg <= -REDUCE_STEP:  return "reduce"
    return None

def next_layer_budget(current_exposure_usdt:float) -> float:
    """
    暂用“线性分层”：每次一层 BASE_BUDGET，上限 MAX_LAYERS。
    """
    layers = int(math.floor(current_exposure_usdt / max(1e-6, BASE_BUDGET)))
    if layers >= MAX_LAYERS: return 0.0
    return BASE_BUDGET

def should_skip_delta(delta_usdt:float) -> bool:
    return abs(delta_usdt) < MIN_DELTA_USDT

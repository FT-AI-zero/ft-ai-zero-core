# -*- coding: utf-8 -*-
"""
Position Manager（仓位管理器，支持自动跟随实盘持仓）
- 读取 data/runtime/pm_policy.json 的参数
- 轮询 INST_LIST ∪ (实际持仓 if AUTO_FOLLOW/PM_AUTO_FOLLOW) ∪ PM_EXTRA_INST
- 按浮盈浮亏触发 scale_in / scale_out（Dry-Run 默认开启）
- 把每次决策写入 pm_experience.db，便于后续学习

环境变量：
  PM_DRY_RUN=1        只打印不下单（默认 1）
  PM_VERBOSE=1        打印更多心跳/细节（默认 0）
  PM_AUTO_FOLLOW=1    自动把“当前持仓”的合约并入盯盘列表
  PM_EXTRA_INST=AAA-USDT-SWAP,BBB-USDT-SWAP  临时额外盯盘
"""

import os, time, math
from typing import List, Dict, Any

from core.pm_runtime import load as load_policy, path as policy_path
from core.okx_trader import OKXTrader
from core.pm_experience import add_experience
from utils.config import LOG_DIR

# ---------- 环境 ----------
DRY_RUN  = os.getenv("PM_DRY_RUN", "1") == "1"
VERBOSE  = os.getenv("PM_VERBOSE", "0") == "1"
LOG_FILE = os.path.join(LOG_DIR, "position_manager.log")
os.makedirs(LOG_DIR, exist_ok=True)

def log(msg: str):
    s = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(s)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except:
        pass

# ---------- 小工具 ----------
def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except:
        return default

def _last_price(t: OKXTrader, instId: str) -> float:
    try:
        tk = t.get_ticker(instId) or {}
        return _safe_float(tk.get("last") or tk.get("lastPx") or tk.get("close") or tk.get("last_price"))
    except:
        return 0.0

def _agg_position(raw_list: Any) -> dict | None:
    """
    把交易所/适配层返回的列表规整为：
      { side('long'|'short'), qty(float), avg_px(float), lev(float) }
      无持仓 -> None
    兼容两种来源：
      A) 我们热修复后的 get_positions： [{'instId','side','qty','avgPx','lever',...}, ...]
      B) 原生风格： [{'posSide','pos','avgPx','lever'} 或 {'longSz','shortSz',...}]
    """
    if not raw_list:
        return None
    try:
        long_qty, long_px = 0.0, 0.0
        short_qty, short_px = 0.0, 0.0
        lev = 0.0

        for r in (raw_list if isinstance(raw_list, list) else []):
            if not isinstance(r, dict):
                continue

            # 优先使用归一化后的 'side'/'qty'
            if "side" in r and "qty" in r:
                side = (r.get("side") or "").lower()
                q = _safe_float(r.get("qty"))
                lev = max(lev, _safe_float(r.get("lever") or r.get("leverage") or r.get("lev"), 0.0))
                px = _safe_float(r.get("avgPx") or r.get("avg_px") or r.get("price"))
                if side == "long" and q > 0:
                    long_qty += q; long_px = px or long_px
                elif side == "short" and q > 0:
                    short_qty += q; short_px = px or short_px
                continue

            # 兼容 OKX 原生字段
            side = (r.get("posSide") or r.get("pos_side") or "").lower()
            if side:
                q  = _safe_float(r.get("pos") or r.get("posQty") or r.get("qty"))
                px = _safe_float(r.get("avgPx") or r.get("avg_px") or r.get("price"))
                lev = max(lev, _safe_float(r.get("lever") or r.get("leverage") or r.get("lev"), 0.0))
                if side == "long" and q > 0:
                    long_qty += abs(q); long_px = px or long_px
                elif side == "short" and q > 0:
                    short_qty += abs(q); short_px = px or short_px
                continue

            # 有些返回用 longSz/shortSz
            l = _safe_float(r.get("longSz") or r.get("long_size"))
            s = _safe_float(r.get("shortSz") or r.get("short_size"))
            px = _safe_float(r.get("avgPx") or r.get("avg_px") or r.get("price"))
            lev = max(lev, _safe_float(r.get("lever") or r.get("leverage") or r.get("lev"), 0.0))
            if l > 0:
                long_qty += l;  long_px  = px or long_px
            if s > 0:
                short_qty += s; short_px = px or short_px

        if long_qty > 0 and long_qty >= short_qty:
            return {"side": "long", "qty": long_qty, "avg_px": long_px, "lev": lev}
        if short_qty > 0 and short_qty > long_qty:
            return {"side": "short", "qty": short_qty, "avg_px": short_px, "lev": lev}
        return None
    except:
        return None

def decide_action(policy: dict, instId: str, pos: dict, last_px: float):
    """
    简单规则（先跑通）：达到阈值就加/减仓；未持仓 / 价格异常 -> 无操作
    返回 (action or None, info_str)
    """
    if not pos or last_px <= 0:
        return None, "no_position_or_no_price"

    side, qty, avg_px = pos["side"], float(pos["qty"]), float(pos["avg_px"] or 0.0)
    lev = float(pos.get("lev") or 0.0)

    # 浮盈/亏百分比
    if side == "long":
        pnl_pct = (last_px / max(1e-9, avg_px)) - 1.0
    else:  # short
        pnl_pct = (avg_px / max(1e-9, last_px)) - 1.0

    target     = float(policy["TARGET_MOVE"])
    step_in    = float(policy["PYRAMID_STEP_PCT"])
    step_out   = float(policy["REDUCE_STEP_PCT"])
    base_budget= float(policy["BASE_BUDGET"])
    min_notional = float(policy.get("MIN_DELTA_USDT", 3.0))
    min_lev, max_lev = float(policy["MIN_LEV"]), float(policy["MAX_LEV"])
    max_layers = int(policy["MAX_LAYERS"])

    action = None

    # 盈利达到阈值 -> 减仓一层
    if pnl_pct >= target and qty > 0:
        notional  = max(min_notional, base_budget * step_out)
        delta_qty = round(notional / last_px, 6)
        action = {
            "type": "scale_out",
            "delta_qty": min(delta_qty, qty),
            "new_lev": max(min_lev, min(max_lev, lev or min_lev)),
            "pnl_pct": pnl_pct
        }

    # 亏损达到阈值 -> 加仓一层（网格思路）
    elif pnl_pct <= -target:
        # 简单限制：不超过 max_layers * (base_budget / price)
        layer_cap = max_layers * (base_budget / max(last_px, 1e-6))
        if qty < layer_cap:
            notional  = max(min_notional, base_budget * step_in)
            delta_qty = round(notional / last_px, 6)
            action = {
                "type": "scale_in",
                "delta_qty": delta_qty,
                "new_lev": max(min_lev, min(max_lev, lev or min_lev)),
                "pnl_pct": pnl_pct
            }

    return action, f"pnl_pct={pnl_pct:.4f}"

# ---------- 动态盯盘列表 ----------
def resolve_watch_list(trader: OKXTrader, cfg: dict, prev: List[str] | None = None) -> List[str]:
    insts = set(cfg.get("INST_LIST", []))

    # 环境变量临时扩展
    extra = os.getenv("PM_EXTRA_INST", "")
    if extra:
        insts |= {s.strip().upper() for s in extra.split(",") if s.strip()}

    # 自动并入当前持仓
    auto_on = os.getenv("PM_AUTO_FOLLOW", "0") == "1" or bool(cfg.get("AUTO_FOLLOW"))
    if auto_on:
        try:
            for p in trader.get_positions() or []:
                if _safe_float(p.get("qty")) > 0:
                    insts.add(p["instId"])
        except Exception as e:
            log(f"[warn] auto_follow failed: {e}")

    max_watch = int(cfg.get("MAX_WATCH", 8))
    watch = sorted(insts)[:max_watch]
    if prev is None or watch != prev:
        log(f"[watch] {prev or []} -> {watch}")
    return watch

# ---------- 主流程 ----------
def main():
    log("position_manager started.")
    policy = load_policy()
    log(f"policy_file={policy_path()} | "
        f"BASE_BUDGET={policy['BASE_BUDGET']} "
        f"STEP_IN={policy['PYRAMID_STEP_PCT']} STEP_OUT={policy['REDUCE_STEP_PCT']} "
        f"TARGET_MOVE={policy['TARGET_MOVE']} MAX_LAYERS={policy['MAX_LAYERS']}")

    t = OKXTrader()
    prev_watch: List[str] | None = None

    while True:
        acted = False
        watch = resolve_watch_list(t, policy, prev_watch)
        prev_watch = watch

        for instId in watch:
            try:
                raw_pos = t.get_positions(instId)  # 你已热修复：paper 下通常返回 []
                pos     = _agg_position(raw_pos)
                last_px = _last_price(t, instId)

                if not pos:
                    if VERBOSE:
                        log(f"[hb] {instId} positions=0 | no position")
                    continue

                action, info = decide_action(policy, instId, pos, last_px)
                state = {
                    "last_px": last_px,
                    "pos": pos,
                    "policy": {
                        "TARGET_MOVE": policy["TARGET_MOVE"],
                        "STEP_IN": policy["PYRAMID_STEP_PCT"],
                        "STEP_OUT": policy["REDUCE_STEP_PCT"],
                        "MAX_LAYERS": policy["MAX_LAYERS"],
                        "BASE_BUDGET": policy["BASE_BUDGET"]
                    }
                }

                if action:
                    if DRY_RUN:
                        log(f"[dry-run] {instId} -> {action} | {info}")
                    else:
                        # === 接交易所下单/调杠杆（留钩子；等你把 OKX 实现接上）===
                        # t.set_leverage(instId, action['new_lev'])
                        # if action['type'] == 'scale_in':
                        #     t.place_order(instId, 'buy' if pos['side']=='long' else 'sell', action['delta_qty'])
                        # elif action['type'] == 'scale_out':
                        #     t.place_order(instId, 'sell' if pos['side']=='long' else 'buy', action['delta_qty'])
                        log(f"[live] {instId} EXEC -> {action} | {info}")

                    # 记经验（reward 先置 0，后续可用真实 pnl/回撤等）
                    add_experience(instId, state, action, reward=0.0,
                                   info="dry-run" if DRY_RUN else "live")
                    acted = True
                else:
                    if VERBOSE:
                        log(f"[hb] {instId} positions=1 | {info}")

            except Exception as e:
                log(f"[err] {instId} loop: {e}")

        time.sleep(2 if acted else 5)

if __name__ == "__main__":
    main()

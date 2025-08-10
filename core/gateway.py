# core/gateway.py
from __future__ import annotations

import math
from typing import Optional, List, Dict, Any

# 统一用包内相对导入
from .okx_trader import OKXTrader


class OkxGateway:
    """
    交易网关（实盘）：对上层暴露统一接口；内部调用 OKXTrader。
    """
    def __init__(self):
        self.t = OKXTrader()

    # —— 查询类 —— #
    def get_positions(self) -> List[Dict[str, Any]]:
        return self.t.get_positions() or []

    def get_ticker(self, instId: str) -> Dict[str, Any]:
        return self.t.get_ticker(instId) or {}

    def cancel_all(self, instId: str, tdMode: str = "cross") -> List[Dict[str, Any]]:
        return self.t.cancel_all_orders(instId, tdMode=tdMode)

    # —— 下单封装（支持 USDT 预算）—— #
    def _budget_to_size(self, instId: str, usdt: float, lev: int = 10) -> str:
        """
        用 USDT 预算换算成合约 sz（自动按 minSz/lotSz 规整）。
        名义金额 = 预算 * 杠杆；sz = 名义金额 / 最新价 / ctVal，
        然后向下取到 lotSz 的整数倍并确保 >= minSz。
        """
        # 最新价
        ticker = self.get_ticker(instId)
        last = float(ticker.get("last", ticker.get("lastPx", 0)) or 0)
        if last <= 0:
            raise ValueError(f"ticker 无效: {ticker}")

        # 合约规格
        meta = self.t._get_inst_meta(instId)  # 已在 OKXTrader 中实现
        ctVal = float(meta["ctVal"])
        lotSz = float(meta["lotSz"])
        minSz = float(meta["minSz"])

        # 预算 -> 张数
        notional = float(usdt) * float(lev)
        denom = last * (ctVal if ctVal > 0 else 1.0)
        raw_sz = notional / denom

        # 规整到 lotSz 的倍数
        k = math.floor(raw_sz / lotSz)
        sz = k * lotSz
        if sz < minSz:
            sz = 0.0

        # OKX 要求字符串
        return f"{sz:.8f}".rstrip("0").rstrip(".") if sz else "0"

    def open_market(
        self,
        instId: str,
        side: str,                       # 'buy' | 'sell'
        sz: Optional[str] = None,        # 直接给数量（字符串）
        *,
        usdt: Optional[float] = None,    # 或者给预算（推荐）
        lev: int = 10,
        tdMode: str = "cross",
        posSide: Optional[str] = None,   # 多空分离时指定 'long' | 'short'
        tp: Optional[float] = None,
        sl: Optional[float] = None,
        reduceOnly: bool = False,
        clOrdId: Optional[str] = None
    ) -> Dict[str, Any]:
        # 预算自动换算
        if (not sz) and (usdt is not None):
            sz = self._budget_to_size(instId, usdt, lev=lev)

        if not sz or float(sz) <= 0:
            return {"code": "SIZE_ZERO", "msg": f"计算得到的下单张数无效（instId={instId}, sz={sz})"}

        return self.t.open_order(
            instId=instId,
            side=side,
            sz=sz,
            lever=lev,
            tdMode=tdMode,
            posSide=posSide,
            ordType="market",
            tp=tp,
            sl=sl,
            reduceOnly=reduceOnly,
            clOrdId=clOrdId
        )

    def set_tp_sl(
        self,
        instId: str,
        sz: Optional[str] = None,        # 不传则自动使用该边持仓可用张数
        *,
        posSide: Optional[str] = None,   # 多空分离时指定 'long' | 'short'
        tp: Optional[float] = None,
        sl: Optional[float] = None,
        trailing_ratio: Optional[float] = None,  # 0.01 表示 1% 回撤触发
        tdMode: str = "cross",
        trigger_px_type: str = "last"    # 'last' | 'index' | 'mark'（目前 OKXTrader 内部默认 last）
    ) -> List[Dict[str, Any]]:
        # 注意这里参数名要对齐 OKXTrader.set_tp_sl 的定义：trailing_ratio
        return self.t.set_tp_sl(
            instId=instId,
            sz=sz,
            tp=tp,
            sl=sl,
            trailing_ratio=trailing_ratio,
            tdMode=tdMode,
            posSide=posSide,
            # trigger_px_type 可在 OKXTrader 内部默认使用 'last'
        )

    def reduce_by(
        self,
        instId: str,
        posSide: str,                     # 'long' | 'short'
        sz: str,                          # 字符串，已规整后的张数
        *,
        tdMode: str = "cross",
        lev: int = 10
    ) -> Dict[str, Any]:
        side = "sell" if posSide == "long" else "buy"
        return self.open_market(
            instId,
            side,
            sz=sz,
            lev=lev,
            tdMode=tdMode,
            posSide=posSide,
            reduceOnly=True
        )

    def close_all(self, instId: str, *, posSide: Optional[str] = None, tdMode: str = "cross") -> Dict[str, Any]:
        return self.t.close_all_positions(instId, mgnMode=tdMode, posSide=posSide)


class PaperGateway(OkxGateway):
    """
    复用 OkxGateway 的逻辑；以后如果接入你自己的撮合/回测，
    在这里把父类的方法替换成 simulator 的调用即可。
    """
    pass

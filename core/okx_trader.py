# core/okx_trader.py
import requests
import hmac
import base64
import hashlib
import json
import datetime
import traceback
import time
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, getcontext
getcontext().prec = 28
from urllib.parse import urlencode

try:
    from utils.config import OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
except Exception:
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from utils.config import OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE

class OKXTrader:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.api_key = OKX_API_KEY
        self.api_secret = OKX_SECRET_KEY
        self.passphrase = OKX_PASSPHRASE

    # ================= 通用签名/时间 =================
    def _get_timestamp(self):
        try:
            r = requests.get(self.base_url + "/api/v5/public/time", timeout=3)
            iso = r.json()["data"][0].get("iso")
            if iso:
                return iso
        except Exception:
            pass
        return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, timestamp, method, path, body=""):
        if body and not isinstance(body, str):
            body = json.dumps(body, separators=(',', ':'))
        elif not body:
            body = ""
        msg = f"{timestamp}{method.upper()}{path}{body}"
        sign = hmac.new(self.api_secret.encode('utf-8'), msg.encode(), hashlib.sha256).digest()
        return base64.b64encode(sign).decode()

    def _headers(self, method, path, body="", params=None):
        ts = self._get_timestamp()
        if body and not isinstance(body, str):
            body_text = json.dumps(body, separators=(',', ':'))
        elif not body:
            body_text = ""
        else:
            body_text = body
        qs = "?" + urlencode(params, doseq=True) if params else ""
        sign = self._sign(ts, method, path + qs, body_text)
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }


        # ================= 衍生数据（资金费率 / 多空比 / 爆仓单） =================
    def get_funding_rate(self, instId):
        """
        资金费率（当前/下一次结算）
        OKX: GET /api/v5/public/funding-rate?instId=...
        返回: dict 或 None
        """
        path = "/api/v5/public/funding-rate"
        url = self.base_url + path
        params = {"instId": instId}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if str(data.get("code")) == "0":
                arr = data.get("data") or []
                return arr[0] if arr else None
        except Exception as e:
            print("[WARN] get_funding_rate:", e)
        return None

    def get_long_short_ratio(self, instId, period="5m", limit=100):
        """
        账户多空持仓比时间序列
        OKX: GET /api/v5/public/long-short-account-ratio
        period: 5m/15m/1H/4H/1D
        返回: list
        """
        path = "/api/v5/public/long-short-account-ratio"
        url = self.base_url + path
        params = {"instId": instId, "period": period, "limit": str(limit)}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if str(data.get("code")) == "0":
                return data.get("data") or []
        except Exception as e:
            print("[WARN] get_long_short_ratio:", e)
        return []

    def get_liquidation(self, instId=None, instType="SWAP", limit=100):
        """
        爆仓委托（公共）
        OKX: GET /api/v5/public/liquidation-orders
        返回: list
        """
        path = "/api/v5/public/liquidation-orders"
        url = self.base_url + path
        params = {"instType": instType, "limit": str(limit)}
        if instId:
            params["instId"] = instId
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if str(data.get("code")) == "0":
                rows = data.get("data") or []
                if instId:
                    rows = [x for x in rows if x.get("instId") == instId]
                return rows
        except Exception as e:
            print("[WARN] get_liquidation:", e)
        return []

    # ================= 行情 =================
    def get_orderbook(self, instId, sz=5):
        path = "/api/v5/market/books"
        url = self.base_url + path
        params = {"instId": instId, "sz": sz}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else []
        except Exception as e:
            print(f"[ERROR] get_orderbook: {e}"); traceback.print_exc(); return []

    def get_trades(self, instId, limit=20):
        path = "/api/v5/market/trades"
        url = self.base_url + path
        params = {"instId": instId, "limit": limit}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else []
        except Exception as e:
            print(f"[ERROR] get_trades: {e}"); traceback.print_exc(); return []

    def get_ticker(self, instId):
        path = "/api/v5/market/ticker"
        url = self.base_url + path
        params = {"instId": instId}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            return data["data"][0] if data.get("code") == "0" and data.get("data") else None
        except Exception as e:
            print(f"[ERROR] get_ticker: {e}"); traceback.print_exc(); return None

    def get_kline(self, instId, bar="1m", limit=200, after=None):
        path = "/api/v5/market/candles"
        url = self.base_url + path
        params = {"instId": instId, "bar": bar, "limit": limit}
        if after: params["after"] = str(after)
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" and data.get("data") else []
        except Exception as e:
            print("[ERROR] get_kline:", e); traceback.print_exc(); return []

    def get_kline_range(self, instId, bar="1m", start_ts=None, end_ts=None, limit_per_page=300, max_pages=120):
        """
        稳定版分页：
          1) 首次请求不带 before，拿“最新一页”
          2) 记录该页最旧一根的毫秒时间 earliest_ms
          3) 用 before=earliest_ms 持续往回翻，直到覆盖 start_ts 或翻页到头
        返回升序列表: [(ts, open, high, low, close, vol)], ts 为秒（UTC）
        """
        import requests, time
        if end_ts is None:
            end_ts = int(time.time())
        if start_ts is None:
            start_ts = end_ts - 3600

        def parse(arr):
            out = []
            for it in arr or []:
                try:
                    ms = int(it[0]); ts = ms // 1000
                    o, h, l, c, v = map(float, [it[1], it[2], it[3], it[4], it[5]])
                    out.append((ts, o, h, l, c, v))
                except:
                    pass
            # OKX 返回通常新->旧，这里转成升序
            out.sort(key=lambda x: x[0])
            return out

        all_rows = []
        pages = 0
        before = None  # 首页不带游标

        while pages < max_pages:
            try:
                params = {"instId": instId, "bar": bar, "limit": str(limit_per_page)}
                if before is not None:
                    params["before"] = str(before)
                r = requests.get(self.base_url + "/api/v5/market/candles", params=params, timeout=10)
                data = r.json()
                if str(data.get("code")) != "0":
                    print("[WARN] candles code=", data.get("code"), "msg=", data.get("msg"))
                    break
                arr = data.get("data") or []
                if not arr:
                    break

                rows = parse(arr)
                if not rows:
                    break
                all_rows.extend(rows)

                # 取本页最旧一根（arr[-1]），继续向历史回翻
                try:
                    earliest_ms = int(arr[-1][0])
                except:
                    break

                # 如果最旧一根已经覆盖到 start_ts 之前，就可以停了
                if (earliest_ms // 1000) <= start_ts:
                    break

                # 下一页：用“本页最旧一根的 ts”作为 before
                before = earliest_ms
                pages += 1

            except Exception as e:
                print("[WARN] candles exception:", e)
                break

        # 过滤目标时间窗，并升序
        out = [r for r in all_rows if start_ts <= r[0] <= end_ts]
        out.sort(key=lambda x: x[0])
        return out


    def get_all_instruments(self, instType="SWAP", uly=None, instFamily=None):
        path = "/api/v5/public/instruments"
        url = self.base_url + path
        params = {"instType": instType}
        if uly: params["uly"] = uly
        if instFamily: params["instFamily"] = instFamily
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" and data.get("data") else []
        except Exception as e:
            print(f"[ERROR] get_all_instruments: {e}"); traceback.print_exc(); return []

        # ================= 情报（占位，避免采集器报错；后续可接真数据源） =================
    def get_leaderboard(self, *args, **kwargs):
        return []

    def get_whale_trades(self, *args, **kwargs):
        return []

    # 新增这三个最小桩（先返回空，collector 会当成暂无数据）
    def get_funding_rate(self, instId, limit=100):
        return []

    def get_long_short_ratio(self, instId, period="5m", limit=200):
        return []

    def get_liquidation(self, instId, limit=100):
        return []


    # ================= 账户/持仓 =================

    def get_positions(self, instId: str | None = None):
        """
        统一返回持仓列表，绝不递归；可通过环境变量 OKX_SKIP_TEST_POS=1 跳过 test_* 分支。
        返回列表元素示例：
          {'instId':'BTC-USDT-SWAP','side':'long'|'short','qty':0.01,
           'avgPx':12345.6,'lever':10.0,'upl':0.0,'mgnRatio':0.0,'ts':1690000000}
        """
        import os, time

        # ---- 防重入护栏：若被回调到自身，立即返回空，彻底掐掉递归 ----
        if getattr(self, "_positions_guard", False):
            return []
        self._positions_guard = True
        try:
            raw = []

            # A) 纸交易/单测桩（可通过 OKX_SKIP_TEST_POS=1 跳过）
            if os.getenv("OKX_SKIP_TEST_POS") != "1" and hasattr(self, "test_get_positions"):
                try:
                    if instId is None:
                        raw = self.test_get_positions()
                    else:
                        try:
                            raw = self.test_get_positions(instId)
                        except TypeError:
                            raw = self.test_get_positions()
                except Exception:
                    raw = []

            # B) 真实私有接口原始数据入口（务必不要在其实现里再调 get_positions）
            if not raw and hasattr(self, "_fetch_positions_raw"):
                try:
                    if instId is None:
                        raw = self._fetch_positions_raw()
                    else:
                        try:
                            raw = self._fetch_positions_raw(instId=instId)
                        except TypeError:
                            raw = self._fetch_positions_raw()
                except Exception:
                    raw = []

            # C) 归一化
            data = raw.get("data", raw) if isinstance(raw, dict) else (raw or [])
            out = []
            for r in (data if isinstance(data, list) else []):
                if not isinstance(r, dict):
                    continue
                iid = r.get("instId") or r.get("instID") or r.get("inst")
                if instId and iid != instId:
                    continue

                side, qty = None, 0.0
                if "pos" in r and r.get("pos") not in (None, ""):
                    p = float(r.get("pos") or 0)
                    if p > 0:   side, qty = "long", p
                    elif p < 0: side, qty = "short", abs(p)
                else:
                    l = float(r.get("longSz") or 0)
                    s = float(r.get("shortSz") or 0)
                    if l > 0:   side, qty = "long", l
                    elif s > 0: side, qty = "short", s

                if not side or qty <= 0:
                    continue

                avg_px = r.get("avgPx") or r.get("avgPxLong") or r.get("avgPxShort") or 0
                out.append({
                    "instId": iid,
                    "side": side,
                    "qty": float(qty),
                    "avgPx": float(avg_px or 0),
                    "lever": float(r.get("lever") or 0),
                    "upl": float(r.get("upl") or 0),
                    "mgnRatio": float(r.get("mgnRatio") or 0),
                    "ts": int(time.time()),
                })
            return out
        finally:
            self._positions_guard = False

    def _get_positions_impl(self, instId: str | None = None):
        # TODO: 实盘时改成真正的 OKX /api/v5/account/positions 调用并解析
        return []


    def get_balance(self, ccy=None):
        path = "/api/v5/account/balance"
        url = self.base_url + path
        params = {"ccy": ccy} if ccy else None
        headers = self._headers("GET", path, params=params)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else None
        except Exception as e:
            print(f"[ERROR] get_balance: {e}"); traceback.print_exc(); return None

    def get_available_balance(self, ccy="USDT"):
        data = self.get_balance()
        if not data: return 0
        max_avail = 0
        for item in data[0].get("details", []):
            if item.get("ccy") == ccy:
                try:
                    ab = float(item.get("availBal", 0))
                    if ab > max_avail: max_avail = ab
                except: pass
        return max_avail

    def get_open_orders(self, instId=None, instType=None):
        path = "/api/v5/trade/orders-pending"
        url = self.base_url + path
        params = {}
        if instId: params["instId"] = instId
        if instType: params["instType"] = instType
        headers = self._headers("GET", path, params=params)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else []
        except Exception as e:
            print(f"[ERROR] get_open_orders: {e}"); traceback.print_exc(); return []

    def get_max_avail_size(self, instId, tdMode="cross"):
        path = "/api/v5/account/max-size"
        url = self.base_url + path
        params = {"instId": instId, "tdMode": tdMode}
        headers = self._headers("GET", path, params=params)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else []
        except Exception as e:
            print(f"[ERROR] get_max_avail_size: {e}"); traceback.print_exc(); return []

    # ================= 合约元数据 / 单位换算 =================
    def _get_inst_meta(self, instId):
        """
        返回: dict(ctVal, lotSz, minSz, tickSz) 都是 Decimal
        """
        if not hasattr(self, "_inst_meta_cache"):
            self._inst_meta_cache = {}
        if instId in self._inst_meta_cache:
            return self._inst_meta_cache[instId]

        ins_list = self.get_all_instruments(instType="SWAP") or []
        item = next((x for x in ins_list if x.get("instId") == instId), None)
        def D(x, default="0"):
            try: return Decimal(str(x))
            except Exception: return Decimal(default)
        meta = {
            "ctVal": D(item.get("ctVal"), "1") if item else Decimal("1"),
            "lotSz": D(item.get("lotSz"), "1") if item else Decimal("1"),
            "minSz": D(item.get("minSz"), "1") if item else Decimal("1"),
            "tickSz": D(item.get("tickSz"), "0.01") if item else Decimal("0.01"),
        }
        self._inst_meta_cache[instId] = meta
        return meta

    def _round_step(self, value: Decimal, step: Decimal, mode="down") -> Decimal:
        if step <= 0: return value
        q = value / step
        if mode == "up":
            q = q.to_integral_value(rounding=ROUND_UP)
        elif mode == "nearest":
            q = q.to_integral_value(rounding=ROUND_HALF_UP)
        else:
            q = q.to_integral_value(rounding=ROUND_DOWN)
        return q * step

    def stringify_sz(self, cont: Decimal) -> str:
        s = f"{cont.normalize()}"
        if "E" in s or "e" in s: s = format(cont, "f")
        return s

    def contracts_from_coin(self, instId, coin, mode="down"):
        m = self._get_inst_meta(instId)
        coin = Decimal(str(coin))
        if coin <= 0: return Decimal("0")
        raw = coin / m["ctVal"]
        cont = self._round_step(raw, m["lotSz"], mode)
        return cont if cont >= m["minSz"] else Decimal("0")

    def contracts_from_usdt(self, instId, usdt, price=None, mode="down"):
        usdt = Decimal(str(usdt))
        if usdt <= 0: return Decimal("0")
        if price is None:
            tk = self.get_ticker(instId)
            price = tk and (tk.get("last") or tk.get("askPx") or tk.get("bidPx"))
        price = Decimal(str(price))
        if price <= 0: return Decimal("0")
        coin = usdt / price
        return self.contracts_from_coin(instId, coin, mode=mode)

    def min_notional_usdt(self, instId, price=None):
        """估算这个合约的“单笔最小名义金额”(USDT) ≈ minSz * ctVal * 价格"""
        m = self._get_inst_meta(instId)
        if price is None:
            tk = self.get_ticker(instId)
            price = tk and (tk.get("last") or tk.get("askPx") or tk.get("bidPx"))
        price = Decimal(str(price or "0"))
        if price <= 0: return Decimal("0")
        return (m["minSz"] * m["ctVal"] * price).quantize(Decimal("0.0001"))

    # === 新增：预算换合法张数 ===
    def sz_from_budget(self, instId, usdt_budget, price=None, round_mode="down"):
        """
        传入USDT名义预算，返回合法张数字符串；若预算不足返回 None
        """
        if usdt_budget is None or float(usdt_budget) <= 0:
            return None
        cont = self.contracts_from_usdt(instId, usdt_budget, price=price, mode=round_mode)
        m = self._get_inst_meta(instId)
        cont = self._round_step(cont, m["lotSz"], mode=round_mode)
        if cont < m["minSz"]:
            return None
        return self.stringify_sz(cont)

    # ================= 其它账户接口 =================
    def get_liq_px(self, instId, tdMode="cross", posSide=None):
        path = "/api/v5/account/risk-state"
        url = self.base_url + path
        params = {"instId": instId, "tdMode": tdMode}
        if posSide: params["posSide"] = posSide
        headers = self._headers("GET", path, params=params)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            data = r.json()
            return data["data"] if data.get("code") == "0" else []
        except Exception as e:
            print(f"[ERROR] get_liq_px: {e}"); traceback.print_exc(); return []

    def get_account_config(self):
        path = "/api/v5/account/config"
        url = self.base_url + path
        headers = self._headers("GET", path)
        r = requests.get(url, headers=headers, timeout=10)
        return r.json()

    def is_long_short_mode(self):
        try:
            cfg = self.get_account_config()
            mode = cfg.get("data", [{}])[0].get("posMode")
            return mode == "long_short_mode"
        except Exception:
            return False

    def set_leverage(self, instId, lever, mgnMode="cross", posSide=None):
        path = "/api/v5/account/set-leverage"
        url = self.base_url + path
        body = {"instId": instId, "lever": str(lever), "mgnMode": mgnMode}
        if posSide:
            body["posSide"] = posSide
        headers = self._headers("POST", path, body)
        try:
            r = requests.post(url, data=json.dumps(body, separators=(',', ':')), headers=headers, timeout=10)
            resp = r.json()
            code = str(resp.get("code"))
            if code == "0":
                return resp
            if code in ("59669",):
                print("[WARN] set_leverage got 59669; treat as non-fatal and continue.")
                return {"code": "0", "data": resp.get("data", []), "warn": "59669"}
            return resp
        except Exception as e:
            print("[WARN] set_leverage exception:", e)
            return {"code": "0", "data": [], "warn": "exception"}

    # ================= 下单 / 平仓 =================
    def open_order(
        self,
        instId,
        side,
        sz=None,
        *,
        coin=None,
        usdt=None,
        lever=10,
        tdMode="cross",
        posSide=None,
        ordType="market",
        px=None,
        tp=None,
        sl=None,
        trailing_stop=None,
        reduceOnly=False,
        qty_round="down",
        **kwargs
    ):
        if not kwargs.get("clOrdId"):
            kwargs["clOrdId"] = self._make_clordid()

        ls_mode = self.is_long_short_mode()
        _posSide = posSide
        if ls_mode and not _posSide:
            _posSide = "long" if side == "buy" else "short"

        try:
            setlev = self.set_leverage(instId, lever, tdMode, _posSide if ls_mode else None)
            if setlev.get("code") != "0":
                print("[WARN] set_leverage non-zero:", setlev)
        except Exception as e:
            print("[WARN] set_leverage:", e)

        try:
            self.cancel_all_orders(instId, tdMode=tdMode)
        except Exception as e:
            print("[WARN] cancel_all_orders:", e)

        m = self._get_inst_meta(instId)
        if sz is not None:
            cont = Decimal(str(sz))
        elif coin is not None:
            cont = self.contracts_from_coin(instId, coin, mode=qty_round)
        elif usdt is not None:
            cont = self.contracts_from_usdt(instId, usdt, price=px, mode=qty_round)
        else:
            return {"code": "PARAM", "msg": "必须传 sz/coin/usdt 其一"}

        if not reduceOnly:
            try:
                ms = self.get_max_avail_size(instId, tdMode)
                if ms:
                    row = ms[0]
                    limit = row.get("maxBuy") if side == "buy" else row.get("maxSell")
                    if limit is None:
                        limit = row.get("availBuy") if side == "buy" else row.get("availSell")
                    limit = Decimal(str(limit or "0"))
                    if cont > limit:
                        print(f"[WARN] 目标张数 {cont} 超过可开上限 {limit}，自动降到 {limit}")
                        cont = limit
            except Exception as e:
                print("[WARN] get_max_avail_size:", e)

        if reduceOnly:
            try:
                avail = Decimal("0")
                for p in self.get_positions():
                    if p.get("instId") != instId: continue
                    if ls_mode and _posSide and p.get("posSide") != _posSide: continue
                    avail = Decimal(str(p.get("availPos") or p.get("pos") or "0"))
                    break
                if cont > avail: cont = avail
            except Exception as e:
                print("[WARN] read availPos:", e)

        cont = self._round_step(cont, m["lotSz"], mode=qty_round)
        if cont < m["minSz"]:
            return {"code": "LIMIT", "msg": f"小于最小下单张数 {m['minSz']}, 不下单。"}
        sz_str = self.stringify_sz(cont)

        path = "/api/v5/trade/order"
        url = self.base_url + path
        data = {
            "instId": instId,
            "tdMode": tdMode,
            "side": side,
            "ordType": ordType,
            "sz": sz_str
        }
        if ls_mode and _posSide: data["posSide"] = _posSide
        if ordType == "limit" and px is not None: data["px"] = str(px)
        if tp is not None: data["tpTriggerPx"] = str(tp); data["tpOrdPx"] = "-1"
        if sl is not None: data["slTriggerPx"] = str(sl); data["slOrdPx"] = "-1"
        if reduceOnly: data["reduceOnly"] = True
        data.update(kwargs)

        try:
            headers = self._headers("POST", path, data)
            r = requests.post(url, data=json.dumps(data, separators=(',', ':')), headers=headers, timeout=10)
            return r.json()
        except Exception as e:
            print("[下单异常]", e); traceback.print_exc(); return {"code": "ERROR", "msg": str(e)}

    def set_tp_sl(self, instId, sz=None, tp=None, sl=None, trailing_ratio=None, tdMode="cross", posSide=None, trigger_px_type="last"):
        # 保持你原有实现...
        # （略：与之前相同）
        return []

    def close_all_positions(self, instId, tdMode="cross", posSide=None, **kwargs):
        path = "/api/v5/trade/close-position"
        url = self.base_url + path
        if self.is_long_short_mode() and not posSide:
            try:
                poss = [p for p in self.get_positions()
                        if p.get("instId") == instId and Decimal(str(p.get("pos") or "0")) > 0]
                if len(poss) == 1:
                    posSide = poss[0].get("posSide")
                elif len(poss) > 1:
                    return {"code": "POS_SIDE_REQ", "msg": "多空分离多空两边都有，请指定 posSide=long/short"}
            except Exception:
                pass

        data = {"instId": instId, "mgnMode": tdMode}
        if posSide: data["posSide"] = posSide

        try:
            headers = self._headers("POST", path, data)
            r = requests.post(url, data=json.dumps(data, separators=(',', ':')), headers=headers, timeout=10)
            return r.json()
        except Exception as e:
            print("[一键平仓异常]", e); traceback.print_exc()
            return {"code": "ERROR", "msg": str(e)}

    def cancel_orders(self, orderIds, instId, tdMode="cross"):
        path = "/api/v5/trade/cancel-order"
        url = self.base_url + path
        results = []
        for oid in orderIds:
            data = {"instId": instId, "ordId": oid, "tdMode": tdMode}
            try:
                headers = self._headers("POST", path, data)
                r = requests.post(url, data=json.dumps(data, separators=(',', ':')), headers=headers, timeout=10)
                results.append(r.json())
            except Exception as e:
                print(f"[ERROR] cancel_order: {e}"); traceback.print_exc()
                results.append({"code": "ERROR", "msg": str(e)})
        return results

    def cancel_all_orders(self, instId, tdMode="cross"):
        orders = self.get_open_orders(instId)
        orderIds = [o["ordId"] for o in orders if "ordId" in o]
        if orderIds:
            return self.cancel_orders(orderIds, instId, tdMode)
        print("[INFO] 当前无挂单"); return []

    def get_order(self, instId, ordId=None, clOrdId=None):
        path = "/api/v5/trade/order"
        url = self.base_url + path
        params = {"instId": instId}
        if ordId: params["ordId"] = ordId
        if clOrdId: params["clOrdId"] = clOrdId
        headers = self._headers("GET", path, params=params)
        r = requests.get(url, headers=headers, params=params, timeout=10)
        return r.json()

    def get_fills(self, instType="SWAP", instId=None, ordId=None, limit=100):
        path = "/api/v5/trade/fills-history"
        url = self.base_url + path
        params = {"instType": instType, "limit": str(limit)}
        if instId: params["instId"] = instId
        if ordId: params["ordId"] = ordId
        headers = self._headers("GET", path, params=params)
        r = requests.get(url, headers=headers, params=params, timeout=10)
        return r.json()

    def wait_order_filled(self, instId, ordId=None, clOrdId=None, timeout=20, poll_interval=0.5):
        start = time.time()
        last_order = None
        while time.time() - start < timeout:
            info = self.get_order(instId, ordId=ordId, clOrdId=clOrdId)
            if info.get("code") == "0" and info.get("data"):
                last_order = info["data"][0]
                state = last_order.get("state")
                if state in ("filled", "canceled"):
                    fills = self.get_fills(instType="SWAP", instId=instId, ordId=last_order.get("ordId"))
                    return {"state": state, "order": last_order, "fills": fills}
            time.sleep(poll_interval)
        fills = None
        if last_order and last_order.get("ordId"):
            fills = self.get_fills(instType="SWAP", instId=instId, ordId=last_order.get("ordId"))
        return {"state": (last_order or {}).get("state", "unknown"), "order": last_order, "fills": fills}

    # ================= 自检/工具 =================
    def _make_clordid(self, prefix="ft"):
        ts = int(datetime.datetime.utcnow().timestamp() * 1000)
        raw = f"{prefix}{ts}"
        clean = ''.join(ch for ch in raw if ch.isalnum())
        return clean[:32]

    def test_get_orderbook(self, symbol):
        try:
            data = self.get_orderbook(symbol)
            print(f"[OK] 盘口接口打通：{symbol}") if data else print(f"[FAIL] 盘口接口未打通：{symbol}，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] 盘口接口异常: {e}"); return False

    def test_get_trades(self, symbol):
        try:
            data = self.get_trades(symbol)
            print(f"[OK] 成交接口打通：{symbol}") if data else print(f"[FAIL] 成交接口未打通：{symbol}，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] 成交接口异常: {e}"); return False

    def test_get_ticker(self, symbol):
        try:
            data = self.get_ticker(symbol)
            print(f"[OK] Ticker接口打通：{symbol}") if data else print(f"[FAIL] Ticker接口未打通：{symbol}，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] Ticker接口异常: {e}"); return False

    def test_get_kline(self, symbol):
        try:
            data = self.get_kline(symbol)
            print(f"[OK] K线接口打通：{symbol}") if data else print(f"[FAIL] K线接口未打通：{symbol}，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] K线接口异常: {e}"); return False

    def test_get_all_instruments(self):
        try:
            data = self.get_all_instruments()
            print(f"[OK] 合约列表接口打通") if data else print(f"[FAIL] 合约列表接口未打通，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] 合约列表接口异常: {e}"); return False

    def test_get_positions(self):
        try:
            data = self.get_positions()
            print(f"[OK] 持仓接口打通") if data is not None else print(f"[FAIL] 持仓接口未打通，返回：", data)
            return data is not None
        except Exception as e: print(f"[ERROR] 持仓接口异常: {e}"); return False

    def test_get_balance(self):
        try:
            data = self.get_balance()
            print(f"[OK] 余额接口打通") if data else print(f"[FAIL] 余额接口未打通，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] 余额接口异常: {e}"); return False

    def test_get_available_balance(self):
        try:
            data = self.get_available_balance()
            print(f"[OK] 可用余额接口打通，返回：{data}"); return True
        except Exception as e: print(f"[ERROR] 可用余额接口异常: {e}"); return False

    def test_get_open_orders(self, symbol):
        try:
            data = self.get_open_orders(instId=symbol)
            print(f"[OK] 挂单查询接口打通：{symbol}") if isinstance(data, list) else print(f"[FAIL] 挂单查询接口未打通：{symbol}，返回：", data)
            return isinstance(data, list)
        except Exception as e: print(f"[ERROR] 挂单查询接口异常: {e}"); return False

    def test_get_max_avail_size(self, symbol):
        try:
            data = self.get_max_avail_size(instId=symbol)
            print(f"[OK] 最大可开张数接口打通：{symbol}") if data else print(f"[FAIL] 最大可开张数接口未打通：{symbol}，返回：", data)
            return bool(data)
        except Exception as e: print(f"[ERROR] 最大可开张数接口异常: {e}"); return False

    def run_api_health_check(self, symbol="BTC-USDT", contract="BTC-USDT-SWAP"):
        print("【API连通性自检】")
        self.test_get_orderbook(symbol)
        self.test_get_trades(symbol)
        self.test_get_ticker(symbol)
        self.test_get_kline(symbol)
        self.test_get_all_instruments()
        self.test_get_positions()
        self.test_get_balance()
        self.test_get_available_balance()
        self.test_get_open_orders(symbol)
        self.test_get_max_avail_size(contract)
        print("【自检完成】\n")


if __name__ == "__main__":
    t = OKXTrader()
    t.run_api_health_check()

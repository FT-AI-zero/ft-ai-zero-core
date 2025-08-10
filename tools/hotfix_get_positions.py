# -*- coding: utf-8 -*-
# tools/hotfix_get_positions.py
import os, re, shutil, sys

ROOT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(ROOT)  # project root
target = os.path.join(ROOT, "core", "okx_trader.py")
bak    = target + ".bak_getpos_v2"

NEW_FUNC = '''
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
'''

def main():
    if not os.path.exists(target):
        print(f"[ERR] not found: {target}")
        sys.exit(1)

    src = open(target, "r", encoding="utf-8").read()

    # 备份一次
    if not os.path.exists(bak):
        shutil.copyfile(target, bak)
        print(f"[bak] {bak}")

    # 用正则替换类中的 def get_positions(...) 块
    pat = re.compile(r"(?ms)^([ \t]*)def[ \t]+get_positions\([^\)]*\):.*?(?=^[ \t]*def[ \t]+|\Z)")
    m = pat.search(src)
    if not m:
        print("[ERR] cannot locate existing get_positions() to replace.")
        sys.exit(2)

    indent = m.group(1) or ""
    new_block = NEW_FUNC.replace("\n    ", "\n" + indent)  # 适配原缩进
    new_src = src[:m.start()] + new_block + src[m.end():]

    open(target, "w", encoding="utf-8").write(new_src)
    print("[ok] patched get_positions() -> core/okx_trader.py")

if __name__ == "__main__":
    main()

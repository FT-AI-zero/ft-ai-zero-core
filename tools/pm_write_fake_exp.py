# tools/pm_write_fake_exp.py
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.pm_experience import add_experience

instId = "BTC-USDT-SWAP"
state = {
    "last_px": 116000,
    "pos": {"side":"long","qty":0.1,"avg_px":115500,"lev":10},
    "policy": {"TARGET_MOVE":0.01,"STEP_IN":0.005,"STEP_OUT":0.007,"MAX_LAYERS":4,"BASE_BUDGET":10}
}
action = {"type":"scale_out","delta_qty":0.005,"new_lev":10,"pnl_pct":0.0045}

add_experience(instId, state, action, reward=0.0, info="manual-test")
print("[ok] inserted 1 fake experience")

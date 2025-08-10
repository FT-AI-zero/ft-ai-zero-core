# core/pm_runtime.py
import os, json

def _default_path():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = os.path.join(root, "data", "runtime", "pm_policy.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p

def path():
    # 允许用环境变量自定义位置：PM_POLICY=D:\xxx\pm_policy.json
    p = os.getenv("PM_POLICY")
    if p:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p
    return _default_path()

def load():
    p = path()
    if not os.path.exists(p):
        # 初始默认策略（安全保守）
        cfg = {
            "MIN_LEV": 2.0,
            "MAX_LEV": 50.0,
            "TARGET_MOVE": 0.01,
            "PYRAMID_STEP_PCT": 0.005,
            "REDUCE_STEP_PCT": 0.008,
            "MAX_LAYERS": 4,
            "BASE_BUDGET": 10.0,
            "MIN_DELTA_USDT": 3.0,
            "BAR": "1m",
            "INST_LIST": ["BTC-USDT-SWAP"],
            "GID_DEFAULT": 9632
        }
        save(cfg)
        return cfg
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save(cfg: dict):
    p = path()
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)

# tools/bundle_state.py
import os, time, zipfile, glob
from utils.config import DATA_DIR, LOG_DIR, DB_DIR

OUT_DIR = os.path.join(DATA_DIR, "baks")
os.makedirs(OUT_DIR, exist_ok=True)

stamp = time.strftime("%Y%m%d_%H%M%S")
zip_path = os.path.join(OUT_DIR, f"state_{stamp}.zip")

keep_logs = ["trade_engine.log","zero_engine.log","position_guard.log","signal_gen.log","live_dist.log","scheduler.log"]
keep_dbs  = ["signals.db","ai_params.db","review.db","strategy_pool.db","simu_trades.db"]

def add(z, path, arcname=None):
    if os.path.exists(path):
        z.write(path, arcname or path)

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
    # 代码（*.py），不打包 venv / __pycache__
    for py in glob.glob("**/*.py", recursive=True):
        if "\\__pycache__\\" in py or "/__pycache__/" in py:
            continue
        z.write(py, py)

    # DB
    for name in keep_dbs:
        p = os.path.join(DB_DIR, name)
        add(z, p, f"data/dbs/{name}")

    # 日志
    for name in keep_logs:
        p = os.path.join(LOG_DIR, name)
        add(z, p, f"data/logs/{name}")

print("Wrote:", zip_path)

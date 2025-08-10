# -*- coding: utf-8 -*-
# jobs/runner_live_pipeline.py
import os, sys, time, datetime, pathlib, subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(str(ROOT))
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

LOG_DIR = ROOT / "data" / "logs"; LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "pipeline.log"
SLEEP_SEC = float(os.getenv("PIPELINE_SLEEP", "5"))
RETRY_STEPS = {"python -m jobs.pnl_replay --once": 3, "python -m jobs.sync_allowlist": 3}
STEPS = [
    "python -m jobs.rollup_live_trades",
    "python -m jobs.pnl_replay --once",
    "python -m jobs.promote_by_pnl_live_v2",
    "python -m jobs.sync_allowlist",
]

def log(line, also_print=True):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {line}"
    if also_print: print(msg, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f: f.write(msg + "\n")

def run_cmd(cmd: str) -> int:
    log(f"[STEP] {cmd}")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(ROOT))
        for line in p.stdout:  # 实时转发
            line = line.rstrip("\n"); print(line); f.write(line + "\n")
        p.wait()
        if p.returncode != 0: log(f"[ERROR] returncode={p.returncode}")
        return p.returncode

def run_with_retry(cmd: str):
    n = RETRY_STEPS.get(cmd, 1)
    for i in range(1, n+1):
        if run_cmd(cmd) == 0: return
        time.sleep(2*i)
    log(f"[GIVEUP] {cmd} failed after {n} attempts")

def main():
    log("START live pipeline")
    while True:
        log("ROUND start")
        for cmd in STEPS: run_with_retry(cmd)
        log(f"SLEEP {SLEEP_SEC}s"); time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()

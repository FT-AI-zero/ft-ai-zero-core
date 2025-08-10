# jobs/nightly.py
import importlib

# jobs/nightly.py（如果你刚才已经替换过就把这行加进去）
PIPELINE = [
    "jobs.clean_and_rollup",
    "jobs.rollup_live_trades",   # ← 加这一行
    "jobs.promote_by_volume",
    "jobs.promote_by_pnl_live",
    "jobs.bandit_update",
    "jobs.sync_allowlist",
]


def main():
    print(">>")
    for mod in PIPELINE:
        try:
            print(f"[RUN] {mod}")
            importlib.import_module(mod).main()
        except Exception as e:
            print(f"[ERR] {mod}: {e}")
    print("[DONE] nightly")

if __name__ == "__main__":
    main()

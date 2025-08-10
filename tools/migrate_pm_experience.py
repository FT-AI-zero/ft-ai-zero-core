# tools/migrate_pm_experience.py
import os, sys

# ---- 让脚本能直接运行时也找到项目根 ----
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.pm_experience import ensure_schema, DB_PATH

if __name__ == "__main__":
    ensure_schema()
    print(f"[migrate] pm_experience schema OK -> {DB_PATH}")

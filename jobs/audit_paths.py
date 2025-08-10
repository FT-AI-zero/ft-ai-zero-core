# tools/audit_paths.py
import os, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # 指到 okx_bot 根
seen = {}
print("[audit] scanning *.db under", ROOT)
for p, _, files in os.walk(ROOT):
    for f in files:
        if not f.lower().endswith(".db"): continue
        fp = Path(p) / f
        st = fp.stat()
        key = f.lower()
        seen.setdefault(key, []).append({
            "path": str(fp),
            "size": st.st_size,
            "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.st_mtime))
        })

for name, items in sorted(seen.items()):
    print("\n==", name, "==")
    for it in sorted(items, key=lambda x: x["path"]):
        print("  ", it["path"], "|", it["size"], "bytes |", it["mtime"])

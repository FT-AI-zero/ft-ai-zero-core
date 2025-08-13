from pathlib import Path
import io
import pandas as pd

BASE = Path(__file__).resolve().parents[1]  # 仓库根
DATA = BASE / "data"                        # 统一数据根（可按需改）

# 你可以在这里给关键数据集取键名，集中注册
REG = {
    "raw": DATA / "raw",
    "staging": DATA / "staging",
    "prod": DATA / "prod",
}

def p(key: str, *parts: str) -> Path:
    """按键取路径并拼子路径"""
    root = REG[key]
    return (root / Path(*parts)).resolve()

def ensure_parent(fp: Path):
    fp.parent.mkdir(parents=True, exist_ok=True)

# 统一读写封装（文本 / 二进制）
def open_read(fp: Path, mode="r", encoding="utf-8"):
    return io.open(fp, mode, encoding=encoding)

def open_write(fp: Path, mode="w", encoding="utf-8"):
    ensure_parent(fp)
    return io.open(fp, mode, encoding=encoding)

# pandas 封装
def read_csv(fp: Path, **kw):
    return pd.read_csv(fp, **kw)

def to_csv(df, fp: Path, index=False, **kw):
    ensure_parent(fp)
    df.to_csv(fp, index=index, **kw)

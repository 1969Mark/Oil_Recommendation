"""一次性資料清理：依 Maker 統一 Equipment 名稱。
- YANMAR / DAIHATSU：A+B 組 → DIESEL GENERATOR ENGINE
- EVERLLENCE / EVERLLENCE B&W / HIMSEN / MAK / STX / SULZER（含 STX-MAN、HHI-HIMSEN）：
    * MAIN ENGINE 變體 → MAIN ENGINE
    * DIESEL GENERATOR / AUX 變體 → DIESEL GENERATOR ENGINE
不做 dedupe，保留原 source_file 多筆紀錄。
"""
import os, re, sys
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from openpyxl import Workbook
from _common import (
    LC_MASTER, NB_MASTER, OEM_MASTER,
    LC_COLOR, NB_COLOR, OEM_COLOR,
    COLS, OEM_COLS,
    write_styled_sheet,
)

DGE = "DIESEL GENERATOR ENGINE"
ME  = "MAIN ENGINE"

# ── 規則 1：YANMAR / DAIHATSU → DGE ───────────────────────────────
YANMAR_DAIHATSU_DGE = {s: DGE for s in [
    "DIESEL GENERATOR", "3X DIESEL GENERATOR", "DIESEL GENERATOR ENGINE X3",
    "DIESEL GENERATOR 3 SETS",
    "AUXILIARY DIESEL ENGINE", "AUXILIARY ENGINE", "A/E", "A/E DIESEL",
    "AE DIESEL", "AUXILIARY ENGINE DIESEL", "A.E DIESEL", "A/E DIESEL ENGINE",
    "A.E. DIESEL", "AUXILIARY DIESEL ENGINES", "2X A/E DIESEL",
    "3X AUXILIARY DIESEL GENERATOR", "3 X AUXILIARY DIESEL ENGINE",
    "AUXILIARY ENGINE 3X",
    "MAIN GENERATOR", "MAIN GENERATOR ENGINE", "2X MAIN GENERATOR ENGINE",
    "GENERATOR ENGINE", "3 X GENERATOR ENGINE", "GENERATOR DIESEL ENGINE",
    "GENERATOR", "AUXILIARY  GENERATOR", "AUXILIARY GENERATOR",
    "AUXILIARY DIESEL GENERATOR", "A/E GENERATOR",
    "MAIN DIESEL GENERATOR ENGINE", "DIESEL GENERATOR NO.3",
    "DIESEL GENERATOR NO.1 & NO.2",
    "AUX. GENERATOR DIESEL\nENGINE & AC GENERATOR",
]}

# ── 規則 2：EVERLLENCE/B&W/HIMSEN/MAK/STX/SULZER ──────────────────
HEAVY_MAKER_PAT = re.compile(r"EVERLLENCE|HIMSEN|\bMAK\b|\bSTX\b|SULZER", re.I)

HEAVY_MAKER_ME = {s: ME for s in [
    "2 X MAIN ENGINE", "2X MAIN ENGINE",
    "MAIN ENGINGE",   # typo
    "MAIN ENGINES",
]}

HEAVY_MAKER_DGE = {s: DGE for s in [
    "DIESEL GENERATOR",
    "AUXILIARY  GENERATOR", "AUXILIARY GENERATOR",
    "AUXILIARY DIESEL ENGINE", "AUXILIARY ENGINE",
    "GENERATOR ENGINE",
    "DIESEL GENERATORS",
    "MAIN GENERATOR ENGINE",
    "DIESEL GENERATOR ENGINE (3 SETS)",
    "2X AUXILIARY ENGINE",
    "DIESEL GENERATOR X 2",
    "A/E DIESEL", "A/E DIESEL ENGINE",
    "D/G ENGINE",
    "MAIN GENERATOR",
    "D/G ENGINE\n& GENERATOR",
]}


def is_yanmar_daihatsu(v) -> bool:
    if not isinstance(v, str): return False
    s = v.upper()
    return ("YANMAR" in s) or ("DAIHATSU" in s)


def is_heavy_maker(v) -> bool:
    if not isinstance(v, str): return False
    return bool(HEAVY_MAKER_PAT.search(v))


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if "Equipment" not in df.columns or "Maker" not in df.columns:
        return df, {}
    eq = df["Equipment"].astype("object")

    # Rule 1: YANMAR / DAIHATSU
    m1 = df["Maker"].apply(is_yanmar_daihatsu) & eq.isin(YANMAR_DAIHATSU_DGE.keys())
    n1 = int(m1.sum())
    if n1: df.loc[m1, "Equipment"] = DGE

    eq = df["Equipment"].astype("object")  # refresh

    # Rule 2a: heavy maker → MAIN ENGINE
    m2a = df["Maker"].apply(is_heavy_maker) & eq.isin(HEAVY_MAKER_ME.keys())
    n2a = int(m2a.sum())
    if n2a: df.loc[m2a, "Equipment"] = ME

    eq = df["Equipment"].astype("object")

    # Rule 2b: heavy maker → DGE
    m2b = df["Maker"].apply(is_heavy_maker) & eq.isin(HEAVY_MAKER_DGE.keys())
    n2b = int(m2b.sum())
    if n2b: df.loc[m2b, "Equipment"] = DGE

    return df, {"yanmar_daihatsu_dge": n1, "heavy_me": n2a, "heavy_dge": n2b}


def write(df: pd.DataFrame, path: str, sheet: str, cols: list, color):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    write_styled_sheet(ws, df, cols, source_color=color)
    wb.save(path)


def run():
    summary = []
    targets = [
        (LC_MASTER, "lube_chart", COLS + ["Count", "Source", "source_file"], LC_COLOR),
        (NB_MASTER, "nb_master",  COLS + ["Source", "source_file"], NB_COLOR),
        (OEM_MASTER, "manual_data", OEM_COLS, OEM_COLOR),
    ]
    for path, sheet, cols, color in targets:
        df = pd.read_excel(path, sheet_name=sheet)
        df, stats = clean(df)
        write(df, path, sheet, cols, color)
        summary.append((os.path.basename(path), stats, len(df)))

    print("\n=== 清理結果 ===")
    print(f"{'master':<26}{'YN/DH→DGE':>12}{'heavy→ME':>12}{'heavy→DGE':>12}{'rows':>10}")
    for name, s, n in summary:
        print(f"{name:<26}{s.get('yanmar_daihatsu_dge',0):>12}{s.get('heavy_me',0):>12}{s.get('heavy_dge',0):>12}{n:>10}")


if __name__ == "__main__":
    run()

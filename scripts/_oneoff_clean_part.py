"""一次性清理：Part to be lubricated 單數化 + 噪音清理 + 燃料修飾詞 + 通用合併。

順序：
  1. 噪音字元清除（_x000D_、末尾連字號）
  2. CYLINDERS-HSF / CYLINDERS HS / CYLINDERS-LSF / CYLINDERS LS / -ALL 等 → 標準 fuel paren
  3. %S / ECA / VLSFO 0.1~0.5%S 等變體 → 7 種標準 keyword
  4. FUEL HFO 後綴 → (HFO)
  5. 字根單數化（CYLINDERS→CYLINDER 等）
  6. 通用 OIL/GREASE 點合併
  7. SYSTEM OIL → SYSTEM（僅獨立字串）

不做 dedupe，保留原列數。
"""
import os, re, sys
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from openpyxl import Workbook
from _common import (
    LC_MASTER, NB_MASTER, OEM_MASTER,
    LC_COLOR, NB_COLOR, OEM_COLOR,
    COLS, OEM_COLS, write_styled_sheet,
)

SINGULARIZE = [
    "CYLINDERS", "BEARINGS", "SEALS", "GEARS", "ROPES", "POINTS",
    "MOTORS", "PUMPS", "VALVES", "SHAFTS", "COUPLINGS", "PISTONS",
    "HOSES", "BLOCKS", "HOOKS", "ENGINES", "GENERATORS", "NIPPLES",
    "BUSHINGS", "BOLTS", "CABLES", "COMPRESSORS",
]

# 通用油點（合併後皆為 OIL POINT / GREASE POINT）
GENERIC_OIL = {"OIL POINT", "OIL FILLING", "OIL LUBRICATION", "OILER",
               "OIL BATH", "OTHER OIL POINT"}
GENERIC_GREASE = {"GREASE POINT", "GREASE LUBRICATION", "GREASE NIPPLE",
                  "LUBE POINT", "GREASE LUBRICATED BEARING"}

# 燃料修飾詞 → 標準格式
FUEL_PATTERNS = [
    # CYLINDERS-HSF, CYLINDERS-HSF (1.5~3.5%S), CYLINDERS HS
    (re.compile(r"\bCYLINDERS?\s*-\s*HSF\b\s*\([^)]*\)", re.I), "CYLINDERS (HSFO)"),
    (re.compile(r"\bCYLINDERS?\s*-\s*HSF\b", re.I), "CYLINDERS (HSFO)"),
    (re.compile(r"\bCYLINDERS?\s+HS\b(?!FO)", re.I), "CYLINDERS (HSFO)"),
    (re.compile(r"\bCYLINDERS?\s*-\s*LSF\b\s*\([^)]*\)", re.I), "CYLINDERS (LSFO)"),
    (re.compile(r"\bCYLINDERS?\s*-\s*LSF\b", re.I), "CYLINDERS (LSFO)"),
    (re.compile(r"\bCYLINDERS?\s+LS\b(?!FO)", re.I), "CYLINDERS (LSFO)"),
    (re.compile(r"\bCYLINDERS?\s*-\s*ALL\b", re.I), "CYLINDERS"),
    (re.compile(r"\bCYLINDERS?\s*-\s*HSFO\s*&\s*LSFO\b", re.I), "CYLINDERS (HSFO/LSFO)"),
    # FUEL HFO 後綴（含 _x000D_ 殘留）
    (re.compile(r"\s+FUEL\s+HFO\b", re.I), " (HFO)"),
    # %S / ECA / 範圍 → 標準 keyword
    (re.compile(r"\(\s*0\.0\s*[–\-]\s*0\.5\s*%\s*S\s*FO\s*\)", re.I), "(VLSFO)"),
    (re.compile(r"\(\s*0\.0\s*[–\-]\s*1\.5\s*%\s*S\s*FO\s*\)", re.I), "(LSFO)"),
    (re.compile(r"\(\s*1\.5\s*[–\-]\s*3\.5\s*%\s*S\s*FO\s*\)", re.I), "(HSFO)"),
    (re.compile(r"\(\s*VLSFO\s*<?\s*0\.5\s*%\s*S\s*\)", re.I), "(VLSFO)"),
    (re.compile(r"\(\s*VLSFO\s*0\.1\s*~\s*0\.5\s*%\s*S\s*\)", re.I), "(VLSFO)"),
    (re.compile(r"\(\s*0\.5\s*%\s*S\s*\)", re.I), "(VLSFO)"),
    (re.compile(r"\(\s*3\.5\s*%\s*S\s*\)", re.I), "(HSFO)"),
    (re.compile(r"\(\s*<\s*0\.1\s*%\s*S\s*-?\s*ECA\s*ZONE\s*\)", re.I), "(ULSFO)"),
    (re.compile(r"\(\s*0\.1\s*%\s*S\s+ECA\s*FUEL\s*\)", re.I), "(ULSFO)"),
    (re.compile(r"\(\s*ECA\s*FUEL\s*\)", re.I), "(ULSFO)"),
    (re.compile(r"\(\s*ULSFO\s*/\s*DISTILLATE\s*\)", re.I), "(ULSFO)"),
    (re.compile(r"\(\s*MGO\s+MAX\s+0\.1\s*%\s*S\s*\)\s*-\s*REMARK\s*\d+", re.I), "(MDO/MGO)"),
    (re.compile(r"\(\s*MGO\s+MAX\s+0\.1\s*%\s*S\s*\)", re.I), "(MDO/MGO)"),
    (re.compile(r"\(\s*MGO\s*\)", re.I), "(MDO/MGO)"),
    (re.compile(r"\(\s*MDO\s*\)", re.I), "(MDO/MGO)"),
    (re.compile(r"\(\s*HFO\s*/\s*IFO\s*\)", re.I), "(HFO)"),
]

SINGULAR_PATTERNS = [(re.compile(rf"\b{w}\b"), w[:-1]) for w in SINGULARIZE]

TRAILING_DASH = re.compile(r"\s*-\s*$")
MULTI_SPACE = re.compile(r"\s{2,}")


def transform(s) -> str:
    if not isinstance(s, str):
        return s
    orig = s
    # 1. 噪音
    s = s.replace("_x000D_", " ").replace("\r", " ").replace("\n", " ").strip()
    s = TRAILING_DASH.sub("", s).strip()
    # 2-4. 燃料修飾
    for pat, repl in FUEL_PATTERNS:
        s = pat.sub(repl, s)
    # 5. 單數化
    for pat, repl in SINGULAR_PATTERNS:
        s = pat.sub(repl, s)
    # 6. 通用 OIL/GREASE 合併
    up = s.upper().strip()
    if up in GENERIC_OIL:
        s = "OIL POINT"
    elif up in GENERIC_GREASE:
        s = "GREASE POINT"
    # 7. SYSTEM OIL → SYSTEM
    if s.upper().strip() == "SYSTEM OIL":
        s = "SYSTEM"
    s = MULTI_SPACE.sub(" ", s).strip()
    return s


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if "Part to be lubricated" not in df.columns:
        return df, 0
    before = df["Part to be lubricated"].astype(str).copy()
    df["Part to be lubricated"] = before.apply(transform)
    n_changed = int((df["Part to be lubricated"] != before).sum())
    return df, n_changed


def write(df, path, sheet, cols, color):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    write_styled_sheet(ws, df, cols, source_color=color)
    wb.save(path)


def run():
    targets = [
        (LC_MASTER, "lube_chart", COLS + ["Count", "Source", "source_file"], LC_COLOR),
        (NB_MASTER, "nb_master", COLS + ["Source", "source_file"], NB_COLOR),
        (OEM_MASTER, "manual_data", OEM_COLS, OEM_COLOR),
    ]
    rows = []
    for path, sheet, cols, color in targets:
        df = pd.read_excel(path, sheet_name=sheet)
        df, n = clean(df)
        write(df, path, sheet, cols, color)
        rows.append((os.path.basename(path), n, len(df)))

    print("\n=== Part 清理結果 ===")
    print(f"{'master':<26}{'changed':>10}{'rows':>10}")
    for name, c, n in rows:
        print(f"{name:<26}{c:>10}{n:>10}")


if __name__ == "__main__":
    run()

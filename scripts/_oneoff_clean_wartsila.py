"""一次性清理：WARTSILA 變體統一為 WARTSILA。
保留 WARTSILA LIPS（CPP 子品牌，產品線不同）。
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from openpyxl import Workbook
from _common import (
    LC_MASTER, NB_MASTER, OEM_MASTER,
    LC_COLOR, NB_COLOR, OEM_COLOR,
    COLS, OEM_COLS, write_styled_sheet,
)

WARTSILA_VARIANTS = {
    "WARTSILA &",
    "WARTSILA / DALIAN",
    "WARTSILA / DOOSAN",
    "WARTSILA / HUDONG",
    "WARTSILA / MHI",
    "WARSTILA",  # 原始 PDF typo
}


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if "Maker" not in df.columns:
        return df, 0
    mask = df["Maker"].astype(str).isin(WARTSILA_VARIANTS)
    n = int(mask.sum())
    if n:
        df.loc[mask, "Maker"] = "WARTSILA"
    return df, n


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

    print("\n=== WARTSILA 統一結果 ===")
    print(f"{'master':<26}{'changed':>10}{'rows':>10}")
    for name, c, n in rows:
        print(f"{name:<26}{c:>10}{n:>10}")


if __name__ == "__main__":
    run()

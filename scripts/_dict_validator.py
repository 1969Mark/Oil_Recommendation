"""
_dict_validator.py
==================
以現有 master Excel 為字典，驗證新解析的 NB 記錄欄位是否「站對位置」。

原理：每欄（Equipment / Maker / Part to be lubricated / Lubricant）建一個已知值集合，
每筆新記錄的欄位若命中對應集合 → hit；否則 → miss。
集中於 parse_report 顯示，供人工抽查解析正確性。

使用：
    from _dict_validator import build_known_sets, annotate_records
    sets = build_known_sets()
    annotated = annotate_records(records, sets)
"""

import os
import pandas as pd
from _config import LC_MASTER, NB_MASTER, OEM_MASTER, COLS

# Master 對應的 sheet 名稱
_SHEETS = {
    LC_MASTER:  'lube_chart',
    NB_MASTER:  'nb_master',
    OEM_MASTER: 'manual_data',
}

# 內部欄位名 → master 欄位名
_FIELD_MAP = {
    '設備名稱':   'Equipment',
    '設備廠家':   'Maker',
    '潤滑部位':   'Part to be lubricated',
    '推薦潤滑油': 'Lubricant',
}


def _norm(s):
    """標準化字串：去頭尾空白、轉大寫；空值 → ''"""
    if s is None:
        return ''
    s = str(s).strip().upper()
    return '' if s in ('', 'NAN', 'NONE') else s


def build_known_sets():
    """讀取三個 master，建立 {欄位: set(已知值)}。"""
    sets = {col: set() for col in COLS}
    for path, sheet in _SHEETS.items():
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_excel(path, sheet_name=sheet)
        except Exception:
            continue
        for col in COLS:
            if col in df.columns:
                for v in df[col].dropna().astype(str):
                    n = _norm(v)
                    if n:
                        sets[col].add(n)
    return sets


def annotate_records(records, sets):
    """
    對每筆記錄加上 hit 標記欄位（_hit_<col>），值為 'Y' / 'N' / '-'。
    '-' 表示該欄為空，無法判斷。
    回傳新 list（不修改原 records）。
    """
    out = []
    for r in records:
        rr = dict(r)
        for fld_cn, col in _FIELD_MAP.items():
            v = _norm(r.get(fld_cn, ''))
            if not v:
                rr[f'_hit_{col}'] = '-'
            elif v in sets.get(col, set()):
                rr[f'_hit_{col}'] = 'Y'
            else:
                rr[f'_hit_{col}'] = 'N'
        out.append(rr)
    return out


def summarize_hits(records):
    """
    回傳 {col: (hit_count, total_non_empty)}。
    用於印出「字典命中率」摘要。
    """
    summary = {}
    for col in _FIELD_MAP.values():
        key = f'_hit_{col}'
        hits = sum(1 for r in records if r.get(key) == 'Y')
        total = sum(1 for r in records if r.get(key) in ('Y', 'N'))
        summary[col] = (hits, total)
    return summary

"""
_filters.py
===========
共用過濾規則：供 process_lube_chart.py / process_nb.py / process_oem.py 使用。
"""

import re
from collections import Counter

INVALID_MODEL = {
    '', '.', '..', '-', '--',
    'N/A', 'NA', 'NONE',
    'TO BE DETERMINED', 'TBD',
}

_QTY_PATTERN = re.compile(
    r'^\(?\s*(?:'
    r'\d+\s*(?:SETS?|UNITS?|PCS?|EA|NOS?|X)?'
    r'|X\s*\d+'
    r')\s*\)?$'
)


def is_quantity_only(model) -> bool:
    """判斷 Model / Type 是否僅為數量描述（例如 (2 SETS)、X2、(3)）。"""
    if not isinstance(model, str):
        return False
    return bool(_QTY_PATTERN.match(model.strip().upper()))


def is_invalid_model(model) -> bool:
    """完整判斷：固定無效值集合 + 數量描述型雜訊。"""
    if not isinstance(model, str):
        return True
    s = model.strip().upper()
    if s in INVALID_MODEL:
        return True
    return is_quantity_only(s)


# === Maker / Model 正規化（方案 A：規則式分群比對鍵） ===

_QUOTES = '"\'＂“”‘’`'

def maker_key(s) -> str:
    """產生 Maker 比對鍵：移除空白/連字號/句點/引號，保留 & 與 /。"""
    if not isinstance(s, str):
        return ''
    t = s.upper().strip()
    for q in _QUOTES:
        t = t.replace(q, '')
    t = t.replace('=', '-')
    t = re.sub(r'\s+', '', t)
    t = t.replace('-', '').replace('.', '')
    return t


def model_key(s) -> str:
    """產生 Model / Type 比對鍵：移除空白、連字號、句點、括號、引號；= 視為 -。"""
    if not isinstance(s, str):
        return ''
    t = s.upper().strip()
    for q in _QUOTES:
        t = t.replace(q, '')
    t = t.replace('=', '-')
    t = re.sub(r'\s+', '', t)
    t = re.sub(r'[\-\.\(\)]', '', t)
    return t


def canonicalize_column(series, key_fn):
    """
    給定一個 pandas Series 與 key_fn，回傳 (canonical_series, merge_groups)。
    - canonical_series：將每列值替換為該 key 群組中出現次數最多的原文
    - merge_groups：list of dict，描述被合併的群組（用於審閱報告）
    """
    counts = Counter()
    keys = []
    for v in series:
        k = key_fn(v) if isinstance(v, str) else ''
        keys.append(k)
        if isinstance(v, str) and k:
            counts[(k, v.strip())] += 1

    # 為每個 key 找出最高頻原文（同頻取字串較長者，再取字典序）
    canonical = {}
    members = {}
    for (k, v), c in counts.items():
        members.setdefault(k, []).append((v, c))
    for k, lst in members.items():
        lst.sort(key=lambda x: (-x[1], -len(x[0]), x[0]))
        canonical[k] = lst[0][0]

    # 替換
    out = []
    for v, k in zip(series, keys):
        if isinstance(v, str) and k in canonical:
            out.append(canonical[k])
        else:
            out.append(v)

    # 產生合併報告（只列出有合併的群組）
    merge_groups = []
    for k, lst in members.items():
        if len(lst) > 1:
            merge_groups.append({
                'key': k,
                'canonical': canonical[k],
                'variants': sorted(set(v for v, _ in lst)),
                'total_rows': sum(c for _, c in lst),
            })
    merge_groups.sort(key=lambda x: -x['total_rows'])
    return out, merge_groups

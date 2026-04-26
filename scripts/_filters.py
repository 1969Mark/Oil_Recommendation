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


_TRAILING_QTY_PAREN = re.compile(
    r'\s*\(\s*(?:'
    r'\d+\s*(?:SETS?|UNITS?|PCS?|EA|NOS?)?'
    r'|X\s*\d+|\d+\s*X'
    r')\s*\)\s*$',
    re.IGNORECASE,
)
_TRAILING_QTY_BARE = re.compile(
    r'\s+\d+\s*(?:SETS?|UNITS?|PCS?|EA|NOS?)\s*$',
    re.IGNORECASE,
)


def strip_quantity_descriptor(s):
    """移除 Model 末端的數量描述：(3 SETS) / (X3) / (2) / 「 300 EA」等。
    僅在剝除後仍非空時才採用，避免把純數量字串變成空字串（後續會被 is_invalid_model 過濾）。"""
    if not isinstance(s, str):
        return s
    out = _TRAILING_QTY_PAREN.sub('', s)
    out = _TRAILING_QTY_BARE.sub('', out).strip()
    return out if out else s


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


# === Part 語意合併清單（明確 allowlist，僅套用於 Lube Chart / NB；OEM 不適用） ===

_HYD_TO_HYDRAULIC_SYSTEM = {
    'HYDRAULIC',
    'HYDRAULIC MEDIUM',
    'HYDRAULIC OIL',
    'HYDRAULIC FLUID',
    'HYD OIL',
    'HYD. OIL',
    'HYD.SYSTEM',
    'HYD.MEDIUM',
    'HYDR. SYSTEM',
    'HYDR.OIL',
    'HYDR.SYSTEM OIL',
    'HYDRULIC SYSTEM',
    'HYDRAULIC SYTEM',
    'HYDRAULI SYSTEM',
    'HYDRAULIC MEDIIUM',
    'A.HYDRAULIC SYSTEM',
    'HYDRAULIC OIL(SYSTEM OIL)',
    'HYDRAULIC OIL (UNIT)',
    'HYDRAULIC SYSTEM (SAME OIL AS SYSTEM)',
    'HYDRAULIC SYSTEM (SAME AS SYSTEM)',
    'HYDRAULIC MEDIUM (SAME OIL AS SYSTEM)',
    'HYDRAULIC SYSTEM (EAL)',
    'HYDRAULIC (EAL)',
}

_GEAR_TO_ENCLOSED_GEAR = {
    'GEAR',
    'GEARS',
    'GEAR BOX',
    'GEARBOX',
    'GEAR UNIT',
    'GEAR OIL',
    'ENCLOSED GEAR',
    'ENCLOSED GEARS',
    'ENCLOSED GEAR-',
    'ENCLSOED GEAR',
    'INCLOSED GEAR',
    'ENCLOSED GEAR BOX',
    'ENCLOSE GEAR BOX',
    'LUBE.OIL FOR GEARBOX',
    'B.LUBE. OIL FOR GEAR BOX',
    'GEARBOX (NOTE 2)',
    'GEARBOX (REMARK 1)',
    'GEARBOX (REMARK 3)',
    'GEARBOX(REMARK 2)',
    'STEERING GEAR',
    'TURNING GEAR',
    'TURING GEAR',
    'REDUCTION GEAR',
}


_COMPRESSOR_PART_KEEP = {
    'MOTOR BEARINGS',  # 附屬電機軸承，潤滑與壓縮機本體不同
    'FAN BEARING',     # 風扇軸承，獨立潤滑點
}


def apply_compressor_part_rule(equipment, part):
    """若 Equipment 含 'COMPRESSOR' 關鍵字，將 Part to be lubricated 統一改為
    'CYLINDERS & BEARINGS'。例外保留：MOTOR BEARINGS / FAN BEARING（壓縮機附屬電機與風扇軸承）。
    僅套用於 Lube Chart 與 NB；OEM 不呼叫此函式。"""
    if isinstance(equipment, str) and 'COMPRESSOR' in equipment.upper():
        if isinstance(part, str) and part.strip().upper() in _COMPRESSOR_PART_KEEP:
            return part
        return 'CYLINDERS & BEARINGS'
    return part


# === Part 括號清理（保留燃油規範與 EAL，移除註記/油路說明等雜訊） ===
# 保留 keyword：燃油等級 + ECA + EAL + 任意 %S 標示
_PART_PAREN_KEEP_KEYWORDS = re.compile(
    r'\b(?:HSFO|VLSFO|ULSFO|LSFO|HSHFO|HFO|IFO|MGO|MDO|LNG|ECA|EAL)\b'
    r'|%\s*S\b',
    re.IGNORECASE,
)
# 括號內的註記雜訊（含分隔符前綴），如 ` - REMARK 1`、`, NOTE 2`
_PART_PAREN_NOISE = re.compile(
    r'\s*[-,]\s*(?:REMARK|NOTE)\s*\d*\s*',
    re.IGNORECASE,
)
_PART_PAREN = re.compile(r'\s*\(([^()]*)\)')
_PART_SQUARE_BRACKET = re.compile(r'\s*\[[^\[\]]*\]')
_PART_TRAILING_ALTERNATE = re.compile(r'\s*[-,]\s*ALTERNATE\s*$', re.IGNORECASE)


def strip_non_fuel_parens(part):
    """Part to be lubricated 括號清理：
    - 括號內含燃油規範 keyword（HSFO/VLSFO/ULSFO/LSFO/HSHFO/HFO/IFO/MGO/MDO/LNG/ECA、%S）
      或 EAL → 保留括號；同時移除括號內的 REMARK/NOTE 註記片段
    - 括號內無 keyword → 整段括號連前置空白移除
    - 方括號 [...] 一律移除
    - 後綴 - ALTERNATE / , ALTERNATE 一律移除
    僅套用於 Lube Chart 與 NB；OEM 不呼叫此函式（Part 不正規化原則）。"""
    if not isinstance(part, str):
        return part
    s = part
    s = _PART_SQUARE_BRACKET.sub('', s)

    def _paren_repl(m):
        inner = m.group(1)
        if _PART_PAREN_KEEP_KEYWORDS.search(inner):
            cleaned = _PART_PAREN_NOISE.sub('', inner).strip()
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return f' ({cleaned})' if cleaned else ''
        return ''

    s = _PART_PAREN.sub(_paren_repl, s)
    s = _PART_TRAILING_ALTERNATE.sub('', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def apply_part_semantic_merge(s):
    """將通用同義詞合併為標準術語：
    - HYDRAULIC / HYD.MEDIUM / 縮寫 / 錯字 → HYDRAULIC SYSTEM
    - GEAR / GEARBOX / GEAR BOX / 通用齒輪敘述 → ENCLOSED GEAR
    保留：HYDRAULIC STARTER/BRAKE/PUMP 等獨立設備、含燃料等級 (EAL)/(VLSFO) 標記、
    OPEN GEAR / STEERING GEAR / 用途特定齒輪箱（HOISTING / SLEWING / WINCH 等）。
    僅套用於 Lube Chart 與 NB；OEM 不呼叫此函式。"""
    if not isinstance(s, str):
        return s
    t = s.strip().upper()
    if t in _HYD_TO_HYDRAULIC_SYSTEM:
        return 'HYDRAULIC SYSTEM'
    if t in _GEAR_TO_ENCLOSED_GEAR:
        return 'ENCLOSED GEAR'
    return s


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

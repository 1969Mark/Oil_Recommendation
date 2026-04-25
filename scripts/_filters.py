"""
_filters.py
===========
共用過濾規則：供 process_lube_chart.py / process_nb.py / process_oem.py 使用。
"""

import re

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

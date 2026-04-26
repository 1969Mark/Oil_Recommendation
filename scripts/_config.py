"""
_config.py
==========
專案層級常數集中：路徑、Excel 色彩、欄位定義、各 master/registry 檔名。

所有腳本應從此模組 import 常數，避免散落在各 process_*.py / deploy.py 中。
函式（sha256 / registry I/O / log / Excel 樣式）仍放在 _common.py。
"""

import os


# ── 專案根與基礎目錄 ────────────────────────────────────────────
PROJECT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LC_DIR   = os.path.join(PROJECT, 'LubeChart_data')
NB_DIR   = os.path.join(PROJECT, 'NB_data')
OEM_DIR  = os.path.join(PROJECT, 'OEM_data')
OUT_DIR  = os.path.join(PROJECT, 'output')
RPT_DIR  = os.path.join(OUT_DIR, 'parse_report')
REG_DIR  = os.path.join(PROJECT, 'registry')
LOG_DIR  = os.path.join(PROJECT, 'logs')

# ── Registry / Log 檔 ──────────────────────────────────────────
LC_REG     = os.path.join(REG_DIR, 'lube_chart_registry.json')
NB_REG     = os.path.join(REG_DIR, 'nb_registry.json')
OEM_REG    = os.path.join(REG_DIR, 'oem_registry.json')
FAIL_REG   = os.path.join(REG_DIR, 'failed_registry.json')
LOG_FILE   = os.path.join(LOG_DIR, 'update_log.txt')

# ── Master Excel ───────────────────────────────────────────────
LC_MASTER  = os.path.join(OUT_DIR, 'lube_chart_master.xlsx')
NB_MASTER  = os.path.join(OUT_DIR, 'nb_master.xlsx')
OEM_MASTER = os.path.join(OUT_DIR, 'oem_master.xlsx')

# ── OEM 專屬 ────────────────────────────────────────────────────
PENDING_DIR = os.path.join(OUT_DIR, 'pending_ai_review')
MANUAL_SRC  = os.path.join(PROJECT, 'OEM_oil_recommendation.xlsx')

# ── App 部署 ────────────────────────────────────────────────────
APP_HTML    = os.path.join(PROJECT, 'lube_query_app.html')
APP_DATA_JS = os.path.join(OUT_DIR, 'app_data.js')


# ── Excel 色彩（依 CLAUDE.md「Excel 輸出格式規範」） ─────────────
HDR_BG    = '1F3864'    # 標題列深藍
ODD_BG    = 'FFFFFF'    # 奇數列白
EVEN_BG   = 'EBF3FB'    # 偶數列淡藍
LC_COLOR  = '375623'    # LUBE CHART 綠
NB_COLOR  = '7F6000'    # NB 金
OEM_COLOR = '1F3864'    # OEM 深藍

SOURCE_COLOR = {
    'LUBE CHART': LC_COLOR,
    'NB':         NB_COLOR,
    'OEM':        OEM_COLOR,
}


# ── 欄位定義 ────────────────────────────────────────────────────
COLS         = ['Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
DEDUP_KEYS   = ['Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
OEM_COLS     = COLS + ['Count', 'Source', 'source_file', 'source_sheet']
DISPLAY_COLS = COLS + ['Count', 'Source']   # 前端 App 顯示欄位

# Source 排序（OEM > NB > LUBE CHART）
SRC_ORDER = {'OEM': 0, 'NB': 1, 'LUBE CHART': 2}

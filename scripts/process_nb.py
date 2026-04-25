"""
process_nb.py
=============
增量處理 NB_data/ 內的 PDF 檔案，輸出 output/nb_master.xlsx 及 parse_report。
依 CLAUDE.md 規格：Format A/B/C 自動偵測、信心分數、增量 registry。
"""

import os, sys, json, hashlib, re
from collections import defaultdict
from datetime import datetime
import pandas as pd
import pdfplumber
import fitz  # PyMuPDF
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ── 路徑設定 ────────────────────────────────────────────────────
PROJECT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NB_DIR     = os.path.join(PROJECT, 'NB_data')
OUT_DIR    = os.path.join(PROJECT, 'output')
RPT_DIR    = os.path.join(OUT_DIR, 'parse_report')
REG_DIR    = os.path.join(PROJECT, 'registry')
LOG_DIR    = os.path.join(PROJECT, 'logs')
MASTER     = os.path.join(OUT_DIR, 'nb_master.xlsx')
REG_FILE   = os.path.join(REG_DIR, 'nb_registry.json')
FAIL_REG   = os.path.join(REG_DIR, 'failed_registry.json')
LOG_FILE   = os.path.join(LOG_DIR, 'update_log.txt')

COLS = ['Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
DEDUP_KEYS = ['Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
INVALID_MODEL = {'', '.', '..', 'TO BE DETERMINED', 'TBD', 'N/A', 'NA', 'NONE', '-', '--'}

# Excel 色彩
HDR_BG  = '1F3864'
ODD_BG  = 'FFFFFF'
EVEN_BG = 'EBF3FB'
NB_COLOR = '7F6000'

# ── Registry 工具 ───────────────────────────────────────────────
def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

def load_registry(path):
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return {'last_updated': '', 'processed_files': {}}

def save_registry(reg, path):
    reg['last_updated'] = datetime.now().isoformat(timespec='seconds')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(reg, f, indent=2, ensure_ascii=False)

def load_failed_registry():
    if os.path.exists(FAIL_REG):
        with open(FAIL_REG, encoding='utf-8') as f:
            return json.load(f)
    return {'last_updated': '', 'failed_files': {}}

def save_failed_registry(freg):
    freg['last_updated'] = datetime.now().isoformat(timespec='seconds')
    with open(FAIL_REG, 'w', encoding='utf-8') as f:
        json.dump(freg, f, indent=2, ensure_ascii=False)

def needs_processing(fname, fpath, reg):
    if fname not in reg['processed_files']:
        return True, 'new'
    rec = reg['processed_files'][fname]
    stat = os.stat(fpath)
    mtime = datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec='seconds')
    if mtime != rec.get('file_mtime', ''):
        return True, 'mtime_changed'
    if sha256(fpath) != rec.get('sha256', ''):
        return True, 'sha256_changed'
    return False, 'unchanged'

# ── PDF 解析工具 ─────────────────────────────────────────────────
def clean(t):
    return re.sub(r'\s+', ' ', str(t or '')).strip()

def strip_cn(t):
    return clean(re.sub(r'[\u4e00-\u9fff（）]+', '', t or ''))

def extract_model(n):
    m = re.search(r'\(([^)]+)\)', n)
    return m.group(1).strip() if m else ''

def strip_model_paren(n):
    return clean(re.sub(r'\s*\([^)]+\)', '', n))

def clean_app(t):
    return clean(re.sub(r'\s+\d+\.?\d*\s*$', '', t or ''))

def clean_oil(t):
    return clean(re.sub(r'\s+[\d,]+\.\d{2,}.*$', '', t or ''))

# ── 格式偵測 ────────────────────────────────────────────────────
def detect_format(pdf_path):
    """
    偵測順序：Format F → Format E → Format J → Format H → Format G → Format I → Format D → Format B → Format C → Format A（fallback）
    Format F：Hyundai HHI 格式，欄位含 PRINCIPAL PARTICULAR + APPLICATION POINT + L.O COMPANY
    Format E：NO|TOTAL OIL|LUBRICATION PARTS|EQUIPMENT AND TYPE|EQUIPMENT MAKER（江南造船廠格式）
    Format J：中英雙語 OIL BRAND + POINT 格式（NDY1305_ 等，無 APPLICATION 關鍵字）
    Format H：NTS 中英雙語（Lub. Oil brand 在 row4，需掃描更多行）
    Format G：K Shipbuilding 格式，A.EQUIPMENT LIST + B.LUB.OIL CHART（EQUIPMENT+APPLICATION POINT+PRODUCT）
    Format I：EQUIPMENT (MAKER/TYPE) + APPLICATION POINT + KIND OF LUB. OIL（HN5801 格式）
    Format D：三欄式 EQUIPMENT | PART | LUB OIL（中國造船廠格式）
    需檢查前 5 頁，因第 1-2 頁可能是設備清單封面
    """
    # Format N 前置掃描（fitz 讀取）：PDF 旋轉 90° + pdfplumber 反向，需用 fitz
    try:
        _doc = fitz.open(pdf_path)
        try:
            _txt = ''
            for _i in range(min(2, len(_doc))):
                _txt += _doc[_i].get_text() + '\n'
            _U = _txt.upper()
            if ('NAME OF MACHINERY' in _U and 'PRINCIPAL PARTICULAR' in _U
                    and 'L.O GRADE' in _U and 'APPLICATION POINT' in _U):
                # 確認是反向 PDF：pdfplumber 取得的文字含反向字串（如 RALUCITRAP）
                with pdfplumber.open(pdf_path) as _pp:
                    _ppt = (_pp.pages[0].extract_text() or '').upper()
                    if 'LAPICNIRP' in _ppt or 'RALUCITRAP' in _ppt or 'EDARG' in _ppt:
                        return 'N'
        finally:
            _doc.close()
    except Exception:
        pass

    with pdfplumber.open(pdf_path) as pdf:
        # Format L 前置掃描：L.O. BRAND + LUBRICATION (PARTS) + MAKER 同時存在
        # 使用 8 行範圍，確保雙層合併標題都被納入掃描
        # 必須在主迴圈前執行，避免被 Format D 原始條件（EQUIPMENT + LUB）早期攔截
        for _pg in pdf.pages[:2]:
            for _tb in (_pg.extract_tables() or [])[:4]:
                _wide = ' '.join(str(c) for row in _tb[:8] for c in row if c).upper()
                if ('L.O.' in _wide and 'BRAND' in _wide
                        and 'LUBRICATION' in _wide and 'MAKER' in _wide):
                    return 'L'

        for page in pdf.pages[:5]:
            text   = page.extract_text() or ''
            tables = page.extract_tables()
            if tables:
                for table in tables[:4]:
                    hdr_str = ' '.join(str(c) for row in table[:4] for c in row if c).upper()
                    # Format F：Hyundai HHI L.O Chart（PRINCIPAL PARTICULAR + APPLICATION POINT）
                    if 'PRINCIPAL PARTICULAR' in hdr_str and 'APPLICATION POINT' in hdr_str:
                        return 'F'
                    # Format E：TOTAL OIL + EQUIPMENT MAKER 同時出現（油品欄在前）
                    if 'TOTAL OIL' in hdr_str and 'EQUIPMENT MAKER' in hdr_str:
                        return 'E'
                    hdr6 = ' '.join(str(c) for row in table[:6] for c in row if c).upper()
                    # Format J：OIL BRAND（無 LUB 前綴）+ POINT（非 APPLICATION POINT）
                    if 'OIL BRAND' in hdr6 and 'POINT' in hdr6 and 'APPLICATION' not in hdr6:
                        return 'J'
                    # Format K：LUBRICATING POINT + KIND OF LUBRICANT（JIT 船东供油料清单 格式）
                    if 'LUBRICATING POINT' in hdr_str and 'KIND OF LUBRICANT' in hdr_str:
                        return 'K'
                    # Format M：Equipment Name(Type) + Manufacturer + Application Point + Recommended Oil
                    if ('MANUFACTURER' in hdr_str and 'APPLICATION' in hdr_str
                            and 'EQUIPMENT' in hdr_str):
                        return 'M'
                    # Format D（早期攔截）：EQUIPMENT + PART + LUB OIL 明確三欄表格
                    # 需比對 'LUB OIL'（非僅 'LUB'），避免將 'Lubrication Parts' 誤攔截
                    if ('EQUIPMENT' in hdr_str and 'PART' in hdr_str
                            and ('LUB OIL' in hdr_str or 'LUBE OIL' in hdr_str)):
                        return 'D'
                    # Format H：NTS 中英雙語（Lub. Oil brand 在 row4，需掃描更多行）
                    if 'BRAND' in hdr6 and ('MAKER' in hdr6 or '制造商' in hdr6):
                        return 'H'
                    # Format G：APPLICATION POINT + PRODUCT（無 PRINCIPAL PARTICULAR）
                    if ('APPLICATION POINT' in hdr_str and 'PRODUCT' in hdr_str
                            and 'EQUIPMENT' in hdr_str
                            and 'PRINCIPAL PARTICULAR' not in hdr_str):
                        return 'G'
                    # Format I：KIND OF LUB + APPLICATION POINT（HN5801 供應商格式）
                    if 'KIND OF LUB' in hdr_str and 'APPLICATION POINT' in hdr_str:
                        return 'I'
                    # Format D：EQUIPMENT + LUB 同時出現於標題列
                    if 'EQUIPMENT' in hdr_str and 'LUB' in hdr_str:
                        return 'D'
                    # Format C：廠家型號合併欄
                    if 'Maker&' in hdr_str or '厂家及型号' in hdr_str or 'Maker &' in hdr_str:
                        return 'C'
            if '船东供油料清单' in text or 'OIL LIST FOR OWNER SUPPLY' in text:
                return 'B'
            if 'MAKER:' in text.upper() or 'MAKER :' in text.upper():
                return 'B'
    return 'A'

# ── Format A 解析 ───────────────────────────────────────────────
BOUNDS_A = {
    'no': (0, 65), 'equip': (65, 205), 'mfr': (205, 300),
    'app': (300, 450), 'oil': (465, 565),
}

def col_A(x):
    for c, (lo, hi) in BOUNDS_A.items():
        if lo <= x < hi:
            return c
    return 'other'

def parse_format_A(pdf_path, ship_id):
    records = []
    page_info = []
    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            words = page.extract_words()
            row_map = defaultdict(lambda: defaultdict(list))
            for w in words:
                y = round(w['top'] / 3) * 3
                row_map[y][col_A(w['x0'])].append(w['text'])
            cur_no = None; cur_eq = ''; cur_mfr = []; cur_entries = []

            def flush():
                nonlocal cur_no, cur_eq, cur_mfr, cur_entries
                if cur_no and cur_eq:
                    mfr = clean(' '.join(cur_mfr))
                    model = extract_model(cur_eq)
                    eq = strip_model_paren(cur_eq)
                    for app, oil in cur_entries:
                        if oil:
                            records.append({'船號': ship_id, '設備名稱': eq,
                                '設備廠家': mfr, '設備型號': model,
                                '潤滑部位': app, '推薦潤滑油': oil, '_page': pg_no})
                cur_no = None; cur_eq = ''; cur_mfr = []; cur_entries = []

            for y in sorted(row_map.keys()):
                row = row_map[y]
                no_t = ' '.join(row.get('no', []))
                eq_t = ' '.join(row.get('equip', []))
                mr_t = ' '.join(row.get('mfr', []))
                ap_t = ' '.join(row.get('app', []))
                ol_t = ' '.join(row.get('oil', []))
                m = re.match(r'^(\d+)$', no_t.strip())
                if m:
                    flush()
                    cur_no = int(m.group(1))
                    cur_eq = clean(eq_t)
                    cur_mfr = [mr_t] if mr_t else []
                    cur_entries = [(clean_app(ap_t), clean_oil(ol_t))]
                elif cur_no is not None:
                    if mr_t: cur_mfr.append(mr_t)
                    if eq_t and not cur_eq: cur_eq = clean(eq_t)
                    if ol_t: cur_entries.append((clean_app(ap_t), clean_oil(ol_t)))
            flush()

    return records

# ── Format B 解析 ───────────────────────────────────────────────
def parse_format_B(pdf_path, ship_id):
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 3: continue
                hdr_idx = None
                for i, row in enumerate(table):
                    if 'LUBRICATING' in ' '.join(str(c) for c in row if c) or \
                       '滑油用途' in ' '.join(str(c) for c in row if c):
                        hdr_idx = i; break
                if hdr_idx is None: continue
                cur_no = ''; cur_eq = ''; cur_mfr = ''; cur_model = ''
                for row in table[hdr_idx + 1:]:
                    if not any(row): continue
                    raw = [str(x or '') for x in row]
                    no_r = raw[0].strip()
                    eq_r = raw[1] if len(raw) > 1 else ''
                    app_r = raw[4] if len(raw) > 4 else ''
                    oil_r = raw[5] if len(raw) > 5 else ''
                    eq_c = clean(eq_r)
                    if any(k in eq_c for k in ['机装部分', '甲板部分', '电气部分', 'MACHINERY PART']): continue
                    if re.match(r'^\d+$', no_r):
                        cur_no = no_r
                        mk_m = re.search(r'MAKER\s*:\s*([^\n]+)', eq_r, re.I)
                        ty_m = re.search(r'TYPE\s*:\s*([^\n]+)', eq_r, re.I)
                        cur_mfr   = clean(mk_m.group(1)) if mk_m else ''
                        cur_model = clean(ty_m.group(1)) if ty_m else ''
                        eq_before = re.split(r'MAKER\s*:', eq_r, flags=re.I)[0]
                        cur_eq = strip_cn(eq_before.split('\n')[0])
                    if not cur_no: continue
                    if eq_r and not cur_mfr:
                        mk_m = re.search(r'MAKER\s*:\s*([^\n]+)', eq_r, re.I)
                        ty_m = re.search(r'TYPE\s*:\s*([^\n]+)', eq_r, re.I)
                        if mk_m: cur_mfr   = clean(mk_m.group(1))
                        if ty_m: cur_model = clean(ty_m.group(1))
                    oil = clean(oil_r)
                    if oil and oil not in ('滑油种类', 'KIND OF LUBRICANT'):
                        records.append({'船號': ship_id, '設備名稱': cur_eq,
                            '設備廠家': cur_mfr, '設備型號': cur_model,
                            '潤滑部位': strip_cn(app_r), '推薦潤滑油': oil, '_page': pg_no})
    return records

# ── Format C 解析 ───────────────────────────────────────────────
def split_maker_model(mk):
    parts = (mk or '').split()
    split_at = len(parts)
    for i, p in enumerate(parts):
        if i > 0 and re.search(r'\d', p):
            split_at = i; break
    maker = ' '.join(parts[:split_at])
    model = ' '.join(parts[split_at:])
    if not model and len(parts) > 1:
        maker = ' '.join(parts[:-1]); model = parts[-1]
    return maker, model

def parse_format_C(pdf_path, ship_id):
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2: continue
                hdr_idx = None
                for i, row in enumerate(table):
                    rs = ' '.join(str(c) for c in row if c)
                    if 'Maker' in rs or '厂家' in rs:
                        hdr_idx = i; break
                if hdr_idx is None: continue
                cur_no = ''; cur_eq = ''; cur_mfr = ''; cur_model = ''
                for row in table[hdr_idx + 1:]:
                    if not any(row): continue
                    c = [str(x or '') for x in row]
                    no_c = c[0].strip()
                    eq_c = c[1] if len(c) > 1 else ''
                    mk_c = c[2] if len(c) > 2 else ''
                    pu_c = c[3] if len(c) > 3 else ''
                    ol_c = c[4] if len(c) > 4 else ''
                    if re.match(r'^\d+$', no_c):
                        cur_no = no_c
                        eq_first = eq_c.split('\n')[0] if '\n' in eq_c else eq_c
                        cur_eq = strip_cn(eq_first)
                        cur_mfr, cur_model = split_maker_model(clean(mk_c))
                    if not cur_no: continue
                    if mk_c.strip() and not cur_mfr:
                        cur_mfr, cur_model = split_maker_model(clean(mk_c))
                    app = strip_cn(re.sub(r'\(.*?[\u4e00-\u9fff].*?\)', '', pu_c))
                    oil = clean(ol_c)
                    if oil:
                        records.append({'船號': ship_id, '設備名稱': cur_eq,
                            '設備廠家': cur_mfr, '設備型號': cur_model,
                            '潤滑部位': app, '推薦潤滑油': oil, '_page': pg_no})
    return records

# ── Format D 解析 ───────────────────────────────────────────────
def _parse_eq_cell(cell_text):
    """
    解析 Format D 的 EQUIPMENT 欄位，回傳 (equipment, maker, model)。
    欄位格式範例：
      1.1 M/E 主机
      Model: MAN B&W
      6S40ME-C9.5-HPSCR
      S.MCR： 6810KW 146RPM
      Maker: CSE
    """
    lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
    equipment = ''
    maker_line = ''          # 來自 Maker:/Marker: 行
    model_parts = []
    model_collecting = False

    SPEC_PAT = re.compile(
        r'^S\.MCR|Speed\s*[Xx×]|^\s*=\d|\d+\s*r/min|\d+\s*KW|\d+\s*kW|'
        r'^\d+\s*[Xx×]\s*\d+|Effective\s*capacity',
        re.I
    )

    for line in lines:
        # 跳過規格行（功率、轉速等）
        if SPEC_PAT.search(line):
            model_collecting = False
            continue

        # Maker: / Marker: 行
        m = re.match(r'(?:Maker|Marker)\s*[：:]\s*(.*)', line, re.I)
        if m:
            model_collecting = False
            maker_line = m.group(1).strip()
            continue

        # Model: / TYPE: 行
        m = re.match(r'(?:Model|TYPE)\s*[：:]\s*(.*)', line, re.I)
        if m:
            model_collecting = True
            val = m.group(1).strip()
            if val:
                model_parts = [val]
            else:
                model_parts = []
            continue

        # Model: 行的延續（緊接在 Model: 之後且不含關鍵字）
        if model_collecting:
            model_parts.append(line)
            model_collecting = False
            continue

        # 設備名稱（第一個非關鍵字行）
        if not equipment:
            eq = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', line)
            eq = re.sub(r'^\d+[\.\d]*\s*', '', eq).strip()
            if eq:
                equipment = eq

    # 處理 model_parts → 分離廠家與型號
    maker_from_model = ''
    model_num = ''
    if model_parts:
        model_str = ' '.join(model_parts).strip()
        # 處理數量前綴：「3 X 6DE-18」→「6DE-18」
        qty_m = re.match(r'^\d+\s*[Xx×]\s*(.+)', model_str)
        if qty_m:
            model_str = qty_m.group(1).strip()

        # 以第一個含數字的 token 為分界，前段為廠家，後段為型號
        parts = model_str.split()
        if parts and re.match(r'^\d', parts[0]):
            # 整串都是型號（無廠家前綴）
            model_num = model_str
        else:
            split_at = len(parts)
            for j, p in enumerate(parts):
                if j > 0 and re.search(r'^\d', p):
                    split_at = j
                    break
            maker_from_model = ' '.join(parts[:split_at]).strip()
            model_num        = ' '.join(parts[split_at:]).strip()
            if not model_num:
                # 整串都是廠家名（無數字），視為純型號
                model_num        = maker_from_model
                maker_from_model = ''

    # 廠家優先級：Model 行廠家 > Maker: 行
    final_maker = maker_from_model if maker_from_model else maker_line
    final_model = model_num

    return equipment.upper(), final_maker.upper(), final_model.upper()


def parse_format_D(pdf_path, ship_id):
    """
    Format D：三欄式 EQUIPMENT | PART | LUB OIL
    EQUIPMENT 欄以 Model:/Maker: 關鍵字包含廠家與型號資訊。
    """
    records = []

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                # 找標題列及欄位索引
                hdr_idx = eq_col = pt_col = oil_col = None
                for i, row in enumerate(table[:4]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'EQUIPMENT' in row_u and 'LUB' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if 'EQUIPMENT' in cu and eq_col is None:
                                eq_col = j
                            elif 'PART' in cu and pt_col is None:
                                pt_col = j
                            elif ('LUB' in cu or 'OIL' in cu) and oil_col is None:
                                oil_col = j
                        break

                if hdr_idx is None or eq_col is None:
                    continue
                if pt_col  is None: pt_col  = eq_col + 1
                if oil_col is None: oil_col = eq_col + 2

                cur_eq = cur_maker = cur_model = ''

                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells    = [str(c or '').strip() for c in row]
                    eq_cell  = cells[eq_col]  if eq_col  < len(cells) else ''
                    pt_cell  = cells[pt_col]  if pt_col  < len(cells) else ''
                    oil_cell = cells[oil_col] if oil_col < len(cells) else ''

                    # 解析 EQUIPMENT 欄（非空時更新當前設備上下文）
                    if eq_cell.strip():
                        cur_eq, cur_maker, cur_model = _parse_eq_cell(eq_cell)

                    # 有潤滑油才記錄
                    if pt_cell and oil_cell:
                        oil_clean = re.sub(r'\s+', ' ', oil_cell).strip()
                        # 跳過無效油品（注意：不能用 0\s*$ 否則會誤殺 BN40/SAE30 等以 0 結尾的油品名稱）
                        if not oil_clean or re.search(
                                r'not supplied|factory charged', oil_clean, re.I):
                            continue
                        # 跳過純數字或空白的 LUB OIL 欄
                        if re.fullmatch(r'[\d\s\.]+', oil_clean):
                            continue
                        # PART 欄去除中文
                        part_clean = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）（）]+', ' ', pt_cell)
                        part_clean = re.sub(r'\s+', ' ', part_clean).strip()

                        records.append({
                            '船號'     : ship_id,
                            '設備名稱' : cur_eq,
                            '設備廠家' : cur_maker,
                            '設備型號' : cur_model,
                            '潤滑部位' : part_clean,
                            '推薦潤滑油': oil_clean,
                            '_page'   : pg_no,
                        })
    return records


def parse_format_E(pdf_path, ship_id):
    """
    Format E：NO | TOTAL OIL | LUBRICATION PARTS | EQUIPMENT AND TYPE | EQUIPMENT MAKER | QTY | REMARK
    油品欄（TOTAL OIL）在前，Equipment 與 Type 合併於同欄用冒號分隔，另有獨立 MAKER 欄。
    典型來源：江南造船廠（JNS230 等）
    """
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                # 找標題列及欄位索引
                hdr_idx = oil_col = pt_col = eq_col = mk_col = None
                for i, row in enumerate(table[:5]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'TOTAL OIL' in row_u and 'EQUIPMENT MAKER' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if 'TOTAL OIL' in cu and oil_col is None:
                                oil_col = j
                            elif 'LUBRICATION PARTS' in cu and pt_col is None:
                                pt_col = j
                            elif 'EQUIPMENT AND TYPE' in cu and eq_col is None:
                                eq_col = j
                            elif 'EQUIPMENT MAKER' in cu and mk_col is None:
                                mk_col = j
                        break

                if hdr_idx is None or oil_col is None or eq_col is None:
                    continue
                if pt_col  is None: pt_col  = oil_col + 1
                if mk_col  is None: mk_col  = eq_col  + 1

                cur_oil = ''
                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells   = [str(c or '').strip() for c in row]
                    oil_raw = cells[oil_col] if oil_col < len(cells) else ''
                    pt_raw  = cells[pt_col]  if pt_col  < len(cells) else ''
                    eq_raw  = cells[eq_col]  if eq_col  < len(cells) else ''
                    mk_raw  = cells[mk_col]  if mk_col  < len(cells) else ''

                    # TOTAL OIL 欄為合併儲存格，非空時更新當前油品
                    oil_clean = re.sub(r'\s+', ' ', oil_raw).strip()
                    if oil_clean:
                        cur_oil = oil_clean

                    if not pt_raw or not eq_raw or not cur_oil:
                        continue

                    # 跳過無效油品（純數字、空白）
                    if re.fullmatch(r'[\d\s\.]+', cur_oil):
                        continue

                    # 拆分 EQUIPMENT AND TYPE：以全形或半形冒號分隔
                    # 例：「MAIN ENGINE：5S50ME-C9.7」→ equipment=MAIN ENGINE, model=5S50ME-C9.7
                    eq_parts = re.split(r'[：:]', eq_raw, maxsplit=1)
                    equipment = re.sub(r'\s+', ' ', eq_parts[0]).strip().upper()
                    model     = re.sub(r'\s+', ' ', eq_parts[1]).strip().upper() if len(eq_parts) > 1 else ''

                    # 去除 Equipment 名稱中的中文
                    equipment = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', equipment).strip()
                    model     = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', model).strip()

                    maker = re.sub(r'\s+', ' ', mk_raw).strip().upper()
                    part  = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', ' ', pt_raw)
                    part  = re.sub(r'\s+', ' ', part).strip()

                    if not equipment and not model:
                        continue

                    records.append({
                        '船號'     : ship_id,
                        '設備名稱' : equipment,
                        '設備廠家' : maker,
                        '設備型號' : model,
                        '潤滑部位' : part,
                        '推薦潤滑油': cur_oil,
                        '_page'   : pg_no,
                    })
    return records


def parse_format_F(pdf_path, ship_id):
    """
    Format F：含 PRINCIPAL PARTICULAR + APPLICATION POINT 的 L.O Chart
    支援兩種子格式：

    子格式 F1（Hyundai HHI，如 H2933/H2934）：
      PRINCIPAL PARTICULAR 以「MAKER: XXX」關鍵字標示廠家，
      緊接下一行為型號；子設備（TURNING GEAR 等）單獨成行。
      油品欄：L.O COMPANY / TOTAL

    子格式 F2（如 010-2 7U）：
      PRINCIPAL PARTICULAR 第一行 = Maker（無關鍵字），
      型號以括號標示（如 (P626)）；延續列第一行 = 子設備名稱，
      保留既有 Maker，僅更新 cur_eq 與 cur_model。
      油品欄：L.O GRADE

    偵測依據：
      - 有「MAKER:」關鍵字 → 子格式 F1
      - 無「MAKER:」，型號在括號內 → 子格式 F2
    """
    records = []

    _MAKER_PAT = re.compile(r'MAKER\s*[：:]\s*(.+)', re.I)
    _PAREN_PAT = re.compile(r'^\((.+)\)$')

    def _has_maker_kw(text):
        return bool(_MAKER_PAT.search(text))

    def _parse_principal_f1(cell_text):
        """
        F1 子格式：MAKER: 關鍵字在前，下一行為型號。
        回傳 (sub_equip, maker, model)
        """
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        maker = model = sub_equip = ''
        maker_found = False
        for i, line in enumerate(lines):
            m = _MAKER_PAT.match(line)
            if m:
                maker = m.group(1).strip().upper()
                maker_found = True
                # 下一行若存在且非 MAKER: 行，視為型號
                if i + 1 < len(lines) and not _MAKER_PAT.match(lines[i + 1]):
                    nxt = lines[i + 1].strip().upper()
                    pm = _PAREN_PAT.match(nxt)
                    model = pm.group(1) if pm else nxt
            elif not maker_found:
                cleaned = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', line).strip()
                if cleaned:
                    sub_equip = cleaned.upper()  # MAKER: 前面的行 = 子設備名稱
        return sub_equip, maker, model

    def _parse_principal_f2(cell_text, is_new_equip):
        """
        F2 子格式：無 MAKER: 關鍵字；括號 = 型號。
        is_new_equip=True（NO. 列）：第一行 = Maker
        is_new_equip=False（延續列）：第一行 = 子設備名稱（cur_eq 更新），保留原 Maker
        回傳 (sub_equip, maker, model)
        """
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        maker = model = sub_equip = ''
        text_lines = []
        for line in lines:
            pm = _PAREN_PAT.match(line)
            if pm:
                if not model:
                    model = pm.group(1).strip().upper()
            else:
                cleaned = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', line).strip()
                if cleaned:
                    text_lines.append(cleaned.upper())

        if text_lines:
            if is_new_equip:
                maker = text_lines[0]          # 第一行 = Maker
                # 第二行（如有）= 子設備描述，可附加到 Equipment 或忽略
            else:
                sub_equip = text_lines[0]      # 延續列第一行 = 子設備名稱
        return sub_equip, maker, model

    def _clean_oil(text):
        t = re.sub(r'^\*+', '', text).strip()
        t = re.sub(r'\s*\([^)]*\)\s*$', '', t)   # 去掉結尾括號備註
        return re.sub(r'\s+', ' ', t).strip().upper()

    def _clean_app(text):
        t = re.sub(r'^[-–—]\s*', '', text.strip())
        t = re.sub(r'\s*\(.*?\)\s*$', '', t)
        return re.sub(r'\s+', ' ', t).strip().upper()

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                # 找標題列
                hdr_idx = no_col = eq_col = pp_col = app_col = oil_col = None
                for i, row in enumerate(table[:5]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'PRINCIPAL PARTICULAR' in row_u and 'APPLICATION POINT' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if re.search(r'\bNO\b\.?', cu) and no_col is None:
                                no_col = j
                            elif 'NAME OF MACHINERY' in cu and eq_col is None:
                                eq_col = j
                            elif 'PRINCIPAL PARTICULAR' in cu and pp_col is None:
                                pp_col = j
                            elif 'APPLICATION POINT' in cu and app_col is None:
                                app_col = j
                            elif ('L.O COMPANY' in cu or 'LO COMPANY' in cu
                                  or 'L.O GRADE' in cu or 'LO GRADE' in cu
                                  or ('TOTAL' in cu and 'L.O' in cu)) and oil_col is None:
                                oil_col = j
                        break

                if hdr_idx is None or app_col is None:
                    continue
                if no_col  is None: no_col  = 0
                if eq_col  is None: eq_col  = 1
                if pp_col  is None: pp_col  = 2
                if oil_col is None: oil_col = len(table[hdr_idx]) - 1

                cur_eq = cur_maker = cur_model = ''

                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]

                    def _get(col):
                        return cells[col] if col < len(cells) else ''

                    no_raw  = _get(no_col)
                    eq_raw  = _get(eq_col)
                    pp_raw  = _get(pp_col)
                    app_raw = _get(app_col)
                    oil_raw = _get(oil_col)

                    is_new = bool(re.match(r'^\d+$', no_raw.strip()))

                    if is_new:
                        # 新主設備
                        cur_eq = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', eq_raw).strip().upper()
                        cur_maker = cur_model = ''
                        if pp_raw:
                            if _has_maker_kw(pp_raw):                 # F1
                                sub_eq, mk, mo = _parse_principal_f1(pp_raw)
                            else:                                      # F2
                                sub_eq, mk, mo = _parse_principal_f2(pp_raw, True)
                            if mk: cur_maker = mk
                            if mo: cur_model = mo
                            if sub_eq and not cur_eq: cur_eq = sub_eq

                    elif pp_raw:
                        # 延續列
                        if _has_maker_kw(pp_raw):                     # F1 子設備切換
                            sub_eq, mk, mo = _parse_principal_f1(pp_raw)
                            if sub_eq: cur_eq = sub_eq
                            if mk: cur_maker = mk
                            if mo: cur_model = mo
                            if sub_eq and not mk:
                                cur_maker = cur_model = ''             # 子設備無 Maker → 清空等待補充
                        else:                                          # F2 子設備切換
                            sub_eq, mk, mo = _parse_principal_f2(pp_raw, False)
                            if sub_eq: cur_eq = sub_eq                # 更新子設備名稱
                            # F2 延續列保留原 cur_maker（不清空）
                            if mo: cur_model = mo

                    elif eq_raw:
                        eq_clean = re.sub(r'[\u4e00-\u9fff\uff08\uff09（）]+', '', eq_raw).strip().upper()
                        if eq_clean: cur_eq = eq_clean

                    if not app_raw or not oil_raw:
                        continue

                    oil_clean = _clean_oil(oil_raw)
                    app_clean = _clean_app(app_raw)

                    if not oil_clean or not app_clean:
                        continue
                    if re.fullmatch(r'[\d\s\.,]+', oil_clean):
                        continue

                    records.append({
                        '船號'     : ship_id,
                        '設備名稱' : cur_eq,
                        '設備廠家' : cur_maker,
                        '設備型號' : cur_model,
                        '潤滑部位' : app_clean,
                        '推薦潤滑油': oil_clean,
                        '_page'   : pg_no,
                    })

    return records


def parse_format_N(pdf_path, ship_id):
    """
    Format N：頁面旋轉 90° CCW 的 LUB. OIL CHART
    pdfplumber 讀取為反向文字；改用 fitz word 座標解析。
    每個 x 群組 = 原始表格的一個 application row；y 範圍對應原始欄位類型。
    """
    records = []

    # y 範圍對應原始欄位（高 y = 原始左側，低 y = 原始右側）
    Y_BANDS = [
        ('no',     780, 820),
        ('name',   665, 780),
        ('princ',  560, 665),
        ('qty',    520, 560),
        ('app',    370, 520),
        ('fill',   330, 370),
        ('ltr',    305, 330),
        ('oil',    170, 305),
    ]

    def y_to_col(y):
        for col, lo, hi in Y_BANDS:
            if lo <= y < hi:
                return col
        return None

    doc = fitz.open(pdf_path)
    try:
        for pg_idx in range(len(doc)):
            page = doc[pg_idx]
            words = page.get_text("words")
            if not words:
                continue

            # 跳過 header column（最左的 x 群組，x ≈ 60-90）
            data_words = [w for w in words if w[0] >= 95]
            if not data_words:
                continue

            # 依 x 分組（tolerance 6 pt）
            x_groups = {}
            for w in data_words:
                x_key = round(w[0] / 6) * 6
                x_groups.setdefault(x_key, []).append(w)

            # 用兩段式處理：先收集所有 x_group 的解析結果，最後 flush
            cur_eq = cur_maker = cur_model = cur_app = ''
            pending = []  # list of (cur_app, oil_clean, page_no)

            def flush_pending():
                for app_t, oil_t, p in pending:
                    records.append({
                        '船號':     ship_id,
                        '設備名稱': cur_eq,
                        '設備廠家': cur_maker,
                        '設備型號': cur_model,
                        '潤滑部位': app_t,
                        '推薦潤滑油': oil_t,
                        '_page':    p,
                    })
                pending.clear()

            for x_key in sorted(x_groups):
                grp = x_groups[x_key]
                # 將 group 內 words 依 col 分類
                col_words = {col: [] for col, _, _ in Y_BANDS}
                for w in grp:
                    c = y_to_col(w[1])
                    if c:
                        col_words[c].append(w)

                # 取每個 col 的文字（依 y 排序）
                def col_text(c):
                    ws = sorted(col_words[c], key=lambda w: w[1])
                    return ' '.join(w[4] for w in ws).strip()

                no_t    = col_text('no')
                name_t  = col_text('name')
                princ_t = col_text('princ')
                app_t   = col_text('app')
                oil_t   = col_text('oil')

                # 新設備偵測：no_t 是純整數
                if no_t and re.fullmatch(r'\d+', no_t):
                    flush_pending()  # 寫入前一設備的所有 records
                    cur_eq = name_t.upper() if name_t else ''
                    cur_maker = princ_t.upper() if princ_t else ''
                    cur_model = ''
                    cur_app = ''
                else:
                    # 延續行：累加 NAME / PRINCIPAL
                    if name_t:
                        cur_eq = (cur_eq + ' ' + name_t.upper()).strip()
                    if princ_t:
                        if not cur_maker:
                            cur_maker = princ_t.upper()
                        elif not cur_model:
                            cur_model = princ_t.upper()
                        else:
                            cur_model = (cur_model + ' ' + princ_t.upper()).strip()

                # 更新 cur_app
                if app_t:
                    cur_app = re.sub(r'\s+', ' ', app_t).strip().upper()

                # 有 oil 才記錄
                if oil_t and cur_app and cur_eq:
                    oil_clean = re.sub(r'\s+', ' ', oil_t).strip().upper()
                    if not re.fullmatch(r'[\d\s\.,/\-\[\]]+', oil_clean):
                        pending.append((cur_app, oil_clean, pg_idx + 1))

            flush_pending()
    finally:
        doc.close()

    return records


def parse_format_M(pdf_path, ship_id):
    """
    Format M：Equipment Name(Type) | Manufacturer | Application Point | Sets | Recommended Oil
    pdfplumber 將每行的所有欄位擠入 col 0，改用 extract_words() + x 座標分欄解析。
    每頁動態偵測 header word 位置 → 推算欄位邊界（不同 PDF 欄位 x 不一致）。
    """
    records = []

    def _detect_bounds(words):
        """從 header words 動態偵測欄位 x 邊界。回傳 dict 或 None。"""
        # y 群組
        by_y = {}
        for w in words:
            y_k = round(w['top'] / 3) * 3
            by_y.setdefault(y_k, []).append(w)

        for y in sorted(by_y):
            row_words = by_y[y]
            row_text = ' '.join(w['text'].upper() for w in row_words)
            if 'MANUFACTURER' not in row_text and 'MAKER' not in row_text:
                continue
            if 'APPLICATION' not in row_text and 'POINT' not in row_text:
                continue

            # 涵蓋上下相鄰 row（雙層 header：Recommended Oil 可能在另一 row）
            extended = list(row_words)
            for dy in (-15, -12, -9, -6, -3, 3, 6, 9):
                ny = y + dy
                if ny in by_y and ny != y:
                    extended.extend(by_y[ny])

            # 找各 header word 的 x_start / x_end
            mfr_x = app_x = oil_x = end_x = None
            equip_end = no_end = sets_end = None
            for w in extended:
                cu = w['text'].upper().strip().rstrip(':')
                if cu in ('MANUFACTURER', 'MAKER') and mfr_x is None:
                    mfr_x = w['x0']
                elif cu == 'APPLICATION' and app_x is None:
                    app_x = w['x0']
                elif cu == 'RECOMMENDED' and oil_x is None:
                    oil_x = w['x0']
                elif cu == 'OIL' and oil_x is None and (app_x is None or w['x0'] > app_x + 50):
                    oil_x = w['x0']
                elif cu in ("Q'TY/SET", "QTY/SET", "Q'TY", 'QTY') and end_x is None:
                    end_x = w['x0']
                elif cu in ('NO.', 'NO') and no_end is None:
                    no_end = w['x1']
                elif cu in ('EQUIPMENT', 'EQUIPMENTNAME', 'NAME'):
                    equip_end = w['x1'] if equip_end is None else max(equip_end, w['x1'])
                elif cu == 'SETS':
                    sets_end = w['x1']

            if mfr_x is None or app_x is None or oil_x is None:
                continue

            # 找 Sets header word x_start（用於 app/sets 分界）
            sets_x = None
            for w in extended:
                cu = w['text'].upper().strip().rstrip(':')
                if cu == 'SETS':
                    sets_x = w['x0']
                    break

            # 計算邊界（需取較保守值避免資料 word 越界）
            equip_start = (no_end + 5) if no_end is not None else 30
            mfr_start   = max((equip_end + mfr_x) / 2 if equip_end else (mfr_x - 30),
                              mfr_x - 30)
            app_start   = max((mfr_x + app_x) / 2, app_x - 50)
            # Sets 獨立 column，避免 Sets 數字落入 app 或 oil
            if sets_x is not None and app_x < sets_x < oil_x:
                app_end_col = sets_x - 5
                sets_start  = sets_x - 5
                sets_end_col = (sets_end + 5) if sets_end else (sets_x + 20)
                if sets_end_col >= oil_x - 10:
                    sets_end_col = oil_x - 10
                oil_start   = sets_end_col
            else:
                app_end_col = oil_x - 25
                sets_start  = None
                oil_start   = oil_x - 25
            oil_end     = (end_x - 5) if end_x is not None else (oil_x + 130)

            bounds_dict = {
                'no':    (0, equip_start),
                'equip': (equip_start, mfr_start),
                'mfr':   (mfr_start, app_start),
                'app':   (app_start, app_end_col),
                'oil':   (oil_start, oil_end),
            }
            if sets_start is not None:
                bounds_dict['sets'] = (sets_start, oil_start)
            return bounds_dict, y
        return None, None

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            words = page.extract_words()
            if not words:
                continue

            bounds, header_y = _detect_bounds(words)
            if bounds is None:
                continue

            def _col(x):
                for c, (lo, hi) in bounds.items():
                    if lo <= x < hi:
                        return c
                return None

            # 依 y 座標分組
            lines = {}
            for w in words:
                col = _col(w['x0'])
                if col is None:
                    continue
                y_key = round(w['top'] / 3) * 3
                lines.setdefault(y_key, {}).setdefault(col, []).append(w['text'])

            cur_eq = cur_maker = cur_model = cur_app = ''
            pending = []   # list of (app, oil, page) — flushed with FINAL cur_eq/maker/model

            def _flush():
                for a, o, p in pending:
                    records.append({
                        '船號':     ship_id,
                        '設備名稱': cur_eq,
                        '設備廠家': cur_maker,
                        '設備型號': cur_model,
                        '潤滑部位': a,
                        '推薦潤滑油': o,
                        '_page':    p,
                    })
                pending.clear()

            data_start_y = round(header_y / 3) * 3 + 3  # 略過 header

            for y in sorted(lines):
                if y <= data_start_y:
                    continue
                line = lines[y]
                no_txt    = ' '.join(line.get('no',    [])).strip()
                equip_txt = ' '.join(line.get('equip', [])).strip()
                mfr_txt   = ' '.join(line.get('mfr',   [])).strip()
                app_txt   = ' '.join(line.get('app',   [])).strip()
                oil_txt   = ' '.join(line.get('oil',   [])).strip()

                # 偵測新設備：No. 欄位為新整數
                is_new_eq = bool(no_txt and re.fullmatch(r'\d+', no_txt))
                if is_new_eq:
                    _flush()  # 先把上一設備的 pending records 用最終 maker/model 寫入
                    cur_model = ''
                    if cur_eq:        # 已存在前一設備 → 同步重置 maker
                        cur_maker = ''
                    cur_eq = ''
                    cur_app = ''

                # 更新設備（Equipment Name 含括號型號）
                if equip_txt:
                    m = re.search(r'\(([^)]+)\)', equip_txt)
                    if m:
                        cur_model = m.group(1).strip().upper()
                    eq_base = re.sub(r'\s*\([^)]+\)', '', equip_txt).strip().upper()
                    if eq_base:
                        if not cur_eq:
                            cur_eq = eq_base
                        elif eq_base not in cur_eq:
                            # 設備名稱多行 — 累加（如 PNEUMATIC CLOSING DAMPER + for No.2 ...）
                            cur_eq = (cur_eq + ' ' + eq_base).strip()

                # mfr 累積（多行 maker，如 "HANHWA\nENGINE"）
                if mfr_txt:
                    mfr_up = mfr_txt.upper()
                    if cur_maker and mfr_up not in cur_maker:
                        cur_maker = (cur_maker + ' ' + mfr_up).strip()
                    else:
                        cur_maker = mfr_up

                # app 累積（多行 application）：若新 app 但無 oil → 視為延續
                if app_txt:
                    app_up = re.sub(r'\s+', ' ', app_txt).strip().upper()
                    # 純整數視為 Sets 數量，忽略
                    if re.fullmatch(r'\d+', app_up):
                        pass
                    elif oil_txt:
                        # 完整資料行：使用本行 app
                        cur_app = app_up
                    else:
                        # 延續行：累加到 cur_app
                        if cur_app and app_up not in cur_app:
                            cur_app = (cur_app + ' ' + app_up).strip()
                        else:
                            cur_app = app_up

                if not cur_eq or not cur_app:
                    continue

                if not oil_txt:
                    continue

                oil_clean = re.sub(r'\s+', ' ', oil_txt).strip().upper()
                if re.fullmatch(r'[\d\s\.,/\-\[\]]+', oil_clean):
                    continue

                pending.append((cur_app, oil_clean, pg_no))

            _flush()  # 頁面結束時把最後一個設備的 pending 寫入

    return records


def parse_format_H(pdf_path, ship_id):
    """
    Format H：NTS 中英雙語 LUB. OIL CHART
    欄位：序号/Code | 名称/Name | 型号/Type | 制造商/MAKER | 用途/Application | 滑油牌号/Lub.Oil brand | ...
    標題跨 row0~row4，每個設備以合併儲存格（Name/Type/Maker 後續行為 None），
    每行對應一個 Application + Oil 組合。
    """
    records = []
    _SPEC_PAT = re.compile(r'^S\.?MCR\s*[:：]|^MCR\s*[:：]|\d+\s*KW|\d+\s*RPM', re.I)

    def _clean_type(raw):
        lines = [l.strip() for l in raw.split('\n') if l.strip()]
        result = []
        for l in lines:
            if _SPEC_PAT.match(l):
                break
            result.append(l)
        if len(result) >= 2 and result[0].endswith('-'):
            result = [result[0] + result[1]] + result[2:]
        return re.sub(r'\s+', ' ', ' '.join(result)).strip().upper()

    def _clean_eq(raw):
        t = re.sub(r'[一-鿿（）（）\(\)]+', ' ', raw)
        return re.sub(r'\s+', ' ', t).strip().upper()

    def _clean_app(raw):
        t = re.sub(r'[一-鿿（）（）]+', ' ', raw)
        return re.sub(r'\s+', ' ', t).strip().upper()

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 5:
                    continue

                name_col = type_col = maker_col = app_col = oil_col = None
                hdr_bottom = 0
                for i, row in enumerate(table[:6]):
                    for j, cell in enumerate(row):
                        c = str(cell or '')
                        cu = c.upper()
                        if ('NAME' in cu or 'EQUIPMENT' in cu
                                or '名称' in c or '名稱' in c or '设备' in c) and name_col is None:
                            name_col = j
                        if ('TYPE' in cu or '型号' in c or '型號' in c) and type_col is None:
                            type_col = j
                        if ('MAKER' in cu or '制造商' in c) and maker_col is None:
                            maker_col = j
                            hdr_bottom = max(hdr_bottom, i)
                        if ('APPLICATION' in cu or 'LUBRICATION' in cu or 'LUBRICATING' in cu
                                or '用途' in c or '用油' in c) and app_col is None:
                            app_col = j
                        if ('LUB. OIL BRAND' in cu or 'LUB OIL BRAND' in cu
                                or 'L.O. BRAND' in cu or 'L.O BRAND' in cu
                                or '滑油牌号' in c) and oil_col is None:
                            oil_col = j

                if maker_col is None or app_col is None or oil_col is None:
                    continue

                cur_eq = cur_maker = cur_model = ''

                for row in table[hdr_bottom + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]

                    def _get(col):
                        return cells[col] if col is not None and col < len(cells) else ''

                    name_raw  = _get(name_col)
                    type_raw  = _get(type_col)
                    maker_raw = _get(maker_col)
                    app_raw   = _get(app_col)
                    oil_raw   = _get(oil_col)

                    # 略過段落標題行（Application 與 Oil 均空白）
                    if not app_raw.strip() and not oil_raw.strip():
                        continue

                    if name_raw.strip():
                        eq = _clean_eq(name_raw)
                        if eq:
                            cur_eq = eq
                    if type_raw.strip():
                        t = _clean_type(type_raw)
                        if t:
                            cur_model = t
                    if maker_raw.strip():
                        cur_maker = _clean_eq(maker_raw)

                    if not cur_eq:
                        continue

                    app_clean = _clean_app(app_raw)
                    oil_clean = re.sub(r'\s+', ' ', oil_raw.split('\n')[0]).strip().upper()

                    if not app_clean or not oil_clean:
                        continue
                    if re.fullmatch(r'[\d\s\.,/\-]+', oil_clean):
                        continue

                    records.append({
                        '船號':     ship_id,
                        '設備名稱': cur_eq,
                        '設備廠家': cur_maker,
                        '設備型號': cur_model,
                        '潤滑部位': app_clean,
                        '推薦潤滑油': oil_clean,
                        '_page':    pg_no,
                    })

    return records


def parse_format_G(pdf_path, ship_id):
    """
    Format G：K Shipbuilding LUB. OIL CHART
    兩段表格結構：
      A. EQUIPMENT LIST — EQUIPMENT | MAKER | MODEL | REMARK（設備廠家對照）
      B. LUB. OIL CHART — EQUIPMENT | APPLICATION POINT | PRODUCT | ...（潤滑油規格）
    每個 EQUIPMENT 列的 APPLICATION POINT 與 PRODUCT 欄均為多行，逐行配對。
    """
    records = []
    equip_maker = {}   # EQUIPMENT.upper() → (maker, model)

    def _clean_model(m):
        # 移除 "/ 1020kW, 900rpm" 等功率規格
        m = re.sub(r'\s*/\s+.*$', '', m).strip()
        m = re.sub(r'\s+/\s*$', '', m).strip()
        return m.upper()

    def _clean_eq(raw):
        # 去除前導 "- " 符號與中文
        t = re.sub(r'^[\-\s]+', '', raw)
        t = re.sub(r'[一-鿿（）（）]+', '', t)
        return re.sub(r'\s+', ' ', t).strip().upper()

    with pdfplumber.open(pdf_path) as pdf:
        # ── Pass 1：建立 EQUIPMENT → (Maker, Model) 對照表 ──────────
        for page in pdf.pages:
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                hdr_idx = eq_col = mk_col = mo_col = None
                for i, row in enumerate(table[:4]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'EQUIPMENT' in row_u and 'MAKER' in row_u and 'MODEL' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if 'EQUIPMENT' in cu and eq_col is None: eq_col = j
                            elif 'MAKER' in cu and mk_col is None: mk_col = j
                            elif 'MODEL' in cu and mo_col is None: mo_col = j
                        break
                if hdr_idx is None or eq_col is None:
                    continue

                cur_parent = ''
                cur_parent_maker = ''
                cur_parent_model = ''
                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]
                    eq_raw = cells[eq_col] if eq_col < len(cells) else ''
                    mk_raw = cells[mk_col] if mk_col < len(cells) else ''
                    mo_raw = cells[mo_col] if mo_col < len(cells) else ''

                    eq_lines = [l for l in eq_raw.split('\n') if l.strip()]
                    mk_lines = [l.strip() for l in mk_raw.split('\n') if l.strip()]
                    mo_lines = [l.strip() for l in mo_raw.split('\n') if l.strip()]

                    if not eq_lines:
                        continue

                    # 每個 EQUIPMENT 列可能有多行（母設備 + 子設備）
                    for li, eq_line in enumerate(eq_lines):
                        is_sub = eq_line.strip().startswith('-')
                        eq_key = _clean_eq(eq_line)
                        if not eq_key:
                            continue
                        maker = mk_lines[li].upper() if li < len(mk_lines) else ''
                        model = _clean_model(mo_lines[li]) if li < len(mo_lines) else ''

                        if not is_sub:
                            cur_parent = eq_key
                            cur_parent_maker = maker
                            cur_parent_model = model
                            if eq_key not in equip_maker:
                                equip_maker[eq_key] = (maker, model)
                        else:
                            # 子設備：本身登記，並補齊母設備空缺
                            if eq_key not in equip_maker and maker:
                                equip_maker[eq_key] = (maker, model)
                            if cur_parent and (not equip_maker.get(cur_parent, ('', ''))[0]) and maker:
                                equip_maker[cur_parent] = (maker, model)

        # ── Pass 2：解析 B. LUB. OIL CHART ──────────────────────────
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                hdr_idx = eq_col = app_col = oil_col = None
                for i, row in enumerate(table[:4]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'APPLICATION POINT' in row_u and 'PRODUCT' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if 'EQUIPMENT' in cu and eq_col is None: eq_col = j
                            elif 'APPLICATION POINT' in cu and app_col is None: app_col = j
                            elif 'PRODUCT' in cu and oil_col is None: oil_col = j
                        break
                if hdr_idx is None or app_col is None or oil_col is None:
                    continue

                cur_eq = ''
                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]
                    eq_raw  = cells[eq_col]  if eq_col  is not None and eq_col  < len(cells) else ''
                    app_raw = cells[app_col] if app_col < len(cells) else ''
                    oil_raw = cells[oil_col] if oil_col < len(cells) else ''

                    if eq_raw.strip():
                        cur_eq = _clean_eq(eq_raw.split('\n')[0])

                    if not cur_eq or not app_raw or not oil_raw:
                        continue

                    app_lines = [l.strip() for l in app_raw.split('\n') if l.strip()]
                    oil_lines = [l.strip() for l in oil_raw.split('\n') if l.strip()]

                    maker, model = equip_maker.get(cur_eq, ('', ''))

                    for app, oil in zip(app_lines, oil_lines):
                        app_clean = re.sub(r'\s+', ' ', app).strip().upper()
                        oil_clean = re.sub(r'\s+', ' ', oil).strip().upper()
                        # 跳過純數字（數量欄溢入）
                        if not oil_clean or re.fullmatch(r'[\d\s\.,/\-]+', oil_clean):
                            continue
                        if not app_clean:
                            continue
                        records.append({
                            '船號': ship_id,
                            '設備名稱': cur_eq,
                            '設備廠家': maker,
                            '設備型號': model,
                            '潤滑部位': app_clean,
                            '推薦潤滑油': oil_clean,
                            '_page': pg_no,
                        })

    print(f"    EQUIPMENT→Maker 對照：{len(equip_maker)} 筆")
    return records


def _parse_eq_cell_I(cell_text):
    """
    解析 Format I 的 EQUIPMENT 欄位。
    支援以下格式（括號內以 '/' 分隔 Maker 與 Model/Type）：
      'MAIN ENGINE (HD HHI / 6G70ME-C10.5 TIER III)' → Maker=HD HHI, Model=6G70ME-C10.5 TIER III
      'HYD. DECK STAND (TAEIL M-TECH)'               → Maker=TAEIL M-TECH, Model=''
      'PROPELLER'                                     → Maker='', Model=''
    回傳 (equipment, maker, model)
    """
    text = re.sub(r'\s+', ' ', cell_text).strip().upper()
    # 格式 A：EQUIPMENT (MAKER / MODEL)
    m = re.match(r'^(.*?)\s*\(([^/]+)/(.+)\)\s*$', text)
    if m:
        equipment = re.sub(r'^\d+[\.\s]+', '', m.group(1)).strip()
        return equipment, m.group(2).strip(), m.group(3).strip()
    # 格式 B：EQUIPMENT (MAKER) — 括號內無斜線
    m = re.match(r'^(.*?)\s*\(([^)]+)\)\s*$', text)
    if m:
        equipment = re.sub(r'^\d+[\.\s]+', '', m.group(1)).strip()
        return equipment, m.group(2).strip(), ''
    # 無括號 → 只有設備名稱
    eq = re.sub(r'^\d+[\.\s]+', '', text).strip()
    return eq, '', ''


def parse_format_I(pdf_path, ship_id):
    """
    Format I：EQUIPMENT (MAKER/TYPE) + APPLICATION POINT + KIND OF LUB. OIL
    典型來源：HN5801 供應商格式（TotalEnergies 欄位）

    實際欄位佈局（依 PDF 擷取順序）：
      col 0  NO.
      col 1  EQUIPMENT (MAKER/TYPE)   → eq_col
      col 2  Q'TY                     → qty_col（section 標題出現於此）
      col 3  APPLICATION POINT        → app_col
      col 4  KIND OF LUB. OIL         → oil_col

    Section 標題行：Q'TY 欄含非數字文字（SYSTEM OIL / CYLINDER OIL），
                   APPLICATION POINT 與 KIND OF LUB. OIL 均為空。
    資料行：APPLICATION POINT 有值且 KIND OF LUB. OIL 有值。
    以 cur_section 作為 Part to be lubricated。
    若資料行的 APPLICATION POINT 本身即為 section 型標籤（如 G/E 的 SYSTEM OIL），
    直接用 APPLICATION POINT。
    """
    records = []
    _IS_NUMERIC = re.compile(r'^[\d\s\.\-,]+$')

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                # 找標題列及欄位索引
                hdr_idx = eq_col = qty_col = app_col = oil_col = None
                for i, row in enumerate(table[:5]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'KIND OF LUB' in row_u and 'APPLICATION POINT' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if 'EQUIPMENT' in cu and eq_col is None:
                                eq_col = j
                            elif re.search(r"Q'?TY|QUANTITY", cu) and qty_col is None and 'EQUIPMENT' not in cu:
                                qty_col = j
                            elif 'APPLICATION POINT' in cu and app_col is None:
                                app_col = j
                            elif 'KIND OF LUB' in cu and oil_col is None:
                                oil_col = j
                        break

                if hdr_idx is None or eq_col is None or app_col is None or oil_col is None:
                    continue
                if qty_col is None:
                    qty_col = eq_col + 1  # fallback

                cur_eq = cur_maker = cur_model = ''
                cur_section = ''

                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]

                    eq_cell  = cells[eq_col]  if eq_col  < len(cells) else ''
                    qty_cell = cells[qty_col] if qty_col < len(cells) else ''
                    app_cell = cells[app_col] if app_col < len(cells) else ''
                    oil_cell = cells[oil_col] if oil_col < len(cells) else ''

                    # 更新設備上下文（EQUIPMENT 欄非空時一律重置 maker/model）
                    if eq_cell.strip():
                        eq, mk, md = _parse_eq_cell_I(eq_cell)
                        if eq: cur_eq = eq
                        cur_maker = mk
                        cur_model = md

                    qty_clean = re.sub(r'\s+', ' ', qty_cell).strip().upper()
                    app_clean = re.sub(r'\s+', ' ', app_cell).strip().upper()
                    oil_clean = re.sub(r'\s+', ' ', oil_cell).strip().upper()

                    # Section 標題行：Q'TY 欄含非數字文字，APP 與 OIL 均空
                    if qty_clean and not _IS_NUMERIC.match(qty_clean) and not app_clean and not oil_clean:
                        cur_section = qty_clean
                        continue

                    # 有潤滑油才記錄
                    if not oil_clean:
                        continue
                    if _IS_NUMERIC.match(oil_clean):
                        continue

                    # Part to be lubricated：
                    #  - 優先用 APPLICATION POINT（若非空）
                    #  - APPLICATION POINT 空 → 用 cur_section
                    part = app_clean if app_clean else cur_section
                    if not part:
                        continue

                    records.append({
                        '船號'     : ship_id,
                        '設備名稱' : cur_eq,
                        '設備廠家' : cur_maker,
                        '設備型號' : cur_model,
                        '潤滑部位' : part,
                        '推薦潤滑油': oil_clean,
                        '_page'   : pg_no,
                    })

    return records


def parse_format_K(pdf_path, ship_id):
    """
    Format K：JIT 船东供油料清单（H2724 / 30408027JL 格式）
    欄位：No.序号 | EQUIPMENT設備名稱 | SET套 | LUBRICATING POINT滑油用途 | KIND OF LUBRICANT滑油種類
    EQUIPMENT 欄以 MAKER: / TYPE: 關鍵字包含廠家與型號（複用 _parse_eq_cell 解析）。
    段落標題行（如「机装部分MACHINERY PART」）APP 與 OIL 欄均空，直接跳過。
    雙語內容去除中文字符，取英文部分。
    """
    records = []

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                hdr_idx = no_col = eq_col = app_col = oil_col = None
                for i, row in enumerate(table[:5]):
                    row_u = ' '.join(str(c or '').upper() for c in row)
                    if 'LUBRICATING POINT' in row_u and 'KIND OF LUBRICANT' in row_u:
                        hdr_idx = i
                        for j, cell in enumerate(row):
                            cu = str(cell or '').upper()
                            if re.search(r'\bNO\b', cu) and no_col is None:
                                no_col = j
                            elif ('EQUIPMENT' in cu or '设备名称' in str(cell or '')) and eq_col is None:
                                eq_col = j
                            elif 'LUBRICATING POINT' in cu and app_col is None:
                                app_col = j
                            elif 'KIND OF LUBRICANT' in cu and oil_col is None:
                                oil_col = j
                        break

                if hdr_idx is None or app_col is None or oil_col is None:
                    continue
                if no_col is None: no_col = 0
                if eq_col is None: eq_col = 1

                cur_eq = cur_maker = cur_model = ''

                for row in table[hdr_idx + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]

                    eq_raw  = cells[eq_col]  if eq_col  < len(cells) else ''
                    app_raw = cells[app_col] if app_col < len(cells) else ''
                    oil_raw = cells[oil_col] if oil_col < len(cells) else ''

                    # 跳過段落標題行（APP 與 OIL 均空）
                    if not app_raw.strip() and not oil_raw.strip():
                        continue

                    # 更新設備上下文（EQUIPMENT 欄非空時解析 MAKER:/TYPE: 關鍵字）
                    if eq_raw.strip():
                        eq, mk, mo = _parse_eq_cell(eq_raw)
                        if eq: cur_eq = eq
                        if mk: cur_maker = mk
                        if mo: cur_model = mo

                    # 去除中文，取英文部分
                    app_clean = re.sub(r'[一-鿿（）（）（）]+', ' ', app_raw.split('\n')[0])
                    app_clean = re.sub(r'\s+', ' ', app_clean).strip().upper()
                    oil_clean = re.sub(r'[一-鿿（）（）（）]+', ' ', oil_raw.split('\n')[0])
                    oil_clean = re.sub(r'\s+', ' ', oil_clean).strip().upper()

                    if not app_clean or not oil_clean:
                        continue
                    if re.fullmatch(r'[\d\s\.,/\-]+', oil_clean):
                        continue

                    records.append({
                        '船號':      ship_id,
                        '設備名稱':  cur_eq,
                        '設備廠家':  cur_maker,
                        '設備型號':  cur_model,
                        '潤滑部位':  app_clean,
                        '推薦潤滑油': oil_clean,
                        '_page':    pg_no,
                    })

    return records


def parse_format_J(pdf_path, ship_id):
    """
    Format J：中英雙語 LUB. OIL CHART（NDY1305_ 格式）
    雙層標題結構：
      Row 0（合併標題）：NO.序号 | Equipment設備信息（跨4欄）| Point潤滑點 | Oil Brand滑油牌號 | Qty | Remark
      Row 1（子標題）：        | Name名稱 | Type型号 | QTY數量 | Maker/Supplier廠家 | | TotalEnergies... | ...

    資料欄位：
      Name/设备名称  → 設備名稱（合併儲存格，沿用至空值）
      Type/设备信息  → 設備型號（取首行，過濾 SMEC/NCR/KW 等規格行）
      Maker/Supplier → 設備廠家（合併儲存格，沿用至空值）
      Point/润滑点   → 潤滑部位（取英文首行，去除中文）
      Oil Brand      → 推薦潤滑油（取英文首行，去除中文）
    """
    records = []
    _SPEC_PAT = re.compile(r'SMEC\s*[：:]|NCR\s*[：:]|\d+\s*KW\b|\d+\s*RPM\b', re.I)

    def _first_en_line(text):
        """取第一個含英文內容的行，去除中文字符"""
        for line in (text or '').split('\n'):
            en = re.sub(r'[一-鿿（）（）（）]+', '', line).strip()
            if en:
                return re.sub(r'\s+', ' ', en).strip().upper()
        return ''

    def _extract_type(text):
        """提取型號，在 SMEC/NCR/KW 規格行前截斷"""
        result = []
        for line in (text or '').split('\n'):
            if _SPEC_PAT.search(line):
                break
            en = re.sub(r'[一-鿿（）（）（）]+', '', line).strip()
            if en:
                result.append(en)
        return re.sub(r'\s+', ' ', ' '.join(result)).strip().upper()

    with pdfplumber.open(pdf_path) as pdf:
        for pg_no, page in enumerate(pdf.pages, start=1):
            for table in page.extract_tables():
                if not table or len(table) < 4:
                    continue

                # 跨多列掃描標題，累積欄位索引
                name_col = type_col = maker_col = point_col = oil_col = None
                hdr_bottom = 0

                for i, row in enumerate(table[:6]):
                    for j, cell in enumerate(row):
                        cu = str(cell or '').upper()
                        c  = str(cell or '')
                        if ('NAME' in cu or '名称' in c or '名稱' in c) and name_col is None:
                            name_col = j; hdr_bottom = i
                        if ('TYPE' in cu or '设备信息' in c or '型号' in c) and type_col is None and name_col is not None:
                            type_col = j; hdr_bottom = i
                        if ('MAKER' in cu or 'SUPPLIER' in cu or '厂家' in c) and maker_col is None:
                            maker_col = j; hdr_bottom = i
                        if ('POINT' in cu or '润滑点' in c or '滑點' in c) and point_col is None:
                            point_col = j; hdr_bottom = i
                        if 'OIL BRAND' in cu and oil_col is None:
                            oil_col = j; hdr_bottom = i

                if name_col is None or point_col is None or oil_col is None or maker_col is None:
                    continue

                cur_name = cur_type = cur_maker = ''

                for row in table[hdr_bottom + 1:]:
                    if not any(c for c in row if c):
                        continue
                    cells = [str(c or '').strip() for c in row]

                    def get(col):
                        return cells[col] if col is not None and col < len(cells) else ''

                    name_raw  = get(name_col)
                    type_raw  = get(type_col) if type_col is not None else ''
                    maker_raw = get(maker_col)
                    point_raw = get(point_col)
                    oil_raw   = get(oil_col)

                    # 跳過次級標題行（Point 與 Oil 均空）
                    if not point_raw.strip() and not oil_raw.strip():
                        continue

                    # 更新設備上下文（合併儲存格模擬）
                    if name_raw.strip():
                        n = _first_en_line(name_raw)
                        if n: cur_name = n
                    if type_raw.strip():
                        t = _extract_type(type_raw)
                        if t: cur_type = t
                    if maker_raw.strip():
                        m = _first_en_line(maker_raw)
                        if m: cur_maker = m

                    point_clean = _first_en_line(point_raw)
                    oil_clean   = _first_en_line(oil_raw)

                    if not point_clean or not oil_clean:
                        continue
                    # 跳過容量數值（如 19000L、10000L(1)）
                    if re.match(r'^\d+\s*[LlKkGg]', oil_clean) or re.fullmatch(r'[\d\s\.,/\-]+', oil_clean):
                        continue

                    records.append({
                        '船號':      ship_id,
                        '設備名稱':  cur_name,
                        '設備廠家':  cur_maker,
                        '設備型號':  cur_type,
                        '潤滑部位':  point_clean,
                        '推薦潤滑油': oil_clean,
                        '_page':    pg_no,
                    })

    return records


def parse_pdf(pdf_path, ship_id, fmt=None):
    if fmt is None:
        fmt = detect_format(pdf_path)
    print(f"    Format {fmt} 偵測")
    if fmt == 'B': return parse_format_B(pdf_path, ship_id), fmt
    if fmt == 'C': return parse_format_C(pdf_path, ship_id), fmt
    if fmt == 'D': return parse_format_D(pdf_path, ship_id), fmt
    if fmt == 'E': return parse_format_E(pdf_path, ship_id), fmt
    if fmt == 'F': return parse_format_F(pdf_path, ship_id), fmt
    if fmt == 'G': return parse_format_G(pdf_path, ship_id), fmt
    if fmt == 'H': return parse_format_H(pdf_path, ship_id), fmt
    if fmt == 'I': return parse_format_I(pdf_path, ship_id), fmt
    if fmt == 'J': return parse_format_J(pdf_path, ship_id), fmt
    if fmt == 'K': return parse_format_K(pdf_path, ship_id), fmt
    if fmt == 'L': return parse_format_H(pdf_path, ship_id), fmt  # 複用 Format H 解析器
    if fmt == 'M': return parse_format_M(pdf_path, ship_id), fmt
    if fmt == 'N': return parse_format_N(pdf_path, ship_id), fmt
    return parse_format_A(pdf_path, ship_id), fmt

def get_ship_id(fname):
    f = fname.upper()
    if '2359' in f: return 'SN2359'
    if '30408027' in f: return 'H2652 & H2653'
    if 'H1872' in f or 'M-53' in f: return 'H1872A'
    if '2265' in f or '2268' in f: return 'SN2265 & SN2268'
    # Hyundai HHI 船號（Format F）
    if '2933' in f and '2934' in f: return 'H2933 & H2934'
    if '2933' in f: return 'H2933'
    if '2934' in f: return 'H2934'
    return os.path.splitext(fname)[0][:40]

# ── 信心分數判斷 ─────────────────────────────────────────────────
def assign_confidence(records, fmt, pdf_path=None):
    """為每筆記錄指定信心分數"""
    results = []
    for r in records:
        page_no = r.get('_page', 1)
        # 判斷條件
        has_maker = bool(r.get('設備廠家', '').strip())
        has_model = bool(r.get('設備型號', '').strip())
        has_app   = bool(r.get('潤滑部位', '').strip())
        has_oil   = bool(r.get('推薦潤滑油', '').strip())

        if fmt in ('A', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N') and has_maker and has_model and has_app and has_oil:
            confidence = 'high'
            reason = '表格邊界清晰，欄位完整'
        elif has_oil and has_app and (has_maker or has_model):
            confidence = 'medium'
            reasons = []
            if not has_maker: reasons.append('缺 Maker')
            if not has_model: reasons.append('缺 Model')
            if fmt == 'B': reasons.append('中英文混合表格')
            if fmt == 'G': reasons.append('設備清單無對應 Maker')
            reason = '，'.join(reasons) if reasons else '跨頁或合併儲存格'
        else:
            confidence = 'low'
            reasons = []
            if not has_oil: reasons.append('缺潤滑油欄位')
            if not has_app: reasons.append('缺潤滑部位')
            if not has_maker and not has_model: reasons.append('缺 Maker 及 Model')
            reason = '，'.join(reasons) if reasons else '無結構化表格或純文字解析'

        results.append({**r, 'confidence': confidence, 'confidence_reason': reason})
    return results

# ── Excel 輸出 ───────────────────────────────────────────────────
def write_master_excel(df, path):
    wb = Workbook()
    ws = wb.active
    ws.title = 'nb_master'
    ws.freeze_panes = 'A2'

    out_cols = COLS + ['Source', 'source_file']
    for ci, col in enumerate(out_cols, 1):
        c = ws.cell(row=1, column=ci, value=col)
        c.font = Font(name='Arial', bold=True, color='FFFFFF', size=10)
        c.fill = PatternFill('solid', fgColor=HDR_BG)
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

    for ri, (_, row) in enumerate(df.iterrows(), start=2):
        bg = ODD_BG if ri % 2 == 1 else EVEN_BG
        for ci, col in enumerate(out_cols, 1):
            val = str(row.get(col, '')) if pd.notna(row.get(col, '')) else ''
            c = ws.cell(row=ri, column=ci, value=val)
            if col == 'Source':
                c.font = Font(name='Arial', size=9, color=NB_COLOR, bold=True)
            else:
                c.font = Font(name='Arial', size=9)
            c.fill = PatternFill('solid', fgColor=bg)
            c.alignment = Alignment(vertical='center', wrap_text=True)

    for ci, col in enumerate(out_cols, 1):
        max_len = max((len(str(v)) for v in df.get(col, pd.Series()).fillna('') if v), default=len(col))
        ws.column_dimensions[get_column_letter(ci)].width = min(max(max_len + 2, 10), 50)
    wb.save(path)

def write_parse_report(records_with_conf, fname):
    """產生 parse_report Excel"""
    rpt_path = os.path.join(RPT_DIR, f'nb_{os.path.splitext(fname)[0]}_report.xlsx')
    wb = Workbook()

    # Sheet 1: all_data
    ws1 = wb.active
    ws1.title = 'all_data'
    report_cols = ['row_id', 'page_no', 'confidence', 'confidence_reason',
                   'Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
    for ci, col in enumerate(report_cols, 1):
        c = ws1.cell(row=1, column=ci, value=col)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', fgColor=HDR_BG)

    for ri, r in enumerate(records_with_conf, start=1):
        vals = [ri, r.get('_page', ''), r.get('confidence', ''), r.get('confidence_reason', ''),
                r.get('設備名稱', ''), r.get('設備廠家', ''), r.get('設備型號', ''),
                r.get('潤滑部位', ''), r.get('推薦潤滑油', '')]
        for ci, v in enumerate(vals, 1):
            ws1.cell(row=ri + 1, column=ci, value=str(v) if v else '')

    # Sheet 2: review_required
    ws2 = wb.create_sheet('review_required')
    review_cols = report_cols + ['review_status']
    for ci, col in enumerate(review_cols, 1):
        c = ws2.cell(row=1, column=ci, value=col)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', fgColor='C00000')

    ri = 2
    for idx, r in enumerate(records_with_conf, start=1):
        if r.get('confidence') in ('medium', 'low'):
            vals = [idx, r.get('_page', ''), r.get('confidence', ''), r.get('confidence_reason', ''),
                    r.get('設備名稱', ''), r.get('設備廠家', ''), r.get('設備型號', ''),
                    r.get('潤滑部位', ''), r.get('推薦潤滑油', ''), '']
            for ci, v in enumerate(vals, 1):
                ws2.cell(row=ri, column=ci, value=str(v) if v else '')
            ri += 1

    wb.save(rpt_path)
    return rpt_path

def append_log(summary):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(summary + '\n')

# ── 主流程 ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("▶  process_nb.py 開始執行")
    print("=" * 50)

    reg  = load_registry(REG_FILE)
    freg = load_failed_registry()
    now  = datetime.now()

    all_pdfs = sorted([f for f in os.listdir(NB_DIR) if f.lower().endswith('.pdf')])
    if not all_pdfs:
        print(f"⚠️  找不到 PDF：{NB_DIR}")
        return

    to_process = []
    skipped    = []
    for fname in all_pdfs:
        fpath = os.path.join(NB_DIR, fname)
        needed, reason = needs_processing(fname, fpath, reg)
        if needed:
            to_process.append((fname, fpath, reason))
        else:
            skipped.append(fname)

    retryable = [
        k for k, v in freg.get('failed_files', {}).items()
        if v.get('source') == 'nb' and v.get('status') == 'retryable' and v.get('retry_count', 0) < 3
    ]
    for key in retryable:
        fname = os.path.basename(key)
        fpath = os.path.join(NB_DIR, fname)
        if os.path.exists(fpath) and fname not in [x[0] for x in to_process]:
            to_process.append((fname, fpath, 'retry'))

    print(f"PDF 總數：{len(all_pdfs)}  ｜  待處理：{len(to_process)}  ｜  跳過：{len(skipped)}")

    if not to_process:
        print("✓ 無需更新，所有檔案未變更。")
        return

    # 載入現有 master
    if os.path.exists(MASTER):
        try:
            df_master = pd.read_excel(MASTER, sheet_name='nb_master')
        except Exception:
            df_master = pd.DataFrame()
    else:
        df_master = pd.DataFrame()

    master_cols = COLS + ['Source', 'source_file']

    stats = {'added': [], 'updated': [], 'failed': []}
    report_summary = []

    for fname, fpath, reason in to_process:
        try:
            print(f"\n  [{fname}]  [{reason}]")
            ship_id = get_ship_id(fname)
            records, fmt = parse_pdf(fpath, ship_id)
            print(f"    解析：{len(records)} 筆原始記錄")

            if not records:
                raise ValueError("EMPTY_DATA：解析後無任何有效資料列")

            # 信心分數
            records_with_conf = assign_confidence(records, fmt, fpath)

            # 產生 parse_report
            os.makedirs(RPT_DIR, exist_ok=True)
            rpt_path = write_parse_report(records_with_conf, fname)

            # 統計信心分數
            conf_count = {'high': 0, 'medium': 0, 'low': 0}
            for r in records_with_conf:
                conf_count[r.get('confidence', 'low')] = conf_count.get(r.get('confidence', 'low'), 0) + 1
            report_summary.append((fname, len(records_with_conf), conf_count))
            print(f"    信心分數  HIGH:{conf_count['high']}  MEDIUM:{conf_count['medium']}  LOW:{conf_count['low']}")

            # 轉換為 DataFrame（排除 low 信心分數）
            records_for_master = [r for r in records_with_conf if r.get('confidence') != 'low']
            low_count = len(records_with_conf) - len(records_for_master)
            if low_count > 0:
                print(f"    排除 low 信心分數：{low_count} 筆不納入 master")

            df_new = pd.DataFrame([{
                'Equipment': r.get('設備名稱', ''),
                'Maker': r.get('設備廠家', ''),
                'Model / Type': r.get('設備型號', ''),
                'Part to be lubricated': r.get('潤滑部位', ''),
                'Lubricant': r.get('推薦潤滑油', ''),
                'Source': 'NB',
                'source_file': fname,
            } for r in records_for_master])

            if df_new.empty:
                print(f"    ⚠️  過濾後無有效記錄，跳過")
                stat = os.stat(fpath)
                reg['processed_files'][fname] = {
                    'processed_at': now.isoformat(timespec='seconds'),
                    'file_size_bytes': stat.st_size,
                    'file_mtime': datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
                    'sha256': sha256(fpath)
                }
                if reason == 'new':
                    stats['added'].append(f'{fname} ✓（無 high/medium 記錄）')
                else:
                    stats['updated'].append(f'{fname} ✓（無 high/medium 記錄）')
                continue

            # 全大寫
            for col in COLS:
                df_new[col] = df_new[col].fillna('').astype(str).str.strip().str.upper()

            # 從 master 移除舊資料
            if not df_master.empty and 'source_file' in df_master.columns:
                df_master = df_master[df_master['source_file'] != fname]

            df_master = pd.concat([df_master, df_new], ignore_index=True)

            # 更新 registry
            stat = os.stat(fpath)
            reg['processed_files'][fname] = {
                'processed_at': now.isoformat(timespec='seconds'),
                'file_size_bytes': stat.st_size,
                'file_mtime': datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
                'sha256': sha256(fpath)
            }
            fail_key = f'nb/{fname}'
            if fail_key in freg.get('failed_files', {}):
                del freg['failed_files'][fail_key]

            if reason == 'retry':
                stats['added'].append(f'{fname} ✓（retry 成功）')
            elif reason == 'new':
                stats['added'].append(f'{fname} ✓')
            else:
                stats['updated'].append(f'{fname} ✓（{reason}）')

        except Exception as e:
            error_msg = str(e)
            print(f"  ❌ 失敗：{error_msg}")
            fail_key = f'nb/{fname}'
            error_type = 'EMPTY_DATA' if 'EMPTY_DATA' in error_msg else 'PDF_PARSE_ERROR'
            if fail_key not in freg.setdefault('failed_files', {}):
                freg['failed_files'][fail_key] = {
                    'source': 'nb', 'first_failed_at': now.isoformat(timespec='seconds'),
                    'last_failed_at': now.isoformat(timespec='seconds'),
                    'retry_count': 1, 'error_type': error_type,
                    'error_message': error_msg[:200], 'status': 'retryable'
                }
            else:
                freg['failed_files'][fail_key]['retry_count'] += 1
                freg['failed_files'][fail_key]['last_failed_at'] = now.isoformat(timespec='seconds')
                if freg['failed_files'][fail_key]['retry_count'] >= 3:
                    freg['failed_files'][fail_key]['status'] = 'manual_required'
            stats['failed'].append(fname)
            continue

    if df_master.empty:
        print("⚠️  無有效資料，跳過寫入。")
        return

    # 確保欄位完整
    for col in master_cols:
        if col not in df_master.columns:
            df_master[col] = ''

    # 過濾無效型號
    before = len(df_master)
    df_master = df_master[~df_master['Model / Type'].isin(INVALID_MODEL)]
    print(f"\n  過濾無效型號：{before - len(df_master)} 列移除")

    # 排除 TALUSIA LS 25
    before = len(df_master)
    df_master = df_master[~df_master['Lubricant'].str.contains('TALUSIA LS 25', na=False)]
    print(f"  排除 TALUSIA LS 25：{before - len(df_master)} 列移除")

    # 去重
    before = len(df_master)
    df_master = df_master.drop_duplicates(subset=DEDUP_KEYS)
    print(f"  去重：{before - len(df_master)} 列移除，剩餘 {len(df_master)} 列")

    # 寫入 master
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        write_master_excel(df_master[master_cols], MASTER)
        print(f"\n✓ nb_master.xlsx 寫入完成：{len(df_master)} 列")
    except Exception as e:
        print(f"\n❌ WRITE_ERROR：{e}")
        return

    save_registry(reg, REG_FILE)
    save_failed_registry(freg)

    manual_req = [k for k, v in freg.get('failed_files', {}).items()
                  if v.get('source') == 'nb' and v.get('status') == 'manual_required']

    # Log
    log_lines = [
        '=' * 40,
        f'時間：{now.strftime("%Y-%m-%d %H:%M:%S")}',
        '動作：更新NB',
        '-' * 40,
    ]
    if stats['added']:   log_lines.append(f'新增處理：{", ".join(stats["added"])}')
    if stats['updated']: log_lines.append(f'重新處理：{", ".join(stats["updated"])}')
    if skipped:          log_lines.append(f'跳過：{len(skipped)} 個檔案（未變更）')
    if stats['failed']:
        log_lines.append('失敗（本次）：')
        for f in stats['failed']: log_lines.append(f'  - {f}')
    if manual_req:
        log_lines.append('\n⚠️  需人工介入（manual_required）：')
        for k in manual_req: log_lines.append(f'  - {k}')
    log_lines.append(f'\n輸出：output/nb_master.xlsx（共 {len(df_master)} 列）')
    log_lines.append('=' * 40)
    append_log('\n'.join(log_lines))

    # 摘要
    print('\n' + '=' * 50)
    print('📊 執行摘要')
    print('-' * 50)
    if stats['added']:   print(f'新增：{len(stats["added"])} 個檔案')
    if stats['updated']: print(f'更新：{len(stats["updated"])} 個檔案')
    if skipped:          print(f'跳過：{len(skipped)} 個檔案')
    if stats['failed']:  print(f'失敗：{len(stats["failed"])} 個檔案')
    print(f'輸出：nb_master.xlsx（{len(df_master):,} 列）')

    print('\n📋 解析報告已產生：')
    for fname, total, cc in report_summary:
        print(f'  - {fname} → 共 {total} 列')
        print(f'    HIGH: {cc["high"]} 列（無需核對）')
        if cc['medium'] > 0:
            rpt_name = f'nb_{os.path.splitext(fname)[0]}_report.xlsx'
            print(f'    MEDIUM: {cc["medium"]} 列（建議抄查）→ parse_report/{rpt_name}')
        if cc['low'] > 0:
            rpt_name = f'nb_{os.path.splitext(fname)[0]}_report.xlsx'
            print(f'    LOW: {cc["low"]} 列（需人工核對）→ parse_report/{rpt_name}')

    if manual_req:
        print('\n⚠️  以下檔案需要人工介入（已失敗 3 次）：')
        for k in manual_req: print(f'   - {k}')
    print('=' * 50)

if __name__ == '__main__':
    main()


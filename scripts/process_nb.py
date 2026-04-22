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
    mtime = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds')
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
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ''
        tables = page.extract_tables()
        if tables:
            hdr_str = ' '.join(str(c) for row in tables[:1] for c in row if c)
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

def parse_pdf(pdf_path, ship_id):
    fmt = detect_format(pdf_path)
    print(f"    Format {fmt} 偵測")
    if fmt == 'B': return parse_format_B(pdf_path, ship_id), fmt
    if fmt == 'C': return parse_format_C(pdf_path, ship_id), fmt
    return parse_format_A(pdf_path, ship_id), fmt

def get_ship_id(fname):
    f = fname.upper()
    if '2359' in f: return 'SN2359'
    if '30408027' in f: return 'H2652 & H2653'
    if 'H1872' in f or 'M-53' in f: return 'H1872A'
    if '2265' in f or '2268' in f: return 'SN2265 & SN2268'
    return os.path.splitext(fname)[0][:40]

# ── 信心分數判斷 ─────────────────────────────────────────────────
def assign_confidence(records, fmt, pdf_path):
    """為每筆記錄指定信心分數"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
    except Exception:
        page_count = 1

    results = []
    for r in records:
        page_no = r.get('_page', 1)
        # 判斷條件
        has_maker = bool(r.get('設備廠家', '').strip())
        has_model = bool(r.get('設備型號', '').strip())
        has_app   = bool(r.get('潤滑部位', '').strip())
        has_oil   = bool(r.get('推薦潤滑油', '').strip())

        if fmt in ('A',) and has_maker and has_model and has_app and has_oil:
            confidence = 'high'
            reason = '表格邊界清晰，欄位完整'
        elif has_oil and (has_maker or has_model):
            confidence = 'medium'
            reasons = []
            if not has_maker: reasons.append('缺 Maker')
            if not has_model: reasons.append('缺 Model')
            if fmt == 'B': reasons.append('中英文混合表格')
            reason = '，'.join(reasons) if reasons else '跨頁或合併儲存格'
        else:
            confidence = 'low'
            reasons = []
            if not has_oil: reasons.append('缺潤滑油欄位')
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

    out_cols = COLS + ['Source']
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

    master_cols = COLS + ['Source']

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
                # 仍更新 registry（PDF 已處理）
                stat = os.stat(fpath)
                reg['processed_files'][fname] = {
                    'processed_at': now.isoformat(timespec='seconds'),
                    'file_size_bytes': stat.st_size,
                    'file_mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
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
                'file_mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
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
            print(f'    MEDIUM: {cc["medium"]} 列（建議抽查）→ parse_report/{rpt_name}')
        if cc['low'] > 0:
            rpt_name = f'nb_{os.path.splitext(fname)[0]}_report.xlsx'
            print(f'    LOW: {cc["low"]} 列（需人工核對）→ parse_report/{rpt_name}')

    if manual_req:
        print('\n⚠️  以下檔案需要人工介入（已失敗 3 次）：')
        for k in manual_req: print(f'   - {k}')
    print('=' * 50)

if __name__ == '__main__':
    main()

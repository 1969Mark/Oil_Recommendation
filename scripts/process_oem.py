"""
process_oem.py
==============
增量處理 OEM_data/ 內的 PDF/Excel 檔案，輸出 output/oem_master.xlsx。
source_data = 程式解析結果，manual_data = 永遠不修改（由使用者手動維護）。
初次執行時，若 oem_master.xlsx 不存在，自動將 OEM_oil_recommendation.xlsx 載入為 manual_data。
"""

import os, sys, json, hashlib, re
from datetime import datetime
import pandas as pd
import pdfplumber
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ── 路徑設定 ────────────────────────────────────────────────────
PROJECT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OEM_DIR     = os.path.join(PROJECT, 'OEM_data')
OUT_DIR     = os.path.join(PROJECT, 'output')
RPT_DIR     = os.path.join(OUT_DIR, 'parse_report')
PENDING_DIR = os.path.join(OUT_DIR, 'pending_ai_review')   # AI 待審閱暫存區
REG_DIR     = os.path.join(PROJECT, 'registry')
LOG_DIR     = os.path.join(PROJECT, 'logs')
MASTER      = os.path.join(OUT_DIR, 'oem_master.xlsx')
REG_FILE    = os.path.join(REG_DIR, 'oem_registry.json')
FAIL_REG    = os.path.join(REG_DIR, 'failed_registry.json')
LOG_FILE    = os.path.join(LOG_DIR, 'update_log.txt')
MANUAL_SRC  = os.path.join(PROJECT, 'OEM_oil_recommendation.xlsx')  # 現有人工資料

COLS = ['Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
DEDUP_KEYS = ['Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
INVALID_MODEL = {'', '.', '..', 'TO BE DETERMINED', 'TBD', 'N/A', 'NA', 'NONE', '-', '--'}

HDR_BG   = '1F3864'
ODD_BG   = 'FFFFFF'
EVEN_BG  = 'EBF3FB'
OEM_COLOR = '1F3864'

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

# ── AI 待審閱：提取全文並存檔 ───────────────────────────────────
def save_pending_review(fpath, fname):
    """
    當 PDF 表格解析無法取得有效資料，但仍含可讀文字時，
    將全文提取並存為 pending_ai_review/{name}_pending.json，
    供 Claude Cowork 事後讀取、彙整並向使用者確認。
    回傳 (pending_path, 有文字的頁數)
    """
    os.makedirs(PENDING_DIR, exist_ok=True)
    pages_data = []
    with pdfplumber.open(fpath) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or '').strip()
            pages_data.append({'page': i, 'text': text})

    text_pages = sum(1 for p in pages_data if p['text'])
    full_text  = '\n\n--- Page {} ---\n'.join(
        p['text'] for p in pages_data if p['text']
    )

    pending = {
        'filename'     : fname,
        'source'       : 'oem',
        'extracted_at' : datetime.now().isoformat(timespec='seconds'),
        'total_pages'  : len(pages_data),
        'text_pages'   : text_pages,
        'pages'        : pages_data,
        'full_text'    : full_text,
        'status'       : 'pending'          # pending → confirmed → written
    }

    stem       = os.path.splitext(fname)[0]
    out_path   = os.path.join(PENDING_DIR, f'{stem}_pending.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(pending, f, indent=2, ensure_ascii=False)

    return out_path, text_pages

# ── PDF 解析（OEM 用簡化版）──────────────────────────────────────
MAX_PAGES_PER_PDF = 60  # 超過此頁數的 PDF 只處理前 MAX_PAGES 頁，其餘標記 medium

def parse_oem_pdf(pdf_path, fname):
    """
    嘗試用 pdfplumber 擷取 OEM PDF 表格。
    回傳 (records, confidence_default)
    """
    records = []
    confidence_default = 'medium'

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            pages_to_process = pdf.pages[:MAX_PAGES_PER_PDF]
            if total_pages > MAX_PAGES_PER_PDF:
                print(f"    ⚠️  共 {total_pages} 頁，僅處理前 {MAX_PAGES_PER_PDF} 頁")
                confidence_default = 'medium'
            for pg_no, page in enumerate(pages_to_process, start=1):
                tables = page.extract_tables()
                if not tables:
                    # fallback: 純文字解析
                    text = page.extract_text() or ''
                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    for line in lines:
                        parts = re.split(r'\s{2,}|\t', line)
                        if len(parts) >= 3:
                            records.append({
                                'Equipment': '', 'Maker': parts[0] if len(parts) > 0 else '',
                                'Model / Type': parts[1] if len(parts) > 1 else '',
                                'Part to be lubricated': parts[2] if len(parts) > 2 else '',
                                'Lubricant': parts[3] if len(parts) > 3 else '',
                                '_page': pg_no, '_confidence': 'low',
                                '_reason': '無結構化表格，純文字解析'
                            })
                    confidence_default = 'low'
                    continue

                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    # 嘗試識別標題列
                    hdr_row = None
                    hdr_idx = 0
                    for i, row in enumerate(table[:3]):
                        row_str = ' '.join(str(c or '') for c in row).upper()
                        if any(k in row_str for k in ['ENGINE', 'OIL', 'LUBRIC', 'TYPE', 'MODEL', 'BN', 'VISCOSITY']):
                            hdr_row = [str(c or '').strip() for c in row]
                            hdr_idx = i
                            break

                    data_rows = table[hdr_idx + 1:] if hdr_row else table

                    for row in data_rows:
                        if not any(row):
                            continue
                        cells = [str(c or '').strip() for c in row]
                        # 嘗試對應欄位
                        non_empty = [c for c in cells if c]
                        if len(non_empty) < 2:
                            continue

                        # 簡單啟發式：嘗試從欄位中提取關鍵資訊
                        record = {
                            'Equipment': 'MAIN ENGINE',
                            'Maker': '',
                            'Model / Type': '',
                            'Part to be lubricated': '',
                            'Lubricant': '',
                            '_page': pg_no,
                            '_confidence': 'medium',
                            '_reason': '表格解析'
                        }

                        # 若標題列存在，嘗試對應
                        if hdr_row:
                            for ci, hdr in enumerate(hdr_row):
                                if ci >= len(cells): break
                                hdr_u = hdr.upper()
                                val = cells[ci]
                                if not val: continue
                                if any(k in hdr_u for k in ['ENGINE TYPE', 'MODEL', 'TYPE']):
                                    record['Model / Type'] = val
                                elif any(k in hdr_u for k in ['ENGINE', 'MAKER', 'BRAND']):
                                    record['Maker'] = val
                                elif any(k in hdr_u for k in ['APPLICATION', 'PART', 'SYSTEM', 'USE']):
                                    record['Part to be lubricated'] = val
                                elif any(k in hdr_u for k in ['OIL', 'LUBRIC', 'PRODUCT', 'GRADE']):
                                    record['Lubricant'] = val
                                elif any(k in hdr_u for k in ['BN', 'VISCOSITY']):
                                    if record['Part to be lubricated']:
                                        record['Part to be lubricated'] += f' {val}'
                                    else:
                                        record['Part to be lubricated'] = val
                        else:
                            # 無標題：依位置猜測
                            if len(cells) >= 2: record['Model / Type'] = cells[0]
                            if len(cells) >= 3: record['Part to be lubricated'] = cells[1]
                            if len(cells) >= 4: record['Lubricant'] = cells[-1]
                            record['_confidence'] = 'low'
                            record['_reason'] = '無標題列，依位置對應'

                        if record['Lubricant'] or record['Model / Type']:
                            records.append(record)

    except Exception as e:
        raise ValueError(f"PDF_PARSE_ERROR：{e}")

    return records, confidence_default

# ── Excel 解析（OEM Excel）──────────────────────────────────────
def parse_oem_excel(excel_path, fname):
    """解析 OEM Excel 檔案，讀取所有 sheets"""
    records = []
    xl = pd.ExcelFile(excel_path)
    for sheet_name in xl.sheet_names:
        try:
            df = xl.parse(sheet_name)
            if df.empty:
                continue
            # 找標題列（可能不在第一列）
            hdr_idx = 0
            for i, row in df.iterrows():
                row_str = ' '.join(str(v) for v in row if pd.notna(v)).upper()
                if any(k in row_str for k in ['MAKER', 'MODEL', 'TYPE', 'OIL', 'LUBRIC']):
                    hdr_idx = i
                    break
            if hdr_idx > 0:
                df.columns = df.iloc[hdr_idx]
                df = df.iloc[hdr_idx + 1:].reset_index(drop=True)

            df.columns = [str(c).strip() for c in df.columns]
            col_map = {}
            for col in df.columns:
                cu = col.upper()
                if 'EQUIPMENT' in cu: col_map[col] = 'Equipment'
                elif 'MAKER' in cu or 'MANUFACTURER' in cu: col_map[col] = 'Maker'
                elif 'MODEL' in cu or 'TYPE' in cu: col_map[col] = 'Model / Type'
                elif 'PART' in cu or 'APPLICATION' in cu or 'SYSTEM' in cu: col_map[col] = 'Part to be lubricated'
                elif 'OIL' in cu or 'LUBRIC' in cu or 'PRODUCT' in cu: col_map[col] = 'Lubricant'
            df = df.rename(columns=col_map)
            for col in COLS:
                if col not in df.columns:
                    df[col] = ''
            df = df[COLS].copy()
            for col in COLS:
                df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
            df = df[df['Lubricant'] != '']
            df['source_file']  = fname
            df['source_sheet'] = sheet_name
            df['Source']       = 'OEM'
            records.append(df)
        except Exception:
            continue

    if not records:
        return pd.DataFrame()
    return pd.concat(records, ignore_index=True)

# ── 信心分數（PDF）──────────────────────────────────────────────
def enrich_confidence(records):
    result = []
    for r in records:
        r['confidence'] = r.pop('_confidence', 'medium')
        r['confidence_reason'] = r.pop('_reason', '')
        r.pop('_page_orig', None)
        result.append(r)
    return result

# ── Excel 輸出 ───────────────────────────────────────────────────
def write_oem_master(df_source, df_manual, path):
    """
    寫入 oem_master.xlsx：
    - source_data sheet（df_source）
    - manual_data sheet（df_manual，只在建立時寫入，之後不碰）
    """
    wb = Workbook()

    source_cols = COLS + ['Source', 'source_file', 'source_sheet']
    manual_cols = COLS + ['Source', 'source_file', 'source_sheet']

    def write_sheet(ws, df, cols, color):
        ws.freeze_panes = 'A2'
        # 補齊欄位
        for col in cols:
            if col not in df.columns:
                df[col] = ''
        for ci, col in enumerate(cols, 1):
            c = ws.cell(row=1, column=ci, value=col)
            c.font = Font(name='Arial', bold=True, color='FFFFFF', size=10)
            c.fill = PatternFill('solid', fgColor=HDR_BG)
            c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        for ri, (_, row) in enumerate(df.iterrows(), start=2):
            bg = ODD_BG if ri % 2 == 1 else EVEN_BG
            for ci, col in enumerate(cols, 1):
                val = str(row.get(col, '')) if pd.notna(row.get(col, '')) else ''
                c = ws.cell(row=ri, column=ci, value=val)
                if col == 'Source':
                    c.font = Font(name='Arial', size=9, color=color, bold=True)
                elif col == 'confidence':
                    fc = {'high': '375623', 'medium': '7F6000', 'low': 'C00000'}.get(val, '000000')
                    c.font = Font(name='Arial', size=9, color=fc, bold=True)
                else:
                    c.font = Font(name='Arial', size=9)
                c.fill = PatternFill('solid', fgColor=bg)
                c.alignment = Alignment(vertical='center', wrap_text=True)
        for ci, col in enumerate(cols, 1):
            if col in df.columns:
                max_len = max((len(str(v)) for v in df[col].fillna('') if v), default=len(col))
            else:
                max_len = len(col)
            ws.column_dimensions[get_column_letter(ci)].width = min(max(max_len + 2, 10), 50)

    ws_source = wb.active
    ws_source.title = 'source_data'
    write_sheet(ws_source, df_source.copy(), source_cols, OEM_COLOR)

    ws_manual = wb.create_sheet('manual_data')
    write_sheet(ws_manual, df_manual.copy(), manual_cols, OEM_COLOR)

    wb.save(path)

def write_parse_report(records, fname):
    rpt_path = os.path.join(RPT_DIR, f'oem_{os.path.splitext(fname)[0]}_report.xlsx')
    wb = Workbook()
    ws1 = wb.active
    ws1.title = 'all_data'
    report_cols = ['row_id', 'page_no', 'confidence', 'confidence_reason',
                   'Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant']
    for ci, col in enumerate(report_cols, 1):
        c = ws1.cell(row=1, column=ci, value=col)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', fgColor=HDR_BG)
    for ri, r in enumerate(records, start=1):
        vals = [ri, r.get('_page', ''), r.get('confidence', ''), r.get('confidence_reason', ''),
                r.get('Equipment', ''), r.get('Maker', ''), r.get('Model / Type', ''),
                r.get('Part to be lubricated', ''), r.get('Lubricant', '')]
        for ci, v in enumerate(vals, 1):
            ws1.cell(row=ri + 1, column=ci, value=str(v) if v else '')

    ws2 = wb.create_sheet('review_required')
    review_cols = report_cols + ['review_status']
    for ci, col in enumerate(review_cols, 1):
        c = ws2.cell(row=1, column=ci, value=col)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', fgColor='C00000')
    ri2 = 2
    for idx, r in enumerate(records, start=1):
        if r.get('confidence') in ('medium', 'low'):
            vals = [idx, r.get('_page', ''), r.get('confidence', ''), r.get('confidence_reason', ''),
                    r.get('Equipment', ''), r.get('Maker', ''), r.get('Model / Type', ''),
                    r.get('Part to be lubricated', ''), r.get('Lubricant', ''), '']
            for ci, v in enumerate(vals, 1):
                ws2.cell(row=ri2, column=ci, value=str(v) if v else '')
            ri2 += 1
    wb.save(rpt_path)
    return rpt_path

def append_log(summary):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(summary + '\n')

# ── 主流程 ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("▶  process_oem.py 開始執行")
    print("=" * 50)

    reg  = load_registry(REG_FILE)
    freg = load_failed_registry()
    now  = datetime.now()

    # 掃描 OEM_data/ 所有檔案
    all_files = sorted([
        f for f in os.listdir(OEM_DIR)
        if f.lower().endswith(('.pdf', '.xlsx', '.xls'))
    ]) if os.path.exists(OEM_DIR) else []

    if not all_files:
        print(f"⚠️  OEM_data/ 目錄為空或不存在：{OEM_DIR}")

    to_process = []
    skipped    = []
    for fname in all_files:
        fpath = os.path.join(OEM_DIR, fname)
        needed, reason = needs_processing(fname, fpath, reg)
        if needed:
            to_process.append((fname, fpath, reason))
        else:
            skipped.append(fname)

    retryable = [
        k for k, v in freg.get('failed_files', {}).items()
        if v.get('source') == 'oem' and v.get('status') == 'retryable' and v.get('retry_count', 0) < 3
    ]
    for key in retryable:
        fname = os.path.basename(key)
        fpath = os.path.join(OEM_DIR, fname)
        if os.path.exists(fpath) and fname not in [x[0] for x in to_process]:
            to_process.append((fname, fpath, 'retry'))

    print(f"OEM 檔案總數：{len(all_files)}  ｜  待處理：{len(to_process)}  ｜  跳過：{len(skipped)}")

    # ── 處理 manual_data ─────────────────────────────────────────
    # 若 oem_master.xlsx 已存在，保留其 manual_data（絕不覆蓋）
    # 若不存在，從 OEM_oil_recommendation.xlsx 初始化
    df_manual = pd.DataFrame()
    master_exists = os.path.exists(MASTER)

    if master_exists:
        try:
            df_manual = pd.read_excel(MASTER, sheet_name='manual_data')
            print(f"✓ 保留既有 manual_data：{len(df_manual)} 列")
        except Exception as e:
            print(f"⚠️  無法讀取 manual_data sheet：{e}")
            df_manual = pd.DataFrame()
    else:
        # 初次建立：從既有 OEM_oil_recommendation.xlsx 載入
        if os.path.exists(MANUAL_SRC):
            try:
                df_manual = pd.read_excel(MANUAL_SRC)
                for col in COLS:
                    if col not in df_manual.columns:
                        df_manual[col] = ''
                df_manual = df_manual[COLS].copy()
                for col in COLS:
                    df_manual[col] = df_manual[col].fillna('').astype(str).str.strip().str.upper()
                # 排除 TALUSIA LS 25
                df_manual = df_manual[~df_manual['Lubricant'].str.contains('TALUSIA LS 25', na=False)]
                df_manual['Source']       = 'OEM'
                df_manual['source_file']  = 'manual'
                df_manual['source_sheet'] = 'OEM_oil_recommendation'
                print(f"✓ 初始化 manual_data（來自 OEM_oil_recommendation.xlsx）：{len(df_manual)} 列")
            except Exception as e:
                print(f"⚠️  無法載入 OEM_oil_recommendation.xlsx：{e}")
        else:
            print("ℹ️  OEM_oil_recommendation.xlsx 不存在，manual_data 為空。")

    # ── 載入現有 source_data ─────────────────────────────────────
    if master_exists:
        try:
            df_source = pd.read_excel(MASTER, sheet_name='source_data')
            print(f"✓ 載入現有 source_data：{len(df_source)} 列")
        except Exception:
            df_source = pd.DataFrame()
    else:
        df_source = pd.DataFrame()

    if not to_process:
        if not master_exists:
            # 第一次執行但無需處理的檔案 → 建立空 master
            print("ℹ️  建立新的 oem_master.xlsx（source_data 為空）")
            os.makedirs(OUT_DIR, exist_ok=True)
            write_oem_master(pd.DataFrame(columns=COLS + ['Source', 'source_file', 'source_sheet', 'confidence', 'confidence_reason']),
                             df_manual, MASTER)
        else:
            print("✓ 無需更新，所有檔案未變更。")
        return

    stats = {'added': [], 'updated': [], 'failed': [], 'pending_review': []}
    report_summary = []

    for fname, fpath, reason in to_process:
        try:
            print(f"\n  [{fname}]  [{reason}]")
            ext = os.path.splitext(fname)[1].lower()

            if ext in ('.xlsx', '.xls'):
                df_new = parse_oem_excel(fpath, fname)
                if df_new.empty:
                    raise ValueError("EMPTY_DATA：Excel 解析後無有效資料")
                df_new['confidence'] = ''
                df_new['confidence_reason'] = ''
                print(f"    Excel 解析：{len(df_new)} 筆")

            elif ext == '.pdf':
                records, _ = parse_oem_pdf(fpath, fname)
                if not records:
                    # ── 嘗試提取全文，存入 pending_ai_review ──────────────
                    try:
                        pending_path, text_pages = save_pending_review(fpath, fname)
                    except Exception as pe:
                        raise ValueError(f"EMPTY_DATA：PDF 解析後無有效資料，且文字提取失敗：{pe}")

                    if text_pages == 0:
                        raise ValueError("EMPTY_DATA：PDF 解析後無有效資料（掃描版或無文字層）")

                    # 標記為 ai_review_pending，不走失敗流程
                    fail_key = f'oem/{fname}'
                    freg.setdefault('failed_files', {})[fail_key] = {
                        'source'         : 'oem',
                        'first_failed_at': now.isoformat(timespec='seconds'),
                        'last_failed_at' : now.isoformat(timespec='seconds'),
                        'retry_count'    : 0,
                        'error_type'     : 'AI_REVIEW_PENDING',
                        'error_message'  : (
                            f'無結構化表格，已提取 {text_pages} 頁文字，'
                            f'待 Claude AI 審閱後確認寫入 manual_data'
                        ),
                        'status'         : 'ai_review_pending',
                        'pending_file'   : pending_path
                    }
                    stats['pending_review'].append(fname)
                    print(f"    ℹ️  無結構化表格，已提取 {text_pages} 頁文字 → pending_ai_review/")
                    save_failed_registry(freg)
                    continue  # 不算失敗，跳過寫入 master

                conf_count = {'high': 0, 'medium': 0, 'low': 0}
                for r in records:
                    conf_count[r.get('_confidence', 'low')] = conf_count.get(r.get('_confidence', 'low'), 0) + 1
                report_summary.append((fname, len(records), conf_count))
                print(f"    PDF 解析：{len(records)} 筆  HIGH:{conf_count['high']}  MEDIUM:{conf_count['medium']}  LOW:{conf_count['low']}")

                # Parse report
                os.makedirs(RPT_DIR, exist_ok=True)
                write_parse_report(records, fname)

                # 排除 low 信心分數
                records_for_master = [r for r in records if r.get('_confidence') != 'low']
                low_count = len(records) - len(records_for_master)
                if low_count > 0:
                    print(f"    排除 low 信心分數：{low_count} 筆不納入 master")

                df_new = pd.DataFrame([{
                    'Equipment': r.get('Equipment', ''),
                    'Maker': r.get('Maker', ''),
                    'Model / Type': r.get('Model / Type', ''),
                    'Part to be lubricated': r.get('Part to be lubricated', ''),
                    'Lubricant': r.get('Lubricant', ''),
                    'Source': 'OEM',
                    'source_file': fname,
                    'source_sheet': '',
                } for r in records_for_master])
                for col in COLS:
                    df_new[col] = df_new[col].fillna('').astype(str).str.strip().str.upper()
            else:
                continue

            # 確保必要欄位
            for col in COLS + ['Source']:
                if col not in df_new.columns:
                    df_new[col] = ''

            # 從 source_data 移除舊資料
            if not df_source.empty and 'source_file' in df_source.columns:
                df_source = df_source[df_source['source_file'] != fname]

            df_source = pd.concat([df_source, df_new], ignore_index=True)

            # 更新 registry
            stat = os.stat(fpath)
            reg['processed_files'][fname] = {
                'processed_at': now.isoformat(timespec='seconds'),
                'file_size_bytes': stat.st_size,
                'file_mtime': datetime.utcfromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
                'sha256': sha256(fpath)
            }
            fail_key = f'oem/{fname}'
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
            fail_key = f'oem/{fname}'
            error_type = 'EMPTY_DATA' if 'EMPTY_DATA' in error_msg else \
                         'PDF_PARSE_ERROR' if fname.lower().endswith('.pdf') else 'FILE_READ_ERROR'
            if fail_key not in freg.setdefault('failed_files', {}):
                freg['failed_files'][fail_key] = {
                    'source': 'oem', 'first_failed_at': now.isoformat(timespec='seconds'),
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

    # 標準化 + 過濾
    if not df_source.empty:
        before = len(df_source)
        df_source = df_source[~df_source['Model / Type'].isin(INVALID_MODEL)]
        print(f"\n  過濾無效型號：{before - len(df_source)} 列移除")

        before = len(df_source)
        df_source = df_source[~df_source['Lubricant'].str.contains('TALUSIA LS 25', na=False)]
        print(f"  排除 TALUSIA LS 25：{before - len(df_source)} 列移除")

        before = len(df_source)
        df_source = df_source.drop_duplicates(subset=DEDUP_KEYS)
        print(f"  去重：{before - len(df_source)} 列移除，剩餘 {len(df_source)} 列")

    # 確保所有欄位存在
    source_cols = COLS + ['Source', 'source_file', 'source_sheet']
    for col in source_cols:
        if col not in df_source.columns:
            df_source[col] = ''

    # 寫入 master（WRITE_ERROR → 中止）
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        write_oem_master(df_source, df_manual, MASTER)
        print(f"\n✓ oem_master.xlsx 寫入完成：source_data {len(df_source)} 列，manual_data {len(df_manual)} 列")
    except Exception as e:
        print(f"\n❌ WRITE_ERROR：{e}")
        return

    save_registry(reg, REG_FILE)
    save_failed_registry(freg)

    manual_req = [k for k, v in freg.get('failed_files', {}).items()
                  if v.get('source') == 'oem' and v.get('status') == 'manual_required']

    # Log
    log_lines = [
        '=' * 40,
        f'時間：{now.strftime("%Y-%m-%d %H:%M:%S")}',
        '動作：更新OEM',
        '-' * 40,
    ]
    if stats['added']:          log_lines.append(f'新增處理：{", ".join(stats["added"])}')
    if stats['updated']:        log_lines.append(f'重新處理：{", ".join(stats["updated"])}')
    if skipped:                 log_lines.append(f'跳過：{len(skipped)} 個檔案（未變更）')
    if stats['pending_review']:
        log_lines.append('待 AI 審閱（文字已提取，無結構化表格）：')
        for f in stats['pending_review']: log_lines.append(f'  - {f}  → pending_ai_review/')
    if stats['failed']:
        log_lines.append('失敗（本次）：')
        for f in stats['failed']: log_lines.append(f'  - {f}')
    if manual_req:
        log_lines.append('\n⚠️  需人工介入（manual_required）：')
        for k in manual_req: log_lines.append(f'  - {k}')
    log_lines.append(f'\n輸出：output/oem_master.xlsx（source_data {len(df_source)} 列 / manual_data {len(df_manual)} 列）')
    log_lines.append('=' * 40)
    append_log('\n'.join(log_lines))

    # 摘要
    print('\n' + '=' * 50)
    print('📊 執行摘要')
    print('-' * 50)
    if stats['added']:          print(f'新增：{len(stats["added"])} 個檔案')
    if stats['updated']:        print(f'更新：{len(stats["updated"])} 個檔案')
    if skipped:                 print(f'跳過：{len(skipped)} 個檔案')
    if stats['pending_review']: print(f'待 AI 審閱：{len(stats["pending_review"])} 個檔案（文字已存入 pending_ai_review/）')
    if stats['failed']:         print(f'失敗：{len(stats["failed"])} 個檔案')
    print(f'source_data：{len(df_source):,} 列')
    print(f'manual_data：{len(df_manual):,} 列（來源：OEM_oil_recommendation.xlsx）')

    if report_summary:
        print('\n📋 OEM PDF 解析報告：')
        for fname, total, cc in report_summary:
            rpt_name = f'oem_{os.path.splitext(fname)[0]}_report.xlsx'
            print(f'  - {fname} → 共 {total} 筆')
            print(f'    HIGH:{cc["high"]}  MEDIUM:{cc["medium"]}  LOW:{cc["low"]}  → parse_report/{rpt_name}')

    if stats['pending_review']:
        print('\n🤖 以下 PDF 含有文字但無結構化表格，已暫存供 AI 審閱：')
        for f in stats['pending_review']:
            stem = os.path.splitext(f)[0]
            print(f'   - {f}  →  output/pending_ai_review/{stem}_pending.json')
        print('   請執行「審閱OEM」讓 Claude 彙整並確認後寫入 manual_data。')

    if manual_req:
        print('\n⚠️  以下檔案需要人工介入（已失敗 3 次）：')
        for k in manual_req: print(f'   - {k}')
    print('=' * 50)

if __name__ == '__main__':
    main()
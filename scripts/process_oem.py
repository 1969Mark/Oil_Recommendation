"""
process_oem.py
==============
增量處理 OEM_data/ 內的 PDF/Excel 檔案。

OEM 資料品質特性：原始檔案多為非結構化（敘述性段落、混合表格），
程式自動解析準確度低，故所有 OEM 檔案一律走 AI 審閱流程：
  原始檔 → 提取全文存 pending_ai_review/*.json → 使用者執行「審閱OEM」
  → Claude 彙整向使用者確認 → 寫入 oem_master.xlsx 的 manual_data sheet

oem_master.xlsx 只有 manual_data 一個 sheet（程式從不讀寫除了首次建立外）。
首次建立時，自動將 OEM_oil_recommendation.xlsx 載入為 manual_data 起始內容。
"""

import os, sys, json
from datetime import datetime
import pandas as pd
import pdfplumber
from openpyxl import Workbook

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _config import (
    OEM_DIR, OUT_DIR,
    OEM_COLOR, COLS,
    PENDING_DIR, MANUAL_SRC,
    OEM_MASTER as MASTER, OEM_REG as REG_FILE, OEM_COLS as MASTER_COLS,
)
from _common import (
    load_registry, save_registry, load_failed_registry,
    save_failed_registry, needs_processing, file_metadata,
    append_log, write_styled_sheet,
)

# ── 提取全文 → pending_ai_review ────────────────────────────────
def save_pending_review(fpath, fname):
    """
    提取檔案文字內容存為 pending_ai_review/{name}_pending.json，供 AI 審閱。
    支援 PDF（每頁文字）與 Excel（每 sheet 文字）。
    回傳 (pending_path, 有文字的單元數, 來源類型)
    """
    os.makedirs(PENDING_DIR, exist_ok=True)
    ext = os.path.splitext(fname)[1].lower()

    if ext == '.pdf':
        pages_data = []
        with pdfplumber.open(fpath) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = (page.extract_text() or '').strip()
                pages_data.append({'page': i, 'text': text})
        text_pages = sum(1 for p in pages_data if p['text'])
        full_text = '\n\n--- Page {} ---\n'.join(
            p['text'] for p in pages_data if p['text']
        )
        pending = {
            'filename'    : fname,
            'source'      : 'oem',
            'source_type' : 'pdf',
            'extracted_at': datetime.now().isoformat(timespec='seconds'),
            'total_pages' : len(pages_data),
            'text_pages'  : text_pages,
            'pages'       : pages_data,
            'full_text'   : full_text,
            'status'      : 'pending',
        }
        unit_count = text_pages
    else:
        # Excel：每個 sheet 轉成 TSV 文字
        sheets_data = []
        xl = pd.ExcelFile(fpath)
        for sn in xl.sheet_names:
            try:
                df = xl.parse(sn, header=None, dtype=str).fillna('')
                lines = ['\t'.join(str(v) for v in row) for row in df.values.tolist()]
                text = '\n'.join(l for l in lines if l.strip())
            except Exception as e:
                text = f'[ERROR reading sheet: {e}]'
            sheets_data.append({'sheet': sn, 'text': text})
        text_sheets = sum(1 for s in sheets_data if s['text'].strip())
        full_text = '\n\n--- Sheet: {} ---\n'.join(
            s['text'] for s in sheets_data if s['text'].strip()
        )
        pending = {
            'filename'    : fname,
            'source'      : 'oem',
            'source_type' : 'excel',
            'extracted_at': datetime.now().isoformat(timespec='seconds'),
            'total_sheets': len(sheets_data),
            'text_sheets' : text_sheets,
            'sheets'      : sheets_data,
            'full_text'   : full_text,
            'status'      : 'pending',
        }
        unit_count = text_sheets

    stem = os.path.splitext(fname)[0]
    out_path = os.path.join(PENDING_DIR, f'{stem}_pending.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(pending, f, indent=2, ensure_ascii=False)

    return out_path, unit_count, ext.lstrip('.')

# ── 寫 master（只剩 manual_data 一個 sheet）─────────────────────
def write_oem_master(df_manual, path):
    df = df_manual.copy()
    for col in MASTER_COLS:
        if col not in df.columns:
            df[col] = '' if col != 'Count' else 1
    df.loc[:, 'Count'] = df['Count'].apply(
        lambda v: 1 if (pd.isna(v) or str(v).strip() in ('', '0')) else int(v))

    wb = Workbook()
    ws = wb.active
    ws.title = 'manual_data'
    write_styled_sheet(ws, df, MASTER_COLS, source_color=OEM_COLOR)
    wb.save(path)

# ── 主流程 ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("▶  process_oem.py 開始執行（AI 審閱模式）")
    print("=" * 50)

    reg  = load_registry(REG_FILE)
    freg = load_failed_registry()
    now  = datetime.now()

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

    print(f"OEM 檔案總數：{len(all_files)}  ｜  待處理：{len(to_process)}  ｜  跳過：{len(skipped)}")

    # ── manual_data：保留既有；首次建立則從 OEM_oil_recommendation.xlsx 載入
    df_manual = pd.DataFrame()
    master_exists = os.path.exists(MASTER)

    if master_exists:
        try:
            df_manual = pd.read_excel(MASTER, sheet_name='manual_data')
            print(f"✓ 保留既有 manual_data：{len(df_manual)} 列")
        except Exception as e:
            print(f"⚠️  無法讀取 manual_data sheet：{e}")
    else:
        if os.path.exists(MANUAL_SRC):
            try:
                df_manual = pd.read_excel(MANUAL_SRC)
                for col in COLS:
                    if col not in df_manual.columns:
                        df_manual[col] = ''
                df_manual = df_manual[COLS].copy()
                for col in COLS:
                    df_manual[col] = df_manual[col].fillna('').astype(str).str.strip().str.upper()
                df_manual = df_manual[~df_manual['Lubricant'].str.contains('TALUSIA LS 25', na=False)]
                df_manual['Source']       = 'OEM'
                df_manual['source_file']  = 'manual'
                df_manual['source_sheet'] = 'OEM_oil_recommendation'
                print(f"✓ 初始化 manual_data（來自 OEM_oil_recommendation.xlsx）：{len(df_manual)} 列")
            except Exception as e:
                print(f"⚠️  無法載入 OEM_oil_recommendation.xlsx：{e}")
        else:
            print("ℹ️  OEM_oil_recommendation.xlsx 不存在，manual_data 為空。")

    # ── 處理新/異動檔：一律走 pending_ai_review ───────────────────
    stats = {'pending_review': [], 'failed': []}

    for fname, fpath, reason in to_process:
        try:
            print(f"\n  [{fname}]  [{reason}]")
            pending_path, unit_count, src_type = save_pending_review(fpath, fname)

            if unit_count == 0:
                raise ValueError(f"EMPTY_DATA：{src_type} 無可讀文字（掃描版或加密）")

            fail_key = f'oem/{fname}'
            existing = freg.get('failed_files', {}).get(fail_key, {})

            freg.setdefault('failed_files', {})[fail_key] = {
                'source'         : 'oem',
                'first_failed_at': existing.get('first_failed_at', now.isoformat(timespec='seconds')),
                'last_failed_at' : now.isoformat(timespec='seconds'),
                'retry_count'    : 0,
                'error_type'     : 'AI_REVIEW_PENDING',
                'error_message'  : f'已提取 {unit_count} 個{"頁" if src_type=="pdf" else "sheet"}的文字，待 AI 審閱',
                'status'         : 'ai_review_pending',
                'pending_file'   : pending_path,
            }

            # 仍記入 success registry（已成功提取文字，避免每次重跑）
            reg['processed_files'][fname] = {
                'processed_at': now.isoformat(timespec='seconds'),
                **file_metadata(fpath),
            }
            stats['pending_review'].append(fname)
            print(f"    ℹ️  已提取 {unit_count} 個{'頁' if src_type=='pdf' else 'sheet'}文字 → pending_ai_review/")

        except Exception as e:
            error_msg = str(e)
            print(f"  ❌ 失敗：{error_msg}")
            fail_key = f'oem/{fname}'
            error_type = 'EMPTY_DATA' if 'EMPTY_DATA' in error_msg else 'FILE_READ_ERROR'
            entry = freg.setdefault('failed_files', {}).get(fail_key)
            if not entry:
                freg['failed_files'][fail_key] = {
                    'source': 'oem',
                    'first_failed_at': now.isoformat(timespec='seconds'),
                    'last_failed_at' : now.isoformat(timespec='seconds'),
                    'retry_count': 1, 'error_type': error_type,
                    'error_message': error_msg[:200], 'status': 'retryable',
                }
            else:
                entry['retry_count'] += 1
                entry['last_failed_at'] = now.isoformat(timespec='seconds')
                if entry['retry_count'] >= 3:
                    entry['status'] = 'manual_required'
            stats['failed'].append(fname)

    # ── 寫 master（只有 manual_data sheet）─────────────────────
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        write_oem_master(df_manual, MASTER)
        print(f"\n✓ oem_master.xlsx 寫入完成：manual_data {len(df_manual)} 列")
    except Exception as e:
        print(f"\n❌ WRITE_ERROR：{e}")
        return

    save_registry(reg, REG_FILE)
    save_failed_registry(freg)

    pending_total = [k for k, v in freg.get('failed_files', {}).items()
                     if v.get('source') == 'oem' and v.get('status') == 'ai_review_pending']
    manual_req = [k for k, v in freg.get('failed_files', {}).items()
                  if v.get('source') == 'oem' and v.get('status') == 'manual_required']

    # Log
    log_lines = [
        '=' * 40,
        f'時間：{now.strftime("%Y-%m-%d %H:%M:%S")}',
        '動作：更新OEM',
        '-' * 40,
    ]
    if stats['pending_review']:
        log_lines.append('待 AI 審閱（已提取文字）：')
        for f in stats['pending_review']:
            log_lines.append(f'  - {f}  → pending_ai_review/')
    if skipped:
        log_lines.append(f'跳過：{len(skipped)} 個檔案（未變更）')
    if stats['failed']:
        log_lines.append('失敗（本次）：')
        for f in stats['failed']:
            log_lines.append(f'  - {f}')
    if manual_req:
        log_lines.append('\n⚠️  需人工介入（manual_required）：')
        for k in manual_req:
            log_lines.append(f'  - {k}')
    log_lines.append(f'\n輸出：output/oem_master.xlsx（manual_data {len(df_manual)} 列）')
    log_lines.append(f'待 AI 審閱總數：{len(pending_total)} 筆')
    log_lines.append('=' * 40)
    append_log('\n'.join(log_lines))

    # 摘要
    print('\n' + '=' * 50)
    print('📊 執行摘要')
    print('-' * 50)
    if stats['pending_review']:
        print(f'本次新增待 AI 審閱：{len(stats["pending_review"])} 個檔案')
    if skipped:
        print(f'跳過：{len(skipped)} 個檔案')
    if stats['failed']:
        print(f'失敗：{len(stats["failed"])} 個檔案')
    print(f'manual_data：{len(df_manual):,} 列')
    print(f'待 AI 審閱（累計）：{len(pending_total)} 筆')

    if pending_total:
        print('\n🤖 待 AI 審閱清單（執行「審閱OEM」彙整並寫入 manual_data）：')
        for k in pending_total:
            print(f'   - {k}')

    if manual_req:
        print('\n⚠️  以下檔案需要人工介入（已失敗 3 次）：')
        for k in manual_req:
            print(f'   - {k}')
    print('=' * 50)


if __name__ == '__main__':
    main()

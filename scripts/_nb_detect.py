"""
_nb_detect.py — NB PDF 格式自動偵測。

由 process_nb.py / _nb_parsers.py 匯入；偵測順序與規則見 detect_format() 內註解。
"""

import pdfplumber
import fitz  # PyMuPDF


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

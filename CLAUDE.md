# CLAUDE.md — ABC Lubrication Data Pipeline

本檔案為 Claude Cowork 的專案指示檔，放置於 Oil_Recommendation/ 根目錄。
每次 Cowork session 開啟此資料夾時，Claude 會自動讀取並遵守以下所有規則。

---

## 📁 專案目錄結構

```
Oil_Recommendation/
├── CLAUDE.md                     ← 本檔案（Cowork 自動讀取）
│
├── data/
│   ├── lube-chart/               ← Lube Chart CSV 檔案（隨時可新增）
│   ├── nb/                       ← Machinery List PDF 檔案（隨時可新增）
│   └── oem/                      ← OEM Excel / PDF 檔案（隨時可新增）
│
├── output/
│   ├── lube_chart_master.xlsx    ← Lube Chart 彙整結果
│   ├── nb_master.xlsx            ← NB Machinery 彙整結果
│   ├── oem_master.xlsx           ← OEM 規格彙整結果（含 source_data + manual_data 兩個 sheet）
│   ├── oem_combined.json         ← 部署用：source_data + manual_data 合併後的輸出
│   └── parse_report/
│       ├── nb_[檔名]_report.xlsx  ← NB PDF 解析核對報告（每個 PDF 一份）
│       └── oem_[檔名]_report.xlsx ← OEM PDF 解析核對報告（每個 PDF 一份）
│
├── registry/
│   ├── lube_chart_registry.json  ← Lube Chart 已處理檔案記錄
│   ├── nb_registry.json          ← NB 已處理檔案記錄
│   ├── oem_registry.json         ← OEM 已處理檔案記錄
│   └── failed_registry.json      ← 所有失敗檔案的統一記錄
│
├── scripts/
│   ├── process_lube_chart.py     ← Lube Chart 增量處理腳本
│   ├── process_nb.py             ← NB 增量處理腳本
│   ├── process_oem.py            ← OEM 增量處理腳本
│   └── deploy.py                 ← GitHub push + Vercel 部署
│
├── logs/
│   └── update_log.txt            ← 每次更新紀錄（時間戳記 + 異動摘要）
│
├── .gitignore                    ← Git 排除清單（原始資料夾、暫存檔等）
└── lube_query_app.html           ← 前端查詢 App（部署至 Vercel）
```

---

## 🔑 核心原則：增量處理（Incremental Processing）

每次處理前，必須先比對 registry，只處理新增或異動的檔案。

### Registry 檔案格式（JSON）

每個 data source 對應一個 registry 檔，記錄已成功處理過的檔案資訊：

```json
{
  "last_updated": "2025-04-22T10:30:00",
  "processed_files": {
    "filename.csv": {
      "processed_at": "2025-04-22T10:30:00",
      "file_size_bytes": 20480,
      "file_mtime": "2025-04-20T08:00:00",
      "sha256": "a3f1c9d2..."
    }
  }
}
```

`failed_registry.json` 格式（所有 data source 共用一個失敗記錄）：

```json
{
  "last_updated": "2025-04-22T10:30:00",
  "failed_files": {
    "oem/supplier_X.pdf": {
      "source": "oem",
      "first_failed_at": "2025-04-22T10:30:00",
      "last_failed_at": "2025-04-22T11:00:00",
      "retry_count": 2,
      "error_type": "PDF_PARSE_ERROR",
      "error_message": "pdfplumber 無法擷取任何表格或文字",
      "status": "retryable"
    }
  }
}
```

`status` 欄位說明：
- `retryable`：可自動重試（retry_count < 3）
- `manual_required`：已達重試上限，需人工介入
- `ai_review_pending`：無結構化表格但含可讀文字，已提取全文存入 `output/pending_ai_review/`，等待 Claude AI 審閱後由使用者確認寫入 `manual_data`；**不自動重試**

### 新增/異動的判斷邏輯

一個檔案需要重新處理，若符合以下任一條件：

1. 檔名不在 registry → 全新檔案，需處理
2. 檔案的 `file_mtime`（修改時間）比 registry 記錄的新 → 檔案被更新，需重新處理
3. 檔案的 `sha256` 與 registry 不符 → 內容有變動（即使檔名相同），需重新處理

三個條件都不符合 → 跳過，無需重新處理。

### Registry 更新時機

- 每個檔案成功處理並寫入 master Excel 後，立即更新該檔案在 registry 的記錄
- 失敗的檔案不寫入成功 registry，改寫入 `failed_registry.json`
- 不得刪除 registry 中已存在的記錄（除非使用者明確執行「重建」指令）

---

## ⚠️ 錯誤處理規格

### 錯誤分類

| 錯誤類型 | 代碼 | 說明 | 處理方式 |
|---|---|---|---|
| 檔案無法開啟 | `FILE_READ_ERROR` | 檔案損毀、權限不足、格式錯誤 | 記錄 → 跳過 → 繼續下一檔 |
| PDF 無法解析 | `PDF_PARSE_ERROR` | 掃描版 PDF、加密、無文字層 | 記錄 → 跳過 → 繼續下一檔 |
| 表格結構異常 | `SCHEMA_MISMATCH` | 欄位名稱或數量與預期不符 | 記錄 → 跳過 → 繼續下一檔 |
| 資料完全為空 | `EMPTY_DATA` | 解析後無任何有效資料列 | 記錄 → 跳過 → 繼續下一檔 |
| Master 寫入失敗 | `WRITE_ERROR` | Excel 鎖定、磁碟空間不足等 | 記錄 → 中止本次執行（避免資料不一致） |

### 失敗隔離原則

單一檔案失敗不影響其他檔案的處理。每個檔案的處理包在獨立的 try/except 區塊內：

```python
for file in files_to_process:
    try:
        process(file)
        update_success_registry(file)
    except Exception as e:
        classify_error(e)
        update_failed_registry(file, error)
        continue   # 繼續處理下一個檔案
```

唯一例外：`WRITE_ERROR`（master Excel 寫入失敗）屬於全域性錯誤，應中止整個流程並告知使用者，避免資料寫入一半造成 master 損毀。

### 重試機制

每次執行時，除了處理新/異動檔案外，也自動重試 `failed_registry.json` 中 `status = "retryable"` 的檔案：

1. 取得失敗清單中 `retry_count < 3` 且 `status = "retryable"` 的檔案
2. 嘗試重新處理
3. 成功 → 移出 `failed_registry.json`，寫入成功 registry
4. 再次失敗 → `retry_count + 1`，更新 `last_failed_at`
5. `retry_count` 達到 3 → 將 `status` 改為 `manual_required`，不再自動重試

### `manual_required` 的處理

當某個檔案被標記為 `manual_required`：
- 每次執行結束時，Claude 必須在摘要中明確列出這些檔案
- 不再自動嘗試，等待使用者手動介入
- 使用者修正檔案後，可用 `重試 [檔名]` 指令重置該檔案的失敗記錄並重新處理

---

## 🔍 PDF 解析核對機制

適用範圍：NB 和 OEM 中的所有 PDF 檔案。

### 信心分數（Confidence Score）定義

| 等級 | 代碼 | 判斷條件 | 建議動作 |
|---|---|---|---|
| 高 | `high` | 表格邊界清晰、無跨頁、無合併儲存格、所有欄位完整 | 信任，無需核對 |
| 中 | `medium` | 跨頁表格、有合併儲存格、部分欄位為空、純文字解析 | 建議抽查核對 |
| 低 | `low` | 無結構化表格（fallback 純文字）、欄位對齊不確定、數值含特殊字元 | **不納入 master**，僅保留於 parse_report 供參考 |

> ⚠️ **`low` 信心分數的記錄不寫入任何 master Excel**，只會出現在 `parse_report` 的 `all_data` 和 `review_required` sheet 中，供使用者判斷是否需要手動補入 `manual_data`。

### 解析報告格式（parse_report）

每個 PDF 處理完後，在 `output/parse_report/` 產生一份 `[source]_[檔名]_report.xlsx`，內含兩個 sheet：

**Sheet 1：`all_data`** — 所有解析結果，含信心分數欄位

| 欄位 | 說明 |
|---|---|
| `row_id` | 列編號（方便對照） |
| `page_no` | 來源 PDF 頁碼 |
| `confidence` | `high` / `medium` / `low` |
| `confidence_reason` | 信心分數原因（例如：「跨頁表格」、「合併儲存格」） |
| `[其他資料欄位]` | 實際解析的業務資料 |

**Sheet 2：`review_required`** — 只列出 `medium` 和 `low` 的資料列，供核對用

- 這個 sheet 是 `all_data` 的子集，只篩出需要核對的列
- 欄位相同，額外加上 `review_status` 欄（預設空白，核對後可填入 `ok` 或 `fix`）
- 核對完不需要做任何特別操作，report 只是參考用，不影響 master 資料

### 核對工作流程

```
執行「更新NB」或「更新OEM」
         ↓
系統自動產生 parse_report/
         ↓
打開 [檔名]_report.xlsx 的 review_required sheet
         ↓
對照原始 PDF 同一頁，確認資料是否正確
         ↓
有錯誤？→ 兩種處理方式：
  A. 直接在 master Excel 的 source_data 手動修正該列（快速）
  B. 若是 OEM 且原始檔案有問題 → 放入 manual_data 補充正確資料
沒錯誤？→ 不需要做任何事
```

### 報告的產生時機

- 只在新增或異動的 PDF 檔案才產生新報告
- 跳過的檔案（未變更）不重新產生報告
- 若舊報告已存在且 PDF 未變更 → 保留舊報告不覆蓋
- `重建` 指令執行時，對應的舊報告會一併清除並重新產生

### 執行摘要中的核對提示

每次處理含 PDF 的資料來源後，Claude 在對話摘要中需加入：

```
📋 解析報告已產生：
  - nb_machinery_list_2025.pdf → 共 45 列
    HIGH: 38 列（無需核對）
    MEDIUM: 6 列（建議抽查）→ parse_report/nb_machinery_list_2025_report.xlsx
    LOW: 1 列（需人工核對）→ 同上報告的 review_required sheet
```

---

## 🗂️ Data Source 處理規格

### 1. Lube Chart (`data/lube-chart/`)

- 格式：CSV
- Registry：`registry/lube_chart_registry.json`
- 輸出：`output/lube_chart_master.xlsx`
- 處理邏輯：
  1. 讀取 registry，取得已處理清單
  2. 掃描 `data/lube-chart/` 所有 `.csv`，找出需處理的新/異動檔案
  3. 讀取現有 `lube_chart_master.xlsx`（若存在）
  4. 對每個需處理的檔案：
     - 若為異動檔：先從 master 中移除該 `source_file` 的舊資料，再插入新解析結果
     - 若為新增檔：直接 append 至 master
     - 每列保留 `source_file` 欄位記錄來源
  5. 合併後去除完全重複列，依 `source_file` 排序
  6. 寫回 `lube_chart_master.xlsx`
  7. 更新 registry（只更新成功處理的檔案）

### 2. NB Machinery List (`data/nb/`)

- 格式：PDF
- Registry：`registry/nb_registry.json`
- 輸出：`output/nb_master.xlsx`

#### PDF 格式類型（自動偵測，無需手動指定）

由 `process_nb.py` 的 `detect_format()` 自動辨識，目前支援以下 14 種：

| 格式 | 偵測條件（標題列關鍵字） | 解析方式 |
|---|---|---|
| **Format A** | （fallback）英文欄位，表格有明確 x 座標邊界 | 依固定 x 座標定位欄位，逐列擷取 |
| **Format B** | 含 `MAKER:` / `TYPE:` 關鍵字、或 `船东供油料清单` / `OIL LIST FOR OWNER SUPPLY` | 以關鍵字為錨點，解析鍵值對 |
| **Format C** | 標題含 `Maker&` / `Maker &` / `厂家及型号`（廠家與型號合併欄） | 以正則表達式拆分廠家與型號 |
| **Format D** | `EQUIPMENT + PART + LUB OIL`（中國造船廠三欄式） | 三欄式表格解析 |
| **Format E** | `TOTAL OIL + EQUIPMENT MAKER`（江南造船廠，油品欄在前） | 自定欄序解析 |
| **Format F** | `PRINCIPAL PARTICULAR + APPLICATION POINT`（Hyundai HHI L.O Chart） | HHI 多欄表格解析 |
| **Format G** | `EQUIPMENT + APPLICATION POINT + PRODUCT`（K Shipbuilding，無 PRINCIPAL PARTICULAR） | A.設備清單 + B.潤滑油表雙區塊解析 |
| **Format H** | `BRAND + (MAKER 或 制造商)`（NTS 中英雙語，Lub. Oil brand 在 row4） | 中英雙語多列標題解析 |
| **Format I** | `KIND OF LUB + APPLICATION POINT`（HN5801 供應商格式） | EQUIPMENT (MAKER/TYPE) 合併欄解析 |
| **Format J** | `OIL BRAND + POINT`（無 APPLICATION 關鍵字，如 NDY1305） | 中英雙語 OIL BRAND 解析 |
| **Format K** | `LUBRICATING POINT + KIND OF LUBRICANT`（JIT 船东供油料清单） | 自定欄序解析 |
| **Format L** | `L.O. BRAND + LUBRICATION + MAKER`（雙層合併標題） | 雙層標題解析 |
| **Format M** | `EQUIPMENT + MANUFACTURER + APPLICATION`（SN2265、SN2662、SN2672 等） | 動態欄位偵測 + word 座標 + pending 列暫存 |
| **Format N** | `NAME OF MACHINERY + PRINCIPAL PARTICULAR + L.O GRADE + APPLICATION POINT` 且 PDF 為 90° 旋轉（pdfplumber 文字反向） | 用 PyMuPDF (fitz) 讀 word 座標，依 Y-bands 分欄 |

**偵測順序**（在 `detect_format()` 中由上而下短路判斷）：

1. **Format N**（前置掃描，需 fitz）：旋轉版 PDF 必須先抓，避免被 pdfplumber 反向文字誤判
2. **Format L**（前置掃描）：先於主迴圈，避免被 Format D 早期攔截
3. 主迴圈（前 5 頁）：Format F → E → J → K → M → D（早期攔截嚴格條件）→ H → G → I → D（寬鬆條件）→ C
4. 文字內容判斷：Format B（船東供油料清單 / MAKER: 關鍵字）
5. **Format A**（fallback）

偵測失敗則改為純文字解析，並標記 `confidence = low`。

#### 🆕 新增 NB PDF 的前置流程（強制）

當 `data/nb/` 出現 registry 中尚未登記的新檔案時，**Claude 不得直接執行 `process_nb.py`**，必須先依序完成以下步驟：

1. **列出新檔名**給使用者，並先暫停解析
2. **請使用者提供原稿截圖**（至少包含表頭與前幾列資料），說明各欄位對應關係：
   - 哪一欄是 Equipment / 設備名稱
   - 哪一欄是 Maker / 製造廠
   - 哪一欄是 Model / Type / 型號
   - 哪一欄是 Application Point / 潤滑部位
   - 哪一欄是 Lubricant / 推薦油品
3. **比對既有 Format A~N**：
   - 若符合既有格式 → 確認 `detect_format()` 能正確判定 → 執行解析
   - 若不符合 → 與使用者討論新增 Format 的偵測條件與解析邏輯，更新 `process_nb.py` 並補上對應記憶後再執行
4. **確認後**才呼叫 `process_nb.py`，並核對 parse_report 的 medium/low 列數

**Why**：歷次新格式（SN2265、010-2、SN2662 等）若無事前確認，常出現欄位錯位、Model 沿用、Maker 累加、表格旋轉未偵測等錯誤，事後修正成本遠高於事前 5 分鐘的截圖溝通。此規則僅適用於 NB PDF；OEM 與 Lube Chart 不受影響。

#### 處理邏輯

  1. 讀取 registry，取得已處理清單
  2. 掃描 `data/nb/` 所有 `.pdf`，找出需處理的新/異動檔案
  3. 讀取現有 `nb_master.xlsx`（若存在）
  4. 對每個需處理的檔案：
     - 使用 `pdfplumber` 擷取表格；無結構化表格時改解析純文字
     - 每列標記 `confidence` 和 `confidence_reason`（依信心分數定義）
     - **`confidence = low` 的記錄不寫入 master，僅保留於 parse_report**
     - 若為異動檔：先從 master 移除該來源舊資料，再插入新資料（high/medium 列）
     - 若為新增檔：append 至 master（high/medium 列）
     - 產生 `output/parse_report/nb_[檔名]_report.xlsx`（含所有列，包含 low）
  5. 去除重複機械項目（唯一鍵：設備名稱 + 型號）
  6. 寫回 `nb_master.xlsx`
  7. 更新 registry

### 3. OEM Data (`data/oem/`)

- 格式：Excel (`.xlsx`, `.xls`) 或 PDF
- Registry：`registry/oem_registry.json`
- 輸出：`output/oem_master.xlsx`（含兩個 sheet，見下方說明）

#### oem_master.xlsx Sheet 結構

| Sheet 名稱 | 維護方式 | 說明 |
|---|---|---|
| `source_data` | 程式自動產生 | 從原始檔案解析的所有資料 |
| `manual_data` | 使用者手動維護 | 人工補充的資料，程式永遠不碰 |

`manual_data` sheet 欄位規範：
- 欄位結構與 `source_data` 相同
- `source_file` 欄位固定填入 `"manual"`
- `source_sheet` 欄位可填入備註說明該筆資料來源（例如供應商名稱）
- 若 `oem_master.xlsx` 不存在時，程式自動建立兩個空 sheet

#### 處理邏輯

1. 讀取 registry，取得已處理清單
2. 掃描 `data/oem/` 所有 `.xlsx`、`.xls`、`.pdf`，找出需處理的新/異動檔案
3. 讀取現有 `oem_master.xlsx` 的 `source_data` sheet（若存在）
4. **絕對不讀取、不修改、不覆蓋 `manual_data` sheet**
5. 對每個需處理的檔案：
   - Excel：讀取所有 sheets（Excel 不需信心分數）
   - PDF：使用 `pdfplumber` 擷取表格，每列標記 `confidence` 和 `confidence_reason`
   - **`confidence = low` 的記錄不寫入 master，僅保留於 parse_report**
   - 若為異動檔：先從 `source_data` 移除該來源舊資料，再插入新資料（high/medium 列）
   - 若為新增檔：append 至 `source_data`（high/medium 列）
   - PDF 檔案額外產生 `output/parse_report/oem_[檔名]_report.xlsx`（含所有列，包含 low）
6. 去除 `source_data` 中的完全重複列
7. 寫回 `oem_master.xlsx` 的 `source_data` sheet
8. 更新 registry

#### OEM 業務規則

以下規則適用於 `process_oem.py` 解析及 `manual_data` 手動維護時的欄位填寫規範：

**MAN B&W 汽缸油命名規範**
- 汽缸油統一以 `CYLINDER OIL` 標記於 `Part to be lubricated` 欄位
- BN（鹼值）標示格式：`CYLINDER OIL BN[數值]`，例如 `CYLINDER OIL BN100`
- 不同 BN 規格視為不同產品，不合併

**WINGD / J-ENG UEC 引擎分類**
- WINGD（原 Wärtsilä 二行程）引擎的潤滑油規格獨立歸檔，不與 MAN B&W 混用
- J-ENG UEC 系列引擎依型號前綴分類：`UEC` 系列統一標記 `source_sheet = "J-ENG UEC"`
- 各 OEM 廠家的推薦油品若與 Lube Chart 資料衝突，以 OEM 資料為準（`manual_data` 優先於 `source_data`）

#### 彙整輸出（供部署用）

部署前，`deploy.py` 會將 `source_data` 和 `manual_data` 合併成單一資料集輸出給前端：
- 合併順序：`source_data` 在前，`manual_data` 在後
- 合併結果不覆蓋 `oem_master.xlsx`，另存為 `output/oem_combined.json`（或前端所需格式）
- `manual_data` 的每列保留 `source_file = "manual"` 標記，前端可據此識別資料來源

#### App 查詢結果排序規則

查詢結果依 `Source` 欄位照以下順序排列：

1. `OEM`（最高優先，廠家原廠規格）
2. `NB`（新造船潤滑油圖表）
3. `LUBE CHART`（最後）

同一 Source 內的資料，依 `Maker` → `Model / Type` 字母順序排列。

---

## 🧹 資料過濾與標準化規則

適用範圍：**所有來源**（Lube Chart CSV、NB PDF、OEM Excel/PDF），每次更新腳本均自動執行。

### 1. Maker 名稱標準化

- **移除數量後綴**：`YANMAR(X3)` / `YANMAR (X3)` → `YANMAR`
- **前綴比對合併**：若某 variant 的筆數 ≤ base 廠牌筆數的 10%（且 variant 筆數 ≤ 200），自動合併至主廠牌；例如 `YANMAR DIESEL` → `YANMAR`
- **保留例外**：高頻（> 200 筆）或含 `/` 的聯合廠牌不合併，例如 `WARTSILA LIPS`、`KAWASAKI PRECISION MACHINERY`

### 2. Part to be lubricated 格式標準化

- **單複數統一**：取頻率較高的形式，例如 `BEARING` → `BEARINGS`、`WIRE ROPES` → `WIRE ROPE`
- **空格差異統一**：`GEAR BOX` → `GEARBOX`、`CRANK CASE` → `CRANKCASE`、`STERN TUBE` → `STERNTUBE`
- **連接詞統一**：`&` vs `AND` → 取頻率較高的形式
- **保留技術後綴**：`EAL`、`VLSFO`、`RUNNING IN` 等技術標記不移除

### 3. 過濾無效型號

`Model / Type` 欄位符合以下條件的記錄一律排除：
- 空白、純空格
- 佔位符號：`.`、`..`、`-`、`--`、`N/A`、`NA`、`NONE`
- 未定義值：`TO BE DETERMINED`、`TBD`

### 4. 去除重複列

唯一鍵：`Maker` + `Model / Type` + `Part to be lubricated` + `Lubricant` 四欄組合，完全相同則去除重複，保留第一筆。

### 5. 排除特定潤滑油

`Lubricant` 欄位含 `TALUSIA LS 25` 的記錄一律刪除（不論來源）。

---

## 🎨 Excel 輸出格式規範

適用範圍：所有 master Excel（`lube_chart_master.xlsx`、`nb_master.xlsx`、`oem_master.xlsx`）及 parse_report。

| 元素 | 規格 |
|---|---|
| 資料內容 | 全大寫，去除前後空格 |
| 標題列背景 | 深藍 `#1F3864` |
| 標題列字型 | 白色、粗體 |
| 奇數資料列 | 白色 `#FFFFFF` |
| 偶數資料列 | 淡藍 `#EBF3FB` |
| `Source` 欄 — `LUBE CHART` | 綠色字 `#375623` |
| `Source` 欄 — `NB` | 金色字 `#7F6000` |
| `Source` 欄 — `OEM` | 深藍字 `#1F3864` |
| 欄寬 | 自動調整（`auto_fit`），最小 10，最大 50 |
| 凍結窗格 | 凍結第一列（標題列） |

---

## 🚀 觸發指令

| 使用者指令 | Claude 執行動作 |
|---|---|
| `更新Lube Chart` | 執行 `process_lube_chart.py`（增量 + 自動重試）→ 記錄 log |
| `更新NB` | 執行 `process_nb.py`（增量 + 自動重試）→ 記錄 log |
| `更新OEM` | 執行 `process_oem.py`（增量 + 自動重試）→ 記錄 log |
| `更新全部` | 依序執行三個 process 腳本（均為增量 + 自動重試）→ 記錄 log |
| `部署` | 提示使用者在本機終端機執行 `python scripts/deploy.py` → Git commit/push → Vercel 自動部署 |
| `更新並部署` | 依序執行 `更新全部`（Claude 處理）→ 提示使用者執行 `python scripts/deploy.py` 完成部署 |
| `審閱OEM` | 讀取 `output/pending_ai_review/` 所有 `_pending.json`，依下方「AI 審閱工作流程」逐檔彙整並向使用者確認，確認後寫入 `manual_data` |
| `查看核對報告` | 列出 `output/parse_report/` 下所有報告，顯示各報告的 medium/low 列數 |
| `查看失敗` | 顯示 `failed_registry.json` 中所有失敗檔案、錯誤原因、重試次數與狀態 |
| `重試 [檔名]` | 重置該檔案的失敗記錄（清除 failed_registry 中該項目）→ 立即重新嘗試處理 |
| `重建Lube Chart` | ⚠️ 詢問確認 → 清除 `lube_chart_registry.json` → 重新處理所有檔案 |
| `重建NB` | ⚠️ 詢問確認 → 清除 `nb_registry.json` → 重新處理所有檔案 |
| `重建OEM` | ⚠️ 詢問確認 → 清除 `oem_registry.json` → 重新處理所有檔案 |
| `重建全部` | ⚠️ 詢問確認 → 清除三個 registry → 全部重新處理 |
| `查看狀態` | 顯示各 registry 統計：已處理檔案數、最後更新時間、待處理新檔案、失敗數量 |

> `重建` 指令會清除 registry 並強制重新處理所有原始檔案。通常只在資料結構異動或 master Excel 損毀時才需要執行。Claude 必須在執行前明確詢問使用者確認。

---

## 📋 Log 格式

每次執行後在 `logs/update_log.txt` 末端 append（不覆蓋）一筆記錄：

```
========================================
時間：2025-04-22 10:30:00
動作：更新OEM
----------------------------------------
新增處理：supplier_A_2025Q1.pdf ✓, supplier_B_specs.xlsx ✓
重新處理：supplier_C_specs.xlsx ✓（sha256 異動）
自動重試：supplier_E.pdf ✓（第 2 次重試，成功）
跳過：supplier_D_old.pdf（未變更，略過）

失敗（本次）：
  - supplier_X.pdf → PDF_PARSE_ERROR：pdfplumber 無法擷取表格（第 1 次失敗）
  - supplier_Y.xlsx → SCHEMA_MISMATCH：欄位 'oil_type' 不存在（第 3 次失敗 → 已標記 manual_required）

⚠️  需人工介入（manual_required）：
  - oem/supplier_Y.xlsx（失敗 3 次，最後錯誤：SCHEMA_MISMATCH）

輸出：output/oem_master.xlsx（共 342 列）
========================================
```

執行結束後，Claude 必須在對話中主動顯示本次摘要，特別是有 `manual_required` 的情況需要明確提醒使用者。

---

## ⚙️ 部署設定

### 專案資訊

| 項目 | 值 |
|---|---|
| GitHub Repo | `https://github.com/1969Mark/Oil_Recommendation` |
| Vercel App URL | `https://oil-recommendation-rcxyufy3w-mark-chuangs-projects.vercel.app` |
| Git user.name | `1969Mark` |
| Git user.email | `mark.chuangjj@gmail.com` |
| 主要分支 | `main` |

### .gitignore 排除範圍

上傳至 GitHub 的檔案**不含**以下項目（由 `.gitignore` 控制）：

- `LubeChart_data/`、`NB_data/`、`OEM_data/`（原始資料，體積過大）
- `output/parse_report/`（暫存解析報告）
- `__pycache__/`、暫存測試檔（`test*.xlsx`、`test*.txt`）

**包含**以下項目：

- `output/lube_chart_master.xlsx`、`nb_master.xlsx`、`oem_master.xlsx`
- `registry/*.json`
- `logs/update_log.txt`
- `scripts/*.py`
- `lube_query_app.html`
- `CLAUDE.md`、`.gitignore`

### 部署流程（deploy.py）

`deploy.py` 由使用者在**本機終端機**執行（Cowork 沙盒環境無法直接執行 git push）：

```bash
python scripts/deploy.py
```

腳本執行步驟：
1. 確認三個 master Excel 均存在且非空
2. `git add` 所有輸出檔、registry、logs、scripts、app html
3. `git commit -m "data: update [YYYY-MM-DD HH:MM]"`
4. `git push origin main`
5. Vercel 透過 GitHub webhook 自動觸發重新部署
6. 將 Git commit hash 與部署結果記錄至 `logs/update_log.txt`

### 首次設定（只需做一次）

在本機終端機執行：

```bash
cd "資料夾路徑/2026_NB_oil_recommendation"
git init
git config user.name "1969Mark"
git config user.email "mark.chuangjj@gmail.com"
git remote add origin https://github.com/1969Mark/Oil_Recommendation.git
git branch -M main
```

GitHub 認證：使用 Personal Access Token（PAT），建立時勾選 `repo` 權限。
Windows 可執行 `git config credential.helper store` 讓系統記住 PAT。

### 標準更新與部署流程

```
1. 將新資料檔放入對應資料夾（LubeChart_data/ / NB_data/ / OEM_data/）
2. 告訴 Claude：「更新全部」（或指定來源）
3. Claude 執行增量處理，更新 master Excel + lube_query_app.html
4. 使用者在本機終端機執行：python scripts/deploy.py
5. Vercel 自動部署，數分鐘內線上版本更新
```

---

## 🤖 AI 審閱工作流程（`審閱OEM` 指令）

當使用者執行「審閱OEM」或「更新OEM」結束後出現 `ai_review_pending` 檔案時，Claude 依以下流程執行：

### 步驟 1：掃描待審閱清單

讀取 `output/pending_ai_review/` 下所有 `*_pending.json`，篩選 `status == "pending"` 的檔案。

### 步驟 2：逐檔理解並彙整

對每個 pending 檔案，Claude 閱讀 `full_text`（含所有頁面文字），從中找出潤滑油相關資訊，依以下欄位格式彙整：

| 欄位 | 說明 |
|---|---|
| `Equipment` | 設備類型，例如 `MAIN ENGINE`、`AUXILIARY ENGINE` |
| `Maker` | 製造廠，例如 `MAN B&W` |
| `Model / Type` | 引擎型號或類型，例如 `ME-C`、`MC-C`；如適用多型號可用通用描述 |
| `Part to be lubricated` | 潤滑部位，例如 `CYLINDER OIL BN40`、`SYSTEM OIL` |
| `Lubricant` | 推薦油品規格，例如 `CAT. II BN 40`、`SAE 30 / ISO VG 100` |

### 步驟 3：向使用者呈現彙整結果

以清楚的表格形式列出所有提取的資料筆數與內容摘要，然後**直接詢問**：

> 「以上共 N 筆資料是否全部寫入 manual_data？」

- 使用者回答「是」或「全部接受」→ 執行步驟 4
- 使用者指定修改某幾筆 → 依指示修正後重新確認
- 使用者回答「否」或「不需要」→ 將 pending 檔的 `status` 改為 `skipped`，不寫入

### 步驟 4：寫入 manual_data

確認後，Claude 用 Python（`openpyxl`）開啟 `output/oem_master.xlsx`，將確認資料 **append** 到 `manual_data` sheet（絕不碰 `source_data`），格式遵守 Excel 輸出格式規範：
- `source_file` 填 `"manual"`
- `source_sheet` 填原始 PDF 檔名（不含副檔名）
- `Source` 填 `"OEM"`
- 所有文字欄位全大寫

### 步驟 5：更新 pending 狀態

寫入成功後：
1. 將 pending JSON 的 `status` 改為 `written`，記錄 `written_at` 時間戳記
2. 從 `failed_registry.json` 移除該 `ai_review_pending` 項目
3. 在對話中回報「已寫入 N 筆至 manual_data」

### pending_ai_review 目錄結構

```
output/
└── pending_ai_review/
    ├── MAN ES_SL2023-737_pending.json      ← status: pending → written
    └── [其他檔名]_pending.json
```

pending JSON 的 `status` 欄位狀態流：
- `pending` → 剛提取，尚未審閱
- `written` → 已確認並寫入 manual_data（含 written_at 時間戳）
- `skipped` → 使用者確認不需要寫入

---

## 🛡️ 安全規則

1. 永遠不刪除 `data/` 下的任何原始檔案
2. 永遠不清空 registry，只做 update/append（重建指令除外，且須先確認）
3. 永遠不修改 `oem_master.xlsx` 的 `manual_data` sheet，任何寫入操作只能針對 `source_data` sheet
4. 任何破壞性操作執行前，Claude 必須先向使用者說明影響並等待確認
5. Script 執行失敗時，不更新成功 registry，改寫入 `failed_registry.json`
6. `WRITE_ERROR` 發生時立即中止執行，避免 master Excel 資料不一致
7. 若 master Excel 不存在，自動從零建立（含 `source_data` 和 `manual_data` 兩個空 sheet），不中止流程
8. 若 `scripts/` 下的腳本不存在，Claude 應自動依本檔規格產生這些腳本後再執行
9. 每次執行結束後，若有 `manual_required` 檔案，Claude 必須在對話中主動提醒使用者

---

## 🔧 Python 依賴套件

```
pandas
openpyxl
pdfplumber
PyMuPDF (fitz)  # 用於取得 PDF 頁碼資訊（配合信心分數報告）
hashlib         # 內建，用於 sha256 計算
```

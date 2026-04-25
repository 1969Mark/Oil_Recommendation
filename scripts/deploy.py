"""
deploy.py — ABC Lubrication Data Pipeline 部署腳本

執行方式：
  python scripts/deploy.py

功能：
  1. 確認三個 master Excel 均存在
  2. git add output/ registry/ logs/ scripts/ lube_query_app.html .gitignore CLAUDE.md
  3. git commit -m "data: update [YYYY-MM-DD HH:MM]"
  4. git push origin main
  5. 寫入 logs/update_log.txt

前置條件：
  - 已執行 git init 並設定 remote origin
  - 已設定 git user.name / user.email
  - GitHub 認證（PAT 或 SSH key）
"""

import subprocess
import sys
import os
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── 路徑設定 ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_FILE = BASE_DIR / "logs" / "update_log.txt"
APP_HTML = BASE_DIR / "lube_query_app.html"

DISPLAY_COLS = ['Equipment', 'Maker', 'Model / Type', 'Part to be lubricated', 'Lubricant', 'Source']
SRC_ORDER = {'OEM': 0, 'NB': 1, 'LUBE CHART': 2}

REQUIRED_FILES = [
    OUTPUT_DIR / "lube_chart_master.xlsx",
    OUTPUT_DIR / "nb_master.xlsx",
    OUTPUT_DIR / "oem_master.xlsx",
]

# 要加入 git 的路徑
GIT_ADD_TARGETS = [
    "output/lube_chart_master.xlsx",
    "output/nb_master.xlsx",
    "output/oem_master.xlsx",
    "registry/",
    "logs/",
    "scripts/",
    "lube_query_app.html",
    ".gitignore",
    "CLAUDE.md",
]

# ── HTML 重建（移除 source_file 欄位）──────────────────────────
def rebuild_html() -> bool:
    """
    讀取三個 master Excel，合併後只保留 DISPLAY_COLS（不含 source_file/source_sheet），
    依 OEM > NB > LUBE CHART 排序後嵌入 lube_query_app.html 的 const DATA。
    """
    frames = []

    lube_master = OUTPUT_DIR / "lube_chart_master.xlsx"
    nb_master   = OUTPUT_DIR / "nb_master.xlsx"
    oem_master  = OUTPUT_DIR / "oem_master.xlsx"

    if lube_master.exists():
        try:
            df = pd.read_excel(lube_master, sheet_name='lube_chart')
            for col in DISPLAY_COLS:
                if col not in df.columns:
                    df[col] = ''
            frames.append(df[DISPLAY_COLS].copy())
        except Exception as e:
            print(f"  ⚠ 讀取 lube_chart_master.xlsx 失敗：{e}")

    if nb_master.exists():
        try:
            df = pd.read_excel(nb_master, sheet_name='nb_master')
            for col in DISPLAY_COLS:
                if col not in df.columns:
                    df[col] = ''
            frames.append(df[DISPLAY_COLS].copy())
        except Exception as e:
            print(f"  ⚠ 讀取 nb_master.xlsx 失敗：{e}")

    if oem_master.exists():
        try:
            df_src = pd.read_excel(oem_master, sheet_name='source_data')
        except Exception:
            df_src = pd.DataFrame()
        try:
            df_man = pd.read_excel(oem_master, sheet_name='manual_data')
        except Exception:
            df_man = pd.DataFrame()
        df_oem = pd.concat([df_src, df_man], ignore_index=True)
        for col in DISPLAY_COLS:
            if col not in df_oem.columns:
                df_oem[col] = ''
        frames.append(df_oem[DISPLAY_COLS].copy())

    if not frames:
        print("  ⚠ 無資料可嵌入 HTML，跳過重建")
        return False

    df_all = pd.concat(frames, ignore_index=True)
    for col in DISPLAY_COLS:
        df_all[col] = df_all[col].fillna('').astype(str).str.strip()

    # 排序：OEM > NB > LUBE CHART，再依 Maker / Model 字母順序
    df_all['_order'] = df_all['Source'].map(SRC_ORDER).fillna(3).astype(int)
    df_all = df_all.sort_values(['_order', 'Maker', 'Model / Type']).drop(columns=['_order'])
    df_all = df_all.reset_index(drop=True)

    records   = df_all.to_dict(orient='records')
    data_json = json.dumps(records, ensure_ascii=False, separators=(',', ':'))

    if not APP_HTML.exists():
        print("  ⚠ lube_query_app.html 不存在，跳過重建")
        return False

    html_lines = APP_HTML.read_text(encoding='utf-8').split('\n')
    new_lines  = []
    replaced   = False
    for line in html_lines:
        if re.match(r'\s*const DATA\s*=\s*\[', line):
            new_lines.append(f'const DATA = {data_json};')
            replaced = True
        else:
            new_lines.append(line)

    if not replaced:
        print("  ⚠ HTML 中未找到 const DATA = [...]; 行，跳過重建")
        return False

    APP_HTML.write_text('\n'.join(new_lines), encoding='utf-8')
    print(f"  ✓ lube_query_app.html 重建完成（{len(records):,} 筆，不含 source_file）")
    return True


# ── Helper ────────────────────────────────────────────────────
def run(cmd: list[str], cwd=None) -> tuple[int, str, str]:
    """執行 shell 指令，回傳 (returncode, stdout, stderr)"""
    result = subprocess.run(
        cmd,
        cwd=cwd or BASE_DIR,
        capture_output=True,
        text=True
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def log(message: str):
    """印出並附加至 log 檔"""
    print(message)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def append_deploy_log(commit_hash: str, success: bool, error_msg: str = ""):
    """在 update_log.txt 末端附加部署記錄"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    separator = "=" * 40
    lines = [
        separator,
        f"時間：{now}",
        "動作：部署（GitHub + Vercel）",
        "-" * 40,
    ]
    if success:
        lines.append(f"Git commit：{commit_hash}")
        lines.append("推送至 GitHub：✓")
        lines.append("Vercel 部署：已透過 webhook 自動觸發")
    else:
        lines.append(f"❌ 部署失敗：{error_msg}")
    lines.append(separator)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ── 主流程 ────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("🚀 ABC Lubrication — 部署流程開始")
    print("=" * 50)

    # 1. 確認 master Excel 均存在
    print("\n[1/5] 確認 master Excel 檔案...")
    missing = [f for f in REQUIRED_FILES if not f.exists()]
    if missing:
        for m in missing:
            print(f"  ❌ 缺少：{m.name}")
        print("\n請先執行 process 腳本產生 master Excel 後再部署。")
        sys.exit(1)
    for f in REQUIRED_FILES:
        print(f"  ✓ {f.name}")

    # 2. 重建 App HTML（移除 source_file 欄位，只保留顯示欄）
    print("\n[2/5] 重建 lube_query_app.html（移除 source_file 欄位）...")
    rebuild_html()

    # 3. Git add
    print("\n[3/5] 加入 git staging area...")
    for target in GIT_ADD_TARGETS:
        full_path = BASE_DIR / target
        if Path(str(full_path)).exists() or target.endswith("/"):
            code, out, err = run(["git", "add", target])
            if code != 0:
                print(f"  ⚠ git add {target} 警告：{err}")
            else:
                print(f"  ✓ git add {target}")
        else:
            print(f"  ⚠ 略過（不存在）：{target}")

    # 確認是否有任何改動
    code, out, err = run(["git", "status", "--porcelain"])
    if not out.strip():
        print("\n📋 沒有需要 commit 的改動，部署流程結束。")
        return

    # 4. Git commit
    print("\n[4/5] 建立 commit...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    commit_msg = f"data: update {now_str}"
    code, out, err = run(["git", "commit", "-m", commit_msg])
    if code != 0:
        print(f"  ❌ commit 失敗：{err}")
        append_deploy_log("", False, err)
        sys.exit(1)

    # 取得 commit hash
    code2, commit_hash, _ = run(["git", "rev-parse", "--short", "HEAD"])
    print(f"  ✓ commit：{commit_hash}  \"{commit_msg}\"")

    # 5. Git push
    print("\n[5/5] 推送至 GitHub...")
    code, out, err = run(["git", "push", "origin", "main"])
    if code != 0:
        # 嘗試設定 upstream 再推送
        code, out, err = run(["git", "push", "--set-upstream", "origin", "main"])
    if code != 0:
        print(f"  ❌ push 失敗：{err}")
        print("\n提示：如果是認證問題，請確認已設定 GitHub PAT 或 SSH key。")
        append_deploy_log(commit_hash, False, err)
        sys.exit(1)

    print("  ✓ 推送成功！")
    print("\n✅ Vercel 將透過 webhook 自動觸發部署，請至 Vercel Dashboard 確認狀態。")

    append_deploy_log(commit_hash, True)

    print("\n" + "=" * 50)
    print("🎉 部署完成！")
    print("=" * 50)


if __name__ == "__main__":
    main()

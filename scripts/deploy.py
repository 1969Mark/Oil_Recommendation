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
from datetime import datetime
from pathlib import Path

# ── 路徑設定 ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_FILE = BASE_DIR / "logs" / "update_log.txt"

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
    print("\n[1/4] 確認 master Excel 檔案...")
    missing = [f for f in REQUIRED_FILES if not f.exists()]
    if missing:
        for m in missing:
            print(f"  ❌ 缺少：{m.name}")
        print("\n請先執行 process 腳本產生 master Excel 後再部署。")
        sys.exit(1)
    for f in REQUIRED_FILES:
        print(f"  ✓ {f.name}")

    # 2. Git add
    print("\n[2/4] 加入 git staging area...")
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

    # 3. Git commit
    print("\n[3/4] 建立 commit...")
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

    # 4. Git push
    print("\n[4/4] 推送至 GitHub...")
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

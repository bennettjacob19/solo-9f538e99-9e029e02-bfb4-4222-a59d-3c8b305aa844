#!/usr/bin/env python3
"""示例构建演示脚本：向运行中的流水线 API 依次提交三个示例并打印结果。

用法:
  python examples/run_demo.py [BASE_URL]
默认 BASE_URL=http://127.0.0.1:8000
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import httpx

TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}
EXAMPLES = ["success_build.json", "failure_build.json", "slow_child_build.json"]


def main(base_url: str) -> int:
    examples_dir = Path(__file__).resolve().parent
    with httpx.Client(base_url=base_url, timeout=30) as client:
        for name in EXAMPLES:
            payload = json.loads((examples_dir / name).read_text(encoding="utf-8"))
            print(f"\n=== 提交示例 {name} ({payload['name']}) ===")
            resp = client.post("/builds", json=payload)
            resp.raise_for_status()
            build_id = resp.json()["id"]

            while True:
                detail = client.get(f"/builds/{build_id}").json()
                if detail["status"] in TERMINAL:
                    break
                time.sleep(0.25)

            print(f"构建 #{build_id} 最终状态: {detail['status']} ({detail.get('detail')})")
            for step in detail["steps"]:
                print(
                    f"  - 步骤 {step['id']:<22} 状态={step['status']:<11} "
                    f"exit={step['exit_code']} detail={step.get('detail') or '-'}"
                )
            for step in detail["steps"]:
                log = client.get(
                    f"/builds/{build_id}/steps/{step['id']}/logs"
                ).json()
                print(f"  --- 日志 {step['id']} ---")
                for line in log["content"].splitlines():
                    print(f"    {line}")

            # 成功构建演示工件发布与下载校验
            if detail["status"] == "succeeded":
                demo_artifact(client, build_id)
    return 0


def demo_artifact(client: httpx.Client, build_id: int) -> None:
    resp = client.post(
        "/artifacts", json={"build_id": build_id, "path": "out/build.txt"}
    )
    if resp.status_code not in (200, 201):
        print(f"  [工件] 发布失败: {resp.status_code} {resp.text}")
        return
    art = resp.json()
    print(
        f"  [工件] path={art['path']} size={art['size']} "
        f"sha256={art['sha256'][:16]}… replayed={art['replayed']}"
    )
    dl = client.get(art["download_url"])
    digest = hashlib.sha256(dl.content).hexdigest()
    print(
        f"  [工件] 下载 {len(dl.content)} 字节，摘要校验 "
        f"{'通过' if digest == art['sha256'] else '失败'}"
    )


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    raise SystemExit(main(url))

"""工件发布端到端测试：真实 uvicorn 子进程 + 真实文件系统。"""
from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path

import httpx
import pytest

from test_integration import (
    ServerHarness,
    _free_port,
    step,
    submit,
    wait_build,
)


def publish(client: httpx.Client, build_id: int, path: str) -> httpx.Response:
    return client.post("/artifacts", json={"build_id": build_id, "path": path})


def make_success_build(server, content: str = "firmware-v1\n", name: str = "artifact") -> int:
    bid = submit(server.client, name, [
        step(
            "build",
            "mkdir -p out && printf '%s' '" + content + "' > out/fw.bin",
        ),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "succeeded"
    return bid


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = ServerHarness(tmp_path_factory.mktemp("artifact-data"), _free_port())
    srv.start()
    yield srv
    srv.stop()


# --------------------------------------------------------------- 发布与下载

def test_publish_and_download_roundtrip(server):
    content = "firmware-v1\n"
    bid = make_success_build(server, content)

    r = publish(server.client, bid, "out/fw.bin")
    assert r.status_code == 201, r.text
    art = r.json()
    assert art["build_id"] == bid
    assert art["path"] == "out/fw.bin"
    assert art["size"] == len(content.encode())
    assert art["sha256"] == hashlib.sha256(content.encode()).hexdigest()
    assert art["replayed"] is False
    assert art["download_url"] == f"/builds/{bid}/artifact/content"

    info = server.client.get(f"/builds/{bid}/artifact")
    assert info.status_code == 200
    assert info.json()["sha256"] == art["sha256"]

    dl = server.client.get(f"/builds/{bid}/artifact/content")
    assert dl.status_code == 200
    assert dl.content == content.encode()
    assert dl.headers["content-length"] == str(len(content.encode()))
    assert dl.headers["x-artifact-sha256"] == art["sha256"]
    assert hashlib.sha256(dl.content).hexdigest() == art["sha256"]


def test_workdir_changes_do_not_affect_published_content(server):
    bid = make_success_build(server, "original\n", name="immutable")
    art = publish(server.client, bid, "out/fw.bin").json()

    # 直接改动、删除源文件，都不应影响已确认工件
    source = server.data_dir / "workdir" / str(bid) / "out" / "fw.bin"
    source.write_text("tampered\n")
    dl = server.client.get(f"/builds/{bid}/artifact/content")
    assert dl.content == b"original\n"
    source.unlink()
    dl2 = server.client.get(f"/builds/{bid}/artifact/content")
    assert dl2.content == b"original\n"
    assert hashlib.sha256(dl2.content).hexdigest() == art["sha256"]


def test_same_path_retry_returns_original(server):
    bid = make_success_build(server, "v1\n", name="retry")
    first = publish(server.client, bid, "out/fw.bin")
    assert first.status_code == 201
    # 改动源文件后重试，仍必须返回首次发布的内容与摘要
    source = server.data_dir / "workdir" / str(bid) / "out" / "fw.bin"
    source.write_text("v2\n")
    second = publish(server.client, bid, "out/fw.bin")
    assert second.status_code == 200, second.text
    body = second.json()
    assert body["replayed"] is True
    for key in ("build_id", "path", "size", "sha256", "created_at"):
        assert body[key] == first.json()[key]
    assert server.client.get(f"/builds/{bid}/artifact/content").content == b"v1\n"


def test_different_path_conflicts(server):
    bid = make_success_build(server, "a\n", name="conflict")
    assert publish(server.client, bid, "out/fw.bin").status_code == 201
    r = publish(server.client, bid, "out/other.bin")
    assert r.status_code == 409
    assert "已发布" in r.json()["detail"]


def test_concurrent_publish_single_commit(server):
    bid = make_success_build(server, "concurrent\n", name="concurrent")
    results: list[httpx.Response] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(publish(server.client, bid, "out/fw.bin"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 8
    assert all(r.status_code in (200, 201) for r in results), [r.text for r in results]
    assert sum(1 for r in results if r.status_code == 201) == 1
    hashes = {r.json()["sha256"] for r in results}
    assert len(hashes) == 1
    # 只留下一个 blob，没有临时文件残留
    blobs = list((server.data_dir / "artifacts" / "blobs").rglob("*.bin"))
    bid_blobs = [p for p in blobs if p.parent.name == str(bid)]
    assert len(bid_blobs) == 1
    assert not list((server.data_dir / "artifacts" / "tmp").iterdir())


# --------------------------------------------------------------- 拒绝场景

def test_failed_or_running_build_rejected(server):
    assert publish(server.client, 999999, "out/fw.bin").status_code == 404

    bid = submit(server.client, "failing", [step("bad", "echo no > out.bin; exit 3")])
    assert wait_build(server.client, bid)["status"] == "failed"
    r = publish(server.client, bid, "out.bin")
    assert r.status_code == 409
    assert "succeeded" in r.json()["detail"]


def test_path_traversal_rejected(server):
    bid = make_success_build(server, "x", name="traversal")
    for bad in ("/etc/passwd", "../outside", "out/../../etc/passwd", "./out/fw.bin",
                "out//fw.bin", "a\\b"):
        r = publish(server.client, bid, bad)
        assert r.status_code == 400, (bad, r.status_code, r.text)
    # 未创建任何记录，可用合法路径正常发布
    assert publish(server.client, bid, "out/fw.bin").status_code == 201


def test_symlink_and_non_regular_rejected(server):
    bid = submit(server.client, "kinds", [
        step("prep", "mkdir -p out && echo secret > out/real.bin "
                     "&& ln -s real.bin out/link.bin "
                     "&& ln -s out outdir && mkdir -p out/dir"),
    ])
    assert wait_build(server.client, bid)["status"] == "succeeded"

    for bad in ("out/link.bin", "outdir/real.bin", "out/dir"):
        r = publish(server.client, bid, bad)
        assert r.status_code == 400, (bad, r.status_code, r.text)
    assert publish(server.client, bid, "out/real.bin").status_code == 201


def test_missing_source_file(server):
    bid = make_success_build(server, "x", name="missing")
    r = publish(server.client, bid, "out/nope.bin")
    assert r.status_code == 404
    # 失败请求不占用首次发布名额
    assert publish(server.client, bid, "out/fw.bin").status_code == 201


def test_artifact_info_before_publish(server):
    bid = make_success_build(server, "x", name="not-yet")
    assert server.client.get(f"/builds/{bid}/artifact").status_code == 404
    assert server.client.get(f"/builds/{bid}/artifact/content").status_code == 404
    assert server.client.get("/builds/999999/artifact").status_code == 404


# --------------------------------------------------- 重启：清理与持久化

def test_restart_preserves_confirmed_artifact_and_cleans_orphans(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("artifact-restart")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = make_success_build(srv, "persisted\n", name="restart-artifact")
        art = publish(srv.client, bid, "out/fw.bin").json()
    finally:
        srv.stop()

    # 模拟上次进程在登记前中断：遗留临时文件与无记录的孤儿 blob
    tmp_dir = data_dir / "artifacts" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    (tmp_dir / "interrupted.part").write_bytes(b"half-written")
    orphan_dir = data_dir / "artifacts" / "blobs" / "999999"
    orphan_dir.mkdir(parents=True, exist_ok=True)
    orphan = orphan_dir / "orphan.bin"
    orphan.write_bytes(b"no-db-record")

    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        health = srv2.client.get("/health").json()
        assert health["artifact_cleanup"]["tmp_removed"] >= 1
        assert health["artifact_cleanup"]["blobs_removed"] >= 1
        assert not (tmp_dir / "interrupted.part").exists()
        assert not orphan.exists()

        info = srv2.client.get(f"/builds/{bid}/artifact")
        assert info.status_code == 200
        assert info.json()["sha256"] == art["sha256"]
        dl = srv2.client.get(f"/builds/{bid}/artifact/content")
        assert dl.content == b"persisted\n"
        assert hashlib.sha256(dl.content).hexdigest() == art["sha256"]

        # 重启后同路径重试仍然幂等
        again = publish(srv2.client, bid, "out/fw.bin")
        assert again.status_code == 200 and again.json()["replayed"] is True
    finally:
        srv2.stop()


def test_interrupted_unconfirmed_publish_can_be_retried(tmp_path_factory):
    """模拟登记前中断：没有记录、只留下临时文件与孤儿 blob。

    重启清理后同一请求可安全重试，且不会出现可下载的半成品。
    """
    data_dir = tmp_path_factory.mktemp("artifact-retry")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = make_success_build(srv, "retry-me\n", name="retry-after-crash")
    finally:
        srv.stop()

    # 该构建没有工件记录；伪造上次发布中断留下的中间产物
    tmp_dir = data_dir / "artifacts" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    (tmp_dir / "interrupted.part").write_bytes(b"half")
    orphan_dir = data_dir / "artifacts" / "blobs" / str(bid)
    orphan_dir.mkdir(parents=True, exist_ok=True)
    (orphan_dir / "orphan.bin").write_bytes(b"incomplete")

    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert srv2.client.get(f"/builds/{bid}/artifact").status_code == 404
        assert srv2.client.get(f"/builds/{bid}/artifact/content").status_code == 404
        retry = publish(srv2.client, bid, "out/fw.bin")
        assert retry.status_code == 201, retry.text
        assert retry.json()["sha256"] == hashlib.sha256(b"retry-me\n").hexdigest()
        dl = srv2.client.get(f"/builds/{bid}/artifact/content")
        assert dl.content == b"retry-me\n"
    finally:
        srv2.stop()


def test_missing_blob_is_not_served(tmp_path_factory):
    """记录指向缺失文件时不得返回内容，也不能伪造成功下载。"""
    data_dir = tmp_path_factory.mktemp("artifact-missing-blob")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = make_success_build(srv, "gone\n", name="gone")
        assert publish(srv.client, bid, "out/fw.bin").status_code == 201
        for blob in (data_dir / "artifacts" / "blobs").rglob("*.bin"):
            blob.unlink()
        r = srv.client.get(f"/builds/{bid}/artifact/content")
        assert r.status_code == 500
        assert "缺失" in r.json()["detail"]
    finally:
        srv.stop()

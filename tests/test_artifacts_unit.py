"""工件路径校验、快照与一致性相关的单元测试。"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from app.artifacts import (
    ArtifactError,
    open_source_file,
    parse_relative_path,
    snapshot_fd,
)
from app import config


@pytest.mark.parametrize(
    "raw",
    ["/etc/passwd", "../x", "a/../../b", "a//b", "./a", "a/./b", "", "a\\b", "a/"],
)
def test_parse_rejects_non_canonical_paths(raw):
    with pytest.raises(ArtifactError) as exc:
        parse_relative_path(raw)
    assert exc.value.status_code == 400


def test_parse_accepts_nested_relative_path():
    assert parse_relative_path("out/fw.bin") == ("out", "fw.bin")
    assert parse_relative_path("fw.bin") == ("fw.bin",)


def test_open_source_file_rejects_symlink(tmp_path):
    (tmp_path / "real.bin").write_text("data")
    (tmp_path / "link.bin").symlink_to(tmp_path / "real.bin")
    with pytest.raises(ArtifactError) as exc:
        open_source_file(tmp_path, ("link.bin",))
    assert exc.value.status_code == 400

    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "target").write_text("x")
    (tmp_path / "dirlink").symlink_to(tmp_path / "sub")
    with pytest.raises(ArtifactError) as exc:
        open_source_file(tmp_path, ("dirlink", "target"))
    assert exc.value.status_code == 400


def test_open_source_file_rejects_non_regular(tmp_path):
    (tmp_path / "dir").mkdir()
    with pytest.raises(ArtifactError) as exc:
        open_source_file(tmp_path, ("dir",))
    assert exc.value.status_code == 400

    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)
    with pytest.raises(ArtifactError) as exc:
        open_source_file(tmp_path, ("pipe",))
    assert exc.value.status_code == 400


def test_open_source_file_missing(tmp_path):
    with pytest.raises(ArtifactError) as exc:
        open_source_file(tmp_path, ("nope.bin",))
    assert exc.value.status_code == 404


def test_snapshot_is_content_addressed_and_source_independent(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(config, "ARTIFACT_ROOT", data / "artifacts")
    monkeypatch.setattr(config, "ARTIFACT_BLOB_ROOT", data / "artifacts" / "blobs")
    monkeypatch.setattr(config, "ARTIFACT_TMP_ROOT", data / "artifacts" / "tmp")
    config.ARTIFACT_BLOB_ROOT.mkdir(parents=True)
    config.ARTIFACT_TMP_ROOT.mkdir(parents=True)

    work = tmp_path / "work"
    work.mkdir()
    source = work / "fw.bin"
    source.write_bytes(b"firmware-v1" * 1000)
    expected = hashlib.sha256(source.read_bytes()).hexdigest()

    fd = open_source_file(work, ("fw.bin",))
    try:
        rel, size, sha = snapshot_fd(fd, 7)
    finally:
        os.close(fd)
    assert size == len(b"firmware-v1" * 1000)
    assert sha == expected
    blob = config.ARTIFACT_ROOT / rel
    assert blob.read_bytes() == b"firmware-v1" * 1000

    # 工作目录后来改动不影响已固化内容
    source.write_bytes(b"changed-later")
    assert blob.read_bytes() == b"firmware-v1" * 1000
    assert not list(config.ARTIFACT_TMP_ROOT.iterdir())

"""工件发布：把构建工作目录中的普通文件固化为不可变快照。

一致性设计（先落盘、后登记）：

1. 用 per-build 异步锁串行化同一构建的发布请求，数据库 ``UNIQUE(build_id)``
   作为多进程场景的最终约束。
2. 源文件通过 ``dirfd + O_NOFOLLOW`` 逐级打开，拒绝绝对路径、``..``、
   任意层级的符号链接以及目录/FIFO/设备等非普通文件；打开后以该 fd 为准
   复制，避免校验与复制之间被替换。
3. 内容写入临时文件，边写边算 SHA-256，``fsync`` 后原子 ``rename`` 到
   ``artifacts/blobs/<build_id>/<uuid>.bin`` 并 ``fsync`` 目录。此时工件
   已持久化，但还没有任何数据库记录，因此绝对不可下载。
4. 最后插入数据库记录，这一步是"确认"的提交点。记录一旦存在，其指向的
    blob 必然已经完整落盘。若在此前崩溃/中断，只留下临时文件或孤儿 blob，
   没有记录，不会暴露半成品；启动时清理，原请求可安全重试。
5. 下载前按记录中的 SHA-256 与大小重新校验 blob，校验通过后才从同一个
   文件描述符流式输出，避免把损坏内容当作已确认工件返回。

路径与 blob 全部位于 ``PIPELINE_DATA_DIR`` 下，随 compose 命名卷持久化，
重启后已确认工件仍可下载。
"""
from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import shutil
import sqlite3
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config
from .db import Database

_CHUNK_SIZE = 1024 * 1024


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ArtifactError(Exception):
    """带 HTTP 语义的工件错误，由路由层转换为 HTTPException。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def parse_relative_path(raw: str) -> tuple[str, ...]:
    """校验并拆分工作目录内的相对路径。

    只接受规范的 POSIX 相对路径：不允许绝对路径、反斜杠、空段、``.`` 或
    ``..`` 段。返回值用于逐级 no-follow 打开，存储时用 ``/`` 重新拼接。
    """
    if not raw:
        raise ArtifactError(400, "工件路径不能为空")
    if "\x00" in raw:
        raise ArtifactError(400, "工件路径包含 NUL 字符")
    if "\\" in raw:
        raise ArtifactError(400, "工件路径必须使用 POSIX 分隔符 '/'")
    if raw.startswith("/"):
        raise ArtifactError(400, "工件路径必须是工作目录内的相对路径")
    if len(raw) > 4096:
        raise ArtifactError(400, "工件路径过长")
    parts = tuple(raw.split("/"))
    if any(part in ("", ".", "..") for part in parts):
        raise ArtifactError(400, "工件路径不允许空段、'.' 或 '..'")
    return parts


def normalize_relative_path(parts: tuple[str, ...]) -> str:
    return "/".join(parts)


def open_source_file(workdir: Path, parts: tuple[str, ...]) -> int:
    """以 no-follow 方式打开工作目录内的普通文件，返回 O_RDONLY fd。

    逐级打开目录并在每一步拒绝符号链接；最终文件先 ``lstat`` 再以
    ``O_NOFOLLOW|O_NONBLOCK`` 打开（``O_NONBLOCK`` 防止竞态中被换成 FIFO
    而阻塞），最后 ``fstat`` 确认是普通文件。
    """
    try:
        dir_fd = os.open(
            workdir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
    except FileNotFoundError as exc:
        raise ArtifactError(404, "构建工作目录不存在") from exc
    except OSError as exc:
        # ELOOP：工作目录自身是符号链接
        raise ArtifactError(400, "工作目录或路径包含符号链接") from exc

    try:
        for name in parts[:-1]:
            try:
                info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
            except FileNotFoundError as exc:
                raise ArtifactError(404, "源文件不存在") from exc
            except NotADirectoryError as exc:
                raise ArtifactError(404, "源文件不存在") from exc
            if stat.S_ISLNK(info.st_mode):
                raise ArtifactError(400, "工件路径不允许符号链接")
            if not stat.S_ISDIR(info.st_mode):
                raise ArtifactError(400, "工件路径中的中间节点不是目录")
            try:
                next_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=dir_fd,
                )
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    raise ArtifactError(400, "工件路径不允许符号链接") from exc
                if isinstance(exc, FileNotFoundError):
                    raise ArtifactError(404, "源文件不存在") from exc
                raise ArtifactError(400, f"无法访问工件路径: {exc.strerror}") from exc
            os.close(dir_fd)
            dir_fd = next_fd

        name = parts[-1]
        try:
            info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise ArtifactError(404, "源文件不存在") from exc
        except NotADirectoryError as exc:
            raise ArtifactError(404, "源文件不存在") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ArtifactError(400, "工件路径不允许符号链接")
        if not stat.S_ISREG(info.st_mode):
            raise ArtifactError(400, "源路径不是普通文件")
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                dir_fd=dir_fd,
            )
        except OSError as exc:
            if exc.errno == errno.ELOOP:  # 竞态中变成符号链接
                raise ArtifactError(400, "工件路径不允许符号链接") from exc
            if isinstance(exc, FileNotFoundError):
                raise ArtifactError(404, "源文件不存在") from exc
            raise ArtifactError(400, f"无法打开源文件: {exc.strerror}") from exc
        # lstat 与 open 之间可能被替换：以实际打开的 fd 再确认一次
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            os.close(fd)
            raise ArtifactError(400, "源路径不是普通文件")
        return fd
    finally:
        os.close(dir_fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: memoryview) -> None:
    while data:
        written = os.write(fd, data)
        data = data[written:]


def snapshot_fd(src_fd: int, build_id: int) -> tuple[str, int, str]:
    """把已打开的源文件复制为不可变 blob，返回 (相对存储路径, 大小, SHA-256)。

    临时文件写完并 fsync 后才原子改名到 blob 路径并 fsync 目录。任何异常都会
    删除临时文件；改名后的异常也会尽力删除 blob，避免留下悬挂文件。
    """
    blob_rel = Path("blobs") / str(build_id) / f"{uuid.uuid4().hex}.bin"
    blob_abs = config.ARTIFACT_ROOT / blob_rel
    blob_abs.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = config.ARTIFACT_TMP_ROOT / f"{uuid.uuid4().hex}.part"
    config.ARTIFACT_TMP_ROOT.mkdir(parents=True, exist_ok=True)

    digest = hashlib.sha256()
    size = 0
    dst_fd = os.open(
        tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        while True:
            chunk = os.read(src_fd, _CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            _write_all(dst_fd, memoryview(chunk))
        os.fsync(dst_fd)
    except BaseException:
        os.close(dst_fd)
        tmp_path.unlink(missing_ok=True)
        raise
    os.close(dst_fd)

    replaced = False
    try:
        os.replace(tmp_path, blob_abs)
        replaced = True
        _fsync_dir(blob_abs.parent)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        if replaced:
            blob_abs.unlink(missing_ok=True)
        raise
    return str(blob_rel), size, digest.hexdigest()


def _hash_fd(fd: int) -> str:
    """从 fd 读取全部内容计算 SHA-256（使用 pread，不影响流式读取偏移）。"""
    digest = hashlib.sha256()
    offset = 0
    while True:
        chunk = os.pread(fd, _CHUNK_SIZE, offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)


def _content_type_for(name: str) -> str:
    """按扩展名固化简洁的 content-type；未知类型按二进制处理。"""
    import mimetypes

    guessed, _ = mimetypes.guess_type(name)
    if guessed and guessed.startswith(("application/", "text/", "image/")):
        return guessed
    return "application/octet-stream"


class ArtifactService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._locks: dict[int, asyncio.Lock] = {}

    # ------------------------------------------------------------- 启动清理

    async def startup(self) -> dict[str, int]:
        """清理上次进程中断遗留的临时文件与无记录孤儿 blob。

        清理发生在应用对外服务之前；所有已确认记录指向的 blob 不会被删除。
        """
        for path in (config.ARTIFACT_ROOT, config.ARTIFACT_TMP_ROOT,
                     config.ARTIFACT_BLOB_ROOT):
            path.mkdir(parents=True, exist_ok=True)

        tmp_removed = 0
        for entry in list(config.ARTIFACT_TMP_ROOT.iterdir()):
            try:
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
                tmp_removed += 1
            except OSError:
                pass

        referenced = set(await self.db.list_artifact_storage_paths())
        blobs_removed = 0
        for entry in list(config.ARTIFACT_BLOB_ROOT.rglob("*")):
            if entry.is_symlink():
                try:
                    entry.unlink()
                    blobs_removed += 1
                except OSError:
                    pass
                continue
            if not entry.is_file():
                continue
            try:
                rel = str(entry.relative_to(config.ARTIFACT_ROOT))
            except ValueError:
                continue
            if rel not in referenced:
                try:
                    entry.unlink()
                    blobs_removed += 1
                except OSError:
                    pass
        # 清掉空目录（保留 blobs 根目录本身）
        for directory in sorted(
            (p for p in config.ARTIFACT_BLOB_ROOT.rglob("*") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass
        return {"tmp_removed": tmp_removed, "blobs_removed": blobs_removed}

    # ------------------------------------------------------------- 发布

    def _lock(self, build_id: int) -> asyncio.Lock:
        lock = self._locks.get(build_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[build_id] = lock
        return lock

    async def publish(self, build_id: int, raw_path: str) -> tuple[dict[str, Any], bool]:
        """发布工件，返回 (记录, 是否本次新建)。"""
        build = await self.db.get_build(build_id)
        if build is None:
            raise ArtifactError(404, "构建不存在")
        if build["status"] != "succeeded":
            raise ArtifactError(
                409,
                f"构建当前状态为 {build['status']}，只有 succeeded 的构建才能发布工件",
            )
        parts = parse_relative_path(raw_path)
        normalized = normalize_relative_path(parts)

        async with self._lock(build_id):
            existing = await self.db.get_artifact(build_id)
            if existing is not None:
                return self._replay_or_conflict(existing, normalized)

            src_fd = open_source_file(config.WORKDIR_ROOT / str(build_id), parts)
            try:
                storage_path, size, sha256 = await asyncio.to_thread(
                    snapshot_fd, src_fd, build_id
                )
            except OSError as exc:
                raise ArtifactError(500, f"读取或保存源文件失败: {exc}") from exc
            finally:
                os.close(src_fd)

            try:
                await self.db.insert_artifact(
                    build_id,
                    normalized,
                    size,
                    sha256,
                    _content_type_for(parts[-1]),
                    storage_path,
                    _now(),
                )
            except sqlite3.IntegrityError:
                # 多进程/极端竞态下的兜底：唯一约束保证只有一个记录生效
                self._remove_blob(storage_path)
                existing = await self.db.get_artifact(build_id)
                if existing is None:
                    raise ArtifactError(500, "工件登记冲突且未找到已有记录")
                return self._replay_or_conflict(existing, normalized)
            except BaseException:
                self._remove_blob(storage_path)
                raise

            row = await self.db.get_artifact(build_id)
            if row is None:  # 理论上不会发生
                raise ArtifactError(500, "工件登记后记录缺失")
            return dict(row), True

    @staticmethod
    def _replay_or_conflict(existing: Any, normalized: str) -> tuple[dict[str, Any], bool]:
        if existing["source_path"] == normalized:
            return dict(existing), False
        raise ArtifactError(
            409,
            f"该构建已发布工件 {existing['source_path']}，不能更换为 {normalized}",
        )

    @staticmethod
    def _remove_blob(storage_path: str) -> None:
        try:
            (config.ARTIFACT_ROOT / storage_path).unlink(missing_ok=True)
        except OSError:
            pass

    # ------------------------------------------------------------- 查询/下载

    async def get(self, build_id: int) -> dict[str, Any]:
        await self._require_build(build_id)
        row = await self.db.get_artifact(build_id)
        if row is None:
            raise ArtifactError(404, "该构建尚未发布工件")
        return dict(row)

    async def open_verified(self, build_id: int) -> tuple[dict[str, Any], int]:
        """打开 blob 并在返回前校验大小与 SHA-256，返回 (记录, fd)。

        校验失败直接抛错，绝不把与摘要不一致的内容交给下载响应。
        """
        row = await self.get(build_id)
        rel = row["storage_path"]
        blob_abs = (config.ARTIFACT_ROOT / rel).resolve()
        root = config.ARTIFACT_ROOT.resolve()
        try:
            blob_abs.relative_to(root)
        except ValueError as exc:
            raise ArtifactError(500, "工件存储路径越界") from exc
        try:
            fd = os.open(blob_abs, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise ArtifactError(500, "已登记工件文件缺失") from exc
        except OSError as exc:
            raise ArtifactError(500, f"已登记工件无法打开: {exc.strerror}") from exc
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise ArtifactError(500, "已登记工件不是普通文件")
            if info.st_size != row["size"]:
                raise ArtifactError(500, "已登记工件大小与记录不一致")
            digest = await asyncio.to_thread(_hash_fd, fd)
            if digest != row["sha256"]:
                raise ArtifactError(500, "已登记工件 SHA-256 与记录不一致")
        except BaseException:
            os.close(fd)
            raise
        return row, fd

    async def _require_build(self, build_id: int) -> None:
        if await self.db.get_build(build_id) is None:
            raise ArtifactError(404, "构建不存在")

"""FastAPI 应用：路由、序列化与启动恢复。"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from . import __version__, config
from .artifacts import ArtifactError, ArtifactService
from .db import Database
from .models import (
    ArtifactOut,
    ArtifactPublishIn,
    BuildCreate,
    BuildOut,
    CancelOut,
    LogOut,
    StepOut,
)
from .scheduler import Scheduler

db = Database()
scheduler = Scheduler(db)
artifact_service = ArtifactService(db)


def _step_out(row) -> StepOut:
    return StepOut(
        id=row["step_uid"],
        command=row["command"],
        depends_on=json.loads(row["depends_on"]),
        timeout_seconds=row["timeout_seconds"],
        status=row["status"],
        exit_code=row["exit_code"],
        detail=row["detail"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


async def _build_out(build_id: int) -> BuildOut:
    row = await db.get_build(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    steps = [_step_out(r) for r in await db.get_steps(build_id)]
    return BuildOut(
        id=row["id"],
        name=row["name"],
        status=row["status"],
        cancel_requested=bool(row["cancel_requested"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        detail=row["detail"],
        steps=steps,
    )


def _safe_filename(name: str) -> str:
    """生成仅含安全 ASCII 的下载文件名，避免响应头注入或编码问题。"""
    cleaned = "".join(
        ch if ch.isascii() and (ch.isalnum() or ch in "._-") else "_" for ch in name
    ).strip("._") or "artifact.bin"
    return cleaned[:200]


def _artifact_out(row: dict, replayed: bool = False) -> ArtifactOut:
    return ArtifactOut(
        build_id=row["build_id"],
        path=row["source_path"],
        size=row["size"],
        sha256=row["sha256"],
        content_type=row["content_type"],
        created_at=row["created_at"],
        download_url=f"/builds/{row['build_id']}/artifact/content",
        replayed=replayed,
    )


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # 顺序不可颠倒：先落库连接，再清空旧构建残留并标记中断，最后才对外服务
    await db.connect()
    # 清理未确认的工件临时文件/孤儿 blob；已登记的工件不受影响
    app.state.artifact_cleanup = await artifact_service.startup()
    app.state.recovery = await scheduler.recover()
    try:
        yield
    finally:
        await db.close()


app = FastAPI(
    title="固件构建流水线 API",
    version=__version__,
    description=(
        "接收最多 12 步的 JSON DAG，在挂载工作目录中执行真实命令；"
        "全局最多两步并行，依赖全成功才启动，失败后代跳过，"
        "超时/取消终止整个进程组，支持逐步状态与日志查询。"
        "成功构建可将工作目录内的单个普通文件发布为不可变工件（固定内容与 SHA-256）。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "max_steps": config.MAX_STEPS,
        "max_parallel": config.MAX_PARALLEL,
        "recovery": getattr(app.state, "recovery", None),
        "artifact_cleanup": getattr(app.state, "artifact_cleanup", None),
    }


@app.post("/builds", response_model=BuildOut, status_code=201)
async def create_build(payload: BuildCreate) -> BuildOut:
    try:
        build_id = await scheduler.submit(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _build_out(build_id)


@app.get("/builds", response_model=list[BuildOut])
async def list_builds(limit: int = Query(100, ge=1, le=500)) -> list[BuildOut]:
    rows = await db.list_builds(limit)
    return [await _build_out(r["id"]) for r in rows]


@app.get("/builds/{build_id}", response_model=BuildOut)
async def get_build(build_id: int) -> BuildOut:
    return await _build_out(build_id)


@app.post("/builds/{build_id}/cancel", response_model=CancelOut)
async def cancel_build(build_id: int) -> CancelOut:
    row, message = await scheduler.cancel(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    # 重新读取以拿到 cancel_requested 的真实落库值（终态上的重复取消不会置位）
    fresh = await db.get_build(build_id)
    return CancelOut(
        id=row["id"],
        status=fresh["status"],
        cancel_requested=bool(fresh["cancel_requested"]),
        detail=message,
    )


@app.get("/builds/{build_id}/steps/{step_id}/logs", response_model=LogOut)
async def get_step_log(
    build_id: int,
    step_id: str,
    tail: int = Query(0, ge=0, description="只返回最后 N 行，0 表示返回全部（受字节上限约束）"),
) -> LogOut:
    step = await db.get_step(build_id, step_id)
    if step is None:
        raise HTTPException(status_code=404, detail="构建或步骤不存在")
    path = Path(step["log_path"])
    if not path.exists():
        return LogOut(build_id=build_id, step_id=step_id, status=step["status"],
                      truncated=False, content="")
    # 在线程池读文件，避免大日志阻塞事件循环
    raw = await asyncio.to_thread(path.read_bytes)
    truncated = False
    if len(raw) > config.LOG_MAX_BYTES:
        raw = raw[-config.LOG_MAX_BYTES:]
        truncated = True
    content = raw.decode("utf-8", errors="replace")
    if tail > 0:
        lines = content.splitlines()[-tail:]
        content = "\n".join(lines)
    return LogOut(
        build_id=build_id,
        step_id=step_id,
        status=step["status"],
        truncated=truncated,
        content=content,
    )


# --------------------------------------------------------------- 工件发布

@app.post(
    "/artifacts",
    response_model=ArtifactOut,
    status_code=201,
    responses={
        200: {
            "model": ArtifactOut,
            "description": "同一构建的同一路径重复发布，返回首次发布的原工件",
        }
    },
)
async def publish_artifact(payload: ArtifactPublishIn, response: Response) -> ArtifactOut:
    """发布成功构建工作目录内的单个普通文件。

    首次发布返回 201；同路径重试返回 200 与首次发布的内容/摘要；换路径返回 409。
    内容在登记前已复制并固化为不可变 blob，工作目录后续改动不影响下载。
    """
    try:
        row, created = await artifact_service.publish(payload.build_id, payload.path)
    except ArtifactError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    response.status_code = 201 if created else 200
    return _artifact_out(row, replayed=not created)


@app.get("/builds/{build_id}/artifact", response_model=ArtifactOut)
async def get_artifact(build_id: int) -> ArtifactOut:
    try:
        row = await artifact_service.get(build_id)
    except ArtifactError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    return _artifact_out(row)


@app.get(
    "/builds/{build_id}/artifact/content",
    response_class=StreamingResponse,
    responses={200: {"content": {"application/octet-stream": {}}}},
)
async def download_artifact(build_id: int) -> StreamingResponse:
    try:
        row, fd = await artifact_service.open_verified(build_id)
    except ArtifactError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    filename = _safe_filename(row["source_path"].rsplit("/", 1)[-1])

    async def _stream() -> AsyncIterator[bytes]:
        try:
            offset = 0
            while True:
                chunk = await asyncio.to_thread(os.pread, fd, 1024 * 1024, offset)
                if not chunk:
                    break
                offset += len(chunk)
                yield chunk
        finally:
            os.close(fd)

    headers = {
        "Content-Length": str(row["size"]),
        "Content-Disposition": f'attachment; filename="{filename}"',
        "ETag": f'"{row["sha256"]}"',
        "X-Artifact-SHA256": row["sha256"],
    }
    return StreamingResponse(
        _stream(), media_type=row["content_type"], headers=headers
    )

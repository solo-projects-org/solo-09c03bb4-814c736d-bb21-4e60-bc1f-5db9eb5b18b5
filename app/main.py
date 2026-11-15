"""FastAPI 入口：双端 FASTQ.gz 单进程接收服务。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

from .config import get_settings
from .models import FileRole
from .schemas import (
    BatchCreateRequest,
    BatchListResponse,
    BatchResponse,
    CancelResponse,
    QuarantineRequest,
    QuarantineResponse,
)
from .service import BatchService

logger = logging.getLogger("fastq_receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

settings = get_settings()
service = BatchService(settings)


class TrackedFileResponse(FileResponse):
    """带在途下载追踪的文件响应。

    无论正常发完、客户端断开还是传输异常，__call__ 退出时都会注销下载句柄，
    保证隔离接口等待的「在途下载」不会因为连接中断而永远不结束。
    已开始的响应不会被中途取消，文件完整发完（能发多少发多少，不主动截断）。
    可选分块节流（download_chunk_delay > 0）用于稳定复现在途下载场景。
    """

    def __init__(self, path, runtime, handle_id, on_finish, chunk_delay: float = 0.0,
                 chunk_size: int | None = None, **kwargs):
        super().__init__(path, **kwargs)
        self._runtime = runtime
        self._handle_id = handle_id
        self._on_finish = on_finish
        self._chunk_delay = chunk_delay
        if chunk_size is not None:
            self.chunk_size = chunk_size

    async def _handle_simple(self, send, send_header_only: bool) -> None:
        await send(
            {"type": "http.response.start", "status": self.status_code,
             "headers": self.raw_headers}
        )
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        if not self._chunk_delay:
            # 不节流时与父类原生实现完全一致
            await self._send_all(send)
            return
        async with await anyio.open_file(self.path, mode="rb") as file:
            more_body = True
            while more_body:
                chunk = await file.read(self.chunk_size)
                more_body = len(chunk) == self.chunk_size
                await send({"type": "http.response.body", "body": chunk, "more_body": more_body})
                if more_body:
                    await anyio.sleep(self._chunk_delay)

    async def _send_all(self, send) -> None:
        async with await anyio.open_file(self.path, mode="rb") as file:
            more_body = True
            while more_body:
                chunk = await file.read(self.chunk_size)
                more_body = len(chunk) == self.chunk_size
                await send({"type": "http.response.body", "body": chunk, "more_body": more_body})

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._on_finish(self._runtime, self._handle_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("data dir: %s, per-batch byte limit: %d", settings.data_dir, settings.batch_byte_limit)
    summary = await service.recover_on_startup()
    logger.info("startup recovery: %s", summary)
    yield


app = FastAPI(
    title="双端 FASTQ.gz 接收 API",
    description=(
        "测序双端数据按样本建批接收。R1、R2 各以请求体流式上传一次，"
        "单成员 gzip、严格四行制 FASTQ，两端条数与读段名逐对校验。"
        "坏包或校验失败整批拒收；单批文件字节上限接收过程中实时守限。"
        "ready 批可被隔离：提交原因后立即阻断新的 R1/R2 下载，"
        "等在途下载全部结束才确认隔离，原文件保留供追查，封锁持久化、重启不解除。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.post("/batches", response_model=BatchResponse, status_code=201, tags=["batches"],
          summary="按样本创建接收批次")
async def create_batch(payload: BatchCreateRequest):
    return await service.create_batch(payload.sample_id)


@app.get("/batches", response_model=BatchListResponse, tags=["batches"], summary="列出全部批次")
async def list_batches():
    return {"batches": await service.list_batches()}


@app.get("/batches/{batch_id}", response_model=BatchResponse, tags=["batches"], summary="查询批次状态")
async def get_batch(batch_id: str):
    return await service.get_batch_detail(batch_id)


@app.post(
    "/batches/{batch_id}/files/{role}",
    response_model=BatchResponse,
    tags=["upload"],
    summary="流式上传 R1 或 R2（请求体即 gzip 字节，不用 multipart）",
    responses={
        409: {"description": "批次非 receiving / 该端已上传 / 批次取消中"},
        413: {"description": "超过单批文件字节上限，本批已失败并清理"},
        422: {"description": "坏 gzip 或 FASTQ 校验失败，本批已拒收并清理"},
    },
)
async def upload_file(batch_id: str, role: FileRole, request: Request):
    return await service.upload(batch_id, role, request)


@app.post("/batches/{batch_id}/cancel", response_model=CancelResponse, tags=["batches"],
          summary="取消批次（等待写入停止、文件删除、空间释放后确认）")
async def cancel_batch(batch_id: str):
    return await service.cancel(batch_id)


@app.post(
    "/batches/{batch_id}/quarantine",
    response_model=QuarantineResponse,
    tags=["quarantine"],
    summary="隔离 ready 批：立即阻断新下载，等在途下载结束后确认（原文件保留）",
    responses={
        404: {"description": "批次不存在"},
        409: {"description": "批次非 ready（仅 ready 批可隔离）；隔离后重复调用返回既有结果"},
        422: {"description": "请求体缺少 reason 或长度不合法"},
    },
)
async def quarantine_batch(batch_id: str, payload: QuarantineRequest):
    return await service.quarantine(batch_id, payload.reason)


@app.get("/batches/{batch_id}/files/{role}/download", tags=["download"],
         summary="下载展开后的 R1 / R2 FASTQ（仅 ready 批次；隔离后立即 409）",
         responses={
             409: {"description": "批次非 ready 或已隔离（quarantined 批新下载一律阻断）"},
         })
async def download_file(batch_id: str, role: FileRole):
    path, filename, runtime, handle_id = await service.begin_download(batch_id, role)
    return TrackedFileResponse(
        path,
        runtime,
        handle_id,
        service.finish_download,
        chunk_delay=settings.download_chunk_delay,
        chunk_size=settings.download_chunk_size,
        media_type="text/plain",
        filename=filename,
    )


@app.get("/health", tags=["system"], summary="健康检查")
async def health():
    usage = await service.space.usage()
    return {
        "status": "ok",
        "batch_byte_limit": settings.batch_byte_limit,
        "active_batches": len(usage),
        "data_dir": str(settings.data_dir),
    }

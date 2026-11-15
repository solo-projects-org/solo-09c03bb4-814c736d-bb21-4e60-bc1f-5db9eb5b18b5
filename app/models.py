"""批次状态模型与运行期注册表。"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class BatchStatus(str, Enum):
    RECEIVING = "receiving"
    READY = "ready"
    FAILED = "failed"
    CANCELLED = "cancelled"
    QUARANTINED = "quarantined"


class FileRole(str, Enum):
    R1 = "R1"
    R2 = "R2"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class ActiveUpload:
    role: FileRole
    task: asyncio.Task


@dataclass
class DownloadHandle:
    """一次已开始的下载：finished 在响应流彻底结束（含客户端断开）时置位。"""

    id: str
    finished: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class BatchRuntime:
    """内存中的批次协调对象：取消事件、在途上传与在途下载。"""

    batch_id: str
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    uploads: dict[FileRole, asyncio.Task] = field(default_factory=dict)
    # 保护「下载准入检查 / 隔离状态转换 / 下载登记」临界区，同批串行
    download_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    downloads: dict[str, DownloadHandle] = field(default_factory=dict)

    def register_download(self) -> DownloadHandle:
        """登记一次即将开始的下载（调用方须持有 download_lock）。"""
        handle = DownloadHandle(id=uuid.uuid4().hex)
        self.downloads[handle.id] = handle
        return handle

    def finish_download(self, handle_id: str) -> None:
        """下载响应结束后注销句柄并唤醒等待者。"""
        handle = self.downloads.pop(handle_id, None)
        if handle is not None:
            handle.finished.set()

    async def wait_downloads(self) -> int:
        """等待本批全部在途下载结束，返回等待的下载数。"""
        pending = list(self.downloads.values())
        if pending:
            await asyncio.gather(*(handle.finished.wait() for handle in pending))
        return len(pending)

"""Pydantic 请求与响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .models import BatchStatus, FileRole


class BatchCreateRequest(BaseModel):
    sample_id: str = Field(min_length=1, max_length=128, description="样本编号")


class FileInfo(BaseModel):
    role: FileRole
    size: int
    uploaded_at: str


class BatchResponse(BaseModel):
    id: str
    sample_id: str
    status: BatchStatus
    byte_limit: int
    bytes_used: int
    error: str | None = None
    quarantine_reason: str | None = None
    quarantined_at: str | None = None
    files: list[FileInfo] = Field(default_factory=list)
    created_at: str
    updated_at: str


class BatchListResponse(BaseModel):
    batches: list[BatchResponse]


class CancelResponse(BaseModel):
    id: str
    status: BatchStatus
    freed_bytes: int
    message: str


class QuarantineRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=1000, description="隔离原因（疑似污染等）")


class QuarantineResponse(BaseModel):
    id: str
    status: BatchStatus
    reason: str = Field(description="本次/既有隔离原因")
    quarantined_at: str
    already_quarantined: bool = Field(description="是否为重复隔离（返回既有结果）")
    waited_downloads: int = Field(description="确认隔离前等待结束的在途下载数")
    files_retained: bool = Field(description="原文件是否保留供追查")
    message: str

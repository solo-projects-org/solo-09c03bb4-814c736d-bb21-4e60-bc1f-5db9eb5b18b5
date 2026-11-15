"""运行期可配置项，均通过环境变量覆盖。"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FASTQ_", env_file=None, extra="ignore")

    # 数据卷根目录：SQLite、压缩临时文件、展开后的 FASTQ 都在这下面
    data_dir: Path = Field(default=Path("/data"))

    # 单批可占用的文件字节上限（压缩临时文件 + 展开 FASTQ 合计），默认 64 MiB
    batch_byte_limit: int = Field(default=64 * 1024 * 1024, ge=1)

    # 上传过程中多久检查一次取消信号，避免客户端迟迟不发数据
    upload_idle_timeout: float = Field(default=15.0, gt=0)

    # 下载响应每发送一块后的让出间隔（秒），0 表示不节流；测试用于稳定制造「在途下载」
    download_chunk_delay: float = Field(default=0.0, ge=0)

    # 下载节流时的分块字节数
    download_chunk_size: int = Field(default=64 * 1024, ge=1)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def batch_dir(self) -> Path:
        return self.data_dir / "batches"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

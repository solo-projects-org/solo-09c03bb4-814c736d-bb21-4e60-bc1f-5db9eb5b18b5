"""SQLite 持久化层。

单进程应用，每次操作开一个连接，WAL 模式保证读写不互斥。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id               TEXT PRIMARY KEY,
    sample_id        TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('receiving','ready','failed','cancelled','quarantined')),
    byte_limit       INTEGER NOT NULL,
    error            TEXT,
    quarantine_reason TEXT,
    quarantined_at   TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('R1','R2')),
    path        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    UNIQUE(batch_id, role)
);
"""

_INIT_LOCK = threading.Lock()
_initialized: set[Path] = set()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """把旧版本库升级到当前 schema：补隔离列、放宽 status 的 CHECK 约束。

    SQLite 无法直接改 CHECK，需要在关闭外键的情况下整表重建。
    """
    info = conn.execute("PRAGMA table_info(batches)").fetchall()
    columns = {row["name"] for row in info}

    if "quarantine_reason" not in columns or "quarantined_at" not in columns:
        # 旧库：CHECK 只认四个状态，必须重建以接受 quarantined；顺带补齐新列
        conn.execute("PRAGMA foreign_keys=OFF")
        try:
            conn.execute(
                """
                CREATE TABLE batches_new (
                    id               TEXT PRIMARY KEY,
                    sample_id        TEXT NOT NULL,
                    status           TEXT NOT NULL CHECK (status IN ('receiving','ready','failed','cancelled','quarantined')),
                    byte_limit       INTEGER NOT NULL,
                    error            TEXT,
                    quarantine_reason TEXT,
                    quarantined_at   TEXT,
                    created_at       TEXT NOT NULL,
                    updated_at       TEXT NOT NULL
                )
                """
            )
            existing = [c for c in (
                "id", "sample_id", "status", "byte_limit", "error", "created_at", "updated_at"
            ) if c in columns]
            placeholders = ", ".join(existing)
            conn.execute(f"INSERT INTO batches_new ({placeholders}) SELECT {placeholders} FROM batches")
            conn.execute("DROP TABLE batches")
            conn.execute("ALTER TABLE batches_new RENAME TO batches")
            conn.commit()
        finally:
            conn.execute("PRAGMA foreign_keys=ON")


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _INIT_LOCK:
        if db_path in _initialized:
            return
        conn = _connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.commit()
            _migrate(conn)
        finally:
            conn.close()
        _initialized.add(db_path)


@contextmanager
def get_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

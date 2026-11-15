"""隔离（quarantine）功能测试：封锁、幂等、在途等待、重启持久、批间隔离与 schema 迁移。"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest
from fastapi import HTTPException

from app.config import Settings
from app.database import get_conn, init_db
from app.models import BatchStatus, FileRole
from app.service import BatchService
from app.storage import directory_size
from tests.helpers import create_batch as _create_batch
from tests.helpers import good_pair
from tests.helpers import gz_bytes as _gz
from tests.helpers import upload_bytes as _upload_bytes


def _make_ready(client, sample_id: str = "quar-sample") -> str:
    r1, r2 = good_pair(n=20, length=40)
    bid = _create_batch(client, sample_id)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    resp = _upload_bytes(client, bid, "R2", _gz(r2))
    assert resp.status_code == 200 and resp.json()["status"] == "ready"
    return bid


def test_quarantine_endpoint_in_openapi(client):
    spec = client.get("/openapi.json").json()
    assert "/batches/{batch_id}/quarantine" in spec["paths"]
    assert "post" in spec["paths"]["/batches/{batch_id}/quarantine"]


def test_quarantine_blocks_new_downloads_and_retains_files(client, settings):
    bid = _make_ready(client, "block-me")
    bdir = settings.batch_dir / bid
    expected = {p.name for p in bdir.iterdir()}
    assert expected == {"R1.fastq", "R2.fastq"}

    # 隔离前可正常下载
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200

    reason = "suspected contamination reported by sequencing operator"
    resp = client.post(f"/batches/{bid}/quarantine", json={"reason": reason})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "quarantined"
    assert body["reason"] == reason
    assert body["already_quarantined"] is False
    assert body["files_retained"] is True
    assert body["quarantined_at"]

    # 状态与隔离原因可查
    detail = client.get(f"/batches/{bid}").json()
    assert detail["status"] == "quarantined"
    assert detail["quarantine_reason"] == reason
    assert detail["quarantined_at"] == body["quarantined_at"]

    # 新的 R1/R2 下载立即被拒
    r1 = client.get(f"/batches/{bid}/files/R1/download")
    r2 = client.get(f"/batches/{bid}/files/R2/download")
    assert r1.status_code == 409 and "quarantin" in r1.json()["detail"]
    assert r2.status_code == 409

    # 原文件完整保留供追查
    assert {p.name for p in bdir.iterdir()} == expected
    assert (bdir / "R1.fastq").stat().st_size > 0

    # 隔离是终态：不能再上传 / 取消
    r1_gz = _gz(good_pair()[0])
    assert _upload_bytes(client, bid, "R1", r1_gz).status_code == 409
    assert client.post(f"/batches/{bid}/cancel").status_code == 409


def test_quarantine_is_idempotent_returns_existing_result(client):
    bid = _make_ready(client, "idem")
    first = client.post(f"/batches/{bid}/quarantine", json={"reason": "first reason"})
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["already_quarantined"] is False

    # 重复隔离：返回既有结果（原因、时间戳沿用第一次），不报错
    second = client.post(f"/batches/{bid}/quarantine", json={"reason": "second reason"})
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["already_quarantined"] is True
    assert second_body["reason"] == "first reason"
    assert second_body["quarantined_at"] == first_body["quarantined_at"]
    assert second_body["status"] == "quarantined"


def test_only_ready_batch_can_be_quarantined(client):
    # 不存在
    assert client.post("/batches/nope/quarantine", json={"reason": "x"}).status_code == 404

    # receiving 批不可隔离
    receiving = _create_batch(client, "not-ready")
    resp = client.post(f"/batches/{receiving}/quarantine", json={"reason": "x"})
    assert resp.status_code == 409

    # failed 批不可隔离
    r1, r2 = good_pair()
    r2[1] = (b"read-WRONG/2", r2[1][1], r2[1][2])
    failed = _create_batch(client, "failed-q")
    _upload_bytes(client, failed, "R1", _gz(r1))
    assert _upload_bytes(client, failed, "R2", _gz(r2)).status_code == 422
    assert client.post(f"/batches/{failed}/quarantine", json={"reason": "x"}).status_code == 409

    # cancelled 批不可隔离
    cancelled = _create_batch(client, "cancelled-q")
    assert client.post(f"/batches/{cancelled}/cancel").status_code == 200
    assert client.post(f"/batches/{cancelled}/quarantine", json={"reason": "x"}).status_code == 409


def test_quarantine_requires_reason(client):
    bid = _make_ready(client, "need-reason")
    assert client.post(f"/batches/{bid}/quarantine", json={}).status_code == 422
    assert client.post(f"/batches/{bid}/quarantine", json={"reason": ""}).status_code == 422
    # 未成功隔离，下载仍然可用
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200


def test_quarantine_one_batch_does_not_affect_others(client, settings):
    blocked = _make_ready(client, "blocked-batch")
    healthy = _make_ready(client, "healthy-batch")

    assert client.post(f"/batches/{blocked}/quarantine",
                       json={"reason": "contamination"}).status_code == 200

    # 被隔离批下载被拒
    assert client.get(f"/batches/{blocked}/files/R1/download").status_code == 409
    # 其他批照常接收（新批）与下载
    assert client.get(f"/batches/{healthy}/files/R2/download").status_code == 200

    another = _make_ready(client, "after-quarantine")
    assert client.get(f"/batches/{another}/files/R1/download").status_code == 200


def test_completed_download_unregisters_handle(client):
    from app.main import service

    bid = _make_ready(client, "handle-cleanup")
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200
    runtime = asyncio.run(service.registry.get(bid))
    # 响应完整结束后在途句柄必须清空，隔离不会被永久挂住
    assert runtime is not None and runtime.downloads == {}


def test_quarantine_waits_for_in_flight_download_without_cutting_it(client, settings):
    """隔离确认必须等在途下载结束；等待期间新下载已被拒。"""
    bid = _make_ready(client, "inflight")
    # 独立 service 实例：所有 asyncio 原语在本测试自己的事件循环里首次绑定
    svc = BatchService(Settings())

    async def scenario():
        # 模拟一个已经开始、尚未结束的 R1 下载
        path, filename, runtime, handle_id = await svc.begin_download(bid, FileRole.R1)
        assert path.exists() and filename.endswith("R1.fastq")
        assert set(runtime.downloads) == {handle_id}

        quarantine_task = asyncio.ensure_future(svc.quarantine(bid, "pollution"))
        # 让隔离流程跑到「封锁已落库、正在等下载」
        await asyncio.sleep(0.05)
        assert not quarantine_task.done(), "quarantine must not confirm while download is in flight"

        # 封锁已经生效：新的 R1/R2 下载立即 409
        with pytest.raises(HTTPException) as exc:
            await svc.begin_download(bid, FileRole.R1)
        assert exc.value.status_code == 409
        with pytest.raises(HTTPException) as exc:
            await svc.begin_download(bid, FileRole.R2)
        assert exc.value.status_code == 409

        # 在途下载正常结束（不被截断），随后隔离确认
        await svc.finish_download(runtime, handle_id)
        result = await asyncio.wait_for(quarantine_task, timeout=5)
        assert result["status"] == BatchStatus.QUARANTINED
        assert result["waited_downloads"] == 1
        assert runtime.downloads == {}

    asyncio.run(scenario())

    # 数据库里封锁已落库
    with get_conn(settings.db_path) as conn:
        row = conn.execute("SELECT status FROM batches WHERE id = ?", (bid,)).fetchone()
    assert row["status"] == "quarantined"
    # 文件仍在
    assert directory_size(settings.batch_dir / bid) > 0


def test_quarantine_survives_restart_and_keeps_quota_from_disk(client, settings):
    blocked = _make_ready(client, "restart-blocked")
    healthy = _make_ready(client, "restart-healthy")
    assert client.post(f"/batches/{blocked}/quarantine",
                       json={"reason": "restart proof"}).status_code == 200

    blocked_size = directory_size(settings.batch_dir / blocked)
    healthy_size = directory_size(settings.batch_dir / healthy)

    # 模拟重启：同数据卷新建 service 并执行启动恢复
    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary["quarantined"] >= 1
    assert summary["ready"] >= 1

    with get_conn(settings.db_path) as conn:
        row = conn.execute("SELECT status, quarantine_reason FROM batches WHERE id = ?",
                           (blocked,)).fetchone()
    assert row["status"] == "quarantined"
    assert row["quarantine_reason"] == "restart proof"

    # 文件保留，配额按磁盘真实占用恢复
    assert directory_size(settings.batch_dir / blocked) == blocked_size
    usage = asyncio.run(new_service.space.usage())
    assert usage[blocked] == blocked_size
    assert usage[healthy] == healthy_size

    # 重启后隔离依旧生效：下载被拒；其他批照常下载
    async def check():
        with pytest.raises(HTTPException) as exc:
            await new_service.begin_download(blocked, FileRole.R1)
        assert exc.value.status_code == 409
        path, _name, _rt, _hid = await new_service.begin_download(healthy, FileRole.R1)
        assert path.exists()

    asyncio.run(check())


def test_old_schema_db_migrates_and_keeps_data(tmp_path):
    """旧版本库（四状态 CHECK、无隔离列）启动时在线迁移，数据不丢且可写 quarantined。"""
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE batches ("
        "id TEXT PRIMARY KEY, sample_id TEXT NOT NULL, status TEXT NOT NULL"
        " CHECK (status IN ('receiving','ready','failed','cancelled')),"
        " byte_limit INTEGER NOT NULL, error TEXT, created_at TEXT NOT NULL,"
        " updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO batches VALUES ('b1','S-OLD','ready',1024,NULL,'t0','t1')"
    )
    conn.commit()
    conn.close()

    init_db(db_path)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)")}
    assert {"quarantine_reason", "quarantined_at"}.issubset(cols)
    row = conn.execute("SELECT * FROM batches WHERE id = 'b1'").fetchone()
    assert row["sample_id"] == "S-OLD" and row["status"] == "ready"

    # 新状态可写入（旧 CHECK 已替换）
    conn.execute("UPDATE batches SET status = 'quarantined' WHERE id = 'b1'")
    conn.commit()
    assert conn.execute("SELECT status FROM batches WHERE id='b1'").fetchone()[0] == "quarantined"
    conn.close()

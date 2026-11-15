# 双端 FASTQ.gz 接收 API

测序数据员把仪器导出的双端 `FASTQ.gz` 交给比对流水线前的单进程接收服务：按样本建批，
R1、R2 各以请求体**流式上传一次**，实时守字节上限、逐对校验读段，错配样本一律不放行。

- 框架：FastAPI + Uvicorn（单进程、单事件循环）
- 存储：SQLite（WAL 模式）+ 数据卷文件
- 交互文档：启动后访问 `http://localhost:8000/docs`（Swagger UI）与 `/openapi.json`

## 1. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

数据保存在命名卷 `fastq-data`（容器内 `/data`），SQLite、压缩临时文件、展开后的 FASTQ 都在其中。
单批字节上限通过环境变量 `FASTQ_BATCH_BYTE_LIMIT` 配置（默认 64 MiB）。

### 本地直接运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
FASTQ_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 配置项（均为环境变量，前缀 `FASTQ_`）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FASTQ_DATA_DIR` | `/data` | 数据卷根目录（SQLite + 批次文件） |
| `FASTQ_BATCH_BYTE_LIMIT` | `67108864`（64 MiB） | 单批文件字节上限，压缩临时文件与展开 FASTQ 合计 |
| `FASTQ_UPLOAD_IDLE_TIMEOUT` | `15`（秒） | 上传请求体两个分块之间的最大静默时间 |
| `FASTQ_DOWNLOAD_CHUNK_DELAY` | `0`（秒） | 下载响应每个分块后的节流间隔，0 表示不节流（仅测试复现在途下载用） |
| `FASTQ_DOWNLOAD_CHUNK_SIZE` | `65536` | 下载节流时的分块字节数 |

## 2. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 按 `sample_id` 创建批次，返回批次 ID |
| `GET` | `/batches` | 列出全部批次 |
| `GET` | `/batches/{id}` | 查询批次状态、已传文件与实时占用字节 |
| `POST` | `/batches/{id}/files/R1` | 流式上传 R1（请求体即 gzip 字节，非 multipart） |
| `POST` | `/batches/{id}/files/R2` | 流式上传 R2 |
| `POST` | `/batches/{id}/cancel` | 取消批次，等待写入停止、文件删除、空间释放后确认 |
| `POST` | `/batches/{id}/quarantine` | 隔离 ready 批：立即阻断新 R1/R2 下载，等在途下载结束后确认（原文件保留） |
| `GET` | `/batches/{id}/files/R1/download` | 下载展开后的 R1（仅 `ready` 批；隔离后立即 409） |
| `GET` | `/batches/{id}/files/R2/download` | 下载展开后的 R2（仅 `ready` 批；隔离后立即 409） |
| `GET` | `/health` | 健康检查 |

批次状态机：`receiving` →（两端齐全且配对通过）→ `ready`；任何校验失败 → `failed`；
操作员取消 → `cancelled`；操作员对 `ready` 批提交隔离原因 → `quarantined`。
`failed` / `cancelled` / `quarantined` 均为终态，不可再上传；其中 `quarantined` 不可下载。

## 3. 校验规则（任一不过，整批拒收并清理）

1. **单成员 gzip**：流式解压，gzip 结束标记后不允许再带数据（多成员归档拒绝）；截断/损坏拒绝。
2. **严格四行制 FASTQ**：`@读段名` / 序列 / `+` / 质量串，序列与质量串等长，文件以换行结尾。
3. **两端配对**：R1、R2 条数相同；逐对读段名取首个空白前的 token、去掉末尾 `/1`、`/2` 后必须一致。
4. 只有两端全部校验并保存成功，批次才置为 `ready` 并允许下载展开后的 FASTQ。

## 4. 字节上限与并发语义

- 接收过程中，**压缩临时文件与展开结果的每一次写入都先向配额器预留字节**，超过
  `FASTQ_BATCH_BYTE_LIMIT` 立即失败（HTTP 413），不会等整包落盘后才检查；解压炸弹同理在途中被拦。
- 配额按批记账。某批超限只清掉该批，其他批继续接收；慢上传占用的是协程而非线程，不挡其他批。
- 取消接口会先置取消信号、等待本批在途上传协程退出（此后不会再落盘），再删除全部文件、
  按磁盘真实占用回收空间、把状态落库为 `cancelled`，最后才返回确认。

## 5. 污染隔离（ready 批）

测序数据员发现已放行样本疑似污染时，对该 `ready` 批调用隔离接口并提交原因：

```bash
curl -X POST localhost:8000/batches/$BID/quarantine \
  -H 'Content-Type: application/json' \
  -d '{"reason":"suspected contamination: foreign reads detected"}'
```

语义保证：

1. **立即阻断新下载**：状态先在数据库落库为 `quarantined` 并与下载准入共用同一把批级锁。
   封锁生效后，新的 R1/R2 下载请求一律 `409`，不会再放行任何一个新下载。
2. **不截断在途传输**：隔离只等待、不取消已经开始的下载。接口在该批所有在途下载的响应
   完整结束（正常发完或客户端断开）后才返回确认，已发出的传输不会被截成半份。
3. **原文件保留供追查**：隔离不删除任何文件，R1/R2 展开 FASTQ 原样保留，配额按磁盘真实
   占用记账，可随时回到数据卷核查。
4. **幂等**：对已隔离批重复提交，返回既有隔离结果（沿用首次原因与时间戳，
   `already_quarantined=true`），不报错、不覆盖原因。
5. **持久且抗重启**：封锁状态先于等待落库。即使隔离请求在等待期间断开、或服务重启，
   批次也不会被重新放行；重启后仍为 `quarantined`，下载继续被拒。
6. **只影响本批**：其他批次的接收与下载完全不受影响，隔离期间它们照常工作。

仅 `ready` 批可隔离；`receiving` / `failed` / `cancelled` 批调用返回 `409`，不存在的批次 `404`。
隔离响应字段：

| 字段 | 说明 |
| --- | --- |
| `status` | 固定 `quarantined` |
| `reason` | 本次（或既有）隔离原因 |
| `quarantined_at` | 首次隔离落库时间 |
| `already_quarantined` | 是否为重复隔离 |
| `waited_downloads` | 确认前等待结束的在途下载数 |
| `files_retained` | 原文件是否保留 |

该接口在 Swagger UI（`/docs`）的 **quarantine** 分组下可直接发起。

## 6. 重启恢复

启动时扫描数据卷：

- `ready` 批：保留展开文件，按磁盘真实字节重建配额账，可继续下载；
- `quarantined` 批：**封锁不因重启解除**——保留展开文件供追查、按磁盘真实字节重建配额账，
  状态保持 `quarantined`，下载接口继续返回 `409`；
- `receiving` 批（未完成）：判为 `failed`，删除残留文件并回收空间后再对外服务；
- `failed` / `cancelled` 批：清理任何残留；磁盘孤儿目录直接回收。

旧版本数据库（四状态 `CHECK`、无隔离列）首次启动时自动在线迁移，已有批次数据不丢。

## 7. 样例与演示

生成正常、错配、超限、损坏四类样例：

```bash
python scripts/make_samples.py samples
```

对已启动的服务跑端到端演示（正常放行 / 错配拒收 / 超限 413）：

```bash
BASE_URL=http://127.0.0.1:8000 SAMPLES_DIR=samples bash scripts/demo.sh
```

典型手工调用：

```bash
BID=$(curl -s -X POST localhost:8000/batches -H 'Content-Type: application/json' \
  -d '{"sample_id":"S-2026-001"}' | python -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -X POST localhost:8000/batches/$BID/files/R1 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R1.fastq.gz
curl -X POST localhost:8000/batches/$BID/files/R2 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R2.fastq.gz
curl localhost:8000/batches/$BID
curl -OJ localhost:8000/batches/$BID/files/R1/download

# 发现疑似污染后隔离该批（Swagger /docs 的 quarantine 分组也可发起）
curl -X POST localhost:8000/batches/$BID/quarantine \
  -H 'Content-Type: application/json' \
  -d '{"reason":"suspected contamination"}'
# 隔离后新下载立即 409，原文件仍保留在数据卷
curl -i localhost:8000/batches/$BID/files/R1/download
```

## 8. 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

- `tests/test_api.py` / `tests/test_lifecycle.py`：ASGI 级单元测试（正常、错配、条数不一致、
  序列质量不等长、坏 gzip、多成员 gzip、413 超限、取消中断、配额双计、重启恢复等）。
- `tests/test_quarantine.py`：隔离封锁、幂等、非 ready 拒绝、在途下载等待不截断、
  重启后封锁持久与配额按磁盘恢复、批间互不影响、旧库 schema 在线迁移。
- `tests/test_server_integration.py`：拉起真实 uvicorn 进程，验证慢上传不挡其他批、
  取消「等待停止→删文件→释放空间」、隔离「先封锁→等在途下载完整结束→确认」与
  重启后隔离不解除的端到端语义，以及正常/错配/超限全链路。

## 9. 目录结构

```
app/
  config.py       环境变量配置
  database.py     SQLite 连接、建表（WAL）与旧库隔离字段在线迁移
  models.py       状态枚举、运行期批次模型与在途下载句柄
  storage.py      按批配额器 SpaceManager、磁盘回收、批次注册表
  validation.py   流式 gunzip、四行制 FASTQ 结构与配对校验
  schemas.py      请求/响应模型（含隔离请求与响应）
  service.py      批次生命周期：创建、上传、取消、下载准入、隔离、重启恢复
  main.py         FastAPI 入口与在途下载追踪响应 TrackedFileResponse
scripts/          样例生成与演示脚本
tests/            单元测试与真实服务器集成测试
docker-compose.yml / Dockerfile
```

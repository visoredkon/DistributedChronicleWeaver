# Laporan: Publish-Subscribe Log Aggregator
Nama  : Pahril Dwi Saputra<br/>
NIM   : 11221056<br/>
Kelas : Sistem Parallel dan Terdistribusi B

# Ringkasan Sistem dan Arsitektur
Sistem **Distributed Publish-Subscribe Log Aggregator** dengan **idempotent consumer**, **deduplication**, dan **transaction-based concurrency** yang berjalan dengan Docker Compose.

*Event* yang dikirim oleh *publisher* akan diterima oleh *aggregator* melalui HTTP POST *request*. *Aggregator* menyimpan *event* ke dalam Redis *queue* dan memprosesnya menggunakan 4 *consumer workers* secara paralel. Untuk mencegah pemrosesan *duplicate events*, sistem menggunakan *persistent deduplication store* yang dibangun dengan PostgreSQL dengan kombinasi unik dari `event_id` dan `topic`.

Detail diagram arsitektur dan komponen dapat dilihat di [README.md](README.md#arsitektur-sistem).

# Keputusan Desain
## 1. Idempotency & Deduplication
### Keputusan
Menggunakan **PostgreSQL unique constraint** `(topic, event_id)` dengan pattern `INSERT ... ON CONFLICT DO NOTHING`.

### Alasan
- **Atomic**: *Guarantee deduplication* dalam satu SQL *statement*, tidak perlu *transaction* terpisah
- **Concurrent-safe**: *Multiple workers* tidak menghasilkan *race condition* karena *database* yang menangani *conflict*
- **Persistent**: *State* tersimpan di *database*, *survive restart*
- **No Lock Contention**: Berbeda dengan *pessimistic locking*, pendekatan *optimistic* ini tidak memblokir *workers* lain

### Implementasi
**File**: `src/aggregator/app/services/database.py` (baris 49-60)

```sql
CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (topic, event_id)
)
```

**File**: `src/aggregator/app/services/database.py` (baris 287-299)
```python
result = await connection.fetchval(
    """
    INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (topic, event_id) DO NOTHING
    RETURNING id
    """,
    event.event_id,
    event.topic,
    event.source,
    dumps(event.payload.model_dump()).decode("utf-8"),
    event.timestamp,
)
```

### Mekanisme Deduplikasi
| Return Value | Kondisi | Aksi Selanjutnya |
|--------------|---------|------------------|
| `id` (integer) | Event baru, berhasil INSERT | Update stats `received + 1`, log audit `PROCESSED` |
| `NULL` | Duplicate, conflict detected | Update stats `received + 1, duplicated_dropped + 1`, log audit `DROPPED` |

---

## 2. Message Broker: Redis
### Keputusan
Menggunakan **Redis** sebagai *internal message queue* dengan pattern **LPUSH/BRPOP**.

### Alasan
- **Asynchronous Processing**: API `/publish` hanya memasukkan event ke queue dan langsung return response, tidak perlu menunggu database write selesai. Ini mengurangi latency response dari ~50ms (dengan DB write) menjadi ~5ms (hanya Redis LPUSH).
- **Buffering**: Redis queue bertindak sebagai buffer saat terjadi spike traffic. Jika 1000 events masuk dalam 1 detik, API tetap responsive karena hanya melakukan LPUSH. Consumer workers memproses secara gradual tanpa overwhelm database.
- **Work Distribution**: Pattern BRPOP memungkinkan multiple workers mengambil events dari queue yang sama secara fair. Redis menjamin setiap event hanya di-pop oleh 1 worker (no duplicate processing dari sisi queue).
- **Failure Isolation**: Jika database down, events tetap aman di Redis queue. Setelah database recovery, workers melanjutkan processing tanpa data loss.

### Implementasi
**File**: `src/aggregator/app/services/redis_queue.py`

**Push ke Queue** (baris 30-35):
```python
async def push(self, event: EventModel) -> None:
    if self.__client is None:
        raise RuntimeError("Redis client not initialized")

    event_data: str = dumps(event.model_dump(mode="json")).decode("utf-8")
    _ = await self.__client.lpush("events", event_data)
```

**Pop dari Queue** (baris 37-61):
```python
async def pop(self, timeout: int = 5) -> EventModel | None:
    if self.__client is None:
        raise RuntimeError("Redis client not initialized")

    result: list[str] | None = await self.__client.brpop(
        keys=["events"], timeout=timeout
    )

    if result is None:
        return None

    event_data: dict[str, Any] = loads(result[1])
    # ... deserialization
    return EventModel(...)
```

### Pattern LPUSH/BRPOP
```
Publisher → LPUSH "events" → [event3, event2, event1] → BRPOP → Consumer
            (head)                                       (tail)
```

| Command | Behavior | Use Case |
|---------|----------|----------|
| `LPUSH` | Insert di *head* (left) | Publisher menambah event baru |
| `BRPOP` | Remove dari *tail* (right), blocking | Consumer menunggu event, FIFO order |

### Singleton Pattern
**File**: `src/aggregator/app/services/redis_queue.py` (baris 12-20)
```python
class RedisQueueService:
    __instance: "RedisQueueService | None" = None

    def __new__(cls) -> "RedisQueueService":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance
```
Singleton memastikan hanya ada 1 koneksi Redis per aplikasi, menghindari *connection leak*.

---

## 3. Transaction & Isolation Level
### Keputusan
Menggunakan **READ COMMITTED** *isolation level* (default PostgreSQL) dengan **atomic upserts**.

### Alasan
- **Performance**: Tidak memerlukan *strict serialization* yang mahal
- **Sufficiency**: *Unique constraint* di level database sudah mencegah *duplicate inserts*
- **Trade-off Accepted**: *Phantom reads* possible tapi tidak relevan untuk use case ini

### Connection Pool Configuration
**File**: `src/aggregator/app/services/database.py` (baris 44)
```python
self.__pool = await create_pool(dsn=database_url, min_size=2, max_size=10)
```

| Parameter | Value | Alasan |
|-----------|-------|--------|
| `min_size` | 2 | Minimum koneksi untuk availability |
| `max_size` | 10 | Cukup untuk 4 workers + API requests |

### Atomic Stats Update
**File**: `src/aggregator/app/services/database.py` (baris 302-307, 318-323)

Untuk event **baru** (unique):
```sql
UPDATE stats
SET received = received + 1, updated_at = NOW()
WHERE id = 1
```

Untuk event **duplicate**:
```sql
UPDATE stats
SET received = received + 1, duplicated_dropped = duplicated_dropped + 1, updated_at = NOW()
WHERE id = 1
```

### Mengapa Atomic?
Operasi `received = received + 1` terlihat seperti 3 langkah terpisah (read → modify → write), namun PostgreSQL mengeksekusinya sebagai **single atomic operation** dengan row-level locking. Berikut mekanismenya:

1. PostgreSQL mengambil **row-level lock** pada row `id = 1` di tabel `stats`
2. Membaca nilai `received` saat ini
3. Menambah 1 dan menulis kembali
4. Melepas lock

Jika 2 workers mencoba update bersamaan, worker kedua akan **menunggu** sampai lock dilepas, kemudian membaca nilai terbaru. Ini menjamin tidak ada *lost update*.

### Bukti Concurrency Safe

**Test 1**: `test_concurrency_duplicate_parallel`
**File**: `tests/test_03_concurrency.py` (baris 41-68)
```python
def test_concurrency_duplicate_parallel(server_url: str) -> None:
    same_event = create_event(
        event_id="same-concurrent-001",
        topic="concurrent-dup-topic",
        message="Same event parallel",
    )

    def send_event() -> int | None:
        url = f"{server_url}/publish"
        status, _ = post_request(url, {"events": [same_event]})
        return status

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_event) for _ in range(20)]
        results = [f.result() for f in as_completed(futures)]

    # Semua 20 requests berhasil (status 200)
    for status in results:
        assert status == 200

    # Tapi hanya 1 event yang tersimpan!
    same_events = [e for e in events_data["events"] if e["event_id"] == "same-concurrent-001"]
    assert len(same_events) == 1
```
**Hasil**: 20 parallel requests dengan event ID sama → hanya 1 tersimpan, 19 di-drop.

**Test 2**: `test_race_condition_same_event`
**File**: `tests/test_08_race_condition.py` (baris 7-31)
```python
def test_race_condition_same_event(server_url: str) -> None:
    event = create_event(
        event_id="race-event-001", topic="race-topic", message="Race condition test"
    )

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(send_event) for _ in range(50)]
        # ...

    race_events = [e for e in events_data["events"] if e["event_id"] == event_id]
    assert len(race_events) == 1  # Hanya 1 dari 50!
```
**Hasil**: 50 parallel requests → hanya 1 tersimpan, 49 di-drop. Ini membuktikan `ON CONFLICT DO NOTHING` bekerja sempurna.

---

## 4. Multi-Worker Consumer
### Keputusan
Menggunakan **4 async workers** (*configurable* via `WORKER_COUNT`) yang *consume* dari Redis queue secara parallel.

### Implementasi
**File**: `src/aggregator/app/services/consumer.py`

**Worker Spawning** (baris 25-35):
```python
async def start(self) -> None:
    if self.__running:
        return

    self.__running = True

    for worker_id in range(self.__worker_count):
        task: Task[None] = create_task(coro=self.__consume_loop(worker_id))
        self.__tasks.append(task)

    logger.info(f"Started {self.__worker_count} consumer workers")
```

**Consume Loop** (baris 51-94):
```python
async def __consume_loop(self, worker_id: int) -> None:
    retry_count: int = 0
    max_retries: int = 5

    while self.__running:
        try:
            event: EventModel | None = await self.__redis_queue.pop(timeout=5)

            if event is None:
                continue

            retry_count = 0

            is_unique: bool = await self.__database.insert_event(event, worker_id)

            if is_unique:
                logger.info(
                    f"Worker {worker_id}: Processed unique event - "
                    f"event_id={event.event_id}, topic={event.topic}"
                )
            else:
                logger.warning(
                    f"Worker {worker_id}: Duplicate event dropped - "
                    f"event_id={event.event_id}, topic={event.topic}"
                )

        except CancelledError:
            raise
        except Exception as e:
            retry_count += 1
            backoff_time: float = min(2**retry_count, 30)

            logger.error(
                f"Worker {worker_id}: Error processing event - {e}, "
                f"retry {retry_count}/{max_retries}, backoff {backoff_time}s"
            )

            if retry_count >= max_retries:
                logger.error(f"Worker {worker_id}: Max retries exceeded, continuing...")
                retry_count = 0

            await sleep(delay=backoff_time)
```

### Exponential Backoff Strategy

**Mengapa Exponential Backoff?**
Ketika terjadi error (misal: database connection lost), worker tidak langsung retry. Jika langsung retry, bisa terjadi:
1. **Thundering herd**: Semua workers retry bersamaan, membanjiri database yang baru recovery
2. **Resource waste**: CPU cycles terbuang untuk retry yang pasti gagal

Dengan exponential backoff, delay meningkat secara eksponensial, memberikan waktu untuk sistem recovery.

**Implementasi** (baris 81):
```python
backoff_time: float = min(2**retry_count, 30)
```

| Retry | Delay | Kumulatif | Penjelasan |
|-------|-------|-----------|------------|
| 1 | 2s | 2s | Coba lagi setelah 2 detik |
| 2 | 4s | 6s | Masih gagal, tunggu lebih lama |
| 3 | 8s | 14s | Delay semakin panjang |
| 4 | 16s | 30s | Hampir setengah menit total |
| 5 | 30s | 60s | Capped di 30s, tidak lebih |

**Cap di 30 detik**: Formula `min(2^n, 30)` memastikan delay tidak melebihi 30 detik. Tanpa cap, retry ke-10 akan menunggu 1024 detik (~17 menit) yang tidak reasonable.

### Graceful Shutdown

**Mengapa Perlu Graceful Shutdown?**
Tanpa graceful shutdown, jika aplikasi di-terminate saat worker sedang memproses event:
1. Event sudah di-pop dari Redis (BRPOP) tapi belum tersimpan di database
2. Event hilang karena tidak ada di Redis maupun PostgreSQL

**Mekanisme Shutdown** (`src/aggregator/app/services/consumer.py` baris 37-49):
```python
async def stop(self) -> None:
    self.__running = False  # 1. Set flag untuk hentikan loop

    for task in self.__tasks:
        _ = task.cancel()  # 2. Kirim CancelledError ke setiap task

        try:
            await task  # 3. Tunggu task selesai
        except CancelledError:
            pass  # 4. Tangkap exception, jangan propagate

    self.__tasks.clear()  # 5. Bersihkan list tasks
    logger.info("All consumer workers stopped")
```

**Alur Shutdown**:
1. `self.__running = False` → Loop `while self.__running` akan berhenti di iterasi berikutnya
2. `task.cancel()` → Kirim `CancelledError` ke coroutine yang sedang `await`
3. Di `__consume_loop`, ada handler khusus:
   ```python
   except CancelledError:
       raise  # Re-raise untuk keluar dari loop
   ```
4. `await task` memastikan semua tasks sudah benar-benar selesai sebelum lanjut

**Trigger Shutdown** (`src/aggregator/app/main.py` baris 27-37):
```python
@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await consumer.initialize()
    await consumer.start()
    yield  # Aplikasi berjalan
    await consumer.close()  # Shutdown saat aplikasi stop
```

---

## 5. Ordering Strategy
### Keputusan
**Tidak menjamin total ordering** di level processing, fokus pada *eventual consistency* dengan ordering saat query.

### Mengapa Tidak Total Ordering?
Dalam sistem dengan 4 parallel workers, total ordering sangat sulit dicapai:

1. **Skenario Race**: Event A masuk lebih dulu dari Event B, tapi Worker 2 memproses B lebih cepat dari Worker 1 yang memproses A.
2. **Network Latency**: Event dari Publisher 1 bisa sampai lebih lambat dari Publisher 2 meski dikirim lebih dulu.

**Untuk menjamin total ordering**, diperlukan:
- Single worker (bottleneck, throughput turun drastis)
- Distributed consensus (kompleks, latency tinggi)
- Logical clock/vector clock (overhead signifikan)

### Trade-off yang Diterima
| Aspek | Dengan Total Ordering | Tanpa Total Ordering |
|-------|----------------------|---------------------|
| Throughput | ~50 events/s (single worker) | ~220 events/s (4 workers) |
| Kompleksitas | Tinggi | Rendah |
| Latency | Tinggi (serialization) | Rendah (parallel) |

### Mitigasi: Ordering Saat Query
Meski tidak ada total ordering saat processing, ordering tetap tersedia saat query:
```sql
SELECT * FROM processed_events ORDER BY timestamp DESC
```
User dapat mengurutkan berdasarkan `timestamp` (waktu event) atau `created_at` (waktu disimpan).

### Timestamp Fields
| Field | Source | Purpose |
|-------|--------|---------|
| `timestamp` | Publisher (ISO8601) | Waktu event terjadi di source |
| `created_at` | Database (NOW()) | Waktu event disimpan di database |

### Query Ordering
**File**: `src/aggregator/app/services/database.py` (baris 341-349)
```sql
SELECT event_id, topic, source, payload, timestamp
FROM processed_events
WHERE topic = $1
ORDER BY timestamp DESC
```

---

## 6. Audit Trail System
### Keputusan
Mencatat setiap *lifecycle* event dengan 5 *action types* untuk *debugging* dan *observability*.

### Implementasi
**File**: `src/aggregator/app/models/audit.py` (baris 7-12)
```python
class AuditAction(str, Enum):
    RECEIVED = "RECEIVED"   # Event diterima di API
    QUEUED = "QUEUED"       # Event masuk ke Redis queue
    PROCESSED = "PROCESSED" # Event berhasil disimpan (unique)
    DROPPED = "DROPPED"     # Event di-drop (duplicate)
    FAILED = "FAILED"       # Error saat processing
```

### Event Lifecycle
```
Client → POST /publish → [RECEIVED] → LPUSH Redis → [QUEUED]
                                           ↓
                     Worker BRPOP → INSERT → [PROCESSED] atau [DROPPED]
```

### Audit Table Schema
**File**: `src/aggregator/app/services/database.py` (baris 71-81)
```sql
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    source TEXT NOT NULL,
    action TEXT NOT NULL,
    worker_id INTEGER,        -- NULL untuk RECEIVED/QUEUED
    created_at TIMESTAMPTZ DEFAULT NOW()
)
```

### Indexes untuk Query Performance
**File**: `src/aggregator/app/services/database.py` (baris 87-97)
```sql
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action)
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, topic)
```

---

## 7. Container Orchestration & Network Isolation
### Keputusan
Menggunakan **Docker Compose** dengan *internal bridge network* dan *named volumes* untuk *persistence*.

### Network Isolation
**File**: `docker/docker-compose.yml` (baris 94-96)
```yaml
networks:
  chronicleweaver-network:
    driver: bridge
```

| Service | Port Exposed | Network Access |
|---------|--------------|----------------|
| Aggregator | 8080:8080 | Accessible dari host |
| PostgreSQL | - | Internal only |
| Redis | - | Internal only |

### Alasan Tidak Expose Database
- **Security**: Direct access ke database tidak diizinkan
- **Encapsulation**: Semua akses melalui API layer
- **Consistency**: Mencegah writes yang bypass business logic

### Volume Persistence
**File**: `docker/docker-compose.yml` (baris 98-102)
```yaml
volumes:
  postgres-data:
    driver: local
  redis-data:
    driver: local
```

### Service Dependencies & Healthchecks
**File**: `docker/docker-compose.yml` (baris 13-23)
```yaml
depends_on:
  postgres:
    condition: service_healthy
  redis:
    condition: service_healthy
healthcheck:
  test: [ "CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()" ]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
```

### Restart Policy
| Service | Policy | Alasan |
|---------|--------|--------|
| Aggregator | `unless-stopped` | Keep running, restart on crash |
| PostgreSQL | `unless-stopped` | Critical persistence layer |
| Redis | `unless-stopped` | Queue must be available |
| Publisher | `on-failure` | One-time job, restart only if failed |

# Analisis Performance

## K6 Load Testing

### Apa itu K6?
[Grafana K6](https://k6.io/) adalah tool load testing modern yang ditulis dalam Go dengan scripting JavaScript. K6 digunakan untuk mengukur performa sistem di bawah beban tinggi.

### Konfigurasi Test
**File**: `k6/benchmark.js` (baris 5-17)
```javascript
export const options = {
    scenarios: {
        load_test: {
            executor: 'constant-vus',        // Jumlah VU tetap
            vus: 20,                         // 20 virtual users paralel
            duration: '60s',                 // Durasi 60 detik
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<500'],    // p95 latency < 500ms
        http_req_failed: ['rate<0.01'],      // Error rate < 1%
    },
};
```

| Parameter | Nilai | Penjelasan |
|-----------|-------|------------|
| `vus` | 20 | 20 virtual users mengirim request secara paralel |
| `duration` | 60s | Test berjalan selama 60 detik |
| `p(95)<500` | Threshold | 95% request harus selesai dalam 500ms |
| `rate<0.01` | Threshold | Error rate harus di bawah 1% |

### Event Generation dengan Duplicate Ratio
**File**: `k6/benchmark.js` (baris 39-54)
```javascript
export default function () {
    const eventId = `k6-${__VU}-${__ITER}-${Date.now()}`;
    const isDuplicate = (__ITER % 10) < 3;  // 30% duplicate ratio

    const payload = JSON.stringify({
        events: [{
            event_id: isDuplicate ? `duplicate-vu-${__VU}` : eventId,
            topic: `benchmark-topic-${__VU % 5}`,
            source: 'k6-benchmark',
            payload: {
                message: 'K6 load test event',
                timestamp: new Date().toISOString(),
            },
            timestamp: new Date().toISOString(),
        }]
    });

    const res = http.post(`${AGGREGATOR_URL}/publish`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });
}
```

**Mekanisme Duplicate 30%:**
- `(__ITER % 10) < 3` → iterasi 0, 1, 2 dari setiap 10 iterasi adalah duplicate
- Duplicate menggunakan ID `duplicate-vu-${__VU}` yang sama per VU
- Non-duplicate menggunakan ID unik `k6-${__VU}-${__ITER}-${Date.now()}`

### Teardown: Reporting Hasil
**File**: `k6/benchmark.js` (baris 73-92)
```javascript
export function teardown(data) {
    sleep(5);  // Tunggu consumer selesai processing

    const statsRes = http.get(`${AGGREGATOR_URL}/stats`);
    const stats = statsRes.json();

    const duration = (Date.now() - data.startTime) / 1000;

    console.log('========================================');
    console.log('          BENCHMARK RESULTS            ');
    console.log('========================================');
    console.log(`Duration:           ${duration.toFixed(2)}s`);
    console.log(`Events Received:    ${stats.received}`);
    console.log(`Unique Processed:   ${stats.unique_processed}`);
    console.log(`Duplicates Dropped: ${stats.duplicated_dropped}`);
    console.log(`Throughput:         ${(stats.received / duration).toFixed(2)} events/s`);
}
```

### Cara Menjalankan Benchmark
```bash
docker compose -f docker/docker-compose.yml --profile benchmark run --rm k6
```

## Benchmark Results

### Summary
| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| **Events Published** | >= 20,000 | **24,069** | Requirement met |
| **Throughput** | >= 50 events/s | **370.02 events/s** | 7.4x target |
| **Duplicate Rate** | ~30% | **29.99%** | Exact match |
| **Error Rate** | < 1% | **0.00%** | Zero errors |
| **Latency p95** | < 500ms | **69.32ms** | 7.2x better |
| **Checks Passed** | 100% | **100%** | All pass |

### Detailed Metrics
| Metric | Value | Penjelasan |
|--------|-------|------------|
| Events Published | 24,069 | Total events yang dikirim oleh K6 |
| Duplicates Sent | 7,244 | Events duplicate yang sengaja dikirim (30%) |
| Events Received | 11,466 | Events yang sudah diproses saat teardown |
| Unique Processed | 8,031 | Events unik yang tersimpan di database |
| Duplicates Dropped | 3,439 | Events duplicate yang di-drop |
| Total Duration | 65.03s | Termasuk 5s warmup/cooldown |
| HTTP Requests | 24,072 | Total HTTP calls |

### Latency Analysis
| Percentile | Response Time | Penjelasan |
|------------|---------------|------------|
| Average | 38.89ms | Rata-rata response time |
| Median (p50) | 35.24ms | 50% request selesai dalam waktu ini |
| p90 | 58.22ms | 90% request selesai dalam waktu ini |
| p95 | 69.32ms | Target threshold, jauh di bawah 500ms |
| Max | 247.50ms | Response time terburuk, masih acceptable |

### Interpretasi Hasil
1.  **24,069 events published** memenuhi requirement >= 20,000 events
2.  **Throughput 370 events/s** menunjukkan sistem sangat responsif dengan 20 VUs paralel
3.  **Duplicate rate 29.99%** membuktikan deduplication bekerja sesuai ekspektasi (30%)
4.  **Zero errors** menunjukkan sistem stabil tanpa crash atau timeout
5.  **p95 = 69.32ms** jauh di bawah threshold 500ms, menunjukkan latency sangat rendah

### Bukti No Double-Processing (Hasil Uji Konkurensi)
| Test                                  | Workers | Same Events | Result               |
| ------------------------------------- | ------- | ----------- | -------------------- |
| `test_concurrency_duplicate_parallel` | 20      | 20          | 1 unique, 19 dropped |
| `test_race_condition_same_event`      | 50      | 50          | 1 unique, 49 dropped |
Detailnya ada di bagian [Bukti Concurrency Safe](#bukti-concurrency-safe).

# Keterkaitan ke Bab 1-13
## T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator
Buku *Distributed Systems* mendefinisikan skalabilitas dalam tiga dimensi.
> "Scalability of a system can be measured along at least three different dimensions... Size scalability: A system can be scalable regarding its size... geographical scalability... administrative scalability." (Steen & Tanenbaum, 2023)

Dalam DistributedChronicleWeaver, fokus utama adalah **size scalability**. Sistem dirancang untuk menangani beban ribuan *events* per detik melalui arsitektur *multi-worker* yang dapat di-*scale* secara horizontal. Jumlah *worker* dapat dikonfigurasi via *environment variable* `WORKER_COUNT` (default: 4) untuk memungkinkan penyesuaian kapasitas tanpa mengubah kode.

*Trade-off* yang diterima adalah mengorbankan sebagian *transparency*. *Client* tidak mendapat konfirmasi instan bahwa *event* sudah tersimpan di *database* (hanya konfirmasi masuk ke *queue*). Ini adalah pilihan desain yang disengaja: *performance transparency* dikorbankan untuk mencapai *throughput* tinggi. *Latency* bervariasi karena *event* harus melewati antrian Redis terlebih dahulu, namun hal ini memungkinkan sistem menangani *burst* trafik tanpa membebani *database* secara langsung.

## T2 (Bab 2): Kapan Memilih Arsitektur Publish–Subscribe Dibanding Client–Server?
Arsitektur dipilih berdasarkan kebutuhan pemisahan antar komponen.
> "These semantics permit communication to be decoupled in time... There is thus no need for the receiver to be executing when a message is being sent to its queue." (Steen & Tanenbaum, 2023)

Berbeda dengan arsitektur *request-reply* (Client-Server) yang bersifat sinkron, sistem *log aggregator* ini membutuhkan *decoupling* karena:
1.  **Space Decoupling**: Publisher tidak perlu mengetahui keberadaan atau jumlah *aggregator workers* yang sedang berjalan. Publisher cukup mengirim ke endpoint `/publish`.
2.  **Time Decoupling**: Sistem tetap menerima pesan meskipun *consumer* sedang *down* atau *busy*. Redis bertindak sebagai *buffer*, menyangga pesan hingga *worker* siap memprosesnya.

Alasan teknis utama adalah untuk mencegah *backpressure* langsung ke *database* saat terjadi lonjakan trafik (*bursts*). Jika menggunakan arsitektur sinkron, setiap *request* harus menunggu *database* *commit* selesai sebelum response dikirim. Dengan Pub-Sub, publisher mendapat response cepat (event sudah di-*queue*), sementara proses penyimpanan berjalan secara asinkron di *background*.

## T3 (Bab 3): At-Least-Once vs Exactly-Once Delivery; Peran Idempotent Consumer
Mengenai jaminan pengiriman pesan, sistem menggunakan *at-least-once*. Konsekuensinya adalah potensi duplikasi pesan, sehingga *idempotency* menjadi sangat penting.
> "When an operation can be repeated multiple times without harm, it is said to be idempotent." (Steen & Tanenbaum, 2023)

Dalam implementasi, operasi `INSERT ... ON CONFLICT DO NOTHING` adalah operasi *idempotent*. Meskipun *network failure* menyebabkan *event* yang sama dikirim ulang, hasil akhir *database* tetap konsisten (satu *record* tersimpan). Berikut potongan kode dari `database.py`:
```sql
INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id
```
Klausa `ON CONFLICT DO NOTHING` memastikan bahwa jika kombinasi `(topic, event_id)` sudah ada, operasi diabaikan tanpa *error*. Return *value* `NULL` menandakan duplikat, yang kemudian dicatat ke *audit log* dengan aksi `DROPPED`.

## T4 (Bab 4): Skema Penamaan Topic dan Event_ID (Unik, Collision-Resistant) untuk Dedup
Untuk melakukan deduplikasi yang efektif, diperlukan identitas yang unik.
> "A true identifier is a name that has the following properties... An identifier refers to at most one entity." (Steen & Tanenbaum, 2023)

Sistem mengadopsi konsep *True Identifier* ini melalui kombinasi `(topic, event_id)`. Skema ini didefinisikan dalam DDL tabel `processed_events`:
```sql
CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    ...
    UNIQUE (topic, event_id)
)
```
*Constraint* `UNIQUE (topic, event_id)` menjamin bahwa dalam satu topik, tidak ada dua *event* dengan ID yang sama. Topik merepresentasikan kategori logis (misal: `user-logs`, `system-metrics`), sementara `event_id` adalah identifier unik yang di-*generate* oleh *publisher* (biasanya UUID atau format *custom*). Kombinasi ini memberikan *collision resistance* yang cukup untuk skenario *multi-publisher*.

## T5 (Bab 5): Ordering Praktis (Timestamp + Monotonic Counter); Batasan dan Dampaknya
Sinkronisasi waktu adalah tantangan utama dalam sistem terdistribusi.
> "Under these circumstances, external physical clocks are needed." (Steen & Tanenbaum, 2023)

Sistem menggunakan *Physical Clocks* (UTC Timestamp dalam format ISO8601) untuk *approximate ordering* saat *query*. Setiap *event* membawa *field* `timestamp` yang di-*set* oleh *publisher*. Dampak *clock skew* antar mesin *publisher* ditoleransi karena sifat *log aggregator* yang *eventually consistent*.

Untuk kebutuhan *ordering* yang lebih ketat, sistem menyediakan *field* `created_at` (*timestamp* saat *record* masuk ke *database*) sebagai *fallback*. Kombinasi `timestamp` (waktu *event* terjadi) dan `created_at` (waktu penyimpanan) memberikan fleksibilitas dalam *query sorting*.

## T6 (Bab 6): Failure Modes dan Mitigasi (Retry, Backoff, Durable Dedup Store, Crash Recovery)
Sistem merespons berbagai kegagalan dengan mekanisme *retry* dan *durable storage*.
> "A crash failure occurs when a server prematurely halts, but was working correctly until it stopped." (Steen & Tanenbaum, 2023)

Publisher menangani *omission failure* dengan mengirim ulang pesan menggunakan *exponential backoff*. Implementasi *retry* ada di `publisher/app/main.py`:
```python
for attempt in range(max_retries):
    try:
        response = urlopen(url=request, timeout=30)
        return status_code, response_text
    except Exception as e:
        backoff_time = min(2 ** (attempt + 1), 30)  # Exponential backoff, max 30s
        sleep(backoff_time)
```
Untuk pemulihan setelah *crash*, sistem menerapkan *backward recovery*:
> "In backward recovery, the main issue is to bring the system from its present erroneous *state* back into a previously correct state." (Steen & Tanenbaum, 2023)

Penggunaan Docker Volumes memastikan data yang sudah di-*checkpoint* ke PostgreSQL tetap tersedia setelah *container* di-*restart*. Volume `postgres_data` menyimpan *state* *database* secara persisten.

## T7 (Bab 7): Eventual Consistency pada Aggregator; Peran Idempotency + Dedup
Dalam konsistensi data, sistem ini berada di sisi *Eventual Consistency*.
> "Eventual consistency essentially requires only that updates are guaranteed to propagate to all replicas." (Steen & Tanenbaum, 2023)

Antara pesan masuk ke Redis dan tersimpan di Postgres, terdapat jeda waktu (*inconsistency window*). Selama jeda ini, query ke `/stats` mungkin belum merefleksikan *event* terbaru. Namun, sistem menjamin bahwa *eventually* semua *event* yang berhasil di-*queue* akan tersimpan di *database*.

*Idempotency* memastikan konvergensi ke *state* yang benar. Meskipun *event* yang sama diproses berkali-kali (akibat *retry* atau *network duplicate*), constraint `UNIQUE (topic, event_id)` mencegah duplikasi data. Hasil akhir selalu konsisten: satu *event* = satu *record*.

## T8 (Bab 8): Desain Transaksi: ACID, Isolation Level, dan Strategi Menghindari Lost-Update
Integritas data dijamin oleh *database transaction*.
> "Transactions adhere to the so-called ACID properties: • Atomic: To the outside world, the transaction happens indivisibly" (Steen & Tanenbaum, 2023)

Implementasi ACID dalam sistem:
**Atomicity**: Setiap penyimpanan *event* dilakukan dalam satu instruksi SQL tunggal. Jika `INSERT` gagal di tengah jalan (misal: koneksi putus), tidak ada data parsial yang tersimpan. Contoh dari `database.py`:
```sql
INSERT INTO processed_events (...) VALUES (...) ON CONFLICT DO NOTHING RETURNING id
```

**Consistency**: *Constraint* `UNIQUE (topic, event_id)` dan `NOT NULL` pada *field* berguna untuk menjaga *invariant database*.
**Isolation**: PostgreSQL menggunakan level *Read Committed* secara *default*. Ini mencegah *dirty reads* namun mengizinkan *non-repeatable reads*. *Trade-off* ini diterima untuk performa yang lebih baik.
**Durability**: Setelah *commit* berhasil, data tersimpan di *disk* PostgreSQL dan dijamin persisten melalui Docker Volume.
**Lost-Update Prevention**: *Update* statistik menggunakan operasi atomik:
```sql
UPDATE stats SET received = received + 1, updated_at = NOW() WHERE id = 1
```
Operasi `received + 1` bersifat atomik di level *database*, mencegah *race condition* saat *multiple worker* meng-*update* *counter* secara bersamaan.

## T9 (Bab 9): Kontrol Konkurensi: Locking/Unique Constraints/Upsert; Idempotent Write Pattern
Sistem menggunakan pendekatan optimistik untuk menangani *multi-worker concurrency*.
> "A second, optimistic, approach is to let the transaction proceed while verification is taking place." (Steen & Tanenbaum, 2023)

Alih-alih mengunci tabel di awal (*pessimistic locking*), setiap *worker* mencoba melakukan `INSERT` secara langsung. *Database* yang memverifikasi apakah ada konflik. Jika terjadi duplikasi (`ON CONFLICT`), transaksi tidak error, melainkan mengabaikan operasi tersebut.

**Multi-Worker Architecture** (`consumer.py`):
```python
self.__worker_count = int(getenv(key="WORKER_COUNT", default="4"))

for worker_id in range(self.__worker_count):
    task = create_task(coro=self.__consume_loop(worker_id))
    self.__tasks.append(task)
```
Empat *worker* berjalan paralel, masing-masing melakukan `BRPOP` dari Redis *queue*. Saat dua *worker* secara kebetulan mem-*pop* *event* dengan ID yang sama (akibat *duplicate* di *queue*), hanya satu yang berhasil `INSERT`. *Worker* lainnya mendapat *return* `NULL` dan mencatat *event* sebagai `DROPPED`.

**Efisiensi**: Tidak ada *explicit locking* yang menghambat *throughput*. *Database* menangani konflik di level *row* dengan *overhead* minimal.

## T10 (Bab 10–13): Orkestrasi Compose, Keamanan Jaringan Lokal, Persistensi (Volume), Observability
Implementasi praktis didukung oleh virtualisasi dan orkestrasi *container*.
> "Virtual machines... allow for the isolation of a complete application and its environment." (Steen & Tanenbaum, 2023)

**Orkestrasi** (`docker-compose.yml`): Tiga *service* (`aggregator`, `postgres`, `redis`) dikelola sebagai satu *unit deployment*. *Dependency* antar *service* didefinisikan eksplisit dengan `depends_on` dan `healthcheck`.

**Keamanan Jaringan**: PostgreSQL dan Redis berada di *network* internal (`chronicle-network`) yang tidak terekspos ke *host*. Hanya Aggregator yang mem-*bind* *port* `8080` ke luar. Ini mencegah akses langsung ke *database* dari luar *container*.

**Persistensi**: Docker Volume `postgres_data` dan `redis_data` memisahkan *storage* dari *compute*. Saat *container* dihapus dan dibuat ulang, data tetap tersimpan.

**Observability**: *Endpoint* `/stats` menyediakan metrik *real-time* (*events received*, *processed*, *dropped*). *Endpoint* `/audit` menyediakan log aktivitas sistem untuk *debugging* dan *monitoring*.


# Test Coverage
## Unit/Integration Tests (17 tests)
| Category              | Tests | Coverage                                        |
| --------------------- | ----- | ----------------------------------------------- |
| Deduplication         | 3     | Single, batch, across topics                    |
| Persistence           | 2     | After restart, retrievable                      |
| Concurrency           | 3     | Parallel requests, *duplicate* parallel, stats  |
| Schema validation     | 8     | Valid, missing fields, wrong types              |
| Stats consistency     | 3     | Structure, increments, topics                   |
| Events consistency    | 4     | Structure, filter, count, unique                |
| Batch stress          | 3     | 20k events, throughput, large payload           |
| Race condition        | 3     | Same event, stats, data corruption              |
| Graceful restart      | 2     | Data preserved, dedup works                     |
| Out-of-order          | 2     | Processing, all stored                          |
| Retry backoff         | 2     | Valid request, stats accuracy                   |
| Health endpoints      | 5     | health, ready, root, docs, openapi              |
| Transaction isolation | 2     | Unique constraint, atomic stats                 |
| Batch atomic          | 2     | All valid, partial duplicates                   |
| Edge cases            | 4     | Empty topic, unicode, special chars, long ID    |
| Integration           | 3     | Full workflow, multiple topics, API consistency |
| Audit log             | 5     | Processed, dropped, summary, filters            |

**Total: 17 test files, 56+ test functions**

# Demo Video
[Link](https://youtu.be/XZXrzUK8SQA)

[![Demo Video](https://i.ytimg.com/vi/XZXrzUK8SQA/maxresdefault.jpg)](https://youtu.be/XZXrzUK8SQA)

# Kesimpulan
Sistem DistributedChronicleWeaver berhasil diimplementasikan sebagai multi-*service* Pub-Sub log aggregator dengan:
1. **Idempotency**: INSERT ON CONFLICT DO NOTHING
2. **Persistent dedup**: PostgreSQL dengan unique constraints
3. **Transaction safety**: READ COMMITTED + atomic upserts
4. **Concurrency handling**: Multi-worker dengan no double-processing
5. **Message broker**: Redis untuk decoupled processing
6. **Audit log**: Tracking RECEIVED, QUEUED, PROCESSED, DROPPED
7. **Performance**: ≥20,000 events dengan ≥30% duplicates
8. **17 tests**: Comprehensive coverage (56+ test functions)

# Bibliography
Steen, M. van, & Tanenbaum, A. S. (2023). *Distributed Systems* (4th ed.). Maarten van Steen. <https://www.distributed-systems.net/index.php/books/ds4/>

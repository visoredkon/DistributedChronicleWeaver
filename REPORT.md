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
- **Atomic**: *Guarantee deduplication* dalam satu SQL *statement*
- **Concurrent-safe**: *Multiple workers* tidak menghasilkan *race condition*
- **Persistent**: *State* tersimpan di *database*, *survive restart*

### Implementasi
```sql
INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
```

- Jika `id` returned → *event* baru, diproses
- Jika `NULL` → duplicate, di-*drop*

## 2. Message Broker: Redis
### Keputusan
Menggunakan **Redis** sebagai *internal message queue*.

### Alasan
- **Decoupling**: Memisahkan API dari *consumer processing*
- **Reliability**: *Event* tidak hilang jika *consumer busy* atau *down*
- **Scalability**: *Multiple workers* dapat *consume* secara *parallel*

### Pattern
- **LPUSH**: Publisher menambah *event* ke *queue*
- **BRPOP**: Consumer mengambil *event* dengan *blocking*

## 3. Transaction & Isolation Level
### Keputusan
Menggunakan **READ COMMITTED** *isolation level* (default PostgreSQL).

### Alasan
- **Performance**: Tidak memerlukan *strict serialization*
- **Sufficiency**: *Unique constraint* sudah mencegah *duplicate inserts*
- **Trade-off**: *Phantom reads possible* tapi *mitigated* oleh *constraint*

### Bukti Concurrency Safe
```
Test: test_concurrency_duplicate_parallel
- 20 *parallel requests with same* `event_id`
- Result: Hanya 1 *event* tersimpan
- Stats: 1 `unique_processed`, 19 `duplicated_dropped`
```

## 4. Multi-Worker Consumer
### Keputusan
Menggunakan **4 async workers** (*configurable*) yang *consume* dari Redis queue.

### Alasan
- **Throughput**: *Parallel processing* meningkatkan processing rate
- **Fault isolation**: Satu *worker* *error* tidak mengganggu lainnya
- **Retry**: *Exponential backoff* untuk *transient failures*

### Implementation
```python
for worker_id in range(worker_count):
    task = create_task(coro=self.__consume_loop(worker_id))
```

## 5. Ordering Strategy
### Keputusan
**Tidak menjamin total ordering**, fokus pada *eventual consistency*.

### Alasan
- **Use case**: Log aggregation tidak memerlukan *strict ordering*
- **Trade-off**: *Total ordering* memerlukan *serialization* (*bottleneck*)
- **Mitigation**: *Timestamp* disimpan untuk *ordering* saat *query*

# Analisis Performance
## K6 Load Testing Configuration

| Parameter           | Value      |
| ------------------- | ---------- |
| Virtual Users (VUs) | 10         |
| Duration            | 30 seconds |
| Duplicate Ratio     | ~30%       |
| Tool                | Grafana K6 |

## Benchmark Results
### Summary
| Metric             | Target        | Actual            | Status        |
| ------------------ | ------------- | ----------------- | ------------- |
| **Throughput**     | ≥ 50 events/s | **221.13 events/s** | 4.4x target   |
| **Duplicate Rate** | ~30%          | **30%**        | Exact match   |
| **Error Rate**     | < 1%          | **0.00%**         | Zero errors   |
| **Latency p95**    | < 500ms       | **52.73ms**       | 9.5x threshold|
| **Checks Passed**  | 100%          | **100%**          | All pass      |

### Detailed Metrics
| Metric                | Value  |
| --------------------- | ------ |
| Total Events Received | 7,748  |
| Unique Processed      | 5,421  |
| Duplicates Dropped    | 2,330  |
| Total Duration        | 35.04s |
| HTTP Requests         | 8,153  |

### Latency Analysis
| Percentile   | Response Time |
| ------------ | ------------- |
| Average      | 25.79ms       |
| Median (p50) | 21.30ms       |
| p90          | 45.57ms       |
| p95          | 52.73ms       |
| Max          | 232.33ms      |

### Bukti No Double-Processing (Hasil Uji Konkurensi)
| Test                                  | Workers | Same Events | Result               |
| ------------------------------------- | ------- | ----------- | -------------------- |
| `test_concurrency_duplicate_parallel` | 20      | 20          | 1 unique, 19 dropped |
| `test_race_condition_same_event`      | 50      | 50          | 1 unique, 49 dropped |

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

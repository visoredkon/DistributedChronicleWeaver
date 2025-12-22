# DistributedChronicleWeaver

Sistem *Distributed Publish-Subscribe Log Aggregator* dengan *idempotent consumer*, *deduplication*, dan *transaction-based concurrency* yang berjalan dengan Docker Compose.

## System Architecture
```mermaid
flowchart TB
    subgraph Docker["Docker Compose Network"]
        Publisher["Publisher<br/>(Event Generator)"]

        subgraph Aggregator["Aggregator (FastAPI)"]
            API["REST API<br/>/publish, /events, /stats, /audit"]
            Consumer["Consumer Workers<br/>(4 async workers)"]
        end

        Redis[("Redis<br/>(Message Broker)")]
        PostgreSQL[("PostgreSQL<br/>(Persistent Storage)")]

        Publisher -->|"HTTP POST /publish"| API
        API -->|"LPUSH"| Redis
        Redis -->|"BRPOP"| Consumer
        Consumer -->|"INSERT ON CONFLICT"| PostgreSQL
        API -->|"SELECT"| PostgreSQL
    end
```

### Components
| Service    | Image                           | Description                      |
| ---------- | ------------------------------- | -------------------------------- |
| Aggregator | `astral/uv:python3.14-bookworm` | API + *Consumer workers*         |
| Publisher  | `astral/uv:python3.14-bookworm` | *Event generator*                |
| Broker     | `redis:8-bookworm`              | *Internal message queue*         |
| Storage    | `postgres:17-bookworm`          | *Persistent deduplication store* |

## Build and Run
### Quick Start (Docker Compose)
```fish
# 1. Build images (optional, karena kalau lompat ke setap 2 otomatis build)
docker compose -f docker/docker-compose.yml build

# 2. Start core services (aggregator, postgres, redis)
docker compose -f docker/docker-compose.yml up -d

# 3. Run publisher untuk generate events
docker compose -f docker/docker-compose.yml --profile publisher run --rm publisher

# 4. Run K6 benchmark
docker compose -f docker/docker-compose.yml --profile benchmark run --rm k6
```

### Profiles
| Profile     | Command                            | Description                                       |
| ----------- | ---------------------------------- | ------------------------------------------------- |
| (default)   | `up -d`                            | *Core services*: aggregator, postgres, redis      |
| `publisher` | `--profile publisher up publisher` | *Event generator* (20,000 events, 30% duplicates) |
| `benchmark` | `--profile benchmark up k6`        | K6 *load testing*                                 |

### Other Commands
```fish
# View logs
docker compose -f docker/docker-compose.yml logs -f

# Stop services
docker compose -f docker/docker-compose.yml down

# Stop and remove volumes (fresh start)
docker compose -f docker/docker-compose.yml down -v
```

### Access Points
- **Aggregator API**: <http://localhost:8080>
- **Health Check**: <http://localhost:8080/health>
- **API Docs**: <http://localhost:8080/docs>

## API Endpoints
### POST `/publish`
*Publish events* ke aggregator.

**Request:**
```json
{
  "events": [
    {
      "event_id": "unique-id",
      "topic": "topic-name",
      "source": "source-service",
      "payload": {
        "message": "Event content",
        "timestamp": "2025-01-01T00:00:00"
      },
      "timestamp": "2025-01-01T00:00:00"
    }
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Published 1 events",
  "events_count": 1
}
```

### GET `/events?topic={topic}`
*Retrieve events* yang sudah diproses.

**Response:**
```json
{
  "count": 100,
  "events": [...]
}
```

### GET `/stats`
*System statistics*.

**Response:**
```json
{
  "received": 5000,
  "unique_processed": 4000,
  "duplicated_dropped": 1000,
  "topics": ["topic-1", "topic-2"],
  "uptime": 3600
}
```

### GET `/health` & `/ready`
*Health check endpoints* untuk monitoring.

### GET `/audit`
*Query audit logs* dengan *filters*.

**Query Parameters:**
- `action`: Filter by action (RECEIVED, QUEUED, PROCESSED, DROPPED)
- `topic`: Filter by topic
- `event_id`: Filter by event_id
- `from`: Start timestamp (ISO8601)
- `to`: End timestamp (ISO8601)
- `limit`: Max records (default 100, max 1000)

**Response:**
```json
{
  "count": 10,
  "audit_logs": [
    {
      "id": 1,
      "event_id": "event-001",
      "topic": "topic-1",
      "action": "PROCESSED",
      "worker_id": 0,
      "created_at": "2025-01-01T00:00:00Z"
    }
  ]
}
```

### GET `/audit/summary`
*Audit summary* per topic dan worker.

## Environment Variables
### Aggregator
| Variable       | Default                                                    | Description             |
| -------------- | ---------------------------------------------------------- | ----------------------- |
| `DATABASE_URL` | `postgresql://chronicle:chronicle@postgres:5432/chronicle` | PostgreSQL connection   |
| `REDIS_URL`    | `redis://redis:6379/0`                                     | Redis connection        |
| `WORKER_COUNT` | `4`                                                        | Consumer *worker* count |

### Publisher
| Variable          | Default                 | Description              |
| ----------------- | ----------------------- | ------------------------ |
| `AGGREGATOR_URL`  | `http://localhost:8080` | Aggregator API URL       |
| `EVENT_COUNT`     | `20000`                 | Total events to generate |
| `DUPLICATE_RATIO` | `0.3`                   | Duplicate *event* ratio  |
| `BATCH_SIZE`      | `1000`                  | Events per batch         |

## Testing
### Run All Tests
```fish
# Start services first
docker compose -f docker/docker-compose.yml up -d

# Run tests
uv run pytest tests/ -v
```

### Test Coverage (17 tests)
| Test File                          | Description                |
| ---------------------------------- | -------------------------- |
| `test_01_deduplication.py`         | Deduplication validation   |
| `test_02_persistence.py`           | Persistence after restart  |
| `test_03_concurrency.py`           | Multi-worker consistency   |
| `test_04_schema_validation.py`     | Event schema validation    |
| `test_05_stats_consistency.py`     | Stats endpoint tests       |
| `test_06_events_consistency.py`    | Events endpoint tests      |
| `test_07_batch_stress.py`          | 20,000+ events stress test |
| `test_08_race_condition.py`        | Race condition prevention  |
| `test_09_graceful_restart.py`      | Graceful restart handling  |
| `test_10_out_of_order.py`          | Out-of-order tolerance     |
| `test_11_retry_backoff.py`         | Retry mechanism tests      |
| `test_12_health_endpoints.py`      | Health check tests         |
| `test_13_transaction_isolation.py` | Transaction isolation      |
| `test_14_batch_atomic.py`          | Batch atomic processing    |
| `test_15_edge_cases.py`            | Edge cases handling        |
| `test_16_integration.py`           | Full integration tests     |
| `test_17_audit_log.py`             | Audit log endpoints        |

## Persistence
Data disimpan dalam *named volumes*:
- `postgres-data`: PostgreSQL database
- `redis-data`: Redis data

```fish
# Verify persistence after restart
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d

# Check stats preserved
curl http://localhost:8080/stats
```

## Transaction & Concurrency
### Isolation Level: READ COMMITTED
PostgreSQL menggunakan `READ COMMITTED` isolation level dengan:
- *Unique constraint* `(topic, event_id)` mencegah *duplicate inserts*
- *Atomic upsert*: `INSERT ... ON CONFLICT DO NOTHING`
- *Multi-worker consumer* dengan transaksi per-*event*

### Deduplication Pattern
```sql
INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
```

## Assumptions
- *Single* PostgreSQL *instance* (no *replication*)
- *Events* dengan *timestamp* identik tidak dijamin *strict ordering*
- *Publisher retry* max 5x dengan *exponential backoff*
- *Consumer workers* default 4 (*configurable* via `WORKER_COUNT`)
- Semua *services* berjalan di *internal* Docker *network* (no *external dependencies*)

from datetime import datetime
from os import getenv
from typing import cast

from asyncpg import Connection, Pool, Record, create_pool
from loguru import logger
from orjson import dumps, loads
from pydantic import BaseModel

from ..models.audit import (
    AuditAction,
    AuditLogModel,
    AuditSummaryModel,
    AuditSummaryTopicModel,
)
from ..models.events import EventModel


class StatsModel(BaseModel):
    received: int = 0
    duplicated_dropped: int = 0


class DatabaseService:
    __instance: "DatabaseService | None" = None
    __pool: Pool | None = None
    __start_time: float = 0.0

    def __new__(cls) -> "DatabaseService":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

        return cls.__instance

    async def initialize(self) -> None:
        from time import time

        self.__start_time = time()
        database_url: str = getenv(
            key="DATABASE_URL",
            default="postgresql://chronicle:chronicle@localhost:5432/chronicle",
        )

        self.__pool = await create_pool(dsn=database_url, min_size=2, max_size=10)

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            await connection.execute("""
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
            """)

            await connection.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    received BIGINT NOT NULL DEFAULT 0,
                    duplicated_dropped BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            await connection.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id SERIAL PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    source TEXT NOT NULL,
                    action TEXT NOT NULL,
                    worker_id INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            await connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_topic ON processed_events(topic)
            """)

            await connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action)
            """)

            await connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)
            """)

            await connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, topic)
            """)

            await connection.execute("""
                INSERT INTO stats (id, received, duplicated_dropped)
                VALUES (1, 0, 0)
                ON CONFLICT (id) DO NOTHING
            """)

        logger.info("Database initialized successfully")

    async def log_audit(
        self,
        event_id: str,
        topic: str,
        source: str,
        action: AuditAction,
        worker_id: int | None = None,
    ) -> None:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            await connection.execute(
                """
                INSERT INTO audit_log (event_id, topic, source, action, worker_id)
                VALUES ($1, $2, $3, $4, $5)
                """,
                event_id,
                topic,
                source,
                action.value,
                worker_id,
            )

    async def get_audit_logs(
        self,
        action: str | None = None,
        topic: str | None = None,
        event_id: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditLogModel]:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            query: str = "SELECT id, event_id, topic, source, action, worker_id, created_at FROM audit_log WHERE 1=1"
            params: list[object] = []
            param_idx: int = 1

            if action:
                query += f" AND action = ${param_idx}"
                params.append(action)
                param_idx += 1

            if topic:
                query += f" AND topic = ${param_idx}"
                params.append(topic)
                param_idx += 1

            if event_id:
                query += f" AND event_id = ${param_idx}"
                params.append(event_id)
                param_idx += 1

            if from_time:
                query += f" AND created_at >= ${param_idx}"
                params.append(from_time)
                param_idx += 1

            if to_time:
                query += f" AND created_at <= ${param_idx}"
                params.append(to_time)
                param_idx += 1

            query += f" ORDER BY created_at DESC LIMIT ${param_idx}"
            params.append(min(limit, 1000))

            rows: list[Record] = await connection.fetch(query, *params)

            return [
                AuditLogModel(
                    id=row["id"],
                    event_id=row["event_id"],
                    topic=row["topic"],
                    source=row["source"],
                    action=AuditAction(row["action"]),
                    worker_id=row["worker_id"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]

    async def get_audit_summary(self) -> AuditSummaryModel:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            totals: list[Record] = await connection.fetch(
                "SELECT action, COUNT(*) as count FROM audit_log GROUP BY action"
            )

            by_topic: list[Record] = await connection.fetch(
                "SELECT topic, action, COUNT(*) as count FROM audit_log GROUP BY topic, action"
            )

            by_worker: list[Record] = await connection.fetch(
                "SELECT worker_id, action, COUNT(*) as count FROM audit_log WHERE worker_id IS NOT NULL GROUP BY worker_id, action"
            )

            total_received: int = 0
            total_queued: int = 0
            total_processed: int = 0
            total_dropped: int = 0
            total_failed: int = 0

            for row in totals:
                action: str = row["action"]
                count: int = row["count"]
                if action == AuditAction.RECEIVED.value:
                    total_received = count
                elif action == AuditAction.QUEUED.value:
                    total_queued = count
                elif action == AuditAction.PROCESSED.value:
                    total_processed = count
                elif action == AuditAction.DROPPED.value:
                    total_dropped = count
                elif action == AuditAction.FAILED.value:
                    total_failed = count

            topic_summary: dict[str, AuditSummaryTopicModel] = {}
            for row in by_topic:
                t: str = row["topic"]
                if t not in topic_summary:
                    topic_summary[t] = AuditSummaryTopicModel()

                action = row["action"]
                count = row["count"]
                if action == AuditAction.RECEIVED.value:
                    topic_summary[t].received = count
                elif action == AuditAction.QUEUED.value:
                    topic_summary[t].queued = count
                elif action == AuditAction.PROCESSED.value:
                    topic_summary[t].processed = count
                elif action == AuditAction.DROPPED.value:
                    topic_summary[t].dropped = count
                elif action == AuditAction.FAILED.value:
                    topic_summary[t].failed = count

            worker_summary: dict[str, AuditSummaryTopicModel] = {}
            for row in by_worker:
                w: str = str(row["worker_id"])
                if w not in worker_summary:
                    worker_summary[w] = AuditSummaryTopicModel()

                action = row["action"]
                count = row["count"]
                if action == AuditAction.PROCESSED.value:
                    worker_summary[w].processed = count
                elif action == AuditAction.DROPPED.value:
                    worker_summary[w].dropped = count
                elif action == AuditAction.FAILED.value:
                    worker_summary[w].failed = count

            return AuditSummaryModel(
                total_received=total_received,
                total_queued=total_queued,
                total_processed=total_processed,
                total_dropped=total_dropped,
                total_failed=total_failed,
                by_topic=topic_summary,
                by_worker=worker_summary,
            )

    async def insert_event(
        self, event: EventModel, worker_id: int | None = None
    ) -> bool:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            result: str | None = await connection.fetchval(
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

            if result is not None:
                await connection.execute(
                    """
                    UPDATE stats
                    SET received = received + 1, updated_at = NOW()
                    WHERE id = 1
                    """,
                )
                await self.log_audit(
                    event.event_id,
                    event.topic,
                    event.source,
                    AuditAction.PROCESSED,
                    worker_id,
                )
                return True
            else:
                await connection.execute(
                    """
                    UPDATE stats
                    SET received = received + 1, duplicated_dropped = duplicated_dropped + 1, updated_at = NOW()
                    WHERE id = 1
                    """,
                )
                await self.log_audit(
                    event.event_id,
                    event.topic,
                    event.source,
                    AuditAction.DROPPED,
                    worker_id,
                )
                return False

    async def get_events_by_topic(self, topic: str) -> list[EventModel]:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            rows: list[Record] = await connection.fetch(
                """
                SELECT event_id, topic, source, payload, timestamp
                FROM processed_events
                WHERE topic = $1
                ORDER BY timestamp DESC
                """,
                topic,
            )

            return [self.__row_to_event(row) for row in rows]

    async def get_all_events(self) -> list[EventModel]:
        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            rows: list[Record] = await connection.fetch(
                """
                SELECT event_id, topic, source, payload, timestamp
                FROM processed_events
                ORDER BY timestamp DESC
                """
            )

            return [self.__row_to_event(row) for row in rows]

    async def get_stats(self) -> dict[str, object]:
        from time import time

        if self.__pool is None:
            raise RuntimeError("Database pool not initialized")

        async with self.__pool.acquire() as connection:
            connection = cast(Connection, connection)

            row: Record | None = await connection.fetchrow(
                "SELECT received, duplicated_dropped FROM stats WHERE id = 1"
            )

            topics_rows: list[Record] = await connection.fetch(
                "SELECT DISTINCT topic FROM processed_events"
            )

            unique_count: int | None = await connection.fetchval(
                "SELECT COUNT(*) FROM processed_events"
            )

            if row is None:
                return {
                    "received": 0,
                    "unique_processed": 0,
                    "duplicated_dropped": 0,
                    "topics": [],
                    "uptime": int(time() - self.__start_time),
                }

            return {
                "received": row["received"],
                "unique_processed": unique_count or 0,
                "duplicated_dropped": row["duplicated_dropped"],
                "topics": [r["topic"] for r in topics_rows],
                "uptime": int(time() - self.__start_time),
            }

    def __row_to_event(self, row: Record) -> EventModel:
        from ..models.events import EventPayloadModel

        payload_data: dict[str, object] = (
            loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        )

        return EventModel(
            event_id=row["event_id"],
            topic=row["topic"],
            source=row["source"],
            payload=EventPayloadModel.model_validate(payload_data),
            timestamp=row["timestamp"],
        )

    async def close(self) -> None:
        if self.__pool:
            await self.__pool.close()
            logger.info("Database pool closed")

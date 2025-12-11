from asyncio import CancelledError, Task, create_task, sleep
from datetime import datetime
from os import getenv

from loguru import logger

from ..models.audit import AuditAction, AuditLogModel, AuditSummaryModel
from ..models.events import EventModel
from .database import DatabaseService
from .redis_queue import RedisQueueService


class ConsumerService:
    def __init__(self) -> None:
        self.__database: DatabaseService = DatabaseService()
        self.__redis_queue: RedisQueueService = RedisQueueService()
        self.__running: bool = False
        self.__tasks: list[Task[None]] = []
        self.__worker_count: int = int(getenv(key="WORKER_COUNT", default="4"))

    async def initialize(self) -> None:
        await self.__database.initialize()
        await self.__redis_queue.initialize()

    async def start(self) -> None:
        if self.__running:
            return

        self.__running = True

        for worker_id in range(self.__worker_count):
            task: Task[None] = create_task(coro=self.__consume_loop(worker_id))
            self.__tasks.append(task)

        logger.info(f"Started {self.__worker_count} consumer workers")

    async def stop(self) -> None:
        self.__running = False

        for task in self.__tasks:
            _ = task.cancel()

            try:
                await task
            except CancelledError:
                pass

        self.__tasks.clear()
        logger.info("All consumer workers stopped")

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
                    logger.error(
                        f"Worker {worker_id}: Max retries exceeded, continuing..."
                    )
                    retry_count = 0

                await sleep(delay=backoff_time)

    async def get_events_by_topic(self, topic: str) -> list[EventModel]:
        return await self.__database.get_events_by_topic(topic)

    async def get_all_events(self) -> list[EventModel]:
        return await self.__database.get_all_events()

    async def get_stats(self) -> dict[str, object]:
        return await self.__database.get_stats()

    async def get_audit_logs(
        self,
        action: str | None = None,
        topic: str | None = None,
        event_id: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditLogModel]:
        return await self.__database.get_audit_logs(
            action=action,
            topic=topic,
            event_id=event_id,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )

    async def get_audit_summary(self) -> AuditSummaryModel:
        return await self.__database.get_audit_summary()

    async def log_audit(
        self,
        event_id: str,
        topic: str,
        source: str,
        action: AuditAction,
        worker_id: int | None = None,
    ) -> None:
        await self.__database.log_audit(event_id, topic, source, action, worker_id)

    async def close(self) -> None:
        await self.stop()
        await self.__redis_queue.close()
        await self.__database.close()

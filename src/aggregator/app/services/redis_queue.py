from datetime import datetime
from os import getenv
from typing import Any

from loguru import logger
from orjson import dumps, loads
from redis.asyncio import Redis

from ..models.events import EventModel


class RedisQueueService:
    __instance: "RedisQueueService | None" = None
    __client: Redis | None = None  # type: ignore[type-arg]

    def __new__(cls) -> "RedisQueueService":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

        return cls.__instance

    async def initialize(self) -> None:
        redis_url: str = getenv(key="REDIS_URL", default="redis://localhost:6379/0")

        self.__client = Redis.from_url(url=redis_url, decode_responses=True)

        _ = await self.__client.ping()  # type: ignore[misc]
        logger.info("Redis connection established")

    async def push(self, event: EventModel) -> None:
        if self.__client is None:
            raise RuntimeError("Redis client not initialized")

        event_data: str = dumps(event.model_dump(mode="json")).decode("utf-8")
        _ = await self.__client.lpush("events", event_data)  # type: ignore[misc]

    async def pop(self, timeout: int = 5) -> EventModel | None:
        from ..models.events import EventPayloadModel

        if self.__client is None:
            raise RuntimeError("Redis client not initialized")

        result: list[str] | None = await self.__client.brpop(  # type: ignore[misc]
            keys=["events"], timeout=timeout
        )

        if result is None:
            return None

        event_data: dict[str, Any] = loads(result[1])

        payload_data: dict[str, Any] = event_data["payload"]
        timestamp_str: str = str(event_data["timestamp"])

        return EventModel(
            event_id=str(event_data["event_id"]),
            topic=str(event_data["topic"]),
            source=str(event_data["source"]),
            payload=EventPayloadModel.model_validate(payload_data),
            timestamp=datetime.fromisoformat(timestamp_str),
        )

    async def length(self) -> int:
        if self.__client is None:
            raise RuntimeError("Redis client not initialized")

        return await self.__client.llen("events")  # type: ignore[return-value]

    async def close(self) -> None:
        if self.__client:
            await self.__client.close()
            logger.info("Redis connection closed")

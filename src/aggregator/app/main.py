from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from os import getenv
from typing import cast

from fastapi import FastAPI, HTTPException, Query
from loguru import logger

from .models.audit import (
    AuditAction,
    AuditLogModel,
    AuditLogResponseModel,
    AuditSummaryModel,
)
from .models.event_response import EventResponseModel
from .models.events import EventModel
from .models.publish_request import PublishRequestModel
from .models.stats_response import StatsResponseModel
from .services.consumer import ConsumerService
from .services.redis_queue import RedisQueueService

consumer: ConsumerService = ConsumerService()
redis_queue: RedisQueueService = RedisQueueService()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await consumer.initialize()
    await consumer.start()

    logger.info("ChronicleWeaver aggregator started")

    yield

    await consumer.close()
    logger.info("ChronicleWeaver aggregator stopped")


app: FastAPI = FastAPI(
    title="ChronicleWeaver",
    description="Publish-Subscribe Log Aggregator",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get(path="/")
def root() -> dict[str, str]:
    return {"message": "ChronicleWeaver is running..."}


@app.post(path="/publish")
async def publish_events(request: PublishRequestModel) -> dict[str, str | int]:
    try:
        for event_request in request.events:
            event: EventModel = EventModel(
                event_id=event_request.event_id,
                topic=event_request.topic,
                source=event_request.source,
                payload=event_request.payload,
                timestamp=event_request.timestamp,
            )

            await consumer.log_audit(
                event_id=event.event_id,
                topic=event.topic,
                source=event.source,
                action=AuditAction.RECEIVED,
            )

            await redis_queue.push(event)

            await consumer.log_audit(
                event_id=event.event_id,
                topic=event.topic,
                source=event.source,
                action=AuditAction.QUEUED,
            )

        logger.info(f"Published {len(request.events)} events to queue")

        return {
            "status": "success",
            "message": f"Published {len(request.events)} events",
            "events_count": len(request.events),
        }
    except Exception as e:
        logger.error(f"Failed to publish events: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to publish events: {str(e)}"
        )


@app.get(path="/events", response_model=EventResponseModel)
async def get_events(topic: str | None = None) -> EventResponseModel:
    try:
        if topic is None:
            events: list[EventModel] = await consumer.get_all_events()
        else:
            events = await consumer.get_events_by_topic(topic)

        return EventResponseModel(count=len(events), events=events)
    except Exception as e:
        logger.error(f"Failed to retrieve events: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve events: {str(e)}"
        )


@app.get(path="/stats", response_model=StatsResponseModel)
async def get_stats() -> StatsResponseModel:
    try:
        stats: dict[str, object] = await consumer.get_stats()

        return StatsResponseModel(
            received=cast(int, stats["received"]),
            unique_processed=cast(int, stats["unique_processed"]),
            duplicated_dropped=cast(int, stats["duplicated_dropped"]),
            topics=cast(list[str], stats["topics"]),
            uptime=cast(int, stats["uptime"]),
        )
    except Exception as e:
        logger.error(f"Failed to retrieve stats: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve stats: {str(e)}"
        )


@app.get(path="/audit", response_model=AuditLogResponseModel)
async def get_audit_logs(
    action: str | None = Query(
        default=None,
        description="Filter by action (RECEIVED, QUEUED, PROCESSED, DROPPED, FAILED)",
    ),
    topic: str | None = Query(default=None, description="Filter by topic"),
    event_id: str | None = Query(default=None, description="Filter by event_id"),
    from_time: datetime | None = Query(
        default=None, alias="from", description="Start timestamp (ISO8601)"
    ),
    to_time: datetime | None = Query(
        default=None, alias="to", description="End timestamp (ISO8601)"
    ),
    limit: int = Query(default=100, ge=1, le=1000, description="Max records to return"),
) -> AuditLogResponseModel:
    try:
        logs: list[AuditLogModel] = await consumer.get_audit_logs(
            action=action,
            topic=topic,
            event_id=event_id,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )

        return AuditLogResponseModel(count=len(logs), audit_logs=logs)
    except Exception as e:
        logger.error(f"Failed to retrieve audit logs: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve audit logs: {str(e)}"
        )


@app.get(path="/audit/summary", response_model=AuditSummaryModel)
async def get_audit_summary() -> AuditSummaryModel:
    try:
        return await consumer.get_audit_summary()
    except Exception as e:
        logger.error(f"Failed to retrieve audit summary: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve audit summary: {str(e)}"
        )


@app.get(path="/health")
async def get_health() -> dict[str, str]:
    return {"status": "healthy"}


@app.get(path="/ready")
async def get_ready() -> dict[str, str]:
    try:
        _ = await consumer.get_stats()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Service not ready")


if __name__ == "__main__":
    import uvicorn

    APP_PORT: int = int(getenv(key="APP_PORT", default="8080"))

    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class AuditAction(str, Enum):
    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    PROCESSED = "PROCESSED"
    DROPPED = "DROPPED"
    FAILED = "FAILED"


class AuditLogModel(BaseModel):
    id: int
    event_id: str
    topic: str
    source: str
    action: AuditAction
    worker_id: int | None
    created_at: datetime


class AuditLogResponseModel(BaseModel):
    count: int
    audit_logs: list[AuditLogModel]


class AuditSummaryTopicModel(BaseModel):
    received: int = 0
    queued: int = 0
    processed: int = 0
    dropped: int = 0
    failed: int = 0


class AuditSummaryModel(BaseModel):
    total_received: int
    total_queued: int
    total_processed: int
    total_dropped: int
    total_failed: int
    by_topic: dict[str, AuditSummaryTopicModel]
    by_worker: dict[str, AuditSummaryTopicModel]

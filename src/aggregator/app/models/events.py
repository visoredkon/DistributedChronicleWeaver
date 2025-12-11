from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class EventPayloadModel(BaseModel):
    model_config = {"extra": "allow"}

    message: str
    timestamp: datetime = Field(default=..., description="ISO 8601 timestamp")


class EventModel(BaseModel):
    event_id: str
    topic: str
    source: str
    payload: EventPayloadModel
    timestamp: datetime = Field(default=..., description="ISO 8601 timestamp")

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        data: dict[str, Any] = super().model_dump(**kwargs)
        if kwargs.get("mode") == "json":
            data["timestamp"] = self.timestamp.isoformat()
            payload: dict[str, Any] = dict(data["payload"])
            payload["timestamp"] = self.payload.timestamp.isoformat()
            data["payload"] = payload
        return data

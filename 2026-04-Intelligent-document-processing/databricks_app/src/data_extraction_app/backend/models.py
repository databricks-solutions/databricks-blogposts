from datetime import datetime
from typing import Any

from pydantic import BaseModel

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class FileInfo(BaseModel):
    name: str
    path: str
    size: int
    modified_at: datetime | None


class FileListOut(BaseModel):
    files: list[FileInfo]


class FileUploadIn(BaseModel):
    name: str
    content_base64: str


class FilesUploadIn(BaseModel):
    files: list[FileUploadIn]


class JobRunTriggerOut(BaseModel):
    """Response after triggering a job run."""
    run_id: int
    job_id: str | None = None


class JobRunOut(BaseModel):
    """Current status and timing of a job run."""
    run_id: int
    life_cycle_state: str
    result_state: str | None = None
    start_time: int | None = None
    end_time: int | None = None
    execution_duration_ms: int | None = None


class AppAiQueryOut(BaseModel):
    """Response for app_ai_query table query."""
    rows: list[dict[str, Any]]
    columns: list[str] = []


class ChatMessageIn(BaseModel):
    """Single message in a chat request."""
    role: str  # "user" | "assistant" | "system"
    content: str


class ChatIn(BaseModel):
    """Request body for agent chat."""
    messages: list[ChatMessageIn]


class ChatMessageOut(BaseModel):
    """Single message in a chat response."""
    role: str
    content: str


class ChatOut(BaseModel):
    """Response from agent chat."""
    message: ChatMessageOut

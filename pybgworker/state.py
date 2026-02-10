from enum import Enum


class TaskState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    FAILED = "failed"
    DEAD = "dead"
    SUCCESS = "success"
    CANCELLED = "cancelled"


ALLOWED_TRANSITIONS = {
    TaskState.QUEUED: {TaskState.RUNNING},
    TaskState.RUNNING: {
        TaskState.SUCCESS,
        TaskState.RETRYING,
        TaskState.FAILED,
        TaskState.DEAD,
        TaskState.CANCELLED,
    },
    TaskState.RETRYING: {TaskState.RUNNING},
}


def validate_transition(old, new):
    if new not in ALLOWED_TRANSITIONS.get(TaskState(old), set()):
        raise ValueError(f"Invalid transition {old} → {new}")

from abc import ABC, abstractmethod

class BaseQueue(ABC):

    @abstractmethod
    def enqueue(self, task: dict):
        pass

    @abstractmethod
    def fetch_next(self, worker_name: str):
        pass

    @abstractmethod
    def ack(self, task_id: str):
        pass

    @abstractmethod
    def fail(self, task_id: str, error: str):
        pass

    @abstractmethod
    def reschedule(self, task_id: str, run_at):
        pass

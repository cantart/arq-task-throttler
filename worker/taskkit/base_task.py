from abc import ABC, abstractmethod
from typing import Any


class BaseTask(ABC):
    def __init__(self, ctx: dict, payload: dict, metadata: dict = None):
        self.ctx = ctx
        self.payload = payload
        self.metadata = metadata

    @abstractmethod
    async def run(self) -> Any:
        """
        Run the task with the provided context and task data.

        Returns:
            Any: The result of the task execution.
        """
        pass
from abc import ABC, abstractmethod
from typing import Any


class BaseTask(ABC):
    name = None
    retry_delay: int = 10
    max_retries: int = 3
    timeout: int = 60 # 1 minute
    
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
    
    
class AppIdempotentBaseTask(BaseTask):
    """
    Base class for idempotent tasks. (e.g. sending prompt to OpenAI API)
    
    This task should be designed to be idempotent, meaning that it can be safely retried.

    Workers must be able to retry the task without causing side effects.
    """
    
class SideEffectBaseTask(BaseTask):
    """
    Base class for side-effect tasks. (e.g. sending emails, updating databases)
    
    This task means that it cannot be safely retried.
    
    Workers want to avoid retrying the task by default with configurable options.
    
    If the task fails, it should be marked as failed and not retried.
    If the task is configured to be retried, task developers must ensure that the task is idempotent by themselves.
    """
    allow_retry: bool = False
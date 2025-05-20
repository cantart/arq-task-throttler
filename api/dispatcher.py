from abc import ABC

from arq import ArqRedis
from redis import Redis


class Dispatcher(ABC):
    """
    Dispatcher class to handle the dispatching of requests to the appropriate handler.
    """
    async def dispatch(self, task_name: str, task_data: dict):
        """
        Dispatch the request to the appropriate handler.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
class ImmediateDispatcher(Dispatcher):
    """
    ImmediateDispatcher class to handle immediate requests.
    """
    
    def __init__(self, arq: ArqRedis, redis_client: Redis):
        """
        Initialize the ImmediateDispatcher with a Redis client.

        Args:
            redis_client (Redis): The Redis client instance.
        """
        self.arq = arq
        self.redis_client = redis_client
    
    async def dispatch(self, task_name: str, task_data: dict):
        """
        Dispatch the request immediately.
        """
        # Dispatch the task immediately
        job = await self.arq.enqueue_job(task_name, task_data)
        
        # Add the job ID to the inflight set
        await self.redis_client.sadd('arq:jobs:inflight', job.job_id)
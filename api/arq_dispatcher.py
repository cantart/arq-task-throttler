from abc import ABC, abstractmethod

from arq import ArqRedis
from redis import Redis


class ConcurrencyAwareDispatcher(ABC):
    """
    ConcurrencyAwareDispatcher class to handle concurrent requests.
    """
    def __init__(self, redis_client: Redis, inflight_key: str = "arq:jobs:inflight"):
        """
        Initialize the dispatcher with a Redis client.

        Args:
            redis_client (Redis): The Redis client instance.
            inflight_key (str): The key for inflight jobs in Redis.
        """
        self.redis_client = redis_client
        self.inflight_key = inflight_key
    
    async def dispatch(self, task_name: str, task_data: dict, concurrency_dimensions: list):
        """
        Dispatch the request to the appropriate handler with concurrency control.
        """
        # Tracking key across all dispatcher instances
        for dimension in concurrency_dimensions:
            # Increase by the dimension
            await self.redis_client.incrby(f"dispatcher:concurrency:{dimension}")
            
        # Call the inner dispatch method to handle the request
        await self._inner_dispatch(task_name, task_data) 
        
    @abstractmethod
    async def _inner_dispatch(self, task_name: str, task_data: dict):
        """
        Inner dispatch method to be implemented by subclasses.
        """
        pass
        
class ImmediateArqDispatcher(ConcurrencyAwareDispatcher):
    """
    ImmediateArqDispatcher class to handle immediate requests.
    """
    
    def __init__(self, arq: ArqRedis, redis_client: Redis, inflight_key: str = "arq:jobs:inflight"):
        """
        Initialize the ImmediateArqDispatcher with a Redis client.

        Args:
            redis_client (Redis): The Redis client instance.
        """
        super().__init__(redis_client, inflight_key)
        self.arq = arq
    
    async def _inner_dispatch(self, task_name: str, task_data: dict):
        """
        Dispatch the request immediately.
        """
        # Dispatch the task immediately
        job = await self.arq.enqueue_job(task_name, task_data)
        
        # Add the job ID to the inflight set
        await self.redis_client.sadd(self.inflight_key, job.job_id)
        
        

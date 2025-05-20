from arq import ArqRedis
from redis import Redis


class ConcurrencyAwareArqDispatcher:
    """
    ConcurrencyAwareArqDispatcher class to handle concurrent requests.
    """
    def __init__(self, arq: ArqRedis, redis_client: Redis, inflight_key: str = "arq:jobs:inflight"):
        """
        Initialize the dispatcher with a Redis client.

        Args:
            arq (ArqRedis): The Arq Redis client instance.
            redis_client (Redis): The Redis client instance.
            inflight_key (str): The key for inflight jobs in Redis.
        """
        self.arq = arq
        self.redis_client = redis_client
        self.inflight_key = inflight_key
    
    async def dispatch(self, task_name: str, task_data: dict, task_metadata: dict = None):
        """
        Dispatch the request to the appropriate handler with concurrency control.
        """
        concurrency_dimensions = task_metadata.get("_concurrency_dimensions", [])
        
        # Tracking key across all dispatcher instances
        self.increse_concurrency(concurrency_dimensions)
            
        # Dispatch the task immediately
        job = await self.arq.enqueue_job(task_name, task_data, task_metadata)
        
        # Add the job ID to the inflight set
        await self.redis_client.sadd(self.inflight_key, job.job_id)

    async def increse_concurrency(self, dimensions: list):
        """
        Increase the concurrency for the specified dimensions.
        """
        
        for dimension in dimensions:
            await self.redis_client.incrby(f"dispatcher:concurrency:{dimension}")
            
    async def decrease_concurrency(self, dimensions: list):
        """
        Decrease the concurrency for the specified dimensions.
        """
        
        for dimension in dimensions:
            await self.redis_client.decrby(f"dispatcher:concurrency:{dimension}")
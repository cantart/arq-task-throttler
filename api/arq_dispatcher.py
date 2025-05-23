import asyncio
import json
import time

from arq import ArqRedis
from redis import Redis
from throttling.policy_base import ThrottlingPolicy


class ConcurrencyAwareArqDispatcher:
    """
    ConcurrencyAwareArqDispatcher class to handle concurrent requests.
    """
    def __init__(self, arq: ArqRedis, redis_client: Redis, throttling_policy: ThrottlingPolicy = None, inflight_key: str = "arq:jobs:inflight", queue_key: str = "dispatcher:queue"):
        """
        Initialize the dispatcher with a Redis client.

        Args:
            arq (ArqRedis): The Arq Redis client instance.
            redis_client (Redis): The Redis client instance.
            throttling_policy (ThrottlingPolicy): The throttling policy instance.
            poll_interval (float): The interval for polling the dispatcher queue.
            inflight_key (str): The key for inflight jobs in Redis.
            queue_key (str): The key for the dispatcher queue in Redis.
        """
        self.arq = arq
        self.redis_client = redis_client
        self.throttling_policy = throttling_policy
        self.inflight_key = inflight_key
        self.queue_key = queue_key
        
    async def start(self):
        """
        Start the dispatcher to redispatch tasks.
        """
        print("[ConcurrencyAwareArqDispatcher] Starting dispatcher...")
        self._running = True
        lock = self.redis_client.lock("dispatcher:lock", timeout=10)
        
        async def run_loop():
            while self._running:
                print("[ConcurrencyAwareArqDispatcher] Acquiring lock...")
                got_lock = await lock.acquire(blocking_timeout=0.5)
                if got_lock:
                    print("[ConcurrencyAwareArqDispatcher] Lock acquired.")
                    try:
                        # Get the next task from the dispatcher queue
                        raw = await self.redis_client.lpop(self.queue_key)
                        if not raw:
                            await asyncio.sleep(1)  # Sleep for a while if no task is available
                            continue
                        
                        # Parse the task data
                        dispatch_args = self._decode_dispatch_args(raw)
                        task_name, task_data, task_metadata = dispatch_args
                        
                        retry_dispatch_interval = task_metadata.get("_retry_dispatch_interval", 10)
                        dispatched_at = task_metadata.get("_dispatched_at", 0)
                        now = int(time.time()) # epoch timestamp
                        if now - dispatched_at < retry_dispatch_interval:
                            # If the task is not ready to be retried, re-add it to the queue
                            await self.redis_client.rpush(self.queue_key, raw)
                            print(f"[ConcurrencyAwareArqDispatcher] Task {task_name} is not ready to be retried. Re-adding to queue.")
                            continue
                        
                        # Dispatch the task
                        await self.dispatch(task_name, task_data, task_metadata)
                    finally:
                        print(f"[ConcurrencyAwareArqDispatcher] Releasing lock after processing task.")
                        await lock.release()
            print("[ConcurrencyAwareArqDispatcher] Stopped dispatcher.")
        asyncio.create_task(run_loop())
        
    async def stop(self):
        """
        Stop the dispatcher.
        """
        print("[ConcurrencyAwareArqDispatcher] Stopping dispatcher...")
        self._running = False
    
    async def dispatch(self, task_name: str, task_data: dict, task_metadata: dict = None):
        """
        Dispatch the request to the appropriate handler with concurrency control.
        """
        now = int(time.time()) # epoch timestamp
        task_metadata["_dispatched_at"] = now
        
        concurrency_dimensions = task_metadata.get("_concurrency_dimensions", [])
        
        if not self.throttling_policy:
            # Tracking key across all dispatcher instances
            await self.increase_concurrency(concurrency_dimensions)
                
            # Dispatch the task immediately
            job = await self.arq.enqueue_job(task_name, task_data, task_metadata)
            
            # Add the job ID to the inflight set
            await self.redis_client.sadd(self.inflight_key, job.job_id)
        else:
            # If it is not allowed at least one dimension, add the job to dispatcher queue
            for dimension in concurrency_dimensions:
                current_value = await self.redis_client.get(f"dispatcher:concurrency:{dimension}")
                if current_value is None:
                    current_value = 0
                else:
                    current_value = int(current_value)
                if not self.throttling_policy.is_allowed(dimension, current_value):
                    # Add the job to dispatcher queue with dispatching argument used to redispatch
                    dispatch_args = self._encode_dispatch_args(task_name, task_data, task_metadata)
                    await self.redis_client.rpush(self.queue_key, dispatch_args)
                    print(f"[ConcurrencyAwareArqDispatcher] Task {task_name} is not allowed for dimension {dimension}. Added to dispatcher queue.")
                    return
                
            # If all dimensions are allowed, increase concurrency and dispatch the task
            await self.increase_concurrency(concurrency_dimensions)
            job = await self.arq.enqueue_job(task_name, task_data, task_metadata)
            await self.redis_client.sadd(self.inflight_key, job.job_id)
            
    def _encode_dispatch_args(self, task_name: str, task_data: dict, task_metadata: dict):
        """
        Encode the dispatch arguments to a JSON-like format.
        """
        return json.dumps({
            "task_name": task_name,
            "task_data": task_data,
            "task_metadata": task_metadata,
        })
        
    def _decode_dispatch_args(self, dispatch_args: str):
        """
        Decode the dispatch arguments from a JSON-like format.
        """
        decoded_args = json.loads(dispatch_args)
        return decoded_args.get("task_name"), decoded_args.get("task_data"), decoded_args.get("task_metadata")
    
    async def increase_concurrency(self, dimensions: list):
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
            # Check whether the dimension exists or value is greater than 0
            current_value = await self.redis_client.get(f"dispatcher:concurrency:{dimension}")
            if current_value is None or int(current_value) <= 0:
                return
            
            await self.redis_client.decrby(f"dispatcher:concurrency:{dimension}")
import asyncio
from typing import Awaitable, Callable, Optional

import redis.asyncio as redis
from arq.jobs import Job, JobStatus
from arq_dispatcher import ConcurrencyAwareArqDispatcher


class ArqJobResultCollector:
    def __init__(
        self,
        redis_client: redis.Redis,
        dispatcher: ConcurrencyAwareArqDispatcher,
        poll_interval: float = 2.0,
        inflight_key: str = "arq:jobs:inflight",
        on_result: Optional[Callable[[str, str, dict | None], Awaitable[None]]] = None,
        verbose: bool = False,
    ):
        self.redis = redis_client
        self.dispatcher = dispatcher
        self.poll_interval = poll_interval
        self.inflight_key = inflight_key
        self.on_result = on_result

        # Instance attributes
        self._running = False
        
        # Verbose mode
        self.verbose = verbose
    

    async def start(self):
        if self.verbose:
            print("[ArqJobResultCollector] Starting result collector...")
        self._running = True
        lock = self.redis.lock("arq:result-collector", timeout=10)
        
        async def run_loop():
            while self._running:
                got_lock = await lock.acquire(blocking_timeout=0.5)
                if got_lock:
                    try:
                        await self._collect_once()
                    finally:
                        await lock.release()
                await asyncio.sleep(self.poll_interval)
            if self.verbose:
                print("[ArqJobResultCollector] Stopped result collector.")
        asyncio.create_task(run_loop())
        
    async def stop(self):
        if self.verbose:
            print("[ArqJobResultCollector] Stopping result collector...")
        self._running = False

    async def _collect_once(self):
        job_ids = await self.redis.smembers(self.inflight_key)
        if not job_ids:
            return

        for job_id_bytes in job_ids:
            job_id = job_id_bytes.decode()
            if self.verbose:
                print(f"[ArqJobResultCollector] Collecting result for job ID: {job_id}")
            job = Job(job_id=job_id, redis=self.redis)
            
            # Get the job status
            status = await job.status()
            if status == JobStatus.complete:
                # Package the job result as a dictionary even if it's exception
                try:
                    job_result = {
                        "result": await job.result(),
                    }
                except Exception as e:
                    job_result = {
                        "error": str(e),
                    }
                    
                if self.verbose:
                    print(f"[ArqJobResultCollector] Collected result for {job_id} → {job_result}")
                await self.redis.srem(self.inflight_key, job_id)
                
                job_info = await job.info()
                concurrency_dimensions = job_info.args[1].get("_concurrency_dimensions", [])
                await self.dispatcher.decrease_concurrency(concurrency_dimensions)
                    
                if self.on_result:
                    await self.on_result(job_id, status, job_result)
            else:
                if self.on_result:
                    await self.on_result(job_id, status)

import uuid
from contextlib import asynccontextmanager

import redis
import redis.asyncio
from arq import create_pool
from arq.connections import RedisSettings
from arq_dispatcher import ConcurrencyAwareArqDispatcher
from arq_result_collector import ArqJobResultCollector
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from persistence import ConnectorRepository
from pydantic import BaseModel
from service import AccountService
from throttling import StaticThrottlingPolicy

REDIS_SETTINGS = RedisSettings(
    host="redis",
    port=6379
)

async def handle_task_result(task_id: str, task_status: str, task_result: dict = None):
    """Handle the result of a task."""
    # Here you can implement your logic to handle the task result
    print(f"Task {task_id} status: {task_status}")
    if task_result:
        print(f"Task {task_id} result: {task_result}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI."""
    # Startup
    app.state.arq = await create_pool(REDIS_SETTINGS)
    app.state.redis_client = redis.asyncio.Redis(
        host="redis",
        port=6379,
    )
    
    # Construct the throttling policy
    limit_config = {}
    account_service = AccountService()
    accounts = account_service.get_all_accounts()

    for account in accounts:
        limit_config[f"account:{account['id']}"] = account['max_concurrency']
        print(f"Policy added for account: {account['id']} with limit: {account['max_concurrency']}")
        
    connector_repository = ConnectorRepository()
    connectors = connector_repository.get_all_connectors()
    for connector in connectors:
        limit_config[f"connector:{connector['id']}"] = connector['max_concurrency']
        print(f"Policy added for connector: {connector['id']} with limit: {connector['max_concurrency']}")
    limit_config["cluster"] = 10
        
    static_policy = StaticThrottlingPolicy(limit_config=limit_config)
    dispatcher = ConcurrencyAwareArqDispatcher(
        arq=app.state.arq,
        redis_client=app.state.redis_client,
        throttling_policy=static_policy,
    )
    await dispatcher.start()
    app.state.dispatcher = dispatcher
    
    result_collector = ArqJobResultCollector(
        redis_client=app.state.redis_client,
        dispatcher=dispatcher,
        on_result=handle_task_result,
    )
    await result_collector.start()
    app.state.result_collector = result_collector
    
    yield
    
    # Shutdown
    await app.state.arq.aclose()
    await app.state.redis_client.close()
    await app.state.result_collector.stop()
    await app.state.dispatcher.stop()

app = FastAPI(lifespan=lifespan)

class TaskSubmissionRequest(BaseModel):
    """Response model for task submission."""
    task_name: str
    task_data: dict

@app.post('/task/{task_name}')
async def submit_task(request: TaskSubmissionRequest):
    dispatcher: ConcurrencyAwareArqDispatcher = app.state.dispatcher
    
    """Submit a task to the queue."""
    task_name = request.task_name
    task_data = request.task_data

    # Map task names to required argument keys
    task_args = {
        'download_content': ['url'],
        'greeting': ['name'],
        'long_running_task_block': [],
        'long_running_task_non_block': [],
    }

    arg_keys = task_args.get(task_name)
    if arg_keys is None:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={'message': 'Invalid task name'}
        )

    for arg_key in arg_keys:
        if arg_key not in task_data:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={'message': f'{arg_key.capitalize()} is required for {task_name} task'}
            )

    # Dispatch the task
    await dispatcher.dispatch(
        task_name=task_name,
        task_data=task_data,
        task_metadata={
            "_concurrency_dimensions": ["account:acct-001", "connector:conn-001", "cluster"],
        }
    )
    
    return JSONResponse({
        'task_id': uuid.uuid4().hex,
    })
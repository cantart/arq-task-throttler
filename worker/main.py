from arq.connections import RedisSettings
from httpx import AsyncClient
from taskkit import arq_task_wrapper
from tasks import (BlockingLongRunningTask, DownloadContentTask, ErrorTask,
                   GreetingTask, NonBlockingLongRunningTask,
                   SideEffectErrorTask,
                   SideEffectNonBlockingLongRunningWithErrorTask)

# Here you can configure the Redis connection.
# The default is to connect to localhost:6379, no password.
REDIS_SETTINGS = RedisSettings(
    host="redis",
    port=6379
)

async def startup(ctx):
    ctx['session'] = AsyncClient()

async def shutdown(ctx):
    await ctx['session'].aclose()

# WorkerSettings defines the settings to use when creating the work,
# It's used by the arq CLI.
# redis_settings might be omitted here if using the default settings
# For a list of all available settings, see https://arq-docs.helpmanual.io/#arq.worker.Worker
class WorkerSettings:
    functions = [
        arq_task_wrapper(DownloadContentTask),
        arq_task_wrapper(GreetingTask),
        arq_task_wrapper(BlockingLongRunningTask),
        arq_task_wrapper(NonBlockingLongRunningTask),
        arq_task_wrapper(ErrorTask),
        arq_task_wrapper(SideEffectErrorTask),
        arq_task_wrapper(SideEffectNonBlockingLongRunningWithErrorTask)
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS
    # allow_abort_jobs = True
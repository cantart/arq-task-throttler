from arq.connections import RedisSettings
from httpx import AsyncClient
from taskkit import arq_task_wrapper
from tasks import (BlockingLongRunningTask, DownloadContentTask, GreetingTask,
                   NonBlockingLongRunningTask)

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
        arq_task_wrapper(DownloadContentTask, name="download_content"),
        arq_task_wrapper(GreetingTask, name="greeting"),
        arq_task_wrapper(BlockingLongRunningTask, name="long_running_task_block"),
        arq_task_wrapper(NonBlockingLongRunningTask, name="long_running_task_non_block"),
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS
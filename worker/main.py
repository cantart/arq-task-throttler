from arq.connections import RedisSettings
from httpx import AsyncClient

# Here you can configure the Redis connection.
# The default is to connect to localhost:6379, no password.
REDIS_SETTINGS = RedisSettings(
    host="redis",
    port=6379
)

async def download_content(ctx, task_data: dict):
    """Download content from a URL."""
    url = task_data.get('url')
    session: AsyncClient = ctx['session']
    response = await session.get(url)
    print(f'{url}: {response.text:.80}...')
    return len(response.text)

async def greeting(ctx, task_data: dict):
    """Send a greeting message."""
    name = task_data.get('name')
    message = f"Hello, {name}"
    return message

async def long_running_task_block(ctx, task_data: dict):
    """Simulate a long-running task."""
    import time
    time.sleep(20)
    return "Long-running task completed (blocking)"

async def long_running_task_non_block(ctx, task_data: dict):
    """Simulate a long-running task."""
    import asyncio
    await asyncio.sleep(20)
    return "Long-running task completed (non-blocking)"

async def startup(ctx):
    ctx['session'] = AsyncClient()

async def shutdown(ctx):
    await ctx['session'].aclose()

# WorkerSettings defines the settings to use when creating the work,
# It's used by the arq CLI.
# redis_settings might be omitted here if using the default settings
# For a list of all available settings, see https://arq-docs.helpmanual.io/#arq.worker.Worker
class WorkerSettings:
    functions = [download_content, greeting, long_running_task_block, long_running_task_non_block]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS
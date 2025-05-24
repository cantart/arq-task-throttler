from tasks.base_task import AppIdempotentBaseTask


class NonBlockingLongRunningTask(AppIdempotentBaseTask):
    """
    Task to simulate a non-blocking long-running task.
    """
    name = 'long_running_task_non_block'

    async def run(self) -> str:
        """
        Run the non-blocking long-running task.

        Returns:
            str: The result of the task execution.
        """
        # Simulate a non-blocking long-running task
        import asyncio
        await asyncio.sleep(10)  # Non-blocking sleep for 10 seconds
        return "Non-blocking long-running task completed."
from taskkit.base_task import AppIdempotentBaseTask


class BlockingLongRunningTask(AppIdempotentBaseTask):
    """
    Task to simulate a blocking long-running task.
    """
    name = 'long_running_task_block'

    async def run(self) -> str:
        """
        Run the blocking long-running task.

        Returns:
            str: The result of the task execution.
        """
        # Simulate a blocking long-running task
        import time
        time.sleep(10)  # Blocking sleep for 10 seconds
        return "Blocking long-running task completed."
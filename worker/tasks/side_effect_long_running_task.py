import asyncio

from taskkit.base_task import SideEffectBaseTask


class SideEffectNonBlockingLongRunningWithErrorTask(SideEffectBaseTask):
    """
    Task to simulate a non-blocking long-running task with side effects and an error.
    """

    async def run(self):
        """
        Run the task and raise an error.
        """
        await asyncio.sleep(20)
        raise Exception("This is a test error from SideEffectErrorTask.")
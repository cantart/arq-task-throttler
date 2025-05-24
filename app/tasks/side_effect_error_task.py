from tasks.base_task import SideEffectBaseTask


class SideEffectErrorTask(SideEffectBaseTask):
    """
    A task that raises an error to test the error handling of the worker.
    """

    async def run(self):
        """
        Run the task and raise an error.
        """
        raise Exception("This is a test error from SideEffectErrorTask.")
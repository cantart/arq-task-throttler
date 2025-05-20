from taskkit import arq_task_wrapper
from taskkit.base_task import BaseTask


class GreetingTask(BaseTask):
    """
    Task to send a greeting message.
    """

    async def run(self) -> str:
        """
        Run the task to send a greeting message.

        Returns:
            str: The greeting message.
        """
        name = self.payload.get('name')
        message = f"Hello, {name}"
        return message
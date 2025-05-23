from taskkit.base_task import AppIdempotentBaseTask


class GreetingTask(AppIdempotentBaseTask):
    """
    Task to send a greeting message.
    """
    name = 'greeting'

    async def run(self) -> str:
        """
        Run the task to send a greeting message.

        Returns:
            str: The greeting message.
        """
        name = self.payload.get('name')
        message = f"Hello, {name}"
        return message
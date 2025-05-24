from schema import TaskIoField
from tasks.base_task import AppIdempotentBaseTask


class GreetingTask(AppIdempotentBaseTask):
    """
    Task to send a greeting message.
    """
    name = 'greeting'
    input_schema = [TaskIoField(name='name', type=str)]
    output_schema = [TaskIoField(name='message', type=str)]

    async def run(self) -> dict:
        """
        Run the task to send a greeting message.
        """
        name_value = self.payload.get('name', 'World')
        message = f"Hello, {name_value}!"
        return {'message': message}
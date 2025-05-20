from taskkit import arq_task_wrapper
from taskkit.base_task import BaseTask


class DownloadContentTask(BaseTask):
    """
    Task to download content from a URL.
    """

    async def run(self) -> int:
        """
        Run the task to download content from the specified URL.

        Returns:
            int: The length of the downloaded content.
        """
        url = self.payload.get('url')
        session = self.ctx['session']
        response = await session.get(url)
        print(f'{url}: {response.text:.80}...')
        return len(response.text)
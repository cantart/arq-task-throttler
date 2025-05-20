from taskkit.base_task import BaseTask


def arq_task_wrapper(task_cls: type[BaseTask], name: str = None):
    async def _wrapped(ctx, payload, metadata):
        task = task_cls(ctx, payload, metadata)
        return await task.run()
    _wrapped.__qualname__ = name or task_cls.__name__
    return _wrapped
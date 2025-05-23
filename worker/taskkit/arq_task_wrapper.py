from arq import Retry, func
from taskkit.base_task import AppIdempotentBaseTask, SideEffectBaseTask


def arq_task_wrapper(
    task_cls: type[AppIdempotentBaseTask] | type[SideEffectBaseTask], 
):
    if not issubclass(task_cls, (AppIdempotentBaseTask, SideEffectBaseTask)):
        raise TypeError(f"task_cls must be a subclass of AppIdempotentBaseTask or SideEffectBaseTask, got {task_cls.__name__}")
    
    if issubclass(task_cls, AppIdempotentBaseTask):
        async def _wrapped(ctx, payload, metadata):
            try:
                task = task_cls(ctx, payload, metadata)
                return await task.run()
            except Exception as e:
                print(f"Task {task_cls.__name__} failed with exception: {e}")
                raise Retry(defer=task_cls.retry_delay)
        return func(
            _wrapped,
            name=task_cls.name or task_cls.__name__,
            max_tries=task_cls.max_retries,
            timeout=task_cls.timeout,
        )
    elif issubclass(task_cls, SideEffectBaseTask):
        async def _wrapped(ctx, payload, metadata):
            if task_cls.allow_retry:
                try:
                    task = task_cls(ctx, payload, metadata)
                    return await task.run()
                except Exception as e:
                    print(f"Task {task_cls.__name__} failed with exception: {e}")
                    raise Retry(defer=task_cls.retry_delay)
            else:
                task = task_cls(ctx, payload, metadata)
                return await task.run()
        return func(
            _wrapped,
            name=task_cls.name or task_cls.__name__,
            max_tries=task_cls.max_retries if task_cls.allow_retry else 1,
            timeout=task_cls.timeout,
        )
    
    

        
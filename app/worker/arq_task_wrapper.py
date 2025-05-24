from arq import Retry, func
from pydantic import ValidationError, create_model
from tasks.base_task import AppIdempotentBaseTask, SideEffectBaseTask


def arq_task_wrapper(
    task_cls: type[AppIdempotentBaseTask] | type[SideEffectBaseTask], 
):
    if not issubclass(task_cls, (AppIdempotentBaseTask, SideEffectBaseTask)):
        raise TypeError(f"task_cls must be a subclass of AppIdempotentBaseTask or SideEffectBaseTask, got {task_cls.__name__}")
    
    if issubclass(task_cls, AppIdempotentBaseTask):
        async def _wrapped(ctx, payload, metadata):
            try:
                 # Create a dynamic Pydantic model for input validation
                input_fields = {param.name: (param.type, ...) for param in task_cls.input_schema}
                InputModel = create_model(f"{task_cls.name or 'BaseTask'}InputModel", **input_fields)
                payload = InputModel(**payload).model_dump()
                
                task = task_cls(ctx, payload, metadata)
                result = await task.run()
                
                # Output validation
                output_fields = {param.name: (param.type, ...) for param in task.output_schema}
                OutputModel = create_model(f"{task_cls.name or task_cls.__name__}OutputModel", **output_fields)
                validated_result = OutputModel(**result).model_dump()
                return validated_result
            except ValidationError as ve:
                print(f"Validation error in task {task_cls.__name__}: {ve}")
                raise ve
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
                    # Create a dynamic Pydantic model for input validation
                    input_fields = {param.name: (param.type, ...) for param in task_cls.input_schema}
                    InputModel = create_model(f"{task_cls.name or 'BaseTask'}InputModel", **input_fields)
                    payload = InputModel(**payload).model_dump()
                    
                    task = task_cls(ctx, payload, metadata)
                    result = await task.run()

                    # Output validation
                    output_fields = {param.name: (param.type, ...) for param in task.output_schema}
                    OutputModel = create_model(f"{task_cls.name or task_cls.__name__}OutputModel", **output_fields)
                    validated_result = OutputModel(**result).model_dump()
                    return validated_result
                except ValidationError as ve:
                    print(f"Validation error in task {task_cls.__name__}: {ve}")
                    raise ve
                except Exception as e:
                    print(f"Task {task_cls.__name__} failed with exception: {e}")
                    raise Retry(defer=task_cls.retry_delay)
            else:
                # Create a dynamic Pydantic model for input validation
                input_fields = {param.name: (param.type, ...) for param in task_cls.input_schema}
                InputModel = create_model(f"{task_cls.name or 'BaseTask'}InputModel", **input_fields)
                payload = InputModel(**payload).model_dump()
                
                task = task_cls(ctx, payload, metadata)
                result = await task.run()

                # Output validation
                output_fields = {param.name: (param.type, ...) for param in task.output_schema}
                OutputModel = create_model(f"{task_cls.name or task_cls.__name__}OutputModel", **output_fields)
                validated_result = OutputModel(**result).model_dump()
                return validated_result
        return func(
            _wrapped,
            name=task_cls.name or task_cls.__name__,
            max_tries=task_cls.max_retries if task_cls.allow_retry else 1,
            timeout=task_cls.timeout,
        )
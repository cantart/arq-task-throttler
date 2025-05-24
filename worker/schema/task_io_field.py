from typing import Type

from pydantic import BaseModel


class TaskIoField(BaseModel):
    name: str
    type: Type

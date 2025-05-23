from abc import ABC, abstractmethod


class ThrottlingPolicy(ABC):
    @abstractmethod
    def is_allowed(self, dimension: str, current: int) -> bool:
        """Return True if task is allowed based on current usage."""
        pass
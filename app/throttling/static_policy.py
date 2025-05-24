from throttling.policy_base import ThrottlingPolicy


class StaticThrottlingPolicy(ThrottlingPolicy):
    def __init__(self, limit_config: dict):
        """
        limit_config: {
            "account:acct-001": 10,
            "connector:conn-001": 1
            "cluster": 100
        }
        """
        self.limits = limit_config

    def is_allowed(self, dimension: str, current: int) -> bool:
        limit = self._get_limit(dimension)
        return (limit is None) or (current < limit)

    def _get_limit(self, dimension: str) -> int:
        return self.limits.get(dimension, float("inf"))
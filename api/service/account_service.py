# Example of account service representing an interaction with an identity service


class AccountService:
    def get_all_accounts(self):
        # we assume that we have
        return [
            {"id": "acct-001", "name": "Account 1", "max_concurrency": 10, "cluster_tier": 1},
            {"id": "acct-002", "name": "Account 2", "max_concurrency": 5, "cluster_tier": 2},
            {"id": "acct-003", "name": "Account 3", "max_concurrency": 10, "cluster_tier": 1},
            {"id": "acct-004", "name": "Account 4", "max_concurrency": 20, "cluster_tier": 1},
        ]
# Example of an application resource repository

class ConnectorRepository:
    def get_all_connectors(self):
        # we assume that we have
        return [
            {"id": "conn-001", "name": "Connector 1", "max_concurrency": 1},
            {"id": "conn-002", "name": "Connector 2", "max_concurrency": 1},
            {"id": "conn-003", "name": "Connector 3", "max_concurrency": 2},
        ]
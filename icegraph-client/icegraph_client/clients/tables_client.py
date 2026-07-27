import requests


class TablesClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def list_tables(self) -> list[str]:
        response = requests.get(f"{self.base_url}/api/v1/tables", **self.requests_kwargs)
        response.raise_for_status()

        return response.json()["tables"]

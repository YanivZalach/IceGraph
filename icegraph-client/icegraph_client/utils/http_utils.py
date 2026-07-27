import requests


def raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        try:
            message = response.json().get("error", response.text)
        except ValueError:
            message = response.text

        raise requests.HTTPError(f"{response.status_code} {response.reason} for {response.url}: {message}", response=response) from e

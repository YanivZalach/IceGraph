"""Permanently record a valid CLA acceptance on a dedicated Git branch."""

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

CLA_VERSION = "1.0"
CLA_STATEMENT = (
    "I have read and agree to the IceGraph Individual Contributor License Agreement, "
    f"Harmony HA-CLA-I-ANY version {CLA_VERSION}, "
    "and I confirm that I have authority to submit my contribution."
)
RECORDS_BRANCH = "cla-records"
MAX_PUSH_ATTEMPTS = 5
RECORD_FIELDS = {
    "github_login",
    "github_user_id",
    "cla_version",
    "cla_sha256",
    "accepted_at",
    "pull_request",
    "comment_id",
    "comment_url",
    "statement",
}


def github_api(arguments, payload=None, allow_failure=False):
    command = ["gh", "api", *arguments]
    if payload is not None:
        command.extend(["--input", "-"])
    result = subprocess.run(
        command,
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode and not allow_failure:
        raise RuntimeError(result.stderr.strip())
    return result


def main():
    repo = os.environ["GITHUB_REPOSITORY"]
    with open(os.environ["GITHUB_EVENT_PATH"]) as file:
        event = json.load(file)

    # Ignore every comment except a personal, exact acceptance of the current CLA.
    comment = event["comment"]
    account = comment["user"]
    if account.get("type") == "Bot" or comment["body"] != CLA_STATEMENT:
        print("Comment is not a CLA acceptance; nothing to record.")
        return 0

    with open("CLA.md", "rb") as file:
        cla_hash = hashlib.sha256(file.read()).hexdigest()

    accepted_at = comment["created_at"]
    if event["action"] == "edited":
        accepted_at = comment["updated_at"]

    record = {
        "github_login": account["login"].lower(),
        "github_user_id": account["id"],
        "cla_version": CLA_VERSION,
        "cla_sha256": cla_hash,
        "accepted_at": accepted_at,
        "pull_request": event["issue"]["number"],
        "comment_id": comment["id"],
        "comment_url": comment["html_url"],
        "statement": CLA_STATEMENT,
    }
    record_path = f"signers/v{CLA_VERSION}/{account['id']}.json"
    record_content = json.dumps(record, indent=2, sort_keys=True) + "\n"

    try:
        # Each attempt reads the latest branch tip. A stale, non-fast-forward update
        # fails safely, then the next attempt rebuilds the commit on the new tip.
        for attempt in range(MAX_PUSH_ATTEMPTS):
            existing_result = github_api(
                [
                    "--method",
                    "GET",
                    f"repos/{repo}/contents/{record_path}",
                    "-f",
                    f"ref={RECORDS_BRANCH}",
                ],
                allow_failure=True,
            )
            if existing_result.returncode == 0:
                response = json.loads(existing_result.stdout)
                existing = json.loads(base64.b64decode(response["content"]).decode())
                missing_fields = RECORD_FIELDS - existing.keys()
                valid_url = None
                if isinstance(existing.get("comment_url"), str):
                    valid_url = re.fullmatch(
                        r"https://github\.com/([^/]+/[^/]+)/pull/(\d+)#issuecomment-(\d+)",
                        existing["comment_url"],
                        re.IGNORECASE,
                    )
                valid_timestamp = False
                if isinstance(existing.get("accepted_at"), str) and re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
                    existing["accepted_at"],
                ):
                    try:
                        datetime.strptime(
                            existing["accepted_at"], "%Y-%m-%dT%H:%M:%S%z"
                        )
                        valid_timestamp = True
                    except ValueError:
                        pass
                valid_metadata = (
                    not missing_fields
                    and isinstance(existing["github_login"], str)
                    and bool(existing["github_login"])
                    and type(existing["github_user_id"]) is int
                    and type(existing["pull_request"]) is int
                    and existing["pull_request"] > 0
                    and type(existing["comment_id"]) is int
                    and existing["comment_id"] > 0
                    and valid_url
                    and valid_url.group(1).lower() == repo.lower()
                    and int(valid_url.group(2)) == existing["pull_request"]
                    and int(valid_url.group(3)) == existing["comment_id"]
                    and valid_timestamp
                )
                same_agreement = (
                    existing.get("github_user_id") == record["github_user_id"]
                    and existing.get("cla_version") == record["cla_version"]
                    and existing.get("cla_sha256") == record["cla_sha256"]
                    and existing.get("statement") == record["statement"]
                )
                if valid_metadata and same_agreement:
                    print(f"CLA acceptance already recorded at {record_path}.")
                    return 0
                raise RuntimeError(
                    f"Invalid or conflicting CLA record at {record_path}"
                )
            if "HTTP 404" not in existing_result.stderr:
                raise RuntimeError(existing_result.stderr.strip())

            tip_result = github_api(
                [f"repos/{repo}/git/ref/heads/{RECORDS_BRANCH}"],
                allow_failure=True,
            )
            if tip_result.returncode == 0:
                parent_sha = json.loads(tip_result.stdout)["object"]["sha"]
            elif "HTTP 404" in tip_result.stderr:
                parent_sha = None
            else:
                raise RuntimeError(tip_result.stderr.strip())

            blob = json.loads(
                github_api(
                    ["-X", "POST", f"repos/{repo}/git/blobs"],
                    {"content": record_content, "encoding": "utf-8"},
                ).stdout
            )
            tree_payload = {
                "tree": [
                    {
                        "path": record_path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob["sha"],
                    }
                ]
            }
            if parent_sha:
                parent = json.loads(
                    github_api([f"repos/{repo}/git/commits/{parent_sha}"]).stdout
                )
                tree_payload["base_tree"] = parent["tree"]["sha"]
            tree = json.loads(
                github_api(
                    ["-X", "POST", f"repos/{repo}/git/trees"], tree_payload
                ).stdout
            )

            commit_payload = {
                "message": (
                    f"Record CLA v{CLA_VERSION} acceptance for GitHub user {account['id']}"
                ),
                "tree": tree["sha"],
                "parents": [parent_sha] if parent_sha else [],
            }
            commit = json.loads(
                github_api(
                    ["-X", "POST", f"repos/{repo}/git/commits"], commit_payload
                ).stdout
            )

            if parent_sha:
                update_result = github_api(
                    [
                        "-X",
                        "PATCH",
                        f"repos/{repo}/git/refs/heads/{RECORDS_BRANCH}",
                    ],
                    {"sha": commit["sha"], "force": False},
                    allow_failure=True,
                )
            else:
                update_result = github_api(
                    ["-X", "POST", f"repos/{repo}/git/refs"],
                    {
                        "ref": f"refs/heads/{RECORDS_BRANCH}",
                        "sha": commit["sha"],
                    },
                    allow_failure=True,
                )

            if update_result.returncode == 0:
                print(f"Recorded CLA acceptance at {record_path}.")
                return 0
            if (
                "HTTP 409" not in update_result.stderr
                and "HTTP 422" not in update_result.stderr
            ):
                raise RuntimeError(update_result.stderr.strip())
            time.sleep(2**attempt)

        raise RuntimeError("CLA records branch kept changing; retry limit reached")
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"::error::Could not record CLA acceptance: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

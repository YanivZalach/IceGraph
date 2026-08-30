"""Permanently record a valid CLA acceptance on a dedicated Git branch."""

import base64
import hashlib
import json
import os
import subprocess
import sys
import time

CLA_PATH = "CLA.md"
CLA_VERSION = "1.0"
RECORDS_BRANCH = "cla-records"
MAX_PUSH_ATTEMPTS = 5


def statement(version):
    return (
        "I have read and agree to the IceGraph Individual Contributor License Agreement, "
        f"Harmony HA-CLA-I-ANY version {version}, "
        "and I confirm that I have authority to submit my contribution."
    )


def gh(args, payload=None, check=True):
    command = ["gh", "api", *args]
    if payload is not None:
        command.extend(["--input", "-"])
    result = subprocess.run(
        command,
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def api_json(args, payload=None):
    return json.loads(gh(args, payload).stdout)


def event_record(event):
    comment = event["comment"]
    account = comment["user"]
    if account.get("type") == "Bot":
        return None
    if comment["body"].strip() != statement(CLA_VERSION):
        return None

    with open(CLA_PATH, "rb") as file:
        cla_hash = hashlib.sha256(file.read()).hexdigest()

    return {
        "github_login": account["login"].lower(),
        "github_user_id": account["id"],
        "cla_version": CLA_VERSION,
        "cla_sha256": cla_hash,
        "accepted_at": (comment["updated_at"] if event["action"] == "edited" else comment["created_at"]),
        "pull_request": event["issue"]["number"],
        "comment_id": comment["id"],
        "comment_url": comment["html_url"],
        "statement": statement(CLA_VERSION),
    }


def record_path(record):
    return f"signers/v{record['cla_version']}/{record['github_user_id']}.json"


def encoded_record(record):
    return json.dumps(record, indent=2, sort_keys=True) + "\n"


def existing_record(repo, path):
    result = gh(
        [
            "--method",
            "GET",
            f"repos/{repo}/contents/{path}",
            "-f",
            f"ref={RECORDS_BRANCH}",
        ],
        check=False,
    )
    if result.returncode:
        if "HTTP 404" in result.stderr:
            return None
        raise RuntimeError(result.stderr.strip())
    response = json.loads(result.stdout)
    return json.loads(base64.b64decode(response["content"]).decode())


def same_acceptance(existing, record):
    immutable_fields = {
        "github_user_id",
        "cla_version",
        "cla_sha256",
        "statement",
    }
    return all(existing.get(field) == record[field] for field in immutable_fields)


def branch_tip(repo):
    result = gh([f"repos/{repo}/git/ref/heads/{RECORDS_BRANCH}"], check=False)
    if result.returncode:
        if "HTTP 404" in result.stderr:
            return None
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout)["object"]["sha"]


def create_blob(repo, content):
    response = api_json(
        ["-X", "POST", f"repos/{repo}/git/blobs"],
        {"content": content, "encoding": "utf-8"},
    )
    return response["sha"]


def create_tree(repo, path, blob_sha, parent):
    payload = {"tree": [{"path": path, "mode": "100644", "type": "blob", "sha": blob_sha}]}
    if parent:
        commit = api_json([f"repos/{repo}/git/commits/{parent}"])
        payload["base_tree"] = commit["tree"]["sha"]
    return api_json(["-X", "POST", f"repos/{repo}/git/trees"], payload)["sha"]


def create_commit(repo, record, tree_sha, parent):
    payload = {
        "message": (f"Record CLA v{record['cla_version']} acceptance for GitHub user {record['github_user_id']}"),
        "tree": tree_sha,
        "parents": [parent] if parent else [],
    }
    return api_json(["-X", "POST", f"repos/{repo}/git/commits"], payload)["sha"]


def update_branch(repo, commit_sha, parent):
    if parent:
        return gh(
            ["-X", "PATCH", f"repos/{repo}/git/refs/heads/{RECORDS_BRANCH}"],
            {"sha": commit_sha, "force": False},
            check=False,
        )
    return gh(
        ["-X", "POST", f"repos/{repo}/git/refs"],
        {"ref": f"refs/heads/{RECORDS_BRANCH}", "sha": commit_sha},
        check=False,
    )


def store(repo, record):
    path = record_path(record)
    for attempt in range(MAX_PUSH_ATTEMPTS):
        existing = existing_record(repo, path)
        if existing is not None:
            if same_acceptance(existing, record):
                print(f"CLA acceptance already recorded at {path}.")
                return
            raise RuntimeError(f"Conflicting CLA record already exists at {path}")

        parent = branch_tip(repo)
        blob_sha = create_blob(repo, encoded_record(record))
        tree_sha = create_tree(repo, path, blob_sha, parent)
        commit_sha = create_commit(repo, record, tree_sha, parent)
        result = update_branch(repo, commit_sha, parent)
        if result.returncode == 0:
            print(f"Recorded CLA acceptance at {path}.")
            return
        if "HTTP 409" not in result.stderr and "HTTP 422" not in result.stderr:
            raise RuntimeError(result.stderr.strip())
        time.sleep(2**attempt)
    raise RuntimeError("CLA records branch kept changing; retry limit reached")


def main():
    with open(os.environ["GITHUB_EVENT_PATH"]) as file:
        event = json.load(file)
    record = event_record(event)
    if record is None:
        print("Comment is not a CLA acceptance; nothing to record.")
        return 0
    try:
        store(os.environ["GITHUB_REPOSITORY"], record)
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"::error::Could not record CLA acceptance: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

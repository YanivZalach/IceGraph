"""Require every contributor on a pull request to accept the IceGraph CLA."""

import base64
import hashlib
import json
import os
import subprocess
import sys

CLA_VERSION = "1.0"
STATUS_CONTEXT = "CLA"
RECORDS_BRANCH = "cla-records"
EXEMPT_LOGINS = {"yanivzalach"}
SIGNER_FIELDS = {
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


def statement(version):
    return (
        "I have read and agree to the IceGraph Individual Contributor License Agreement, "
        f"Harmony HA-CLA-I-ANY version {version}, "
        "and I confirm that I have authority to submit my contribution."
    )


def gh(args, check=True):
    result = subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=False)
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def api_get(path):
    return json.loads(gh([path]).stdout)


def repo_get(repo, path):
    return api_get(f"repos/{repo}/{path}")


def repo_list(repo, path):
    output = gh(["--paginate", f"repos/{repo}/{path}", "--jq", ".[]"]).stdout
    return [json.loads(line) for line in output.splitlines() if line]


def signer_identity(account):
    if not account:
        return None
    login = account["login"].lower()
    if account.get("type") == "Bot" or login in EXEMPT_LOGINS:
        return None
    return account["id"], login


def contributors(pr, commits):
    required = {}
    unlinked = set()
    identity = signer_identity(pr["user"])
    if identity:
        required[identity[0]] = identity[1]
    for commit in commits:
        if commit.get("author"):
            identity = signer_identity(commit["author"])
            if identity:
                required[identity[0]] = identity[1]
        else:
            email = commit.get("commit", {}).get("author", {}).get("email")
            unlinked.add((email or "commit with no author email").lower())
    return required, unlinked


def cla_hash():
    with open("CLA.md", "rb") as file:
        return hashlib.sha256(file.read()).hexdigest()


def verify_signer_record(record, user_id):
    missing = SIGNER_FIELDS - record.keys()
    if missing:
        raise RuntimeError(f"Incomplete signer record, missing: {', '.join(sorted(missing))}")
    if record["github_user_id"] != user_id:
        raise RuntimeError(f"CLA signer ID mismatch: {record['github_login']}")
    if record["cla_version"] != CLA_VERSION:
        raise RuntimeError(f"CLA version mismatch: {record['github_login']}")
    if record["statement"] != statement(CLA_VERSION):
        raise RuntimeError(f"CLA statement mismatch: {record['github_login']}")
    if record["cla_sha256"] != cla_hash():
        raise RuntimeError(f"CLA document hash mismatch: {record['github_login']}")
    return user_id


def permanent_signers(repo, required):
    accepted = set()
    for user_id in required:
        path = f"signers/v{CLA_VERSION}/{user_id}.json"
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
                continue
            raise RuntimeError(result.stderr.strip())
        response = json.loads(result.stdout)
        record = json.loads(base64.b64decode(response["content"]).decode())
        try:
            accepted.add(verify_signer_record(record, user_id))
        except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
            print(f"::warning::Ignoring signer record: {error}")
    return accepted


def set_status(repo, sha, state, description, target_url):
    gh(
        [
            "-X",
            "POST",
            f"repos/{repo}/statuses/{sha}",
            "-f",
            f"state={state}",
            "-f",
            f"context={STATUS_CONTEXT}",
            "-f",
            f"description={description[:140]}",
            "-f",
            f"target_url={target_url}",
        ]
    )


def verify(repo, number, target_url):
    pr = repo_get(repo, f"pulls/{number}")
    commits = repo_list(repo, f"pulls/{number}/commits")
    required, unlinked = contributors(pr, commits)
    accepted = permanent_signers(repo, required)
    missing = [required[user_id] for user_id in sorted(required.keys() - accepted)]
    if unlinked:
        problem = f"Commit email not linked to a GitHub account: {', '.join(sorted(unlinked))}"
    elif missing:
        problem = f"CLA not accepted by: {', '.join(missing)}"
    else:
        message = f"All contributors accepted CLA v{CLA_VERSION}."
        set_status(repo, pr["head"]["sha"], "success", message, target_url)
        print(message)
        return 0
    set_status(repo, pr["head"]["sha"], "failure", problem, target_url)
    print(f"::error::{problem}")
    print(f"Required statement: {statement(CLA_VERSION)}")
    return 1


def main():
    repo = os.environ["REPO"]
    number = os.environ["PR_NUMBER"]
    target_url = f"{os.environ['SERVER_URL']}/{repo}/actions/runs/{os.environ['RUN_ID']}"
    try:
        return verify(repo, number, target_url)
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        try:
            sha = repo_get(repo, f"pulls/{number}")["head"]["sha"]
            set_status(repo, sha, "error", f"CLA check could not run: {error}", target_url)
        except (RuntimeError, OSError, ValueError, KeyError, TypeError):
            pass
        print(f"::error::CLA check could not run: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

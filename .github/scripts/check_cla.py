"""Require every contributor on a pull request to accept the IceGraph CLA."""

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime

CLA_VERSION = "1.0"
CLA_STATEMENT = (
    "I have read and agree to the IceGraph Individual Contributor License Agreement, "
    f"Harmony HA-CLA-I-ANY version {CLA_VERSION}, "
    "and I confirm that I have authority to submit my contribution."
)
RECORDS_BRANCH = "cla-records"
STATUS_CONTEXT = "CLA"
EXEMPT_LOGINS = {"yanivzalach"}
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


def github_api(arguments, allow_failure=False):
    result = subprocess.run(
        ["gh", "api", *arguments], capture_output=True, text=True, check=False
    )
    if result.returncode and not allow_failure:
        raise RuntimeError(result.stderr.strip())
    return result


def main():
    repo = os.environ["REPO"]
    pr_number = os.environ["PR_NUMBER"]
    target_url = (
        f"{os.environ['SERVER_URL']}/{repo}/actions/runs/{os.environ['RUN_ID']}"
    )

    try:
        # Read the PR and every commit, then identify each GitHub account that
        # personally needs to accept the CLA.
        pr = json.loads(github_api([f"repos/{repo}/pulls/{pr_number}"]).stdout)
        commits_output = github_api(
            [
                "--paginate",
                f"repos/{repo}/pulls/{pr_number}/commits",
                "--jq",
                ".[]",
            ]
        ).stdout
        commits = [json.loads(line) for line in commits_output.splitlines() if line]

        required = {}
        unlinked_emails = set()
        accounts = [pr["user"]]
        for commit in commits:
            if commit.get("author"):
                accounts.append(commit["author"])
            else:
                email = commit.get("commit", {}).get("author", {}).get("email")
                unlinked_emails.add((email or "commit with no author email").lower())

        for account in accounts:
            login = account["login"].lower()
            if account.get("type") != "Bot" and login not in EXEMPT_LOGINS:
                required[account["id"]] = login

        # A stored record is valid only for this user, CLA version, exact
        # statement, and exact contents of the current CLA document.
        with open("CLA.md", "rb") as file:
            expected_cla_hash = hashlib.sha256(file.read()).hexdigest()

        accepted = set()
        for user_id in required:
            record_path = f"signers/v{CLA_VERSION}/{user_id}.json"
            result = github_api(
                [
                    "--method",
                    "GET",
                    f"repos/{repo}/contents/{record_path}",
                    "-f",
                    f"ref={RECORDS_BRANCH}",
                ],
                allow_failure=True,
            )
            if result.returncode:
                if "HTTP 404" in result.stderr:
                    continue
                raise RuntimeError(result.stderr.strip())

            try:
                response = json.loads(result.stdout)
                record = json.loads(base64.b64decode(response["content"]).decode())
            except (ValueError, KeyError, TypeError) as error:
                print(f"::warning::Ignoring unreadable CLA record: {error}")
                continue
            missing_fields = RECORD_FIELDS - record.keys()
            if missing_fields:
                print(
                    f"::warning::Ignoring incomplete CLA record, missing: {', '.join(sorted(missing_fields))}"
                )
                continue
            if (
                not isinstance(record["github_login"], str)
                or not record["github_login"]
            ):
                print("::warning::Ignoring CLA record with an invalid GitHub login")
                continue
            if (
                type(record["github_user_id"]) is not int
                or record["github_user_id"] != user_id
            ):
                print(
                    f"::warning::Ignoring CLA signer ID mismatch: {record['github_login']}"
                )
                continue
            if record["cla_version"] != CLA_VERSION:
                print(
                    f"::warning::Ignoring CLA version mismatch: {record['github_login']}"
                )
                continue
            if record["statement"] != CLA_STATEMENT:
                print(
                    f"::warning::Ignoring CLA statement mismatch: {record['github_login']}"
                )
                continue
            if record["cla_sha256"] != expected_cla_hash:
                print(
                    f"::warning::Ignoring CLA document hash mismatch: {record['github_login']}"
                )
                continue
            if type(record["pull_request"]) is not int or record["pull_request"] <= 0:
                print(
                    f"::warning::Ignoring invalid CLA PR number: {record['github_login']}"
                )
                continue
            if type(record["comment_id"]) is not int or record["comment_id"] <= 0:
                print(
                    f"::warning::Ignoring invalid CLA comment ID: {record['github_login']}"
                )
                continue
            if isinstance(record["comment_url"], str):
                comment_url = re.fullmatch(
                    r"https://github\.com/([^/]+/[^/]+)/pull/(\d+)#issuecomment-(\d+)",
                    record["comment_url"],
                    re.IGNORECASE,
                )
            else:
                comment_url = None
            if (
                not comment_url
                or comment_url.group(1).lower() != repo.lower()
                or int(comment_url.group(2)) != record["pull_request"]
                or int(comment_url.group(3)) != record["comment_id"]
            ):
                print(
                    f"::warning::Ignoring invalid CLA comment URL: {record['github_login']}"
                )
                continue
            if not isinstance(record["accepted_at"], str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", record["accepted_at"]
            ):
                print(
                    f"::warning::Ignoring invalid CLA timestamp: {record['github_login']}"
                )
                continue
            try:
                datetime.strptime(record["accepted_at"], "%Y-%m-%dT%H:%M:%S%z")
            except ValueError:
                print(
                    f"::warning::Ignoring invalid CLA timestamp: {record['github_login']}"
                )
                continue
            accepted.add(user_id)

        missing_logins = [
            required[user_id] for user_id in sorted(required.keys() - accepted)
        ]
        if unlinked_emails:
            state = "failure"
            description = f"Commit email not linked to a GitHub account: {', '.join(sorted(unlinked_emails))}"
            exit_code = 1
        elif missing_logins:
            state = "failure"
            description = f"CLA not accepted by: {', '.join(missing_logins)}"
            exit_code = 1
        else:
            state = "success"
            description = f"All contributors accepted CLA v{CLA_VERSION}."
            exit_code = 0

        # Publish one status after every check has completed.
        github_api(
            [
                "-X",
                "POST",
                f"repos/{repo}/statuses/{pr['head']['sha']}",
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
        if exit_code:
            print(f"::error::{description}")
            print(f"Required statement: {CLA_STATEMENT}")
        else:
            print(description)
        return exit_code

    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        # If possible, publish an error status instead of leaving an old result
        # on the PR after an unexpected API or record failure.
        try:
            if "pr" not in locals():
                pr = json.loads(github_api([f"repos/{repo}/pulls/{pr_number}"]).stdout)
            error_description = f"CLA check could not run: {error}"
            github_api(
                [
                    "-X",
                    "POST",
                    f"repos/{repo}/statuses/{pr['head']['sha']}",
                    "-f",
                    "state=error",
                    "-f",
                    f"context={STATUS_CONTEXT}",
                    "-f",
                    f"description={error_description[:140]}",
                    "-f",
                    f"target_url={target_url}",
                ]
            )
        except (RuntimeError, OSError, ValueError, KeyError, TypeError):
            pass
        print(f"::error::CLA check could not run: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

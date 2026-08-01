#!/usr/bin/env python3
"""Run an opaque PixelDrain upload without writing media details to Actions logs."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote

import requests

MAX_UPLOAD_BYTES = 10_000_000_000
DOWNLOAD_TIMEOUT_SECONDS = 4 * 60 * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    args.broker_url = args.broker_url.rstrip("/")
    return args


def mask(value: str) -> None:
    if value:
        print(f"::add-mask::{value}")


def workflow_oidc_token(audience: str) -> str:
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    if not request_url or not request_token:
        raise RuntimeError("GitHub OIDC is unavailable.")
    separator = "&" if "?" in request_url else "?"
    response = requests.get(
        f"{request_url}{separator}audience={quote(audience, safe='')}",
        headers={"Authorization": f"Bearer {request_token}"},
        timeout=30,
    )
    response.raise_for_status()
    token = str(response.json().get("value") or "")
    if not token:
        raise RuntimeError("GitHub OIDC did not return a token.")
    return token


def broker_request(
    method: str,
    broker_url: str,
    path: str,
    oidc_token: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    response = requests.request(
        method,
        f"{broker_url}{path}",
        headers={"Authorization": f"Bearer {oidc_token}"},
        json=payload,
        timeout=60,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Broker returned HTTP {response.status_code}.")
    value = response.json()
    if not isinstance(value, dict):
        raise RuntimeError("Broker returned an invalid response.")
    return value


def safe_filename(filename: str) -> str:
    return re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", filename)[:220] or "media"


def stage_source(source_url: str, filename: str, directory: str) -> tuple[str, int]:
    """Download before upload so sources without Content-Length work reliably."""
    destination = os.path.join(directory, safe_filename(filename))
    command = [
        "aria2c",
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--file-allocation=none",
        "--max-connection-per-server=1",
        "--min-split-size=64M",
        "--summary-interval=0",
        "--console-log-level=warn",
        "--timeout=60",
        "--retry-wait=10",
        "--max-tries=5",
        "--dir",
        directory,
        "--out",
        os.path.basename(destination),
        source_url,
    ]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("source_download_failed") from exc
    if completed.returncode != 0 or not os.path.isfile(destination):
        raise RuntimeError("source_download_failed")
    size = os.path.getsize(destination)
    if not 0 < size <= MAX_UPLOAD_BYTES:
        raise RuntimeError("Source exceeds the configured size limit.")
    return destination, size


def upload_once(source_url: str, filename: str, api_key: str) -> tuple[str, int]:
    mask(api_key)
    with tempfile.TemporaryDirectory(prefix="pixeldrain-") as directory:
        staged_path, size = stage_source(source_url, filename, directory)
        try:
            with open(staged_path, "rb") as staged:
                response = requests.put(
                    f"https://pixeldrain.com/api/file/{quote(safe_filename(filename))}",
                    auth=("", api_key),
                    data=staged,
                    headers={
                        "Content-Length": str(size),
                        "Content-Type": "application/octet-stream",
                        "Accept": "application/json",
                        "User-Agent": "OpaquePixelDrainAction/2.0",
                    },
                    timeout=(30, 4 * 60 * 60),
                )
        except requests.RequestException as exc:
            raise RuntimeError("pixeldrain_upload_failed") from exc
    payload = response.json() if response.headers.get("Content-Type", "").startswith("application/json") else {}
    if not response.ok or payload.get("success") is False:
        raise RuntimeError("PixelDrain rejected the upload.")
    file_id = str(payload.get("id") or "")
    if not re.fullmatch(r"[A-Za-z0-9]+", file_id):
        raise RuntimeError("PixelDrain did not return a valid file ID.")
    return f"https://pixeldrain.com/u/{file_id}", size


def run_job(job: dict[str, object]) -> dict[str, object]:
    source_url = str(job.get("source_url") or "")
    filename = str(job.get("filename") or "")
    keys = [str(value).strip() for value in job.get("pixeldrain_api_keys", []) if str(value).strip()]
    if not source_url or not filename or not keys:
        raise RuntimeError("Broker job is incomplete.")
    mask(source_url)
    mask(filename)
    last_error = "PixelDrain upload failed."
    started = time.monotonic()
    for key in keys:
        try:
            url, size = upload_once(source_url, filename, key)
            mask(url)
            return {
                "ok": True,
                "pixeldrain_url": url,
                "size_bytes": size,
                "elapsed_seconds": round(time.monotonic() - started, 2),
            }
        except Exception as exc:  # Do not reveal provider responses in public logs.
            last_error = str(exc)[:120] or f"{type(exc).__name__}."
    return {"ok": False, "error": last_error}


def main() -> int:
    args = parse_args()
    try:
        oidc_token = workflow_oidc_token(args.broker_url)
        job = broker_request(
            "POST", args.broker_url, f"/v1/jobs/{args.job_id}/claim", oidc_token,
            {"run_id": os.environ.get("GITHUB_RUN_ID", "")},
        )
        result = run_job(job)
        result["run_id"] = os.environ.get("GITHUB_RUN_ID", "")
        broker_request("POST", args.broker_url, f"/v1/jobs/{args.job_id}/result", oidc_token, result)
        print("PixelDrain job completed." if result["ok"] else "PixelDrain job failed.")
        return 0 if result["ok"] else 1
    except Exception as exc:  # Do not reveal job data in public logs.
        print(f"PixelDrain broker job failed: {type(exc).__name__}.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

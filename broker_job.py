#!/usr/bin/env python3
"""Run an opaque PixelDrain upload without writing media details to Actions logs."""

from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import tempfile
from urllib.parse import quote

import requests

MAX_UPLOAD_BYTES = 10_000_000_000
CHUNK_BYTES = 1024 * 1024


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


def upload_once(source_url: str, filename: str, api_key: str) -> tuple[str, int]:
    mask(api_key)
    try:
        source_request = requests.get(
            source_url,
            stream=True,
            allow_redirects=True,
            headers={"Accept-Encoding": "identity", "User-Agent": "OpaquePixelDrainAction/1.0"},
            timeout=(30, 300),
        )
        source_request.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("source_download_failed") from exc
    with source_request as source:
        size = int(source.headers.get("Content-Length") or 0)
        if size <= 0:
            try:
                probe = requests.get(
                    source_url,
                    headers={"Range": "bytes=0-0", "Accept-Encoding": "identity"},
                    allow_redirects=True,
                    timeout=(30, 60),
                )
                content_range = probe.headers.get("Content-Range") or ""
                match = re.search(r"/(\d+)$", content_range)
                size = int(match.group(1)) if match else 0
            except requests.RequestException:
                size = 0
        if size > MAX_UPLOAD_BYTES:
            raise RuntimeError("Source exceeds the configured size limit.")
        encoded_key = base64.b64encode(f":{api_key}".encode("utf-8")).decode("ascii")
        safe_name = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", filename)[:220] or "media"
        def send_upload(body: object, content_length: int) -> requests.Response:
            return requests.put(
                f"https://pixeldrain.com/api/file/{quote(safe_name)}",
                data=body,
                headers={
                    "Authorization": f"Basic {encoded_key}",
                    "Content-Type": source.headers.get("Content-Type") or "application/octet-stream",
                    "Content-Length": str(content_length),
                    "Accept": "application/json",
                    "User-Agent": "OpaquePixelDrainAction/1.0",
                },
                timeout=(30, 3600),
            )

        try:
            if size > 0:
                response = send_upload(source.iter_content(chunk_size=CHUNK_BYTES), size)
            else:
                with tempfile.TemporaryFile() as staged:
                    size = 0
                    for chunk in source.iter_content(chunk_size=CHUNK_BYTES):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > MAX_UPLOAD_BYTES:
                            raise RuntimeError("Source exceeds the configured size limit.")
                        staged.write(chunk)
                    if size <= 0:
                        raise RuntimeError("Source download was empty.")
                    staged.seek(0)
                    response = send_upload(staged, size)
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
    for key in keys:
        try:
            url, size = upload_once(source_url, filename, key)
            mask(url)
            return {"ok": True, "pixeldrain_url": url, "size_bytes": size}
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

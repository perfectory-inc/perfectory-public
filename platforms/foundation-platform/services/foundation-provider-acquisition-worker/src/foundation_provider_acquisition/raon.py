from __future__ import annotations

import argparse
import base64
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


RaonProbeStatus = Literal["acquired", "provider_acquisition_blocked", "probe_error"]

DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class RaonAcquisitionJob:
    download_ds_id: str
    file_no: str
    page_url: str


@dataclass(frozen=True)
class CapturedNetworkResponse:
    url: str
    status: int | None
    size_bytes: int

    def to_public_dict(self) -> dict[str, object]:
        return {
            "url": redact_url(self.url),
            "status": self.status,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class RaonUploadedFile:
    order: str
    original_name: str
    storage_path: str
    size_bytes: int
    provider_identity: str

    def to_public_dict(self) -> dict[str, object]:
        return {
            "order": self.order,
            "original_name": self.original_name,
            "storage_path": self.storage_path,
            "size_bytes": self.size_bytes,
            "provider_identity": self.provider_identity,
        }


@dataclass(frozen=True)
class RaonReplayPostCandidate:
    url: str
    post_data: str
    content_type: str | None
    request_content_type: str | None
    content_disposition: str | None
    content_length: str | None
    status: int | None
    resource_type: str
    method: str = "POST"


@dataclass(frozen=True)
class RaonPrivateReplayRequest:
    landing_object_key: str
    replay_url: str
    request_content_type: str
    post_data: str
    provider_declared_size_bytes: int
    cookie_header: str | None
    user_agent: str | None
    referer_url: str
    method: str = "POST"

    def to_private_dict(self) -> dict[str, object]:
        return {
            "schema_version": "foundation-platform.provider_acquisition_replay_request.v1",
            "landing_object_key": self.landing_object_key,
            "replay_url": self.replay_url,
            "method": self.method,
            "request_content_type": self.request_content_type,
            "post_data": self.post_data,
            "provider_declared_size_bytes": self.provider_declared_size_bytes,
            "cookie_header": self.cookie_header,
            "user_agent": self.user_agent,
            "referer_url": self.referer_url,
        }


@dataclass(frozen=True)
class RaonDownloadReplayProof:
    download_event_filename: str | None
    download_event_url: str | None
    zip_candidate_count: int
    replay_status: int | None
    replay_content_type: str | None
    replay_content_disposition: str | None
    replay_content_length: str | None
    replay_first4_hex: str | None
    replay_looks_zip: bool
    error_message: str | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "download_event_filename": self.download_event_filename,
            "download_event_url": (
                redact_url(self.download_event_url) if self.download_event_url else None
            ),
            "zip_candidate_count": self.zip_candidate_count,
            "replay_status": self.replay_status,
            "replay_content_type": self.replay_content_type,
            "replay_content_disposition": self.replay_content_disposition,
            "replay_content_length": self.replay_content_length,
            "replay_first4_hex": self.replay_first4_hex,
            "replay_looks_zip": self.replay_looks_zip,
            "acquisition_strategy": "browser_replay_candidate",
            "raw_validation_owner": "foundation_platform_rust_importer",
            "raw_payload_validated": False,
            "replay_validation_scope": (
                "transport_prefix_only"
                if self.replay_status is not None or self.replay_first4_hex is not None
                else "not_replayed"
            ),
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class RaonReplayAcquisition:
    proof: RaonDownloadReplayProof
    private_request: RaonPrivateReplayRequest | None


@dataclass(frozen=True)
class RaonProbeResult:
    status: RaonProbeStatus
    download_ds_id: str
    file_no: str
    page_url: str
    final_url: str | None
    http_status: int | None
    html_contains_raon: bool
    uploaded_files: list[RaonUploadedFile]
    captured_xhr: list[CapturedNetworkResponse]
    error_message: str | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "download_ds_id": self.download_ds_id,
            "file_no": self.file_no,
            "page_url": redact_url(self.page_url),
            "final_url": redact_url(self.final_url) if self.final_url else None,
            "http_status": self.http_status,
            "html_contains_raon": self.html_contains_raon,
            "uploaded_files": [file.to_public_dict() for file in self.uploaded_files],
            "captured_xhr": [xhr.to_public_dict() for xhr in self.captured_xhr],
            "error_message": self.error_message,
        }


def raon_page_url(download_ds_id: str, file_no: str) -> str:
    if not download_ds_id or not file_no:
        raise ValueError("download_ds_id and file_no are required")
    return (
        "https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do"
        f"?ds_file_sq={download_ds_id}{file_no}"
    )


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    redacted_query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if _is_sensitive_query_key(key):
            redacted_query.append((key, "[REDACTED]"))
        else:
            redacted_query.append((key, value))
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(redacted_query),
            parts.fragment,
        )
    )


def _cookies_from_cookie_header(cookie_header: str | None) -> list[dict[str, object]]:
    if not cookie_header:
        return []
    cookies: list[dict[str, object]] = []
    for raw_part in cookie_header.split(";"):
        part = raw_part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": ".vworld.kr",
                "path": "/",
                "secure": True,
                "httpOnly": False,
            }
        )
    return cookies


def _cookie_header_with_value(
    cookie_header: str | None,
    name: str,
    value: str,
) -> str:
    pairs: list[tuple[str, str]] = []
    replaced = False
    for raw_part in (cookie_header or "").split(";"):
        part = raw_part.strip()
        if not part or "=" not in part:
            continue
        part_name, part_value = part.split("=", 1)
        if part_name.strip() == name:
            pairs.append((name, value))
            replaced = True
        else:
            pairs.append((part_name.strip(), part_value.strip()))
    if not replaced:
        pairs.append((name, value))
    return "; ".join(f"{part_name}={part_value}" for part_name, part_value in pairs)


def _cookie_value_from_set_cookie(set_cookie_header: str, name: str) -> str | None:
    pattern = re.compile(rf"(?:^|,\s*){re.escape(name)}=([^;,]+)")
    match = pattern.search(set_cookie_header)
    return match.group(1) if match else None


def _is_sensitive_query_key(key: str) -> bool:
    normalized = key.lower()
    return any(
        marker in normalized
        for marker in (
            "token",
            "session",
            "cookie",
            "password",
            "secret",
            "credential",
            "raonk",
            "k00",
        )
    )


def extract_uploaded_files_from_script(script_text: str) -> list[RaonUploadedFile]:
    files: list[RaonUploadedFile] = []
    for match in re.finditer(
        r"RAONKUPLOAD\.AddUploadedFile\s*\((?P<args>.*?)\)\s*;",
        script_text,
        flags=re.DOTALL,
    ):
        string_args = _extract_single_quoted_js_strings(match.group("args"))
        if len(string_args) < 5:
            continue
        size_text = string_args[3]
        if not size_text.isdigit():
            continue
        files.append(
            RaonUploadedFile(
                order=string_args[0],
                original_name=string_args[1],
                storage_path=string_args[2],
                size_bytes=int(size_text),
                provider_identity=string_args[4],
            )
        )
    return files


def _extract_single_quoted_js_strings(text: str) -> list[str]:
    values: list[str] = []
    for match in re.finditer(r"'((?:\\'|[^'])*)'", text):
        values.append(match.group(1).replace("\\'", "'"))
    return values


def _choose_replay_post_candidate(
    candidates: list[RaonReplayPostCandidate],
) -> RaonReplayPostCandidate | None:
    replayable = [
        candidate
        for candidate in candidates
        if candidate.post_data or candidate.method.upper() == "GET"
    ]
    if not replayable:
        return None

    provider_filedown_gets = [
        candidate
        for candidate in replayable
        if candidate.method.upper() == "GET"
        and "k00=" in candidate.url
        and candidate.content_length not in {"0", "0.0"}
    ]
    if provider_filedown_gets:
        return provider_filedown_gets[-1]

    non_empty_downloads = [
        candidate
        for candidate in replayable
        if candidate.content_length not in {"0", "0.0"}
    ]
    if non_empty_downloads:
        return non_empty_downloads[-1]
    return replayable[-1]


def _uploaded_file_for_identity(
    files: list[RaonUploadedFile],
    download_ds_id: str,
    file_no: str,
) -> RaonUploadedFile | None:
    expected_identity = f"{download_ds_id}|{file_no}"
    for file in files:
        if file.provider_identity == expected_identity:
            return file
    return files[0] if files else None


def build_private_replay_request(
    candidate: RaonReplayPostCandidate,
    uploaded_file: RaonUploadedFile,
    *,
    landing_object_key: str,
    cookie_header: str | None,
    user_agent: str | None,
    referer_url: str,
) -> RaonPrivateReplayRequest:
    if not landing_object_key.startswith("landing/"):
        raise ValueError("landing_object_key must start with landing/")
    return RaonPrivateReplayRequest(
        landing_object_key=landing_object_key,
        replay_url=candidate.url,
        request_content_type=(
            candidate.request_content_type or "application/x-www-form-urlencoded"
            if candidate.method.upper() == "POST"
            else ""
        ),
        post_data=candidate.post_data,
        provider_declared_size_bytes=uploaded_file.size_bytes,
        cookie_header=cookie_header,
        user_agent=user_agent,
        referer_url=referer_url,
        method=candidate.method.upper(),
    )


def _zip_candidate_from_browser_response(
    response: object,
) -> RaonReplayPostCandidate | None:
    headers = getattr(response, "headers", None)
    if not isinstance(headers, dict):
        return None

    content_type = _header_value(headers, "content-type")
    content_disposition = _header_value(headers, "content-disposition")

    request = getattr(response, "request", None)
    if request is None:
        return None

    method = getattr(request, "method", None)
    if method not in {"GET", "POST"}:
        return None

    post_data = getattr(request, "post_data", None)
    if not isinstance(post_data, str):
        post_data = ""

    url = getattr(response, "url", None)
    if not isinstance(url, str):
        return None

    looks_zip = _looks_like_zip_download(content_type, content_disposition)
    k00_token = _k00_token(url)
    long_k00_get = method == "GET" and k00_token is not None and len(k00_token) > 100
    if not looks_zip and not long_k00_get:
        return None
    if method == "GET" and k00_token is None:
        return None

    status = getattr(response, "status", None)
    if not isinstance(status, int):
        status = None

    resource_type = getattr(request, "resource_type", "")
    if not isinstance(resource_type, str):
        resource_type = ""
    request_headers = getattr(request, "headers", None)
    request_content_type = (
        _header_value(request_headers, "content-type")
        if isinstance(request_headers, dict)
        else None
    )

    return RaonReplayPostCandidate(
        url=url,
        post_data=post_data,
        content_type=content_type,
        request_content_type=request_content_type,
        content_disposition=content_disposition,
        content_length=_header_value(headers, "content-length"),
        status=status,
        resource_type=resource_type,
        method=method,
    )


def _looks_like_zip_download(
    content_type: str | None,
    content_disposition: str | None,
) -> bool:
    normalized_type = (content_type or "").lower()
    normalized_disposition = (content_disposition or "").lower()
    return "application/zip" in normalized_type or ".zip" in normalized_disposition


def _looks_like_agent_runtime_request(url: str, content_type: str | None) -> bool:
    normalized_url = url.lower()
    normalized_type = (content_type or "").lower()
    return (
        "raonkupload/agent/" in normalized_url
        or "raonksetup" in normalized_url
        or "installguide" in normalized_url
        or "application/x-msdownload" in normalized_type
    )


def _k00_token(url: str) -> str | None:
    for key, value in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if key == "k00":
            return value
    return None


def _header_value(headers: dict[object, object], name: str) -> str | None:
    expected = name.lower()
    for key, value in headers.items():
        if str(key).lower() == expected and value is not None:
            return str(value)
    return None


def _invoke_provider_filedown(page: object) -> object:
    wait_for_function = getattr(page, "wait_for_function", None)
    if callable(wait_for_function):
        wait_for_function(
            "() => typeof window.fileDown === 'function' || typeof fileDown === 'function'",
            timeout=15_000,
        )
    return page.evaluate(
        """
        () => {
          try {
            if (typeof window.fileDown === 'function') {
              window.fileDown();
              return { ok: true, invocation: 'provider_fileDown' };
            }
            if (typeof fileDown === 'function') {
              fileDown();
              return { ok: true, invocation: 'provider_fileDown' };
            }
            return { ok: false, error: 'provider fileDown function was not found' };
          } catch (error) {
            return {
              ok: false,
              error: String(error && (error.stack || error.message || error))
            };
          }
        }
        """
    )


def _replay_zip_prefix(
    candidate: RaonReplayPostCandidate,
    *,
    cookie_header: str | None,
    user_agent: str | None,
    referer_url: str,
    timeout_seconds: int,
) -> dict[str, object]:
    import requests

    method = candidate.method.upper()
    headers = {"referer": referer_url}
    if method == "POST":
        headers["content-type"] = (
            candidate.request_content_type or "application/x-www-form-urlencoded"
        )
    if user_agent:
        headers["user-agent"] = user_agent
    if cookie_header:
        headers["cookie"] = cookie_header

    if method == "GET":
        response = requests.get(
            candidate.url,
            headers=headers,
            stream=True,
            timeout=timeout_seconds,
        )
    elif method == "POST":
        response = requests.post(
            candidate.url,
            headers=headers,
            data=candidate.post_data,
            stream=True,
            timeout=timeout_seconds,
        )
    else:
        raise ValueError(f"unsupported RAON replay method: {candidate.method}")
    try:
        first_bytes = b""
        for chunk in response.iter_content(chunk_size=4):
            if not chunk:
                continue
            first_bytes += chunk
            if len(first_bytes) >= 4:
                break
        first_bytes = first_bytes[:4]
        return {
            "status": response.status_code,
            "content_type": response.headers.get("content-type"),
            "content_disposition": response.headers.get("content-disposition"),
            "content_length": response.headers.get("content-length"),
            "first4_hex": first_bytes.hex() if first_bytes else None,
            "looks_zip": first_bytes.startswith(b"PK"),
        }
    finally:
        response.close()


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def probe_raon_page(
    download_ds_id: str,
    file_no: str,
    *,
    headless: bool = True,
    cookie_header: str | None = None,
    user_agent: str | None = None,
) -> RaonProbeResult:
    page_url = raon_page_url(download_ds_id, file_no)
    try:
        from scrapling.fetchers import DynamicSession
    except Exception as exc:  # pragma: no cover - depends on runtime environment
        return RaonProbeResult(
            status="probe_error",
            download_ds_id=download_ds_id,
            file_no=file_no,
            page_url=page_url,
            final_url=None,
            http_status=None,
            html_contains_raon=False,
            uploaded_files=[],
            captured_xhr=[],
            error_message=f"failed to import Scrapling: {exc}",
        )

    try:
        with DynamicSession(
            capture_xhr=r".*(raon|RAON|download|Download|dtmk|jsp).*",
            headless=headless,
            disable_resources=False,
            network_idle=True,
            extra_headers=_extra_headers(cookie_header, user_agent),
        ) as session:
            page = session.fetch(page_url, load_dom=True)
    except Exception as exc:  # pragma: no cover - live browser/provider dependent
        return RaonProbeResult(
            status="probe_error",
            download_ds_id=download_ds_id,
            file_no=file_no,
            page_url=page_url,
            final_url=None,
            http_status=None,
            html_contains_raon=False,
            uploaded_files=[],
            captured_xhr=[],
            error_message=str(exc),
        )

    page_text = _response_text(page)
    captured = [_captured_response(xhr) for xhr in getattr(page, "captured_xhr", [])]
    captured = [item for item in captured if item is not None]
    acquired = any(item.size_bytes > 0 and "download" in item.url.lower() for item in captured)
    uploaded_files = extract_uploaded_files_from_script(page_text)

    return RaonProbeResult(
        status="acquired" if acquired else "provider_acquisition_blocked",
        download_ds_id=download_ds_id,
        file_no=file_no,
        page_url=page_url,
        final_url=_response_url(page),
        http_status=_response_status(page),
        html_contains_raon=_contains_raon_marker(page_text),
        uploaded_files=uploaded_files,
        captured_xhr=captured,
        error_message=None,
    )


def prove_raon_replay(
    *,
    download_ds_id: str,
    file_no: str,
    headless: bool = True,
    cookie_header: str | None = None,
    user_agent: str | None = None,
    replay_timeout_seconds: int = 60,
) -> RaonDownloadReplayProof:
    return acquire_raon_replay(
        download_ds_id=download_ds_id,
        file_no=file_no,
        headless=headless,
        cookie_header=cookie_header,
        user_agent=user_agent,
        replay_timeout_seconds=replay_timeout_seconds,
        landing_object_key=None,
    ).proof


def acquire_raon_replay(
    *,
    download_ds_id: str,
    file_no: str,
    headless: bool = True,
    cookie_header: str | None = None,
    user_agent: str | None = None,
    replay_timeout_seconds: int = 60,
    landing_object_key: str | None = None,
) -> RaonReplayAcquisition:
    page_url = raon_page_url(download_ds_id, file_no)
    probe = probe_raon_page(
        download_ds_id,
        file_no,
        headless=headless,
        cookie_header=cookie_header,
        user_agent=user_agent,
    )
    uploaded_file = _uploaded_file_for_identity(
        probe.uploaded_files,
        download_ds_id,
        file_no,
    )
    if uploaded_file is None:
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename=None,
                download_event_url=None,
                zip_candidate_count=0,
                replay_status=None,
                replay_content_type=None,
                replay_content_disposition=None,
                replay_content_length=None,
                replay_first4_hex=None,
                replay_looks_zip=False,
                error_message="RAON uploaded-file metadata was not found in provider page",
            ),
            private_request=None,
        )

    try:
        from scrapling.fetchers import DynamicSession
    except Exception as exc:  # pragma: no cover - depends on runtime environment
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename=None,
                download_event_url=None,
                zip_candidate_count=0,
                replay_status=None,
                replay_content_type=None,
                replay_content_disposition=None,
                replay_content_length=None,
                replay_first4_hex=None,
                replay_looks_zip=False,
                error_message=f"failed to import Scrapling: {exc}",
            ),
            private_request=None,
        )

    candidates: list[RaonReplayPostCandidate] = []
    capture: dict[str, Any] = {}

    def page_setup(page: object) -> None:
        def on_response(response: object) -> None:
            response_url = getattr(response, "url", None)
            headers = getattr(response, "headers", None)
            content_type = (
                _header_value(headers, "content-type")
                if isinstance(headers, dict)
                else None
            )
            if isinstance(response_url, str) and _looks_like_agent_runtime_request(
                response_url,
                content_type,
            ):
                capture["agent_runtime_required"] = True
            if isinstance(headers, dict):
                set_cookie = _header_value(headers, "set-cookie")
                if set_cookie:
                    jsessionid = _cookie_value_from_set_cookie(set_cookie, "JSESSIONID")
                    if jsessionid:
                        capture["replay_cookie_header"] = _cookie_header_with_value(
                            cookie_header,
                            "JSESSIONID",
                            jsessionid,
                        )
            candidate = _zip_candidate_from_browser_response(response)
            if candidate is not None:
                candidates.append(candidate)

        page.on("response", on_response)
        try:
            page.on(
                "download",
                lambda download: (
                    capture.setdefault(
                        "download_event_filename",
                        getattr(download, "suggested_filename", None),
                    ),
                    capture.setdefault("download_event_url", getattr(download, "url", None)),
                ),
            )
        except Exception:
            pass

    def page_action(page: object) -> None:
        try:
            provider_action_result = _invoke_provider_filedown(page)
            capture["provider_action_result"] = provider_action_result
            if (
                isinstance(provider_action_result, dict)
                and provider_action_result.get("ok", False)
            ):
                page.wait_for_timeout(10_000)
                if candidates:
                    return
                capture["provider_filedown_no_candidate"] = True
        except Exception as exc:  # pragma: no cover - live provider dependent
            capture["error_message"] = str(exc)

    try:
        with DynamicSession(
            headless=headless,
            disable_resources=False,
            network_idle=True,
            extra_headers=_extra_headers(cookie_header, user_agent),
            timeout=60_000,
        ) as session:
            session.fetch(
                page_url,
                load_dom=True,
                wait=1_000,
                page_setup=page_setup,
                page_action=page_action,
            )
    except Exception as exc:  # pragma: no cover - live browser/provider dependent
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename=None,
                download_event_url=None,
                zip_candidate_count=len(candidates),
                replay_status=None,
                replay_content_type=None,
                replay_content_disposition=None,
                replay_content_length=None,
                replay_first4_hex=None,
                replay_looks_zip=False,
                error_message=str(exc),
            ),
            private_request=None,
        )

    candidate = _choose_replay_post_candidate(candidates)
    if candidate is None:
        default_error = (
            "provider page required RAON agent runtime; Scrapling replay cannot acquire raw bytes without agent"
            if capture.get("agent_runtime_required")
            else "provider fileDown did not produce a replayable zip request"
        )
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename=_string_or_none(
                    capture.get("download_event_filename")
                ),
                download_event_url=_string_or_none(capture.get("download_event_url")),
                zip_candidate_count=len(candidates),
                replay_status=None,
                replay_content_type=None,
                replay_content_disposition=None,
                replay_content_length=None,
                replay_first4_hex=None,
                replay_looks_zip=False,
                error_message=_string_or_none(capture.get("error_message"))
                or default_error,
            ),
            private_request=None,
        )

    replay = _replay_zip_prefix(
        candidate,
        cookie_header=_string_or_none(capture.get("replay_cookie_header")) or cookie_header,
        user_agent=user_agent,
        referer_url=page_url,
        timeout_seconds=replay_timeout_seconds,
    )
    private_request = (
        build_private_replay_request(
            candidate,
            uploaded_file,
            landing_object_key=landing_object_key,
            cookie_header=_string_or_none(capture.get("replay_cookie_header"))
            or cookie_header,
            user_agent=user_agent,
            referer_url=page_url,
        )
        if landing_object_key
        else None
    )
    return RaonReplayAcquisition(
        proof=RaonDownloadReplayProof(
            download_event_filename=_string_or_none(capture.get("download_event_filename")),
            download_event_url=_string_or_none(capture.get("download_event_url")),
            zip_candidate_count=len(candidates),
            replay_status=replay["status"],
            replay_content_type=replay["content_type"],
            replay_content_disposition=replay["content_disposition"],
            replay_content_length=replay["content_length"],
            replay_first4_hex=replay["first4_hex"],
            replay_looks_zip=bool(replay["looks_zip"]),
            error_message=_string_or_none(capture.get("error_message")),
        ),
        private_request=private_request,
    )


def write_probe_result(path: Path, result: RaonProbeResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(result.to_public_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_replay_proof(path: Path, proof: RaonDownloadReplayProof) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(proof.to_public_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_private_replay_request(path: Path, request: RaonPrivateReplayRequest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(request.to_private_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe V-World RAON/KUpload acquisition page")
    parser.add_argument("--download-ds-id", required=True)
    parser.add_argument("--file-no", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--env-file")
    parser.add_argument("--use-vworld-login", action="store_true")
    parser.add_argument("--prove-raon-replay", action="store_true")
    parser.add_argument("--private-replay-request-output")
    parser.add_argument("--landing-object-key")
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args(argv)

    env = dict(os.environ)
    if args.env_file:
        env.update(load_env_file(Path(args.env_file)))

    cookie_header = env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER")
    user_agent = env.get(
        "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_USER_AGENT",
        env.get("FOUNDATION_PLATFORM_VWORLD_USER_AGENT", DEFAULT_BROWSER_USER_AGENT),
    )
    if args.use_vworld_login and not cookie_header:
        cookie_header = fetch_vworld_cookie_header(
            username=env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME")
            or env.get("VWORLD_USERNAME", ""),
            password=env.get("FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD")
            or env.get("VWORLD_PASSWORD", ""),
            user_agent=user_agent,
        )

    if args.prove_raon_replay:
        if args.private_replay_request_output and not args.landing_object_key:
            parser.error("--landing-object-key is required with --private-replay-request-output")
        acquisition = acquire_raon_replay(
            download_ds_id=args.download_ds_id,
            file_no=args.file_no,
            headless=not args.headed,
            cookie_header=cookie_header,
            user_agent=user_agent,
            landing_object_key=args.landing_object_key,
        )
        write_replay_proof(Path(args.output), acquisition.proof)
        if args.private_replay_request_output:
            if acquisition.private_request is None:
                return 2
            write_private_replay_request(
                Path(args.private_replay_request_output),
                acquisition.private_request,
            )
        return 0 if acquisition.proof.replay_looks_zip else 2

    result = probe_raon_page(
        args.download_ds_id,
        args.file_no,
        headless=not args.headed,
        cookie_header=cookie_header,
        user_agent=user_agent,
    )
    write_probe_result(Path(args.output), result)
    return 0 if result.status != "probe_error" else 2


def _response_text(response: object) -> str:
    for attr in ("html_content", "body", "content", "text"):
        value = getattr(response, attr, None)
        if isinstance(value, str):
            if value:
                return value
            continue
        if isinstance(value, bytes):
            if value:
                return value.decode("utf-8", errors="replace")
            continue
        if attr in {"html_content", "text"} and value is not None:
            text = str(value)
            if text:
                return text
    return str(response)


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        values[key] = value
    return values


def fetch_vworld_cookie_header(
    *,
    username: str,
    password: str,
    user_agent: str,
    base_uri: str = "https://www.vworld.kr",
) -> str:
    if not username or not password:
        raise ValueError("VWorld username and password are required for provider login")

    import requests

    login_url = f"{base_uri.rstrip('/')}/v4po_usrlogin_a004.do"
    referer_url = f"{base_uri.rstrip('/')}/anyId/login.do"
    response = requests.post(
        login_url,
        headers={
            "user-agent": user_agent,
            "x-requested-with": "XMLHttpRequest",
            "origin": base_uri.rstrip("/"),
            "referer": referer_url,
        },
        data={
            "usrIdeE": base64.b64encode(username.encode()).decode(),
            "usrPwdE": base64.b64encode(password.encode()).decode(),
            "nextUrl": "/v4po_main.do",
        },
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    login_result = str(result.get("resultMap", {}).get("result", ""))
    if login_result not in {"success", "expirePw"}:
        raise RuntimeError("VWorld login did not return a session-bearing result")
    if not response.cookies:
        raise RuntimeError("VWorld login did not return session cookies")
    return "; ".join(f"{cookie.name}={cookie.value}" for cookie in response.cookies)


def _extra_headers(cookie_header: str | None, user_agent: str | None) -> dict[str, str] | None:
    headers: dict[str, str] = {}
    if cookie_header:
        headers["Cookie"] = cookie_header
    if user_agent:
        headers["User-Agent"] = user_agent
    return headers or None


def _response_url(response: object) -> str | None:
    value = getattr(response, "url", None)
    return value if isinstance(value, str) else None


def _response_status(response: object) -> int | None:
    value = getattr(response, "status", None)
    return value if isinstance(value, int) else None


def _captured_response(response: object) -> CapturedNetworkResponse | None:
    url = getattr(response, "url", None)
    if not isinstance(url, str):
        return None

    body = getattr(response, "body", b"")
    if isinstance(body, str):
        size_bytes = len(body.encode("utf-8"))
    elif isinstance(body, bytes):
        size_bytes = len(body)
    else:
        size_bytes = 0

    status = getattr(response, "status", None)
    if not isinstance(status, int):
        status = None

    return CapturedNetworkResponse(url=url, status=status, size_bytes=size_bytes)


def _contains_raon_marker(text: str) -> bool:
    upper = text.upper()
    return "RAONKUPLOAD" in upper or "ADDUPLOADEDFILE" in upper


if __name__ == "__main__":
    raise SystemExit(main())

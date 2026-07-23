import json
import sys
import builtins
from pathlib import Path
from types import SimpleNamespace

import pytest

import foundation_provider_acquisition.raon as raon_module
from foundation_provider_acquisition.raon import (
    CapturedNetworkResponse,
    RaonPrivateReplayRequest,
    RaonReplayAcquisition,
    RaonDownloadReplayProof,
    RaonReplayPostCandidate,
    RaonProbeResult,
    RaonUploadedFile,
    build_private_replay_request,
    extract_uploaded_files_from_script,
    fetch_vworld_cookie_header,
    load_env_file,
    probe_raon_page,
    raon_page_url,
    redact_url,
    _choose_replay_post_candidate,
    _cookie_header_with_value,
    _cookies_from_cookie_header,
    _invoke_provider_filedown,
    _replay_zip_prefix,
    _response_text,
    _zip_candidate_from_browser_response,
)


def test_raon_page_url_builds_provider_entry_url() -> None:
    assert (
        raon_page_url("20991231DS99991", "9001")
        == "https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001"
    )


def test_redact_url_masks_session_like_query_values() -> None:
    assert (
        redact_url("https://example.test/file?JSESSIONID=abc&token=secret&safe=ok")
        == "https://example.test/file?JSESSIONID=%5BREDACTED%5D&token=%5BREDACTED%5D&safe=ok"
    )


def test_redact_url_masks_raon_transient_query_values() -> None:
    assert (
        redact_url("https://dw.vworld.kr/raonkupload/handler/raonkhandler.jsp?k00=abc&raonk=secret&safe=ok")
        == "https://dw.vworld.kr/raonkupload/handler/raonkhandler.jsp?k00=%5BREDACTED%5D&raonk=%5BREDACTED%5D&safe=ok"
    )


def test_load_env_file_ignores_comments_and_keeps_values(tmp_path) -> None:
    env_path = tmp_path / ".env.local"
    env_path.write_text(
        "# comment\nVWORLD_USERNAME=alice\nVWORLD_PASSWORD=\"secret value\"\nEMPTY=\n",
        encoding="utf-8",
    )

    assert load_env_file(env_path) == {
        "VWORLD_USERNAME": "alice",
        "VWORLD_PASSWORD": "secret value",
        "EMPTY": "",
    }


def test_cookies_from_cookie_header_builds_browser_cookie_objects() -> None:
    cookies = _cookies_from_cookie_header("PJSESSIONID=pj; JSESSIONID=js")

    assert cookies == [
        {
            "name": "PJSESSIONID",
            "value": "pj",
            "domain": ".vworld.kr",
            "path": "/",
            "secure": True,
            "httpOnly": False,
        },
        {
            "name": "JSESSIONID",
            "value": "js",
            "domain": ".vworld.kr",
            "path": "/",
            "secure": True,
            "httpOnly": False,
        },
    ]


def test_cookie_header_with_value_replaces_or_appends_cookie() -> None:
    assert (
        _cookie_header_with_value("PJSESSIONID=pj; JSESSIONID=old", "JSESSIONID", "new")
        == "PJSESSIONID=pj; JSESSIONID=new"
    )
    assert (
        _cookie_header_with_value("PJSESSIONID=pj", "JSESSIONID", "new")
        == "PJSESSIONID=pj; JSESSIONID=new"
    )


def test_main_rejects_native_agent_artifact_mode(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        raon_module,
        "acquire_native_agent_artifact",
        lambda **kwargs: pytest.fail("native agent must not be an operational CLI mode"),
        raising=False,
    )

    with pytest.raises(SystemExit):
        raon_module.main(
            [
                "--download-ds-id",
                "20991231DS99991",
                "--file-no",
                "9001",
                "--output",
                str(tmp_path / "native-agent.json"),
                "--acquire-native-agent-artifact",
            ]
        )


def test_fetch_vworld_cookie_header_uses_encoded_login_form(monkeypatch) -> None:
    captured_request: dict[str, object] = {}

    class FakeResponse:
        cookies = [
            SimpleNamespace(name="JSESSIONID", value="session-value"),
            SimpleNamespace(name="VWORLD", value="vworld-value"),
        ]

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"resultMap": {"result": "expirePw"}}

    def fake_post(url: str, **kwargs: object) -> FakeResponse:
        captured_request["url"] = url
        captured_request.update(kwargs)
        return FakeResponse()

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=fake_post))

    cookie_header = fetch_vworld_cookie_header(
        username="alice",
        password="secret",
        user_agent="test-agent",
        base_uri="https://example.vworld.test",
    )

    assert cookie_header == "JSESSIONID=session-value; VWORLD=vworld-value"
    assert captured_request["url"] == "https://example.vworld.test/v4po_usrlogin_a004.do"
    assert captured_request["headers"] == {
        "user-agent": "test-agent",
        "x-requested-with": "XMLHttpRequest",
        "origin": "https://example.vworld.test",
        "referer": "https://example.vworld.test/anyId/login.do",
    }
    assert captured_request["data"] == {
        "usrIdeE": "YWxpY2U=",
        "usrPwdE": "c2VjcmV0",
        "nextUrl": "/v4po_main.do",
    }


def test_fetch_vworld_cookie_header_rejects_missing_credentials() -> None:
    with pytest.raises(ValueError, match="username and password"):
        fetch_vworld_cookie_header(username="", password="", user_agent="test-agent")


def test_extract_uploaded_files_from_script_parses_raon_file_identity() -> None:
    files = extract_uploaded_files_from_script(
        """
        function fileDown() {
          RAONKUPLOAD.AddUploadedFile(
            '1',
            'SYNTHETIC_SINGLE_20991231.zip',
            '/filestore/down_store/dtna/209912/synthetic-fixture.zip',
            '4096',
            '20991231DS99991|9001',
            G_UploadID
          );
        }
        """
    )

    assert [file.to_public_dict() for file in files] == [
        {
            "order": "1",
            "original_name": "SYNTHETIC_SINGLE_20991231.zip",
            "storage_path": "/filestore/down_store/dtna/209912/synthetic-fixture.zip",
            "size_bytes": 4096,
            "provider_identity": "20991231DS99991|9001",
        }
    ]


def test_download_replay_proof_public_dict_excludes_replay_payload() -> None:
    proof = RaonDownloadReplayProof(
        download_event_filename="download.zip",
        download_event_url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?raonk=secret",
        zip_candidate_count=2,
        replay_status=200,
        replay_content_type="application/zip;charset=utf-8",
        replay_content_disposition="attachment; filename*=utf-8''download.zip;",
        replay_content_length=None,
        replay_first4_hex="504b0304",
        replay_looks_zip=True,
        error_message=None,
    )

    assert proof.to_public_dict() == {
        "download_event_filename": "download.zip",
        "download_event_url": "https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?raonk=%5BREDACTED%5D",
        "zip_candidate_count": 2,
        "replay_status": 200,
        "replay_content_type": "application/zip;charset=utf-8",
        "replay_content_disposition": "attachment; filename*=utf-8''download.zip;",
        "replay_content_length": None,
        "replay_first4_hex": "504b0304",
        "replay_looks_zip": True,
        "acquisition_strategy": "browser_replay_candidate",
        "raw_validation_owner": "foundation_platform_rust_importer",
        "raw_payload_validated": False,
        "replay_validation_scope": "transport_prefix_only",
        "error_message": None,
    }


def test_download_replay_proof_marks_missing_replay_as_not_checked() -> None:
    proof = RaonDownloadReplayProof(
        download_event_filename=None,
        download_event_url=None,
        zip_candidate_count=0,
        replay_status=None,
        replay_content_type=None,
        replay_content_disposition=None,
        replay_content_length=None,
        replay_first4_hex=None,
        replay_looks_zip=False,
        error_message="no replay",
    )

    public = proof.to_public_dict()

    assert public["raw_payload_validated"] is False
    assert public["replay_validation_scope"] == "not_replayed"
    assert public["acquisition_strategy"] == "browser_replay_candidate"
    assert public["raw_validation_owner"] == "foundation_platform_rust_importer"


def test_choose_replay_post_candidate_prefers_real_zip_document_post() -> None:
    candidates = [
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=first",
            post_data="k00=first",
            content_type="application/zip;charset=utf-8",
            request_content_type="application/x-www-form-urlencoded",
            content_disposition='attachment; filename="SYNTHETIC_SINGLE_20991231.zip";',
            content_length="0",
            status=200,
            resource_type="document",
        ),
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=real",
            post_data="k00=real",
            content_type="application/zip;charset=utf-8",
            request_content_type="application/x-www-form-urlencoded",
            content_disposition='attachment; filename="download.zip";',
            content_length=None,
            status=200,
            resource_type="document",
        ),
    ]

    chosen = _choose_replay_post_candidate(candidates)

    assert chosen is not None
    assert chosen.post_data == "k00=real"


def test_choose_replay_post_candidate_ignores_payloadless_zip_candidates() -> None:
    candidates = [
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            post_data="",
            content_type="application/zip;charset=utf-8",
            request_content_type="application/x-www-form-urlencoded",
            content_disposition='attachment; filename="download.zip";',
            content_length=None,
            status=200,
            resource_type="document",
        )
    ]

    assert _choose_replay_post_candidate(candidates) is None


def test_choose_replay_candidate_accepts_provider_filedown_get() -> None:
    candidates = [
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=long-provider-token",
            post_data="",
            content_type="application/zip;charset=utf-8",
            request_content_type=None,
            content_disposition='attachment; filename="download.zip";',
            content_length="4096",
            status=200,
            resource_type="document",
            method="GET",
        )
    ]

    chosen = _choose_replay_post_candidate(candidates)

    assert chosen is not None
    assert chosen.method == "GET"
    assert "k00=" in chosen.url


def test_choose_replay_candidate_prefers_provider_filedown_get_over_synthetic_post() -> None:
    candidates = [
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=provider-filedown",
            post_data="",
            content_type="application/zip;charset=utf-8",
            request_content_type=None,
            content_disposition='attachment; filename="download.zip";',
            content_length="4096",
            status=200,
            resource_type="document",
            method="GET",
        ),
        RaonReplayPostCandidate(
            url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            post_data="k00=probe-post",
            content_type="application/zip;charset=utf-8",
            request_content_type="application/x-www-form-urlencoded",
            content_disposition='attachment; filename="download.zip";',
            content_length=None,
            status=200,
            resource_type="document",
            method="POST",
        ),
    ]

    chosen = _choose_replay_post_candidate(candidates)

    assert chosen is not None
    assert chosen.method == "GET"
    assert chosen.url.endswith("k00=provider-filedown")


def test_zip_candidate_from_browser_response_accepts_provider_filedown_get() -> None:
    response = SimpleNamespace(
        headers={
            "content-type": "application/zip;charset=utf-8",
            "content-disposition": 'attachment; filename="download.zip";',
            "content-length": "4096",
        },
        request=SimpleNamespace(
            method="GET",
            post_data=None,
            resource_type="document",
            headers={},
        ),
        url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=long-provider-token",
        status=200,
    )

    candidate = _zip_candidate_from_browser_response(response)

    assert candidate is not None
    assert candidate.method == "GET"
    assert candidate.post_data == ""
    assert candidate.content_length == "4096"


def test_zip_candidate_from_browser_response_accepts_long_k00_get_before_zip_headers() -> None:
    long_k00 = "x" * 160
    response = SimpleNamespace(
        headers={
            "content-type": "text/plain;charset=utf-8",
            "content-length": "0",
        },
        request=SimpleNamespace(
            method="GET",
            post_data=None,
            resource_type="document",
            headers={},
        ),
        url=f"https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00={long_k00}",
        status=200,
    )

    candidate = _zip_candidate_from_browser_response(response)

    assert candidate is not None
    assert candidate.method == "GET"
    assert candidate.post_data == ""
    assert candidate.url.endswith(long_k00)


def test_invoke_provider_filedown_uses_original_provider_page_function() -> None:
    captured: dict[str, object] = {}

    class FakePage:
        def wait_for_function(self, script: str, *, timeout: int) -> None:
            captured["wait_script"] = script
            captured["wait_timeout"] = timeout

        def evaluate(self, script: str) -> dict[str, object]:
            captured["script"] = script
            return {"ok": True, "invocation": "provider_fileDown"}

    result = _invoke_provider_filedown(FakePage())

    assert result == {"ok": True, "invocation": "provider_fileDown"}
    assert "fileDown" in captured["wait_script"]
    assert captured["wait_timeout"] == 15_000
    assert "fileDown" in captured["script"]
    assert "probe_html5" not in captured["script"]


def test_main_writes_raon_replay_proof_when_flag_enabled(monkeypatch, tmp_path) -> None:
    output_path = tmp_path / "proof.json"

    def fake_acquire_raon_replay(**kwargs: object) -> RaonReplayAcquisition:
        assert kwargs["download_ds_id"] == "20991231DS99991"
        assert kwargs["file_no"] == "9001"
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename="download.zip",
                download_event_url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?raonk=secret",
                zip_candidate_count=1,
                replay_status=200,
                replay_content_type="application/zip;charset=utf-8",
                replay_content_disposition="attachment; filename*=utf-8''download.zip;",
                replay_content_length=None,
                replay_first4_hex="504b0304",
                replay_looks_zip=True,
                error_message=None,
            ),
            private_request=None,
        )

    monkeypatch.setattr(raon_module, "acquire_raon_replay", fake_acquire_raon_replay)

    exit_code = raon_module.main(
        [
            "--download-ds-id",
            "20991231DS99991",
            "--file-no",
            "9001",
            "--output",
            str(output_path),
            "--prove-raon-replay",
        ]
    )

    assert exit_code == 0
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["replay_looks_zip"] is True
    assert written["download_event_url"].endswith("raonk=%5BREDACTED%5D")


def test_raon_replay_operational_path_uses_scrapling_only(monkeypatch) -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )

    monkeypatch.setattr(
        raon_module,
        "probe_raon_page",
        lambda *args, **kwargs: RaonProbeResult(
            status="provider_acquisition_blocked",
            download_ds_id="20991231DS99991",
            file_no="9001",
            page_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            final_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            http_status=200,
            html_contains_raon=True,
            uploaded_files=[uploaded_file],
            captured_xhr=[],
            error_message=None,
        ),
    )
    monkeypatch.setattr(
        raon_module,
        "_acquire_raon_provider_filedown_playwright",
        lambda **kwargs: pytest.fail("direct browser runtime must not be an operational RAON path"),
        raising=False,
    )
    monkeypatch.setattr(
        raon_module,
        "_replay_zip_prefix",
        lambda *args, **kwargs: {
            "status": 200,
            "content_type": "application/zip;charset=utf-8",
            "content_disposition": "attachment; filename*=utf-8''download.zip;",
            "content_length": None,
            "first4_hex": "504b0304",
            "looks_zip": True,
        },
    )

    class FakePage:
        def __init__(self) -> None:
            self._response_handler = None

        def on(self, event: str, handler) -> None:
            if event == "response":
                self._response_handler = handler

        def wait_for_function(self, script: str, *, timeout: int) -> None:
            return None

        def evaluate(self, script: str, *args: object) -> dict[str, object]:
            return {"ok": True, "invocation": "provider_fileDown"}

        def wait_for_timeout(self, timeout: int) -> None:
            if self._response_handler is None:
                return None
            response = SimpleNamespace(
                headers={
                    "content-type": "application/zip;charset=utf-8",
                    "content-disposition": 'attachment; filename="download.zip";',
                    "content-length": "42",
                },
                request=SimpleNamespace(
                    method="GET",
                    post_data=None,
                    resource_type="document",
                    headers={},
                ),
                url=(
                    "https://dw.vworld.kr/vwDnMng/raonkupload/handler/"
                    f"raonkhandler.jsp?k00={'x' * 160}"
                ),
                status=200,
            )
            self._response_handler(response)
            return None

    class FakeDynamicSession:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch(
            self,
            url: str,
            *,
            load_dom: bool,
            wait: int,
            page_setup,
            page_action,
        ) -> None:
            page = FakePage()
            page_setup(page)
            page_action(page)
            return None

    monkeypatch.setitem(
        sys.modules,
        "scrapling.fetchers",
        SimpleNamespace(DynamicSession=FakeDynamicSession),
    )

    acquisition = raon_module.acquire_raon_replay(
        download_ds_id="20991231DS99991",
        file_no="9001",
        cookie_header="PJSESSIONID=pj",
        user_agent="test-agent",
        landing_object_key="landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/download.zip",
    )

    assert acquisition.private_request is not None
    assert acquisition.private_request.method == "GET"
    assert acquisition.proof.replay_looks_zip is True


def test_raon_replay_merges_handler_jsessionid_into_replay_cookie(monkeypatch) -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )
    replay_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        raon_module,
        "probe_raon_page",
        lambda *args, **kwargs: RaonProbeResult(
            status="provider_acquisition_blocked",
            download_ds_id="20991231DS99991",
            file_no="9001",
            page_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            final_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            http_status=200,
            html_contains_raon=True,
            uploaded_files=[uploaded_file],
            captured_xhr=[],
            error_message=None,
        ),
    )

    def fake_replay_zip_prefix(*args: object, **kwargs: object) -> dict[str, object]:
        replay_calls.append(kwargs)
        return {
            "status": 200,
            "content_type": "application/zip;charset=utf-8",
            "content_disposition": "attachment; filename*=utf-8''download.zip;",
            "content_length": None,
            "first4_hex": "504b0304",
            "looks_zip": True,
        }

    monkeypatch.setattr(raon_module, "_replay_zip_prefix", fake_replay_zip_prefix)

    class FakePage:
        def __init__(self) -> None:
            self._response_handler = None

        def on(self, event: str, handler) -> None:
            if event == "response":
                self._response_handler = handler

        def wait_for_function(self, script: str, *, timeout: int) -> None:
            return None

        def evaluate(self, script: str, *args: object) -> dict[str, object]:
            return {"ok": True, "invocation": "provider_fileDown"}

        def wait_for_timeout(self, timeout: int) -> None:
            if self._response_handler is None:
                return None
            self._response_handler(
                SimpleNamespace(
                    headers={
                        "content-type": "text/plain;charset=utf-8",
                        "set-cookie": "JSESSIONID=raon-session; Path=/; HttpOnly",
                    },
                    request=SimpleNamespace(
                        method="POST",
                        post_data="raonk=init",
                        resource_type="xhr",
                        headers={"content-type": "application/x-www-form-urlencoded"},
                    ),
                    url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?raonk=init",
                    status=200,
                )
            )
            self._response_handler(
                SimpleNamespace(
                    headers={
                        "content-type": "application/zip;charset=utf-8",
                        "content-disposition": 'attachment; filename="download.zip";',
                        "content-length": "42",
                    },
                    request=SimpleNamespace(
                        method="GET",
                        post_data=None,
                        resource_type="document",
                        headers={},
                    ),
                    url=(
                        "https://dw.vworld.kr/vwDnMng/raonkupload/handler/"
                        f"raonkhandler.jsp?k00={'x' * 160}"
                    ),
                    status=200,
                )
            )
            return None

    class FakeDynamicSession:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch(
            self,
            url: str,
            *,
            load_dom: bool,
            wait: int,
            page_setup,
            page_action,
        ) -> None:
            page = FakePage()
            page_setup(page)
            page_action(page)
            return None

    monkeypatch.setitem(
        sys.modules,
        "scrapling.fetchers",
        SimpleNamespace(DynamicSession=FakeDynamicSession),
    )

    acquisition = raon_module.acquire_raon_replay(
        download_ds_id="20991231DS99991",
        file_no="9001",
        cookie_header="PJSESSIONID=pj",
        user_agent="test-agent",
        landing_object_key="landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/download.zip",
    )

    assert replay_calls[0]["cookie_header"] == "PJSESSIONID=pj; JSESSIONID=raon-session"
    assert acquisition.private_request is not None
    assert acquisition.private_request.cookie_header == "PJSESSIONID=pj; JSESSIONID=raon-session"


def test_raon_replay_does_not_fallback_to_synthetic_upload(monkeypatch) -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )
    synthetic_upload_called = False

    monkeypatch.setattr(
        raon_module,
        "probe_raon_page",
        lambda *args, **kwargs: RaonProbeResult(
            status="provider_acquisition_blocked",
            download_ds_id="20991231DS99991",
            file_no="9001",
            page_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            final_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            http_status=200,
            html_contains_raon=True,
            uploaded_files=[uploaded_file],
            captured_xhr=[],
            error_message=None,
        ),
    )

    def fake_synthetic_upload(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal synthetic_upload_called
        synthetic_upload_called = True
        return {"ok": True}

    monkeypatch.setattr(
        raon_module,
        "_install_raon_html5_file",
        fake_synthetic_upload,
        raising=False,
    )

    class FakePage:
        def on(self, event: str, handler) -> None:
            return None

        def wait_for_function(self, script: str, *, timeout: int) -> None:
            return None

        def evaluate(self, script: str, *args: object) -> dict[str, object]:
            return {"ok": True, "invocation": "provider_fileDown"}

        def wait_for_timeout(self, timeout: int) -> None:
            return None

    class FakeDynamicSession:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch(
            self,
            url: str,
            *,
            load_dom: bool,
            wait: int,
            page_setup,
            page_action,
        ) -> None:
            page = FakePage()
            page_setup(page)
            page_action(page)
            return None

    monkeypatch.setitem(
        sys.modules,
        "scrapling.fetchers",
        SimpleNamespace(DynamicSession=FakeDynamicSession),
    )

    acquisition = raon_module.acquire_raon_replay(
        download_ds_id="20991231DS99991",
        file_no="9001",
        cookie_header="PJSESSIONID=pj",
        user_agent="test-agent",
    )

    assert synthetic_upload_called is False
    assert acquisition.private_request is None
    assert acquisition.proof.replay_looks_zip is False
    assert "provider fileDown" in (acquisition.proof.error_message or "")


def test_raon_replay_reports_agent_runtime_requirement(monkeypatch) -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )

    monkeypatch.setattr(
        raon_module,
        "probe_raon_page",
        lambda *args, **kwargs: RaonProbeResult(
            status="provider_acquisition_blocked",
            download_ds_id="20991231DS99991",
            file_no="9001",
            page_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            final_url="https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?ds_file_sq=20991231DS999919001",
            http_status=200,
            html_contains_raon=True,
            uploaded_files=[uploaded_file],
            captured_xhr=[],
            error_message=None,
        ),
    )

    class FakePage:
        def __init__(self) -> None:
            self._response_handler = None

        def on(self, event: str, handler) -> None:
            if event == "response":
                self._response_handler = handler

        def wait_for_function(self, script: str, *, timeout: int) -> None:
            return None

        def evaluate(self, script: str, *args: object) -> dict[str, object]:
            return {"ok": True, "invocation": "provider_fileDown"}

        def wait_for_timeout(self, timeout: int) -> None:
            if self._response_handler is None:
                return None
            self._response_handler(
                SimpleNamespace(
                    headers={
                        "content-type": "application/x-msdownload",
                        "content-length": "2048",
                    },
                    request=SimpleNamespace(
                        method="GET",
                        post_data=None,
                        resource_type="document",
                        headers={},
                    ),
                    url="https://www.vworld.kr/raonkupload/agent/raonkSetup.exe?",
                    status=200,
                )
            )
            return None

    class FakeDynamicSession:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch(
            self,
            url: str,
            *,
            load_dom: bool,
            wait: int,
            page_setup,
            page_action,
        ) -> None:
            page = FakePage()
            page_setup(page)
            page_action(page)
            return None

    monkeypatch.setitem(
        sys.modules,
        "scrapling.fetchers",
        SimpleNamespace(DynamicSession=FakeDynamicSession),
    )

    acquisition = raon_module.acquire_raon_replay(
        download_ds_id="20991231DS99991",
        file_no="9001",
        cookie_header="PJSESSIONID=pj",
        user_agent="test-agent",
    )

    assert acquisition.private_request is None
    assert acquisition.proof.replay_looks_zip is False
    assert "RAON agent runtime" in (acquisition.proof.error_message or "")


def test_raon_module_does_not_embed_direct_playwright_or_native_agent_paths() -> None:
    source = Path(raon_module.__file__).read_text(encoding="utf-8")

    assert "sync_playwright" not in source
    assert "_acquire_raon_provider_filedown_playwright" not in source
    assert "--acquire-native-agent-artifact" not in source
    assert "_install_raon_html5_file" not in source
    assert "probe_html5" not in source
    assert "html5" not in source.lower()


def test_private_replay_request_carries_landing_identity_and_provider_request() -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )
    candidate = RaonReplayPostCandidate(
        url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
        post_data="k00=secret-payload",
        content_type="application/zip;charset=utf-8",
        request_content_type="application/x-www-form-urlencoded",
        content_disposition='attachment; filename="download.zip";',
        content_length=None,
        status=200,
        resource_type="document",
    )

    request = build_private_replay_request(
        candidate,
        uploaded_file,
        landing_object_key=(
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
            "job_id=job-001/download.zip"
        ),
        cookie_header="JSESSIONID=secret-cookie",
        user_agent="foundation-platform-test",
        referer_url="https://www.vworld.kr/download-page",
    )

    assert request.to_private_dict() == {
        "schema_version": "foundation-platform.provider_acquisition_replay_request.v1",
        "landing_object_key": (
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
            "job_id=job-001/download.zip"
        ),
        "replay_url": "https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
        "method": "POST",
        "request_content_type": "application/x-www-form-urlencoded",
        "post_data": "k00=secret-payload",
        "provider_declared_size_bytes": 42,
        "cookie_header": "JSESSIONID=secret-cookie",
        "user_agent": "foundation-platform-test",
        "referer_url": "https://www.vworld.kr/download-page",
    }


def test_private_replay_request_supports_provider_filedown_get() -> None:
    uploaded_file = RaonUploadedFile(
        order="1",
        original_name="download.zip",
        storage_path="/filestore/down_store/dtna/209912/synthetic-fixture.zip",
        size_bytes=42,
        provider_identity="20991231DS99991|9001",
    )
    candidate = RaonReplayPostCandidate(
        url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=secret-payload",
        post_data="",
        content_type="application/zip;charset=utf-8",
        request_content_type=None,
        content_disposition='attachment; filename="download.zip";',
        content_length=None,
        status=200,
        resource_type="document",
        method="GET",
    )

    request = build_private_replay_request(
        candidate,
        uploaded_file,
        landing_object_key=(
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
            "job_id=job-001/download.zip"
        ),
        cookie_header="JSESSIONID=secret-cookie",
        user_agent="foundation-platform-test",
        referer_url="https://www.vworld.kr/download-page",
    )

    assert request.to_private_dict()["method"] == "GET"
    assert request.to_private_dict()["post_data"] == ""
    assert request.to_private_dict()["replay_url"].endswith("k00=secret-payload")


def test_main_writes_private_replay_request_when_explicitly_requested(monkeypatch, tmp_path) -> None:
    proof_path = tmp_path / "proof.json"
    private_path = tmp_path / "private-replay.json"

    def fake_acquire_raon_replay(**kwargs: object) -> RaonReplayAcquisition:
        assert kwargs["landing_object_key"] == (
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
            "job_id=job-001/download.zip"
        )
        assert "Mozilla/5.0" in kwargs["user_agent"]
        assert "Chrome/" in kwargs["user_agent"]
        return RaonReplayAcquisition(
            proof=RaonDownloadReplayProof(
                download_event_filename="download.zip",
                download_event_url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?raonk=secret",
                zip_candidate_count=1,
                replay_status=200,
                replay_content_type="application/zip;charset=utf-8",
                replay_content_disposition="attachment; filename*=utf-8''download.zip;",
                replay_content_length=None,
                replay_first4_hex="504b0304",
                replay_looks_zip=True,
                error_message=None,
            ),
            private_request=RaonPrivateReplayRequest(
                landing_object_key=(
                    "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
                    "job_id=job-001/download.zip"
                ),
                replay_url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
                request_content_type="application/x-www-form-urlencoded",
                post_data="k00=secret-payload",
                provider_declared_size_bytes=42,
                cookie_header="JSESSIONID=secret-cookie",
                user_agent="foundation-platform-test",
                referer_url="https://www.vworld.kr/download-page",
            ),
        )

    monkeypatch.setattr(raon_module, "acquire_raon_replay", fake_acquire_raon_replay)

    exit_code = raon_module.main(
        [
            "--download-ds-id",
            "20991231DS99991",
            "--file-no",
            "9001",
            "--output",
            str(proof_path),
            "--prove-raon-replay",
            "--private-replay-request-output",
            str(private_path),
            "--landing-object-key",
            (
                "landing/provider=vworldkr/acquisition=raon_kupload_browser/"
                "job_id=job-001/download.zip"
            ),
        ]
    )

    assert exit_code == 0
    public_proof = proof_path.read_text(encoding="utf-8")
    private_request = json.loads(private_path.read_text(encoding="utf-8"))
    assert "secret-payload" not in public_proof
    assert private_request["post_data"] == "k00=secret-payload"
    assert private_request["provider_declared_size_bytes"] == 42


def test_replay_zip_prefix_uses_original_request_content_type(monkeypatch) -> None:
    captured_request: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        headers = {
            "content-type": "application/zip;charset=utf-8",
            "content-disposition": "attachment; filename=\"download.zip\";",
        }

        def iter_content(self, *, chunk_size: int):
            assert chunk_size == 4
            yield b"PK\x03\x04"

        def close(self) -> None:
            return None

    def fake_post(url: str, **kwargs: object) -> FakeResponse:
        captured_request["url"] = url
        captured_request.update(kwargs)
        return FakeResponse()

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=fake_post))
    candidate = RaonReplayPostCandidate(
        url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp",
        post_data="k00=payload",
        content_type="application/zip;charset=utf-8",
        request_content_type="application/x-www-form-urlencoded",
        content_disposition="attachment",
        content_length=None,
        status=200,
        resource_type="document",
    )

    proof = _replay_zip_prefix(
        candidate,
        cookie_header=None,
        user_agent="test-agent",
        referer_url="https://www.vworld.kr/page",
        timeout_seconds=60,
    )

    assert proof["looks_zip"] is True
    assert captured_request["headers"]["content-type"] == "application/x-www-form-urlencoded"


def test_replay_zip_prefix_uses_get_for_provider_filedown_candidate(monkeypatch) -> None:
    captured_request: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        headers = {
            "content-type": "application/zip;charset=utf-8",
            "content-disposition": "attachment; filename=\"download.zip\";",
        }

        def iter_content(self, *, chunk_size: int):
            assert chunk_size == 4
            yield b"PK\x03\x04"

        def close(self) -> None:
            return None

    def fake_get(url: str, **kwargs: object) -> FakeResponse:
        captured_request["method"] = "GET"
        captured_request["url"] = url
        captured_request.update(kwargs)
        return FakeResponse()

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(get=fake_get))
    candidate = RaonReplayPostCandidate(
        url="https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=payload",
        post_data="",
        content_type="application/zip;charset=utf-8",
        request_content_type=None,
        content_disposition="attachment",
        content_length=None,
        status=200,
        resource_type="document",
        method="GET",
    )

    proof = _replay_zip_prefix(
        candidate,
        cookie_header="JSESSIONID=secret-cookie",
        user_agent="test-agent",
        referer_url="https://www.vworld.kr/page",
        timeout_seconds=60,
    )

    assert proof["looks_zip"] is True
    assert captured_request["method"] == "GET"
    assert captured_request["headers"]["cookie"] == "JSESSIONID=secret-cookie"


def test_response_text_prefers_non_empty_html_body_over_empty_text() -> None:
    response = SimpleNamespace(text="", body=b"<html>RAONKUPLOAD.AddUploadedFile()</html>")

    assert _response_text(response) == "<html>RAONKUPLOAD.AddUploadedFile()</html>"


def test_probe_raon_page_import_error_keeps_uploaded_files_empty(monkeypatch) -> None:
    original_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "scrapling.fetchers":
            raise ImportError("missing scrapling")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = probe_raon_page("20991231DS99991", "9001")

    assert result.status == "probe_error"
    assert result.uploaded_files == []


def test_probe_result_dict_redacts_captured_urls() -> None:
    result = RaonProbeResult(
        status="provider_acquisition_blocked",
        download_ds_id="20991231DS99991",
        file_no="9001",
        page_url="https://example.test/page?session=secret",
        final_url="https://example.test/final?token=secret",
        http_status=302,
        html_contains_raon=True,
        uploaded_files=[],
        captured_xhr=[
            CapturedNetworkResponse(
                url="https://example.test/handler?token=secret&safe=ok",
                status=200,
                size_bytes=10,
            )
        ],
        error_message=None,
    )

    assert result.to_public_dict() == {
        "status": "provider_acquisition_blocked",
        "download_ds_id": "20991231DS99991",
        "file_no": "9001",
        "page_url": "https://example.test/page?session=%5BREDACTED%5D",
        "final_url": "https://example.test/final?token=%5BREDACTED%5D",
        "http_status": 302,
        "html_contains_raon": True,
        "uploaded_files": [],
        "captured_xhr": [
            {
                "url": "https://example.test/handler?token=%5BREDACTED%5D&safe=ok",
                "status": 200,
                "size_bytes": 10,
            }
        ],
        "error_message": None,
    }

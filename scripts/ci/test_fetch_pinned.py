from __future__ import annotations

import hashlib
import http.server
import pathlib
import tempfile
import threading
import unittest
import urllib.request

import fetch_pinned


PAYLOAD = b"pinned release archive\n" * 64
PAYLOAD_SHA = hashlib.sha256(PAYLOAD).hexdigest()
URL = "https://releases.example.invalid/download/v1.0.0/tool_1.0.0_linux_amd64.tar.gz"


class _Server:
    """A real HTTP server whose answers are scripted per request: a status code, or 'short'."""

    def __init__(self) -> None:
        self.script: list[object] = []
        self.requests = 0
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - the http.server hook name
                server.requests += 1
                answer = server.script.pop(0) if server.script else 200
                if answer == "short":
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(PAYLOAD)))
                    self.end_headers()
                    self.wfile.write(PAYLOAD[: len(PAYLOAD) // 2])
                    self.close_connection = True
                    return
                if answer == "tampered":
                    body = PAYLOAD + b"x"
                    self.send_response(200)
                elif answer == 200:
                    body = PAYLOAD
                    self.send_response(200)
                else:
                    body = b"upstream error"
                    self.send_response(int(answer))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def opener(self, request: urllib.request.Request, timeout: float):
        # The fetcher only accepts HTTPS pins; the fixture sends the same request to this server.
        path = request.full_url.split("example.invalid", 1)[1]
        local = urllib.request.Request(
            f"http://127.0.0.1:{self.httpd.server_port}{path}", headers=dict(request.header_items())
        )
        return urllib.request.urlopen(local, timeout=timeout)

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class FetchPinnedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _Server()
        self.addCleanup(self.server.close)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.cache = pathlib.Path(temporary.name)
        self.waits: list[float] = []
        self.messages: list[str] = []

    def fetch(self, sha256: str = PAYLOAD_SHA, backoff: tuple[float, ...] = (1, 2, 4, 8)) -> pathlib.Path:
        return fetch_pinned.fetch(
            URL,
            sha256,
            cache_dir=self.cache,
            backoff=backoff,
            timeout=10,
            opener=self.server.opener,
            sleep=self.waits.append,
            log=self.messages.append,
        )

    def cached_files(self) -> list[pathlib.Path]:
        return sorted(path for path in self.cache.rglob("*") if path.is_file())

    def test_a_server_error_on_a_cold_cache_is_retried_and_the_next_run_needs_no_network(self) -> None:
        self.server.script = [500, 504, 200]
        path = self.fetch()
        self.assertEqual(path.read_bytes(), PAYLOAD)
        self.assertEqual(self.server.requests, 3)
        self.assertEqual(self.waits, [1, 2])

        # The CDN is now down for good; the pinned file still comes back, from the cache.
        self.server.script = [500] * 10
        self.assertEqual(self.fetch(), path)
        self.assertEqual(self.server.requests, 3)
        self.assertIn("cache hit", self.messages[-1])

    def test_a_truncated_body_and_a_rate_limit_are_retried(self) -> None:
        self.server.script = ["short", 429, 200]
        self.assertEqual(self.fetch().read_bytes(), PAYLOAD)
        self.assertEqual(self.server.requests, 3)

    def test_a_checksum_mismatch_fails_at_once_and_caches_nothing(self) -> None:
        self.server.script = ["tampered", 200]
        with self.assertRaisesRegex(fetch_pinned.FetchError, "SHA-256 mismatch"):
            self.fetch()
        self.assertEqual(self.server.requests, 1)
        self.assertEqual(self.cached_files(), [])

    def test_a_missing_asset_is_not_retried(self) -> None:
        self.server.script = [404, 200]
        with self.assertRaisesRegex(fetch_pinned.FetchError, "HTTP 404"):
            self.fetch()
        self.assertEqual(self.server.requests, 1)
        self.assertEqual(self.waits, [])

    def test_an_outage_longer_than_the_backoff_fails_after_every_attempt(self) -> None:
        self.server.script = [503] * 10
        with self.assertRaisesRegex(fetch_pinned.FetchError, "gave up after 3 attempts"):
            self.fetch(backoff=(1, 2))
        self.assertEqual(self.server.requests, 3)
        self.assertEqual(self.cached_files(), [])

    def test_a_corrupted_cache_entry_is_fetched_again(self) -> None:
        self.server.script = [200, 200]
        path = self.fetch()
        path.write_bytes(b"bit rot")
        self.assertEqual(self.fetch().read_bytes(), PAYLOAD)
        self.assertEqual(self.server.requests, 2)

    def test_prune_keeps_only_current_pins(self) -> None:
        self.server.script = [200]
        current = self.fetch()
        stale = self.cache / ("0" * 64) / "tool_0.9.0_linux_amd64.tar.gz"
        stale.parent.mkdir()
        stale.write_bytes(b"an older pin")
        self.assertEqual(fetch_pinned.prune(self.cache, {PAYLOAD_SHA}), 1)
        self.assertEqual(self.cached_files(), [current])

    def test_only_https_pins_are_admitted(self) -> None:
        with self.assertRaisesRegex(fetch_pinned.FetchError, "HTTPS"):
            fetch_pinned.fetch("http://example.invalid/a.tar.gz", PAYLOAD_SHA, cache_dir=self.cache)


class ExtractMemberTest(unittest.TestCase):
    """One file comes out of either archive kind, executable; a missing one is refused."""

    def test_zip_and_tar_gz_yield_the_member(self) -> None:
        import io
        import os
        import tarfile
        import zipfile

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            zipped = root / "tool.zip"
            with zipfile.ZipFile(zipped, "w") as archive:
                archive.writestr("tool.exe", b"zip payload")
            tarred = root / "tool.tar.gz"
            with tarfile.open(tarred, "w:gz") as archive:
                info = tarfile.TarInfo("tool")
                info.size = len(b"tar payload")
                archive.addfile(info, io.BytesIO(b"tar payload"))

            fetch_pinned.extract_member(zipped, "tool.exe", root / "out" / "tool.exe")
            fetch_pinned.extract_member(tarred, "tool", root / "out" / "tool")
            self.assertEqual((root / "out" / "tool.exe").read_bytes(), b"zip payload")
            self.assertEqual((root / "out" / "tool").read_bytes(), b"tar payload")
            if os.name != "nt":
                self.assertTrue(os.access(root / "out" / "tool", os.X_OK))
            with self.assertRaisesRegex(fetch_pinned.FetchError, "has no member"):
                fetch_pinned.extract_member(tarred, "other", root / "out" / "other")
            self.assertFalse((root / "out" / "other").exists())


class RegistryTest(unittest.TestCase):
    """Every registered tool resolves on every platform it declares, from the real pin files."""

    def test_every_declared_platform_has_a_pin(self) -> None:
        registry = fetch_pinned.load_registry()
        resolved = 0
        for name, spec in registry["tools"].items():
            for system in spec["os"]:
                self.assertIn(system, spec["extension"], f"{name} has no extension for {system}")
                for machine in spec["arch"]:
                    url, sha256 = fetch_pinned.tool_pin(registry, name, system, machine)
                    self.assertTrue(url.startswith("https://"), url)
                    self.assertNotIn("{", url, url)
                    self.assertEqual(len(sha256), 64)
                    resolved += 1
        self.assertGreater(resolved, 0)

    def test_a_ci_runner_resolves_every_tool_and_the_static_release_contract(self) -> None:
        registry = fetch_pinned.load_registry()
        names = {name for name, _, _ in fetch_pinned.all_pins(registry, "linux", "x86_64")}
        self.assertTrue(set(registry["tools"]) <= names)
        self.assertGreater(len(names), len(registry["tools"]))

    def test_a_pin_file_missing_its_checksum_is_refused(self) -> None:
        registry = fetch_pinned.load_registry()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            pins = root / registry["tools"]["actionlint"]["pins"]
            pins.parent.mkdir(parents=True)
            pins.write_text("ACTIONLINT_VERSION=1.0.0\n", encoding="utf-8")
            with self.assertRaisesRegex(fetch_pinned.FetchError, "must set"):
                fetch_pinned.tool_pin(registry, "actionlint", "linux", "x86_64", root)


if __name__ == "__main__":
    unittest.main()

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


class PinnedFileTest(unittest.TestCase):
    """A download that unpacks to one file (a DuckDB extension, root ADR-0559 §6) is held to that
    file's own pin: the published .gz and the file the publisher loads are both named."""

    def setUp(self) -> None:
        import gzip

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.archive = self.root / "engine.duckdb_extension.gz"
        self.archive.write_bytes(gzip.compress(PAYLOAD))
        self.file = {"name": "engine.duckdb_extension", "compression": "gzip", "sha256": PAYLOAD_SHA,
                     "size": len(PAYLOAD)}

    def test_the_unpacked_file_is_written_when_it_is_the_pinned_one(self) -> None:
        output = self.root / "out" / "engine.duckdb_extension"
        fetch_pinned.unpack_pinned_file(self.archive, self.file, output)
        self.assertEqual(output.read_bytes(), PAYLOAD)

    def test_another_file_or_another_size_is_refused_and_nothing_is_left(self) -> None:
        output = self.root / "out" / "engine.duckdb_extension"
        for wrong in ({"sha256": "0" * 64}, {"size": len(PAYLOAD) + 1}, {"compression": "zstd"}):
            with self.subTest(wrong=wrong), self.assertRaises(fetch_pinned.FetchError):
                fetch_pinned.unpack_pinned_file(self.archive, {**self.file, **wrong}, output)
            self.assertEqual(list(output.parent.glob("*")) if output.parent.exists() else [], [])

    def test_a_download_that_is_not_gzip_is_refused(self) -> None:
        self.archive.write_bytes(PAYLOAD)
        with self.assertRaisesRegex(fetch_pinned.FetchError, "does not unpack"):
            fetch_pinned.unpack_pinned_file(self.archive, self.file, self.root / "out" / "x")

    def test_every_registered_contract_distribution_resolves_by_name(self) -> None:
        registry = fetch_pinned.load_registry()
        resolved = 0
        for relative in registry["contracts"]:
            contract = fetch_pinned.ROOT / relative
            for name, distribution in fetch_pinned.json.loads(contract.read_text(encoding="utf-8"))["distributions"].items():
                for key in distribution["platforms"]:
                    system, machine = key.split("-", 1)
                    url, sha256, file = fetch_pinned.distribution_pin(contract, name, system, machine)
                    self.assertTrue(url.startswith("https://"), url)
                    self.assertEqual(len(sha256), 64)
                    if file is not None:
                        self.assertEqual(len(file["sha256"]), 64)
                        self.assertGreater(file["size"], 0)
                    resolved += 1
        self.assertGreater(resolved, 0)
        with self.assertRaisesRegex(fetch_pinned.FetchError, "pins no distribution"):
            fetch_pinned.distribution_pin(fetch_pinned.ROOT / registry["contracts"][0], "absent", "linux", "x86_64")


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
        ci_tools = {name for name, spec in registry["tools"].items() if spec.get("ci", True) is not False}
        self.assertTrue(ci_tools <= names)
        self.assertGreater(len(names), len(ci_tools))

    def test_the_saved_cache_key_names_every_file_that_pins_a_download(self) -> None:
        # The cache main fills is saved under a key hashed from the pin files (root ADR-0197). A
        # contract the key does not hash keeps the old key when its pins change: the save is skipped
        # (the key exists) and every job downloads the new files itself, the 2026-10-12 failure again.
        registry = fetch_pinned.load_registry()
        saves = [
            (workflow.name, line)
            for workflow in sorted((fetch_pinned.ROOT / ".github" / "workflows").glob("*.yml"))
            for line in workflow.read_text(encoding="utf-8").splitlines()
            if "pinned-release-binaries-v1-" in line and "hashFiles(" in line
        ]
        self.assertTrue(saves)
        for name, line in saves:
            for hashed in ("tools/pinned-artifacts.json", "tools/*.env", *registry["contracts"]):
                self.assertIn(f"'{hashed}'", line, f"{name} saves the pinned cache without hashing {hashed}")

    def test_a_server_only_tool_stays_out_of_the_ci_cache(self) -> None:
        # cosign is fetched by the server's release admission only (root ADR-0276 §3); prefetching it
        # put 52 MB into every CI pinned cache (2026-10-19).
        registry = fetch_pinned.load_registry()
        names = {name for name, _, _ in fetch_pinned.all_pins(registry, "linux", "x86_64")}
        self.assertNotIn("cosign", names)
        self.assertIsNotNone(fetch_pinned.tool_pin(registry, "cosign", "linux", "x86_64"))

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

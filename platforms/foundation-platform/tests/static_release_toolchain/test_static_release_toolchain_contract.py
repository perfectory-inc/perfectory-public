from __future__ import annotations

import hashlib
import io
import json
import pathlib
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts" / "tiles"))

import static_release_toolchain_contract as subject


FIXTURE_TOOLS = frozenset({"demo"})


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def tar_gz(member: str, value: bytes) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        info = tarfile.TarInfo(member)
        info.size = len(value)
        archive.addfile(info, io.BytesIO(value))
    return output.getvalue()


def contract_for(archive: bytes, executable: bytes) -> dict[str, object]:
    return {
        "schema_version": 1,
        "tools": {
            "demo": {
                "version": "1.2.3",
                "version_command": ["--version"],
                "banner_prefix": "demo ",
                "banner_suffix": "",
                "distribution": "demo-release",
                "compatibility_reason": "fixture",
            }
        },
        "distributions": {
            "demo-release": {
                "source": "fixture",
                "oci": {
                    "environment_variable": "PERFECTORY_DEMO_IMAGE",
                    "repository": "registry.example.invalid/demo",
                    "tag_tool": "demo",
                    "tag_prefix": "v",
                    "digest": "0" * 64,
                },
                "platforms": {
                    "linux-x86_64": {
                        "url": "https://example.invalid/demo.tar.gz",
                        "archive_format": "tar.gz",
                        "sha256": sha256(archive),
                        "executables": {
                            "demo": {
                                "member": "demo",
                                "filename": "demo",
                                "sha256": sha256(executable),
                            }
                        },
                    }
                },
            }
        },
    }


class StaticReleaseToolchainContractTest(unittest.TestCase):
    def test_repository_contract_projects_digest_pinned_images(self) -> None:
        contract = subject.load_contract()
        environment = subject.image_environment(contract)
        self.assertEqual(
            {"PERFECTORY_MARTIN_IMAGE", "PERFECTORY_PMTILES_IMAGE"},
            set(environment),
        )
        for image in environment.values():
            self.assertRegex(image, r"^[^@]+@sha256:[0-9a-f]{64}$")

    def test_production_validation_rejects_an_incomplete_fixture_tool_set(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        with self.assertRaisesRegex(subject.ContractError, "complete supported tool set"):
            subject.validate_contract(contract_for(archive, executable))

    def test_shell_projection_rejects_an_unsafe_environment_variable(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        contract = contract_for(archive, executable)
        contract["distributions"]["demo-release"]["oci"]["environment_variable"] = (
            "BAD;touch injected"
        )
        with self.assertRaisesRegex(subject.ContractError, "shell-safe name"):
            subject.validate_contract(contract, _expected_tools=FIXTURE_TOOLS)

    def test_unsupported_platform_is_rejected_before_download(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        with self.assertRaisesRegex(subject.ContractError, "unsupported platform"):
            subject.install_contract(
                contract_for(archive, executable),
                pathlib.Path("unused"),
                platform_key="plan9-mips",
                downloader=mock.Mock(side_effect=AssertionError("must not download")),
                _expected_tools=FIXTURE_TOOLS,
            )

    def test_archive_digest_mismatch_leaves_no_executable(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        altered = archive + b"different"
        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory)
            with self.assertRaisesRegex(subject.ContractError, "archive SHA-256"):
                subject.install_contract(
                    contract_for(archive, executable),
                    destination,
                    platform_key="linux-x86_64",
                    downloader=lambda _url, _timeout: altered,
                    _expected_tools=FIXTURE_TOOLS,
                )
            self.assertEqual([], list(destination.iterdir()))

    def test_executable_digest_mismatch_leaves_no_executable(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", b"other")
        contract = contract_for(archive, executable)
        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory)
            with self.assertRaisesRegex(subject.ContractError, "executable SHA-256"):
                subject.install_contract(
                    contract,
                    destination,
                    platform_key="linux-x86_64",
                    downloader=lambda _url, _timeout: archive,
                    _expected_tools=FIXTURE_TOOLS,
                )
            self.assertEqual([], list(destination.iterdir()))

    def test_bounded_download_failure_leaves_no_executable(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        observed: list[float] = []

        def timeout(_url: str, seconds: float) -> bytes:
            observed.append(seconds)
            raise TimeoutError("bounded fixture timeout")

        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory)
            with self.assertRaisesRegex(subject.ContractError, "download failed"):
                subject.install_contract(
                    contract_for(archive, executable),
                    destination,
                    platform_key="linux-x86_64",
                    downloader=timeout,
                    timeout_seconds=7,
                    _expected_tools=FIXTURE_TOOLS,
                )
            self.assertEqual([7], observed)
            self.assertEqual([], list(destination.iterdir()))

    def test_occupied_destination_is_rejected_before_download(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory)
            existing = destination / "keep"
            existing.write_bytes(b"operator-owned")
            with self.assertRaisesRegex(subject.ContractError, "destination"):
                subject.install_contract(
                    contract_for(archive, executable),
                    destination,
                    platform_key="linux-x86_64",
                    downloader=mock.Mock(side_effect=AssertionError("must not download")),
                    _expected_tools=FIXTURE_TOOLS,
                )
            self.assertEqual(b"operator-owned", existing.read_bytes())

    def test_matching_destination_is_an_idempotent_install_without_download(self) -> None:
        executable = b"demo"
        archive = tar_gz("demo", executable)
        with tempfile.TemporaryDirectory() as directory:
            destination = pathlib.Path(directory)
            existing = destination / "demo"
            existing.write_bytes(executable)
            installed = subject.install_contract(
                contract_for(archive, executable),
                destination,
                platform_key="linux-x86_64",
                downloader=mock.Mock(side_effect=AssertionError("must not download")),
                _expected_tools=FIXTURE_TOOLS,
            )
            self.assertEqual([existing], installed)


if __name__ == "__main__":
    unittest.main()

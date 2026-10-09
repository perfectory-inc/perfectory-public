from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
DOCKERFILE = REPO_ROOT / "services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof"
BATCH_DOCKERFILE = REPO_ROOT / "services/foundation-provider-acquisition-worker/Dockerfile.raon-batch"
ENTRYPOINT = REPO_ROOT / "services/foundation-provider-acquisition-worker/scripts/raon-agent-container-proof.sh"
BATCH_ENTRYPOINT = REPO_ROOT / "services/foundation-provider-acquisition-worker/scripts/raon-batch-entrypoint.sh"
PYPROJECT = REPO_ROOT / "services/foundation-provider-acquisition-worker/pyproject.toml"
RUNBOOK = REPO_ROOT / "docs/runbooks/provider-acquisition-fargate.md"
NAMING_CONTRACT_COPY = (
    "COPY config/environment-variable-naming.contract.json "
    "/app/config/environment-variable-naming.contract.json"
)


def test_dockerignore_excludes_secret_and_heavy_paths_from_image_context() -> None:
    lines = {
        line.strip()
        for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }

    assert ".env" in lines
    assert ".env.*" in lines
    assert "!.env.example" in lines
    assert "!.env.local.example" in lines
    assert ".git" in lines
    assert "target" in lines
    assert "**/__pycache__" in lines


def test_raon_agent_container_proof_is_explicitly_pinned_and_runtime_local() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    entrypoint = ENTRYPOINT.read_text(encoding="utf-8")

    assert_installs_the_pinned_package(dockerfile)
    assert "PLAYWRIGHT_BROWSERS_PATH=/ms-playwright" in dockerfile
    assert "COPY services/foundation-provider-acquisition-worker" in dockerfile
    assert "COPY . ." not in dockerfile
    assert "COPY .env" not in dockerfile
    assert (
        "chmod +x /app/services/foundation-provider-acquisition-worker/scripts/raon-agent-container-proof.sh"
        in dockerfile
    )
    assert "USER app" in dockerfile
    assert "chown -R app:app /work /app /ms-playwright" in dockerfile

    assert "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR" in entrypoint
    assert "foundation_provider_acquisition.raon" in entrypoint
    assert "--prove-raon-replay" in entrypoint
    assert "/opt/raonk-2018/raonk-2018 --no-sandbox" in entrypoint
    assert "scripts/service.sh" not in entrypoint
    assert "PROVIDER_ACQUISITION_USE_VWORLD_LOGIN" in entrypoint
    assert "--use-vworld-login" in entrypoint
    assert "foundation-outbox-publisher" not in entrypoint
    assert "R2_" not in entrypoint
    assert "DATABASE_URL" not in entrypoint


def assert_installs_the_pinned_package(dockerfile: str) -> None:
    # Root ADR-0170: the package URL and sha256 are build arguments without defaults; the one place
    # they are pinned is config/provider-agent-packages.contract.json, which the build script reads.
    # The downloaded bytes are checked before they are installed.
    assert "\nARG RAON_DEB_URL\n" in dockerfile
    assert "\nARG RAON_DEB_SHA256\n" in dockerfile
    assert 'RUN test -n "${RAON_DEB_URL}"' in dockerfile
    assert 'test -n "${RAON_DEB_SHA256}"' in dockerfile
    fetch = dockerfile.index('curl -fsSL "${RAON_DEB_URL}" -o /tmp/raonk-2018_amd64.deb')
    check = dockerfile.index('echo "${RAON_DEB_SHA256}  /tmp/raonk-2018_amd64.deb" | sha256sum -c -')
    install = dockerfile.index("apt-get install -y --no-install-recommends /tmp/raonk-2018_amd64.deb")
    assert fetch < check < install, "the bytes are checked before they are installed"
    assert "raonk.com" not in dockerfile and "vworld.kr" not in dockerfile, "no URL is spelled here"


def test_raon_batch_container_runs_the_releases_importer_and_the_batch_entrypoint() -> None:
    dockerfile = BATCH_DOCKERFILE.read_text(encoding="utf-8")
    entrypoint = BATCH_ENTRYPOINT.read_text(encoding="utf-8")

    # No Rust is built into the image: the run mounts the admitted release's publisher (ADR-0170).
    instructions = "\n".join(
        line for line in dockerfile.splitlines() if not line.lstrip().startswith("#")
    )
    assert "FROM rust:" not in instructions
    assert "cargo build" not in instructions
    assert "foundation-outbox-publisher" not in instructions
    assert_installs_the_pinned_package(dockerfile)
    assert "Dockerfile.raon-agent-proof" not in dockerfile
    assert "COPY .env" not in dockerfile
    assert "USER app" in dockerfile

    assert "[[ ! -x /usr/local/bin/foundation-outbox-publisher ]]" in entrypoint
    assert entrypoint.index("foundation-outbox-publisher is not mounted") < entrypoint.index("Xvfb "), (
        "a missing importer stops the run before the browser starts"
    )
    assert "foundation_provider_acquisition.raon_batch" in entrypoint
    assert "PROVIDER_ACQUISITION_SELECTION_JSON" in entrypoint
    assert "PROVIDER_ACQUISITION_SELECTION_JSON_INLINE" in entrypoint
    assert "PROVIDER_ACQUISITION_SELECTION_JSON_BASE64" in entrypoint
    assert "BATCH_ID" in entrypoint
    assert "--rust-binary" in entrypoint
    assert "/usr/local/bin/foundation-outbox-publisher" in entrypoint
    assert "/opt/raonk-2018/raonk-2018 --no-sandbox" in entrypoint
    assert "powershell" not in entrypoint.lower()
    assert ".ps1" not in entrypoint.lower()


def test_raon_images_carry_the_naming_contract_consumed_by_python_and_rust() -> None:
    proof_dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    batch_dockerfile = BATCH_DOCKERFILE.read_text(encoding="utf-8")

    assert NAMING_CONTRACT_COPY in proof_dockerfile
    assert NAMING_CONTRACT_COPY in batch_dockerfile


def test_provider_acquisition_worker_installs_scrapling_browser_fetcher_extra() -> None:
    pyproject = PYPROJECT.read_text(encoding="utf-8")

    assert '"scrapling[fetchers]"' in pyproject
    assert '"scrapling",' not in pyproject


def test_runbook_documents_container_proof_before_fargate_selection() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "Dockerfile.raon-agent-proof" in runbook
    assert "Dockerfile.raon-batch" in runbook
    assert "RAON_DEB_SHA256" in runbook
    assert "RAON_DEB_URL" in runbook
    assert "raonk.com" not in runbook, "the package URL is pinned in one contract (root ADR-0170)"
    # Translated by the Korean-first migration (558c5beb); anchored on the sentences the runbook now
    # carries so a reword does not fail this and a removal does.
    assert "관리형 후보로 깔끔하지만 이 런북에서 선택하지 않는다" in runbook
    # Root ADR-0170 chose the data host for the VWorld RAON large files, with a supervised first run.
    assert "ADR-0170이 데이터 호스트를 골랐다" in runbook
    assert "첫 감독 실행" in runbook

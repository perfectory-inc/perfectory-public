from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
DOCKERFILE = REPO_ROOT / "services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof"
BATCH_DOCKERFILE = REPO_ROOT / "services/foundation-provider-acquisition-worker/Dockerfile.raon-batch"
ENTRYPOINT = REPO_ROOT / "services/foundation-provider-acquisition-worker/scripts/raon-agent-container-proof.sh"
BATCH_ENTRYPOINT = REPO_ROOT / "services/foundation-provider-acquisition-worker/scripts/raon-batch-entrypoint.sh"
PYPROJECT = REPO_ROOT / "services/foundation-provider-acquisition-worker/pyproject.toml"
RUNBOOK = REPO_ROOT / "docs/runbooks/provider-acquisition-fargate.md"


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

    assert "ARG RAON_DEB_URL" in dockerfile
    assert "ARG RAON_DEB_SHA256" in dockerfile
    assert "PLAYWRIGHT_BROWSERS_PATH=/ms-playwright" in dockerfile
    assert "sha256sum -c" in dockerfile
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


def test_raon_batch_container_includes_rust_importer_and_batch_entrypoint() -> None:
    dockerfile = BATCH_DOCKERFILE.read_text(encoding="utf-8")
    entrypoint = BATCH_ENTRYPOINT.read_text(encoding="utf-8")

    assert (
        "FROM rust:1.96.0-bookworm@sha256:"
        "5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc"
        " AS rust-builder"
    ) in dockerfile
    assert "cargo build --locked --release -p foundation-outbox-publisher" in dockerfile
    assert (
        "COPY --from=rust-builder /src/target/release/foundation-outbox-publisher "
        "/usr/local/bin/foundation-outbox-publisher"
    ) in dockerfile
    assert "ARG RAON_DEB_URL" in dockerfile
    assert "ARG RAON_DEB_SHA256" in dockerfile
    assert "sha256sum -c" in dockerfile
    assert "Dockerfile.raon-agent-proof" not in dockerfile
    assert "COPY .env" not in dockerfile
    assert "USER app" in dockerfile

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


def test_provider_acquisition_worker_installs_scrapling_browser_fetcher_extra() -> None:
    pyproject = PYPROJECT.read_text(encoding="utf-8")

    assert '"scrapling[fetchers]"' in pyproject
    assert '"scrapling",' not in pyproject


def test_runbook_documents_container_proof_before_fargate_selection() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "Dockerfile.raon-agent-proof" in runbook
    assert "Dockerfile.raon-batch" in runbook
    assert "RAON_DEB_SHA256" in runbook
    assert "Fargate remains the clean managed candidate" in runbook
    assert "it is not selected by this runbook" in runbook
    assert "ai-server is a" in runbook
    assert "not the production collector" in runbook

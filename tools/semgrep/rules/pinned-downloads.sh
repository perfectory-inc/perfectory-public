#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/pinned-downloads.yml (`opengrep scan --test`). Not run.
# ruleid: release-assets-come-through-the-pinned-fetcher
curl -fsSL -o tool.tar.gz https://github.com/acme/tool/releases/download/v1/tool.tar.gz
# ruleid: release-assets-come-through-the-pinned-fetcher
wget "https://github.com/acme/tool/releases/download/v${version}/tool_linux_amd64"
# ok: release-assets-come-through-the-pinned-fetcher
python3 scripts/ci/fetch_pinned.py tool opengrep --output "$runtime_root/opengrep"
# ok: release-assets-come-through-the-pinned-fetcher
curl -fsSL https://api.github.com/repos/acme/tool/releases/latest

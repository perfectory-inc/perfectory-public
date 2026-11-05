# Fixture for tools/semgrep/rules/pinned-downloads.yml (`opengrep scan --test`). Not executed.
# ruleid: release-assets-come-through-the-pinned-fetcher
URL = "https://github.com/acme/tool/releases/download/v{version}/tool.zip"
# ok: release-assets-come-through-the-pinned-fetcher
REGISTRY = "tools/pinned-artifacts.json"

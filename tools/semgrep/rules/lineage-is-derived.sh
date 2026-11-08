#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/lineage-is-derived.yml (`opengrep scan --test`). Not run.
# The accepted forms — a command that opens an input and names nothing, and a publication command
# that names a revision row and opens nothing — are the real tree, which the scan keeps at zero.
# ruleid: a-script-does-not-name-the-input-it-hands-over
FOUNDATION_X_INPUT_OBJECT_KEY="$key" \
  FOUNDATION_X_SOURCE_RECORD_ID="$key" "$PUBLISHER" convert

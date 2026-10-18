#!/usr/bin/env bash
# Proves scripts/ci/release-push.sh can fail (root ADR-0276 §5, §9): every image goes up by its own
# digest before the release tag; an image the registry holds under another digest fails and no
# release tag follows it; the same release again is a pass; a release tag holding another manifest
# is never replaced. A fake skopeo stands in for GitHub's container registry.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
script="$root/scripts/ci/release-push.sh"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT

fail() {
  echo "FAIL release-push-self-test: $*" >&2
  exit 1
}

mkdir -p "$work/bin" "$work/registry/blobs" "$work/release/blobs/sha256"
# The registry is a directory: <tag> holds the raw manifest it serves, blobs/<hex> every blob.
cat >"$work/bin/skopeo" <<'SKOPEO'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_REGISTRY/calls"
tagfile() { local ref="${1#docker://}"; printf '%s/%s' "$FAKE_REGISTRY" "${ref##*:}"; }
case "$1" in
  login) cat >/dev/null ;;
  copy)
    shift
    while [[ "$1" == --* ]]; do [[ "$1" == --retry-times ]] && shift; shift; done
    source="$1" destination="$2"
    if [[ "$source" == oci:* ]]; then
      layout="${source#oci:}"; name="${layout##*:}"; layout="${layout%:*}"
      digest="$(python3 -c 'import json, sys; print(next(m["digest"] for m in json.load(open(sys.argv[1]))["manifests"] if m["annotations"]["org.opencontainers.image.ref.name"] == sys.argv[2]))' "$layout/index.json" "$name" | tr -d '\r')"
      cp "$layout"/blobs/sha256/* "$FAKE_REGISTRY/blobs/"
      if [ -n "${FAKE_RECOMPRESS:-}" ]; then printf 'changed' >"$(tagfile "$destination")"; exit 0; fi
      cp "$layout/blobs/sha256/${digest#sha256:}" "$(tagfile "$destination")"
    else
      target="${destination#dir:}"
      mkdir -p "$target"
      cp "$(tagfile "$source")" "$target/manifest.json"
      for layer in $(python3 -c 'import json, sys; print(" ".join(l["digest"].split(":")[1] for l in json.load(open(sys.argv[1]))["layers"]))' "$target/manifest.json" | tr -d '\r'); do
        cp "$FAKE_REGISTRY/blobs/$layer" "$target/$layer"
      done
    fi
    ;;
  inspect) [ -e "$(tagfile "$3")" ] && cat "$(tagfile "$3")" ;;
  *) exit 2 ;;
esac
SKOPEO
chmod +x "$work/bin/skopeo"

commit=1111111111111111111111111111111111111111
python3 - "$work/release" "$commit" <<'PY'
import hashlib, json, pathlib, sys
release, commit = pathlib.Path(sys.argv[1]), sys.argv[2]
images, manifests = {}, []
for name in ("publisher", "files"):
    body = json.dumps({"schemaVersion": 2, "name": name, "layers": []}).encode()
    digest = hashlib.sha256(body).hexdigest()
    (release / "blobs/sha256" / digest).write_bytes(body)
    images[name] = {"oci_manifest": f"sha256:{digest}", "config": "sha256:" + "0" * 64}
    manifests.append({"digest": f"sha256:{digest}", "annotations": {"org.opencontainers.image.ref.name": name}})
(release / "index.json").write_text(json.dumps({"manifests": manifests}))
(release / "manifest.json").write_text(json.dumps({"source_commit": commit, "images": images}) + "\n")
(release / "manifest.sigstore.json").write_text("{}\n")
PY

push() {
  PATH="$work/bin:$PATH" FAKE_REGISTRY="$work/registry" GITHUB_TOKEN=fixture GITHUB_ACTOR=fixture \
    bash "$script" "$work/release" ghcr.io/example-org/foundation-release
}

: >"$work/registry/calls"
output="$(push)" || fail "the first push failed"
[ -e "$work/registry/$commit-publisher" ] && [ -e "$work/registry/$commit-files" ] || fail "an image was not pushed under <commit>-<name>"
[[ "$output" == *"release-push: ghcr.io/example-org/foundation-release:release-$commit"* ]] || fail "the release tag was not pushed: $output"
[[ "$(grep '^copy ' "$work/registry/calls" | tail -n 1)" == *":release-$commit" ]] || fail "the release tag was not the last copy"

output="$(push)" || fail "pushing the same release again failed"
[[ "$output" == *"already published with this manifest"* ]] || fail "the same release again was not a pass: $output"

cp "$work/release/manifest.json" "$work/manifest.kept"
python3 -c 'import json, sys; m = json.load(open(sys.argv[1])); m["other"] = 1; open(sys.argv[1], "w").write(json.dumps(m) + "\n")' "$work/release/manifest.json"
if push >/dev/null 2>&1; then fail "replaced a published release manifest"; fi
cp "$work/manifest.kept" "$work/release/manifest.json"

rm -rf "$work/registry" && mkdir -p "$work/registry/blobs"
: >"$work/registry/calls"
if FAKE_RECOMPRESS=1 push >/dev/null 2>&1; then fail "accepted an image the registry holds under another digest"; fi
[ ! -e "$work/registry/release-$commit" ] || fail "a release tag was published after an image failed"

echo "OK release-push-self-test"

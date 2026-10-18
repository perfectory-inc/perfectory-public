#!/usr/bin/env bash
# Push one built and signed release to GitHub's container registry (root ADR-0276 §5, owner decision
# 2026-10-19), with the job's own GITHUB_TOKEN: no stored secret.
#
#   release-push.sh <release-dir> <owner>
#
# The package is tools/release-build.contract.json registry.package under <owner>, the one name the
# server's release admission pulls from too.
#
# <release-dir> is what scripts/deploy/foundation-release-build.py wrote (an OCI layout over
# blobs/sha256 naming each image) plus the workflow's manifest.sigstore.json. Each image goes up
# byte for byte (skopeo --preserve-digests), so the registry holds exactly the digests the
# provenance attestation signed, as <commit>-<image>. Last, the manifest and its bundle go up as
# one small image tagged release-<commit>: a release is visible only once everything it names is
# there. A release-<commit> already there with another manifest is never replaced; the run fails.
set -euo pipefail

release="${1:?usage: release-push.sh <release-dir> <owner>}"
owner="$(printf '%s' "${2:?usage: release-push.sh <release-dir> <owner>}" | tr '[:upper:]' '[:lower:]')"
contract="$(cd "$(dirname "$0")/../.." && pwd)/tools/release-build.contract.json"
package="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["registry"]["package"])' "$contract" | tr -d '\r')"
repository="ghcr.io/$owner/$package"
: "${GITHUB_TOKEN:?the job token}" "${GITHUB_ACTOR:?}"
[[ "$repository" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+$ ]] || { echo "FAIL release-push: $repository is not ghcr.io/<owner>/<package>" >&2; exit 1; }

json() { python3 -c "import json, sys; m = json.load(open(sys.argv[1], encoding='utf-8')); $1" "$release/manifest.json" | tr -d '\r'; }
commit="$(json 'print(m["source_commit"])')"
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || { echo "FAIL release-push: the manifest names no commit" >&2; exit 1; }
[ -s "$release/manifest.sigstore.json" ] || { echo "FAIL release-push: $release has no manifest.sigstore.json" >&2; exit 1; }

printf '%s' "$GITHUB_TOKEN" | skopeo login --username "$GITHUB_ACTOR" --password-stdin ghcr.io >/dev/null

while read -r name digest; do
  skopeo copy --quiet --retry-times 3 --preserve-digests "oci:$release:$name" "docker://$repository:$commit-$name"
  stored="sha256:$(skopeo inspect --raw "docker://$repository:$commit-$name" | sha256sum | cut -d' ' -f1)"
  [ "$stored" = "$digest" ] || { echo "FAIL release-push: $name is $stored in the registry, not $digest" >&2; exit 1; }
  echo "release-push: $repository@$digest ($name)"
done < <(json 'print("\n".join(n + " " + i["oci_manifest"] for n, i in m["images"].items()))')

tag="$repository:release-$commit"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
if skopeo inspect --raw "docker://$tag" >/dev/null 2>&1; then
  mkdir "$work/existing"
  skopeo copy --quiet "docker://$tag" "dir:$work/existing"
  # The manifest is one small layer of two files; it must be this manifest, byte for byte.
  layer="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["layers"][0]["digest"].split(":")[1])' "$work/existing/manifest.json")"
  tar -xzf "$work/existing/$layer" -C "$work/existing" manifest.json
  cmp -s "$work/existing/manifest.json" "$release/manifest.json" || {
    echo "FAIL release-push: $tag is already published with another manifest; a release is never replaced (root ADR-0276 §9)" >&2
    exit 1
  }
  echo "release-push: $tag already published with this manifest"
  exit 0
fi
python3 "$(dirname "$0")/release-manifest-image.py" "$release" "$work/layout"
skopeo copy --quiet --retry-times 3 --preserve-digests "oci:$work/layout:release" "docker://$tag"
echo "release-push: $tag"

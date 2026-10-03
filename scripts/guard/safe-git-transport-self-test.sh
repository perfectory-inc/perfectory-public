#!/usr/bin/env bash
# Proves global and environment-injected URL rewrites cannot redirect transport,
# while a local rewrite in a repository context fails closed.
set -euo pipefail
# Fixtures below are disposable repositories. A hook runs with GIT_DIR pointing
# at the real checkout, which would redirect their commits into it; sourcing the
# shared definition releases that binding for the rest of this script.
. "$(dirname "$0")/lib/fixture-repo.sh"
cd "$(dirname "$0")/../.."

transport="scripts/github/safe-git-transport.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || rm -rf -- "$test_root" ;;
    *) echo "safe-git-transport-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

make_remote() {
  local name="$1"
  local contents="$2"
  local work="$test_root/${name}-work"
  mkdir -p "$work"
  git -C "$work" init --quiet
  git -C "$work" config user.name Synthetic
  git -C "$work" config user.email synthetic@example.invalid
  printf '%s\n' "$contents" >"$work/value.txt"
  git -C "$work" add value.txt
  git -C "$work" commit --quiet -m "$name"
  git clone --quiet --bare "$work" "$test_root/$name.git"
}

make_remote intended intended
make_remote wrong wrong
intended_url="file://$(cd "$test_root/intended.git" && pwd -P)/"
wrong_url="file://$(cd "$test_root/wrong.git" && pwd -P)/"
expected="$(git --git-dir="$test_root/intended.git" rev-parse refs/heads/master)"

global_config="$test_root/global.gitconfig"
GIT_CONFIG_GLOBAL="$global_config" GIT_CONFIG_NOSYSTEM=1 \
  git config --global "url.${wrong_url}.insteadOf" "$intended_url"
observed="$(GIT_CONFIG_GLOBAL="$global_config" "$transport" --no-repository \
  ls-remote "$intended_url" refs/heads/master | awk '{print $1}')"
if [ "$observed" != "$expected" ]; then
  echo "FAIL safe-git-transport-self-test: global URL rewrite redirected transport" >&2
  exit 1
fi

observed="$(
  GIT_CONFIG_COUNT=1 \
  GIT_CONFIG_KEY_0="url.${wrong_url}.insteadOf" \
  GIT_CONFIG_VALUE_0="$intended_url" \
  "$transport" --no-repository ls-remote "$intended_url" refs/heads/master \
    | awk '{print $1}'
)"
if [ "$observed" != "$expected" ]; then
  echo "FAIL safe-git-transport-self-test: injected URL rewrite redirected transport" >&2
  exit 1
fi

context="$test_root/context"
mkdir -p "$context"
git -C "$context" init --quiet
git -C "$context" config "url.${wrong_url}.insteadOf" "$intended_url"
if "$transport" --repository "$context" \
  ls-remote "$intended_url" refs/heads/master >/dev/null 2>&1; then
  echo "FAIL safe-git-transport-self-test: accepted a local URL rewrite" >&2
  exit 1
fi

git -C "$context" config --local --unset-all "url.${wrong_url}.insteadOf"
git -C "$context" config extensions.worktreeConfig true
git -C "$context" config --worktree "url.${wrong_url}.insteadOf" "$intended_url"
if "$transport" --repository "$context" \
  ls-remote "$intended_url" refs/heads/master >/dev/null 2>&1; then
  echo "FAIL safe-git-transport-self-test: accepted a worktree URL rewrite" >&2
  exit 1
fi

observed="$(
  cd "$context"
  "$OLDPWD/$transport" --no-repository \
    ls-remote "$intended_url" refs/heads/master | awk '{print $1}'
)"
if [ "$observed" != "$expected" ]; then
  echo "FAIL safe-git-transport-self-test: caller repository redirected no-repository mode" >&2
  exit 1
fi
git -C "$context" config --worktree --unset-all "url.${wrong_url}.insteadOf"

for dangerous_key in \
  core.fsmonitor core.attributesFile core.worktree core.excludesFile \
  filter.synthetic.process diff.synthetic.textconv diff.external \
  merge.synthetic.driver http.proxy credential.synthetic.helper \
  remote.synthetic.proxy protocol.ext.allow; do
  git config --file "$context/.git/config" "$dangerous_key" synthetic-command
  if "$transport" --repository "$context" status >/dev/null 2>&1; then
    echo "FAIL safe-git-transport-self-test: accepted dangerous config $dangerous_key" >&2
    exit 1
  fi
  git config --file "$context/.git/config" --unset-all "$dangerous_key"
done

fake_bin="$test_root/fake-bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/git" <<'SH'
#!/usr/bin/env bash
for name in \
  HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY \
  http_proxy https_proxy all_proxy no_proxy \
  BASH_ENV ENV CDPATH SSL_CERT_FILE SSL_CERT_DIR CURL_CA_BUNDLE \
  SSH_ASKPASS SSH_AUTH_SOCK; do
  if [ -n "${!name:-}" ]; then
    echo "unsafe environment survived: $name" >&2
    exit 91
  fi
done
printf 'sanitized\n'
SH
chmod +x "$fake_bin/git"
sanitized="$(
  PATH="$fake_bin:$PATH" \
  HTTP_PROXY=http://proxy.invalid HTTPS_PROXY=http://proxy.invalid \
  ALL_PROXY=socks://proxy.invalid NO_PROXY=example.invalid \
  SSL_CERT_FILE="$test_root/evil-ca" SSH_AUTH_SOCK="$test_root/evil-agent" \
  "$transport" --no-repository version 2>/dev/null
)"
if [ "$sanitized" != sanitized ]; then
  echo "FAIL safe-git-transport-self-test: proxy/execution environment was not stripped" >&2
  exit 1
fi

trusted_index="$test_root/trusted.index"
"$transport" --trusted-index-file "$trusted_index" --repository "$context" \
  read-tree --empty
if [ ! -s "$trusted_index" ]; then
  echo "FAIL safe-git-transport-self-test: trusted temporary index was not honored" >&2
  exit 1
fi
if "$transport" --trusted-index-file "$trusted_index" --no-repository \
  version >/dev/null 2>&1; then
  echo "FAIL safe-git-transport-self-test: accepted trusted index without a repository" >&2
  exit 1
fi
if "$transport" --trusted-index-file $'bad\nindex' --repository "$context" \
  status >/dev/null 2>&1; then
  echo "FAIL safe-git-transport-self-test: accepted multiline index path" >&2
  exit 1
fi

if GIT_AUTHOR_NAME=$'Synthetic\nInjected' \
  GIT_AUTHOR_EMAIL=synthetic@example.invalid \
  GIT_AUTHOR_DATE='@1 +0000' \
  GIT_COMMITTER_NAME=Synthetic \
  GIT_COMMITTER_EMAIL=synthetic@example.invalid \
  GIT_COMMITTER_DATE='@1 +0000' \
  "$transport" --trusted-commit-identity --repository "$context" \
    rev-parse --git-dir >/dev/null 2>&1; then
  echo "FAIL safe-git-transport-self-test: accepted multiline commit identity" >&2
  exit 1
fi

# Anonymous mode (release admission, ADR-0136) must not consult any credential
# helper. A fake `gh` records every call; the default mode proves it is reachable.
helper_bin="$test_root/helper-bin"
mkdir -p "$helper_bin"
cat >"$helper_bin/gh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_GH_CALLS"
SH
chmod +x "$helper_bin/gh"
credential_request=$'protocol=https\nhost=github.com\npath=perfectory-inc/perfectory-public.git\n\n'
gh_calls="$test_root/gh.calls"
printf '%s' "$credential_request" | PATH="$helper_bin:$PATH" FAKE_GH_CALLS="$gh_calls" \
  "$transport" --no-repository credential fill >/dev/null 2>&1 || true
if ! grep -Fq 'auth git-credential get' "$gh_calls" 2>/dev/null; then
  echo "FAIL safe-git-transport-self-test: default mode no longer reaches the gh credential helper" >&2
  exit 1
fi
rm -f -- "$gh_calls"
printf '%s' "$credential_request" | PATH="$helper_bin:$PATH" FAKE_GH_CALLS="$gh_calls" \
  "$transport" --anonymous --no-repository credential fill >/dev/null 2>&1 || true
if [ -e "$gh_calls" ]; then
  echo "FAIL safe-git-transport-self-test: anonymous mode invoked a credential helper" >&2
  exit 1
fi
anonymous_helpers="$(PATH="$helper_bin:$PATH" \
  "$transport" --anonymous --no-repository config --get-all credential.helper || true)"
if [ -n "$anonymous_helpers" ]; then
  echo "FAIL safe-git-transport-self-test: anonymous mode configured a credential helper" >&2
  exit 1
fi

# Git reads $HOME/.netrc even with no helper; anonymous mode must not hand it the caller's HOME.
home_bin="$test_root/home-bin"
mkdir -p "$home_bin"
cat >"$home_bin/git" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$HOME"
SH
chmod +x "$home_bin/git"
anonymous_home="$(PATH="$home_bin:$PATH" HOME="$test_root" \
  "$transport" --anonymous --no-repository version 2>/dev/null || true)"
if [ "$anonymous_home" != /dev/null ]; then
  echo "FAIL safe-git-transport-self-test: anonymous mode kept a HOME that can hold a .netrc" >&2
  exit 1
fi

echo "OK safe-git-transport-self-test"

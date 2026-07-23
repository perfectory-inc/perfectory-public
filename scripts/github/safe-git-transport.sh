#!/usr/bin/env bash
# Run a Git transport with no system/global/injected Git configuration. This
# prevents url.* rewrites (and object/config environment injection) from sending
# an audited tree to a target other than the literal URL/path in the command.
set -euo pipefail

repository=""
repository_mode=""
trusted_commit_identity=0
trusted_index_file=""
while :; do
  case "${1:-}" in
    --trusted-commit-identity)
      trusted_commit_identity=1
      shift
      ;;
    --trusted-index-file)
      trusted_index_file="${2:?usage: $0 [--trusted-index-file <path>] --repository <repo> <git-command...>}"
      shift 2
      ;;
    *) break ;;
  esac
done
case "${1:-}" in
  --repository)
    repository_mode=repository
    repository="${2:?usage: $0 --repository <repo> <git-transport-command...>}"
    shift 2
    ;;
  --no-repository)
    repository_mode=no-repository
    shift
    ;;
  *)
    echo "FAIL safe-git-transport: choose --repository or --no-repository" >&2
    exit 2
    ;;
esac
if [ "$#" -eq 0 ]; then
  echo "FAIL safe-git-transport: missing Git command" >&2
  exit 2
fi
for command_name in basename dirname env git grep mktemp realpath rm; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL safe-git-transport: missing command '$command_name'" >&2
    exit 1
  }
done

for argument in "$@"; do
  case "$argument" in
    -c|--config-env|--config-env=*|--git-dir|--git-dir=*|-C|--exec-path|--exec-path=*)
      echo "FAIL safe-git-transport: caller-controlled Git configuration/context is forbidden" >&2
      exit 2
      ;;
  esac
done

clean_environment=(env)
while IFS='=' read -r entry; do
  name="${entry%%=*}"
  case "$name" in
    GIT_*|HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY|http_proxy|https_proxy|all_proxy|no_proxy|\
    BASH_ENV|ENV|CDPATH|LD_*|DYLD_*|SSL_CERT_FILE|SSL_CERT_DIR|CURL_CA_BUNDLE|\
    SSH_ASKPASS|SSH_AUTH_SOCK)
      clean_environment+=(-u "$name")
      ;;
  esac
done < <(env)
clean_environment+=(
  GIT_CONFIG_NOSYSTEM=1
  GIT_CONFIG_GLOBAL=/dev/null
  GIT_NO_REPLACE_OBJECTS=1
  GIT_TERMINAL_PROMPT=0
)
if [ -n "$trusted_index_file" ]; then
  if [ "$repository_mode" != repository ]; then
    echo "FAIL safe-git-transport: a trusted index requires repository mode" >&2
    exit 2
  fi
  case "$trusted_index_file" in
    *$'\r'*|*$'\n'*)
      echo "FAIL safe-git-transport: trusted index path must be one line" >&2
      exit 2
      ;;
  esac
  trusted_index_parent="$(realpath "$(dirname "$trusted_index_file")")"
  trusted_index_file="$trusted_index_parent/$(basename "$trusted_index_file")"
  clean_environment+=("GIT_INDEX_FILE=$trusted_index_file")
fi
safe_cwd=""
if [ "$repository_mode" = no-repository ]; then
  safe_cwd="$(mktemp -d)"
  clean_environment+=("GIT_CEILING_DIRECTORIES=$safe_cwd")
  cleanup() {
    case "${safe_cwd:-}" in
      /tmp/*|/var/tmp/*|[A-Za-z]:/*)
        [ ! -e "$safe_cwd" ] || rm -rf -- "$safe_cwd"
        ;;
      *) echo "safe-git-transport: refusing unsafe temporary cleanup" >&2 ;;
    esac
  }
  trap cleanup EXIT
fi
if [ "$trusted_commit_identity" -eq 1 ]; then
  for identity_name in \
    GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE \
    GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE; do
    identity_value="${!identity_name:-}"
    case "$identity_value" in
      ""|*$'\r'*|*$'\n'*)
        echo "FAIL safe-git-transport: trusted commit identity is incomplete or multiline" >&2
        exit 2
        ;;
    esac
    clean_environment+=("$identity_name=${!identity_name}")
  done
  if ! printf '%s\n' "$GIT_AUTHOR_DATE" "$GIT_COMMITTER_DATE" \
    | grep -Eq '^@[0-9]+ \+0000$' \
    || [ "$GIT_AUTHOR_DATE" != "$GIT_COMMITTER_DATE" ]; then
    echo "FAIL safe-git-transport: trusted commit dates must be one exact UTC epoch" >&2
    exit 2
  fi
fi
safe_git=(
  "${clean_environment[@]}"
  git
  --no-pager
  -c credential.helper=
  -c 'credential.helper=!gh auth git-credential'
  -c core.hooksPath=/dev/null
  -c core.fsmonitor=false
  -c core.untrackedCache=false
  -c core.excludesFile=/dev/null
  -c init.templateDir=
  -c http.sslVerify=true
  -c diff.external=
  -c interactive.diffFilter=
  -c commit.gpgSign=false
)

if [ -n "$repository" ]; then
  repository="$(realpath "$repository")"
  set +e
  repository_local_config="$(
    "${safe_git[@]}" -C "$repository" config --local --show-origin --name-only --list \
      2>/dev/null
  )"
  local_config_status=$?
  worktree_config_enabled="$(
    "${safe_git[@]}" -C "$repository" config --local --type=bool \
      --get extensions.worktreeConfig 2>/dev/null
  )"
  worktree_enabled_status=$?
  repository_worktree_config=""
  worktree_config_status=0
  if [ "$worktree_config_enabled" = true ]; then
    repository_worktree_config="$(
      "${safe_git[@]}" -C "$repository" config --worktree --show-origin --name-only --list \
        2>/dev/null
    )"
    worktree_config_status=$?
  fi
  set -e
  if [ "$local_config_status" -ne 0 ] \
    || [ "$worktree_enabled_status" -gt 1 ] \
    || { [ -n "$worktree_config_enabled" ] \
      && [ "$worktree_config_enabled" != true ] \
      && [ "$worktree_config_enabled" != false ]; } \
    || [ "$worktree_config_status" -ne 0 ]; then
    echo "FAIL safe-git-transport: repository context is unreadable" >&2
    exit 1
  fi
  all_local_config="$(printf '%s\n%s\n' \
    "$repository_local_config" "$repository_worktree_config")"
  dangerous_local_config="$(printf '%s\n' "$all_local_config" | grep -Ei \
    '([[:space:]]|^)(url\..*\.(insteadof|pushinsteadof)|include(if)?\..*|core\.(fsmonitor|hookspath|attributesfile|worktree|excludesfile|sshcommand|askpass|gitproxy|alternaterefscommand|sparsecheckout|sparsecheckoutcone)|filter\..*|diff\.(external|.*\.(command|textconv))|merge\..*\.driver|credential\..*|http\..*|remote\..*\.(proxy|proxyauthmethod|uploadpack|receivepack|vcs)|protocol\..*|pager\..*|interactive\.difffilter)$' \
    || true)"
  if [ -n "$dangerous_local_config" ]; then
    echo "FAIL safe-git-transport: executable or transport-changing repository config is forbidden" >&2
    printf '%s\n' "$dangerous_local_config" >&2
    exit 1
  fi
  exec "${safe_git[@]}" -C "$repository" "$@"
fi

"${safe_git[@]}" -C "$safe_cwd" "$@"

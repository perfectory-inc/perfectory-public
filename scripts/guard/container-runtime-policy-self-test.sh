#!/usr/bin/env bash
# Synthetic negative fixtures prove that mutable container images and
# externally published development ports cannot bypass the repository policy.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/check-container-runtime-policy.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL container-runtime-policy-self-test: missing $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

make_repo() {
  local root="$1"
  mkdir -p "$root"
  git init -q --initial-branch=main "$root"
}

expect_rejected() {
  local label="$1"
  local root="$2"
  git -C "$root" add -A
  if bash "$checker" "$root" >/dev/null 2>&1; then
    echo "FAIL container-runtime-policy-self-test: accepted $label" >&2
    exit 1
  fi
}

valid="$test_root/valid"
make_repo "$valid"
mkdir -p "$valid/tools"
cat >"$valid/compose.yml" <<'YAML'
services:
  database:
    image: example/database:1.2.3@sha256:1111111111111111111111111111111111111111111111111111111111111111
    ports:
      - "127.0.0.1:${SYNTHETIC_DB_PORT:-15432}:5432"
  application:
    image: synthetic-application:local
    ports: ["${SYNTHETIC_APP_BIND_ADDR:-127.0.0.1}:18080:8080"]
YAML
cat >"$valid/Dockerfile" <<'DOCKERFILE'
FROM example/builder:1.2.3@sha256:2222222222222222222222222222222222222222222222222222222222222222 AS builder
RUN true
FROM builder AS output
RUN true
DOCKERFILE
cat >"$valid/tools/container-images.env" <<'ENV'
SYNTHETIC_IMAGE=example/tool:1.2.3@sha256:3333333333333333333333333333333333333333333333333333333333333333
ENV
cat >"$valid/run.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
source tools/container-images.env
docker pull example/tool:1.2.3@sha256:3333333333333333333333333333333333333333333333333333333333333333
docker run --rm example/tool:1.2.3@sha256:3333333333333333333333333333333333333333333333333333333333333333 true
docker run --rm --pids-limit 512 "$SYNTHETIC_IMAGE" true
SH
git -C "$valid" add -A
bash "$checker" "$valid" >/dev/null

mutable_compose="$test_root/mutable-compose"
make_repo "$mutable_compose"
cat >"$mutable_compose/compose.yml" <<'YAML'
services:
  database:
    image: example/database:latest
YAML
expect_rejected mutable-compose-image "$mutable_compose"

mutable_dockerfile="$test_root/mutable-dockerfile"
make_repo "$mutable_dockerfile"
printf '%s\n' 'FROM example/runtime:latest' >"$mutable_dockerfile/Dockerfile"
expect_rejected mutable-dockerfile-image "$mutable_dockerfile"

mutable_run="$test_root/mutable-run"
make_repo "$mutable_run"
printf '%s\n' '#!/usr/bin/env bash' 'docker run --rm example/tool:latest true' >"$mutable_run/run.sh"
expect_rejected mutable-docker-run-image "$mutable_run"

mutable_pull="$test_root/mutable-pull"
make_repo "$mutable_pull"
printf '%s\n' '#!/usr/bin/env bash' 'docker pull example/tool:latest' >"$mutable_pull/pull.sh"
expect_rejected mutable-docker-pull-image "$mutable_pull"

wildcard_port="$test_root/wildcard-port"
make_repo "$wildcard_port"
cat >"$wildcard_port/docker-compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    ports:
      - "18080:8080"
YAML
expect_rejected wildcard-compose-port "$wildcard_port"

unsafe_default="$test_root/unsafe-default"
make_repo "$unsafe_default"
cat >"$unsafe_default/example.compose.yaml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    ports: ["${SYNTHETIC_BIND_ADDR:-0.0.0.0}:18080:8080"]
YAML
expect_rejected non-loopback-compose-default "$unsafe_default"

literal_secret="$test_root/literal-secret"
make_repo "$literal_secret"
cat >"$literal_secret/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    environment:
      APPLICATION_PASSWORD: known-development-password
YAML
expect_rejected literal-compose-secret "$literal_secret"

quoted_keys="$test_root/quoted-keys"
make_repo "$quoted_keys"
cat >"$quoted_keys/compose.yml" <<'YAML'
"services":
  application:
    "image": example/application:latest
YAML
expect_rejected quoted-compose-keys "$quoted_keys"

spaced_key="$test_root/spaced-key"
make_repo "$spaced_key"
cat >"$spaced_key/compose.yml" <<'YAML'
services:
  application:
    image : example/application:latest
YAML
expect_rejected noncanonical-compose-key "$spaced_key"

explicit_key="$test_root/explicit-key"
make_repo "$explicit_key"
cat >"$explicit_key/compose.yml" <<'YAML'
services:
  application:
    ? image
    : example/application:latest
YAML
expect_rejected explicit-compose-key "$explicit_key"

service_anchor="$test_root/service-anchor"
make_repo "$service_anchor"
cat >"$service_anchor/compose.yml" <<'YAML'
x-service: &service-defaults
  image: synthetic-application:local
services:
  application:
    <<: *service-defaults
YAML
expect_rejected unsupported-compose-service-anchor "$service_anchor"

flow_mapping="$test_root/flow-mapping"
make_repo "$flow_mapping"
cat >"$flow_mapping/compose.yml" <<'YAML'
services: {application: {image: example/application:latest}}
YAML
expect_rejected compose-flow-mapping "$flow_mapping"

host_network="$test_root/host-network"
make_repo "$host_network"
cat >"$host_network/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    network_mode: host
YAML
expect_rejected compose-host-network "$host_network"

remote_build="$test_root/remote-build"
make_repo "$remote_build"
cat >"$remote_build/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    build:
      context: https://example.invalid/source.git
YAML
expect_rejected remote-compose-build-context "$remote_build"

compose_include="$test_root/compose-include"
make_repo "$compose_include"
cat >"$compose_include/compose.yml" <<'YAML'
include:
  - path: hidden-services.yml
services:
  application:
    image: synthetic-application:local
YAML
printf '%s\n' 'services:' '  hidden:' '    image: example/hidden:latest' >"$compose_include/hidden-services.yml"
expect_rejected compose-include-bypass "$compose_include"

dynamic_namespace="$test_root/dynamic-namespace"
make_repo "$dynamic_namespace"
cat >"$dynamic_namespace/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    network_mode: ${SYNTHETIC_NETWORK_MODE:-bridge}
YAML
expect_rejected dynamic-compose-network-mode "$dynamic_namespace"

copy_from="$test_root/copy-from"
make_repo "$copy_from"
cat >"$copy_from/Dockerfile" <<'DOCKERFILE'
FROM example/runtime:1.2.3@sha256:5555555555555555555555555555555555555555555555555555555555555555
COPY --from=example/tool:latest /tool /tool
DOCKERFILE
expect_rejected mutable-dockerfile-copy-from "$copy_from"

syntax_frontend="$test_root/syntax-frontend"
make_repo "$syntax_frontend"
cat >"$syntax_frontend/Dockerfile" <<'DOCKERFILE'
# syntax=example/dockerfile:latest
FROM example/runtime:1.2.3@sha256:5555555555555555555555555555555555555555555555555555555555555555
DOCKERFILE
expect_rejected mutable-dockerfile-syntax-frontend "$syntax_frontend"

copy_from_later_flag="$test_root/copy-from-later-flag"
make_repo "$copy_from_later_flag"
cat >"$copy_from_later_flag/Dockerfile" <<'DOCKERFILE'
FROM example/runtime:1.2.3@sha256:5555555555555555555555555555555555555555555555555555555555555555
COPY --link --from=example/tool:latest /tool /tool
DOCKERFILE
expect_rejected mutable-copy-from-after-another-flag "$copy_from_later_flag"

run_mount="$test_root/run-mount"
make_repo "$run_mount"
cat >"$run_mount/Dockerfile" <<'DOCKERFILE'
FROM example/runtime:1.2.3@sha256:5555555555555555555555555555555555555555555555555555555555555555
RUN --mount=type=cache,target=/cache --mount=type=bind,from=example/tool:latest,target=/tool true
DOCKERFILE
expect_rejected mutable-second-run-mount-from "$run_mount"

arbitrary_compose_name="$test_root/arbitrary-compose-name"
make_repo "$arbitrary_compose_name"
cat >"$arbitrary_compose_name/stack.yml" <<'YAML'
services:
  application:
    image: example/application:latest
YAML
expect_rejected mutable-image-in-arbitrarily-named-compose "$arbitrary_compose_name"

remote_docker_build="$test_root/remote-docker-build"
make_repo "$remote_docker_build"
printf '%s\n' '#!/usr/bin/env bash' 'docker build https://example.invalid/source.git' >"$remote_docker_build/build.sh"
expect_rejected remote-docker-build-context "$remote_docker_build"

privileged_service="$test_root/privileged-service"
make_repo "$privileged_service"
cat >"$privileged_service/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    privileged: true
YAML
expect_rejected privileged-compose-service "$privileged_service"

docker_socket="$test_root/docker-socket"
make_repo "$docker_socket"
cat >"$docker_socket/compose.yml" <<'YAML'
services:
  application:
    image: synthetic-application:local
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
YAML
expect_rejected compose-docker-socket-mount "$docker_socket"

image_pull_alias="$test_root/image-pull-alias"
make_repo "$image_pull_alias"
printf '%s\n' '#!/usr/bin/env bash' 'docker image pull example/tool:latest' >"$image_pull_alias/pull.sh"
expect_rejected mutable-docker-image-pull "$image_pull_alias"

container_run_alias="$test_root/container-run-alias"
make_repo "$container_run_alias"
printf '%s\n' '#!/usr/bin/env bash' 'docker container run --rm example/tool:latest true' >"$container_run_alias/run.sh"
expect_rejected mutable-docker-container-run "$container_run_alias"

late_override="$test_root/late-override"
make_repo "$late_override"
mkdir -p "$late_override/tools"
cat >"$late_override/tools/container-images.env" <<'ENV'
SYNTHETIC_IMAGE=example/tool:1.2.3@sha256:4444444444444444444444444444444444444444444444444444444444444444
ENV
cat >"$late_override/run.sh" <<'SH'
#!/usr/bin/env bash
source tools/container-images.env
docker run --rm "$SYNTHETIC_IMAGE" true
export SYNTHETIC_IMAGE=example/tool:latest
SH
expect_rejected later-image-assignment-override "$late_override"

echo "OK container-runtime-policy-self-test"

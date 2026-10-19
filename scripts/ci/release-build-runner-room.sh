#!/usr/bin/env bash
# Room on a GitHub-hosted runner for the release build (root ADR-0276 §9).
# Prevents: the publisher's build, which peaked at 16.7 GiB on the server (ADR-0137), being killed
# on a 16 GB runner, and the images and their OCI archives filling its disk.
set -euo pipefail

before="$(df -B1 --output=avail / | tail -1)"
# Preinstalled toolchains the build never uses (the build runs in BuildKit containers).
sudo rm -rf /usr/local/lib/android /usr/share/dotnet /opt/ghc /usr/local/.ghcup /usr/share/swift \
  /usr/local/share/powershell /usr/local/share/chromium /opt/hostedtoolcache/CodeQL
after="$(df -B1 --output=avail / | tail -1)"

# Swap on the larger temporary disk, beside the runner's own.
sudo fallocate -l 16G /mnt/release-build.swap
sudo chmod 0600 /mnt/release-build.swap
sudo mkswap /mnt/release-build.swap >/dev/null
sudo swapon /mnt/release-build.swap

printf 'runner-room: freed %s bytes on /, %s bytes free\n' "$((after - before))" "$after"
free -b | sed 's/^/runner-room: /'
nproc | sed 's/^/runner-room: cpus /'

#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/root-python-runs-isolated.yml (`opengrep scan --test`). Not run.

# ruleid: root-deploy-python-runs-isolated
python3 - "${jobs_file}" <<'PY'
PY
# ruleid: root-deploy-python-runs-isolated
repository="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["full_name"])' "${identity}")"
# ruleid: root-deploy-python-runs-isolated
python3 "${preflight}/scripts/deploy/migration-approval-preflight.py" "${preflight}"
# ruleid: root-deploy-python-runs-isolated
printf '%s' "${listing}" | EMAIL="${email}" python3 -B -c 'import sys'

# ok: root-deploy-python-runs-isolated
python3 -I - "${jobs_file}" <<'PY'
PY
# ok: root-deploy-python-runs-isolated
repository="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["full_name"])' "${identity}")"
# ok: root-deploy-python-runs-isolated
python3 -I -B "${release_root}/current/scripts/deploy/job_units.py" running ollama.service
# ok: root-deploy-python-runs-isolated
"${release_root}/venv/bin/python3" -I -B script.py
# ok: root-deploy-python-runs-isolated
# A comment that mentions python3 - is not a command.

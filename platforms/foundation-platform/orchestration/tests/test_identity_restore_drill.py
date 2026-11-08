"""The weekly identity restore drill (scripts/recovery/identity-restore-drill.sh) against a fake docker.

The drill failed on 2026-10-25 and 11-01 although the restored login worked (users/me 200): it waited
for a signing key, and Zitadel holds none after a quiet 30 hours (live /oauth/v2/keys answered
{"keys":[]} on 2026-11-03). The fake plays the live databases, the bucket, the restored copies and the
restored Zitadel; each case is one answer a real restore can give.
"""

import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/recovery/identity-restore-drill.sh"
PG_IMAGE = "postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
ZITADEL_IMAGE = "ghcr.io/zitadel/zitadel:v2.65.1@sha256:013d23b69aa681f03d36a7fd61e4837a7b049a7e22bd7215eb3e98e9dbf5543c"

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
sys.stdout.reconfigure(newline="\n")
args = sys.argv[1:]
state = json.load(open(os.environ["FAKE_STATE"]))
def http(status, body):
    reason = {200: "OK", 401: "Unauthorized", 503: "Service Unavailable"}[status]
    print(f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\r\n{body}", end="")
if args[0] == "ps":
    if "-aq" not in args:
        label = dict(a.split("=", 1)[1].split("=", 1) for a in args if a.startswith("label=com.docker.compose."))
        print("live-{}-{}".format(label["com.docker.compose.project"], label["com.docker.compose.service"]))
elif args[0] == "inspect":
    what, name = args[2], args[3]
    if what == "{{.Config.Image}}":
        print(state["zitadel_image"] if name.endswith("-zitadel") else state["pg_image"])
    elif what.startswith("{{range .Config.Env}}"):
        if name.endswith("-zitadel"):
            print("ZITADEL_MASTERKEY=synthetic-masterkey-for-the-test-0")
            print("ZITADEL_EXTERNALDOMAIN=127.0.0.1")
            print("ZITADEL_EXTERNALPORT=18453")
            print("ZITADEL_PORT=18453")
        else:
            print("POSTGRES_USER=fake")
            print("POSTGRES_DB=fake")
    else:  # {{.State.Running}} of the restored Zitadel
        print("true" if state["zitadel_runs"] else "false")
elif args[0] == "exec":
    if "-i" in args:  # pg_restore reads the dump, over TCP only (the init server is socket-only)
        sys.stdin.read()
        if "pg_restore -h 127.0.0.1 " not in args[-1]:
            sys.exit('pg_restore: error: ... FATAL:  database "zitadel" does not exist')
    elif "pg_isready" in " ".join(args):
        sys.exit(0 if state["ready"] else 2)
    else:
        live = args[1].startswith("live-")
        if "information_schema" in args[-1]:
            print(state["tables"] if live else state["restored_tables"])
        else:
            print(state["rows"])
elif args[0] == "logs":
    print('level=INFO msg="registered route" endpoint=/oauth/v2/revoke')
elif args[0] == "run":
    if any(a.startswith("amazon/aws-cli") for a in args):
        if "list-objects-v2" in args:
            name = args[args.index("--prefix") + 1].split("/")[1]
            print(f"postgres/{name}/20261101014852.dump.gpg")
        else:
            print("dump bytes")
    elif any(a.startswith("alpine/socat") for a in args):
        request = sys.stdin.read()
        http(*(state["keys"] if request.startswith("GET /oauth/v2/keys") else state["me"]))
'''

FAKE_GPG = "#!/usr/bin/env bash\ncat\n"


class IdentityRestoreDrill(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="identity-restore-drill-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "gpg").write_text(FAKE_GPG)
        for name in ("docker", "gpg"):
            (bin_dir / name).chmod(0o755)
        if os.name == "nt":  # a laptop run: Windows Python ends its lines with CR LF, the host's does not
            real = pathlib.Path(shutil.which("python")).as_posix()
            (bin_dir / "python3").write_text(f'#!/usr/bin/env bash\nset -o pipefail\n"{real}" "$@" | tr -d "\\r"\n')
        (self.root / "token").write_text("synthetic-token")
        self.evidence = self.root / "evidence"
        self.state = {"pg_image": PG_IMAGE, "zitadel_image": ZITADEL_IMAGE, "zitadel_runs": True, "ready": True,
                      "tables": 129, "restored_tables": 129, "rows": 1284,
                      "keys": [200, '{"keys":[]}'], "me": [200, '{"user":{"id":"synthetic"}}']}
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FAKE_STATE": str(self.root / "state.json"),
                    "FOUNDATION_RECOVERY_EVIDENCE_DIR": str(self.evidence),
                    "FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID": "synthetic-reader",
                    "FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY": "synthetic-reader-secret",
                    "FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS": "synthetic-pass",
                    "FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET": "synthetic-bucket",
                    "FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_ENDPOINT_HOST": "r2.invalid",
                    "FOUNDATION_RESTORE_DRILL_READY_TRIES": "2",
                    "ZITADEL_DRILL_TOKEN_FILE": str(self.root / "token")}

    def drill(self):
        (self.root / "state.json").write_text(json.dumps(self.state))
        return subprocess.run([shutil.which("bash") or "bash", str(SCRIPT)], env=self.env,
                              capture_output=True, text=True, timeout=300)

    def test_an_empty_key_set_and_a_working_login_pass(self):
        # 2026-10-25 and 11-01: the restored login worked, production itself held no signing key.
        run = self.drill()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("identity-restore-drill-ok zitadel_signing_keys=0", run.stdout)
        evidence = json.loads((self.evidence / "identity-restore-drill.json").read_text())
        self.assertEqual((evidence["result"], evidence["signing_keys"]), ("pass", 0))

    def test_a_key_set_with_keys_passes_and_counts_them(self):
        self.state["keys"] = [200, '{"keys":[{"use":"sig","kid":"1"},{"use":"sig","kid":"2"}]}']
        run = self.drill()
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("zitadel_signing_keys=2", run.stdout)

    def test_a_refused_live_token_fails_it(self):
        # A wrong masterkey serves the public keys and refuses the token (proven 2026-10-12).
        self.state["me"] = [401, '{"message":"Errors.Token.Invalid"}']
        run = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("refused the live token (HTTP/1.1 401", run.stderr)

    def test_a_zitadel_that_never_serves_its_key_set_fails_it(self):
        self.state["keys"] = [503, "upstream not ready"]
        self.state["zitadel_runs"] = False  # it exited: the drill stops waiting at once
        run = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("did not serve its key set (HTTP/1.1 503", run.stderr)

    def test_a_restore_with_fewer_tables_than_live_fails_before_zitadel(self):
        self.state["restored_tables"] = 128
        run = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("restored 128 tables, live has 129", run.stderr)

    def test_a_copy_that_never_gets_ready_fails_before_the_restore(self):
        # 2026-11-08: the wait ran out, fell through, and pg_restore met the image's init-only server.
        self.state["ready"] = False
        run = self.drill()
        self.assertEqual(run.returncode, 1)
        self.assertIn("zitadel: the restored database was not ready within 2 tries", run.stderr)


if __name__ == "__main__":
    unittest.main()

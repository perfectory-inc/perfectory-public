"""A loopback Alertmanager that records what scripts/ops/provider_alert.py posts to /api/v2/alerts."""

import http.server
import json
import threading


class AlertmanagerStub:
    """Start in setUp, read `.alerts` (every posted alert, in order), stop with `.close`."""

    def __init__(self, status=200):
        self.alerts = []
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 (the stdlib's name)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                stub.alerts.extend(json.loads(body))
                self.send_response(status)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = str(self.server.server_address[1])
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def named(self, alertname):
        return [alert for alert in self.alerts if alert["labels"]["alertname"] == alertname]

    def close(self):
        self.server.shutdown()
        self.server.server_close()

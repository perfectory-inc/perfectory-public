"""Project Compose's existing API connection onto its published loopback port.

Input and output contain credentials: callers must capture both, never log them.
Compose, rather than this adapter, resolves environment variables and defaults.
"""

import json
import sys
from urllib.parse import parse_qsl, urlsplit, urlunsplit


def host_database_url(model):
    services = model["services"]
    source = urlsplit(services["foundation-api"]["environment"]["DATABASE_URL"])
    if (source.scheme not in {"postgres", "postgresql"} or source.hostname != "postgres"
            or not source.username or not source.password or not source.path.strip("/")
            or not source.port or source.fragment):
        raise ValueError("invalid API connection")
    # libpq query options can override the authority we are about to project.
    if any(key.lower() in {"host", "hostaddr", "port", "service", "servicefile"}
           for key, _ in parse_qsl(source.query)):
        raise ValueError("connection overrides transport")
    ports = [port for port in services["postgres"]["ports"]
             if port["target"] == source.port and port.get("protocol", "tcp") == "tcp"]
    if len(ports) != 1 or ports[0].get("host_ip") not in {"127.0.0.1", "::1"}:
        raise ValueError("one explicit loopback port is required")
    port = int(ports[0]["published"])
    if not 1 <= port <= 65535:
        raise ValueError("invalid host port")
    host = ports[0]["host_ip"]
    authority = source.netloc.rsplit("@", 1)[0] + "@"
    authority += (f"[{host}]" if ":" in host else host) + f":{port}"
    return urlunsplit((source.scheme, authority, source.path, source.query, ""))


if __name__ == "__main__":
    try:
        connection = host_database_url(json.load(sys.stdin))
    except (KeyError, TypeError, ValueError):
        print("runtime Compose model has no unambiguous host database connection", file=sys.stderr)
        sys.exit(78)
    print(connection)

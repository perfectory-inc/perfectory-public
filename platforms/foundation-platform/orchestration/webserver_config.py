"""Staff sign in to Airflow through Zitadel only (root ADR-0122 §6).

The FAB auth manager hands the browser to Zitadel by OIDC. There is no self sign-up: a person
gets in only if `airflow-runtime.sh provision` created their account first, keyed by email, the
same rule as the data catalog. Password sign-in does not exist here.
"""

import os

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.security.manager import AUTH_OAUTH

# The issuer the browser and this server both use (root ADR-0081); the loopback sidecar makes it
# reachable from inside the container.
ISSUER = "http://127.0.0.1:18453"

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = False
OAUTH_PROVIDERS = [
    {
        "name": "zitadel",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["OIDC_CLIENT_ID"],
            "client_secret": os.environ["OIDC_CLIENT_SECRET"],
            "server_metadata_url": f"{ISSUER}/.well-known/openid-configuration",
            "client_kwargs": {"scope": "openid email profile"},
        },
    }
]


class ZitadelSecurityManager(FabAirflowSecurityManagerOverride):
    """Names the Airflow user by the verified email in Zitadel's ID token."""

    def get_oauth_user_info(self, provider, response):
        if provider != "zitadel":
            return {}
        claims = response.get("userinfo") or {}
        email = claims.get("email")
        if not email or not claims.get("email_verified", False):
            return {}
        return {
            "username": email,
            "email": email,
            "first_name": claims.get("given_name", ""),
            "last_name": claims.get("family_name", ""),
        }


SECURITY_MANAGER_CLASS = ZitadelSecurityManager

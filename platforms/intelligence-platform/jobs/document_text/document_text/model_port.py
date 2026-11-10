"""The local model, reached only through its OpenAI-compatible chat endpoint (root ADR-0478).

The engine (Ollama today, Strata later) is whatever MODEL_RUNTIME_BASE_URL points at; the
names are those of config/local-ollama.env.example and the Rust client
(crates/normalization/intelligence-normalization-infrastructure/src/openai_compatible.rs), so
swapping engines changes no code here. Standard library only.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
import urllib.request
from dataclasses import dataclass

# A PDF text layer can carry NUL and other control characters; a C-string runtime ends the
# prompt at the first NUL (2026-11-11, root ADR-0478: the model saw 255 of ~9,000 tokens).
CONTROL = re.compile(r"[\x00-\x08\x0b\x0e-\x1f]")


@dataclass(frozen=True)
class Port:
    base_url: str
    chat_path: str
    model: str
    api_key: str | None
    reasoning_effort: str | None
    timeout: int

    @classmethod
    def from_env(cls, timeout: int, env: dict | None = None) -> "Port | None":
        env = os.environ if env is None else env
        base, model = env.get("MODEL_RUNTIME_BASE_URL"), env.get("MODEL_RUNTIME_DEFAULT_MODEL")
        if not base or not model:
            return None  # no model configured: the run keeps rule facts only and says so
        return cls(
            base.rstrip("/"),
            env.get("MODEL_RUNTIME_CHAT_PATH", "/v1/chat/completions"),
            model,
            env.get("MODEL_RUNTIME_API_KEY") or None,
            env.get("MODEL_RUNTIME_REASONING_EFFORT") or None,
            timeout,
        )

    def chat(self, messages: list[dict], *, model: str | None = None, schema: dict | None = None, max_tokens: int = 4096) -> tuple[str, float]:
        body = {
            "model": model or self.model,
            "messages": messages,
            "temperature": 0,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if schema is not None:
            body["response_format"] = {"type": "json_schema", "json_schema": {"name": "facts", "strict": True, "schema": schema}}
        if self.reasoning_effort:
            body["reasoning_effort"] = self.reasoning_effort
        headers = {"content-type": "application/json"}
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.base_url + self.chat_path, data=json.dumps(body).encode(), headers=headers)
        started = time.monotonic()
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            answer = json.loads(response.read())
        return answer["choices"][0]["message"].get("content") or "", time.monotonic() - started


def clean(text: str) -> str:
    return CONTROL.sub(" ", text)


def image_part(png: bytes) -> dict:
    return {"type": "image_url", "image_url": {"url": "data:image/png;base64," + base64.b64encode(png).decode()}}


def parse_json(content: str) -> dict | None:
    match = re.search(r"\{.*\}", content or "", re.S)
    if not match:
        return None
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None

"""OllamaLLMClient — offline LLM backend via Ollama REST API.

Ollama runs models locally (no internet, no API key). Best models for
code/schema generation tasks:
  - deepseek-coder:6.7b   (best for code, fast on CPU/GPU)
  - codellama:7b           (good code generation, widely available)
  - llama3:8b              (solid general-purpose)
  - mistral:7b             (fast, good reasoning)

Pull a model first:
  ollama pull deepseek-coder:6.7b

Start the server (usually auto-starts):
  ollama serve
"""

from __future__ import annotations

import json
import os
from typing import Any

import requests


_DEFAULT_HOST = "http://localhost:11434"
_DEFAULT_MODEL = "deepseek-coder:6.7b"


class OllamaLLMClient:
    """LLMClient-compatible wrapper around the Ollama REST API.

    Implements the LLMClient Protocol so it can be injected into any
    AgentOrchestrator or workflow that accepts an LLMClient.

    Args:
        model:   Ollama model tag, e.g. "deepseek-coder:6.7b". Falls back
                 to the OLLAMA_MODEL env var, then ``deepseek-coder:6.7b``.
        host:    Ollama server URL. Falls back to OLLAMA_HOST env var,
                 then ``http://localhost:11434``.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        model: str | None = None,
        host: str | None = None,
        timeout: int = 300,
        num_ctx: int = 2048,
    ) -> None:
        self._model = model or os.getenv("OLLAMA_MODEL", _DEFAULT_MODEL)
        self._host = (host or os.getenv("OLLAMA_HOST", _DEFAULT_HOST)).rstrip("/")
        self._timeout = timeout
        self._num_ctx = num_ctx   # context window — smaller = faster on CPU
        self._generate_url = f"{self._host}/api/generate"

    # ------------------------------------------------------------------
    # LLMClient Protocol
    # ------------------------------------------------------------------

    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str:
        """Send a prompt to Ollama and return the response text.

        Combines system_prompt and user prompt into a single request since
        the Ollama /api/generate endpoint accepts a ``system`` field directly.
        """
        payload: dict[str, Any] = {
            "model": self._model,
            "system": system_prompt,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_predict": max_tokens,
                "num_ctx": self._num_ctx,   # smaller context = faster CPU inference
                "temperature": 0.1,         # low temp for deterministic schema/code output
            },
        }

        try:
            response = requests.post(
                self._generate_url,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"Cannot connect to Ollama at {self._host}. "
                "Make sure Ollama is running: `ollama serve`"
            )
        except requests.exceptions.Timeout:
            raise RuntimeError(
                f"Ollama request timed out after {self._timeout}s. "
                "Try a smaller model or increase timeout."
            )
        except requests.exceptions.HTTPError as exc:
            raise RuntimeError(f"Ollama API error: {exc}") from exc

        data = response.json()
        return (data.get("response") or "").strip()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if the Ollama server is reachable."""
        try:
            requests.get(f"{self._host}/api/tags", timeout=3)
            return True
        except requests.exceptions.RequestException:
            return False

    def list_models(self) -> list[str]:
        """Return names of locally available Ollama models."""
        try:
            resp = requests.get(f"{self._host}/api/tags", timeout=5)
            resp.raise_for_status()
            return [m["name"] for m in resp.json().get("models", [])]
        except requests.exceptions.RequestException:
            return []

    @property
    def model(self) -> str:
        return self._model

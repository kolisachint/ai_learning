"""VertexLLMClient — Vertex AI / Gemini backend."""

from __future__ import annotations

import os
from typing import Optional

import vertexai
from vertexai.generative_models import GenerationConfig, GenerativeModel


class VertexLLMClient:
    """LLMClient-compatible wrapper around Vertex AI generative models.

    Supports both base Gemini models (e.g. ``gemini-1.5-pro``) and full
    tuned/custom model resource names. Configure via env vars or constructor.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> None:
        self._project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self._location = location or os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

        if not self._project_id:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT is not set. "
                "Set it in your environment or pass project_id explicitly."
            )

        vertexai.init(project=self._project_id, location=self._location)

        self._model_name = (
            model_name
            or os.getenv("VERTEX_MODEL_NAME")
            or "gemini-1.5-pro"
        )
        self._model = GenerativeModel(self._model_name)

    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str:
        full_prompt = f"{system_prompt}\n\n{prompt}"
        response = self._model.generate_content(
            full_prompt,
            generation_config=GenerationConfig(max_output_tokens=max_tokens),
        )
        try:
            text = response.text
        except ValueError:
            text = "Response blocked by safety filters."
        return (text or "").strip()

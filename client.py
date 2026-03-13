from __future__ import annotations

import os
import vertexai
from vertexai.generative_models import GenerativeModel

class VertexClient:
    """Wrapper for Vertex AI SDK initialization and model retrieval."""

    def __init__(self, project_id: str | None = None, location: str | None = None) -> None:
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = location or os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
        
        # Initialize the Vertex AI SDK
        vertexai.init(project=self.project_id, location=self.location)

    def get_model(self, model_name: str = "gemini-1.5-pro") -> GenerativeModel:
        """Returns a generative model instance."""
        return GenerativeModel(model_name)
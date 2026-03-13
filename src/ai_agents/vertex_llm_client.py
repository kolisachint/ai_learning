# Backward-compatibility shim.
# VertexLLMClient now lives in:
#   src/ai_agents/integrations/vertex/client.py

from .integrations.vertex.client import VertexLLMClient  # noqa: F401

__all__ = ["VertexLLMClient"]

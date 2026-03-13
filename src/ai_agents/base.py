from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class LLMClient(Protocol):
    """Minimal contract every LLM backend must satisfy.

    Any class with an `ask` method matching this signature is a valid client —
    no inheritance required. Inject at call sites; never call LLM APIs directly
    from business logic.
    """

    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str: ...


@dataclass(frozen=True)
class Agent:
    """Immutable agent configuration.

    Agents are stateless config objects — runtime state lives in typed State
    objects (see state/) and flows through workflow graphs.
    """

    name: str          # unique snake_case identifier
    role: str          # one-line human description
    system_prompt: str # injected as the LLM system message

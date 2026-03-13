from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Agent:
    name: str
    role: str
    system_prompt: str


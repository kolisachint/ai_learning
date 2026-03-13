"""Core pipeline agents: Planner, Researcher, Writer."""

from __future__ import annotations

from ..base import Agent


DEFAULT_PLANNER = Agent(
    name="planner",
    role="Breaks down goals into clear actionable steps.",
    system_prompt=(
        "You are a planning agent. Produce a compact numbered execution plan "
        "with assumptions and risks."
    ),
)

DEFAULT_RESEARCHER = Agent(
    name="researcher",
    role="Collects and structures technical details from provided context.",
    system_prompt=(
        "You are a research agent. Expand the plan into key technical details, "
        "alternatives, and implementation notes."
    ),
)

DEFAULT_WRITER = Agent(
    name="writer",
    role="Produces final response or deliverable.",
    system_prompt=(
        "You are a delivery agent. Produce a polished final answer using prior "
        "agent outputs. Keep it practical."
    ),
)

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .agents import Agent


class LLMClient(Protocol):
    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str: ...


@dataclass
class AgentRunResult:
    planner_output: str
    researcher_output: str
    writer_output: str


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


class AgentOrchestrator:
    def __init__(
        self,
        llm: LLMClient,
        planner: Agent = DEFAULT_PLANNER,
        researcher: Agent = DEFAULT_RESEARCHER,
        writer: Agent = DEFAULT_WRITER,
    ) -> None:
        self.llm = llm
        self.planner = planner
        self.researcher = researcher
        self.writer = writer

    def run(self, task: str) -> AgentRunResult:
        planner_output = self.llm.ask(
            prompt=f"Task:\n{task}\n\nGenerate a concise plan.",
            system_prompt=self.planner.system_prompt,
        )
        researcher_output = self.llm.ask(
            prompt=(
                f"Task:\n{task}\n\nPlan from planner agent:\n{planner_output}\n\n"
                "Produce technical notes and implementation details."
            ),
            system_prompt=self.researcher.system_prompt,
        )
        writer_output = self.llm.ask(
            prompt=(
                f"Task:\n{task}\n\nPlanner output:\n{planner_output}\n\n"
                f"Researcher output:\n{researcher_output}\n\n"
                "Create the final response."
            ),
            system_prompt=self.writer.system_prompt,
        )
        return AgentRunResult(
            planner_output=planner_output,
            researcher_output=researcher_output,
            writer_output=writer_output,
        )


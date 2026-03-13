from __future__ import annotations

from dataclasses import dataclass

from .base import Agent, LLMClient
from .agents.core import DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER


@dataclass
class AgentRunResult:
    planner_output: str
    researcher_output: str
    writer_output: str


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


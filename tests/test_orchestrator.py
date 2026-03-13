from ai_agents.orchestrator import AgentOrchestrator


class FakeLLM:
    def __init__(self) -> None:
        self.calls = []

    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str:
        self.calls.append((prompt, system_prompt, max_tokens))
        if "Generate a concise plan." in prompt:
            return "1. Step one\n2. Step two"
        if "technical notes" in prompt:
            return "Use a Python package layout and keep prompts explicit."
        return "Final answer."


def test_agent_orchestrator_flow() -> None:
    fake = FakeLLM()
    orchestrator = AgentOrchestrator(fake)
    result = orchestrator.run("Build a Claude-based agent app")

    assert result.planner_output.startswith("1.")
    assert "Python package layout" in result.researcher_output
    assert result.writer_output == "Final answer."
    assert len(fake.calls) == 3


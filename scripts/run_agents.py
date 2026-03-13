#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

from ai_agents.orchestrator import AgentOrchestrator
from ai_agents.vertex_llm_client import VertexLLMClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a simple Claude-powered multi-agent workflow."
    )
    parser.add_argument(
        "task",
        help="Task description for the agent system.",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Print planner/researcher outputs before the final answer.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    if not os.getenv("GOOGLE_CLOUD_PROJECT"):
        print(
            "Error: GOOGLE_CLOUD_PROJECT is not set. "
            "Copy .env.example to .env and configure your Vertex AI project.",
            file=sys.stderr,
        )
        return 1

    client = VertexLLMClient()
    orchestrator = AgentOrchestrator(client)
    result = orchestrator.run(args.task)

    if args.show_all:
        print("\n=== Planner ===")
        print(result.planner_output)
        print("\n=== Researcher ===")
        print(result.researcher_output)
        print("\n=== Writer (Final) ===")
    print(result.writer_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


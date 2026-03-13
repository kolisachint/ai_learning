# Backward-compatibility shim.
# The Agent dataclass and agent constants now live in:
#   base.py          → Agent, LLMClient
#   agents/core.py   → DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER
#   agents/bq_terraform.py → BQ_TERRAFORM_AGENT, SCHEMA_EXTRACTOR_AGENT

from .base import Agent
from .agents.core import DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER

__all__ = ["Agent", "DEFAULT_PLANNER", "DEFAULT_RESEARCHER", "DEFAULT_WRITER"]

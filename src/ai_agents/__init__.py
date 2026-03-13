"""ai_agents — Claude/Ollama-powered multi-agent framework.

Public surface area:
  from ai_agents.base import Agent, LLMClient
  from ai_agents.orchestrator import AgentOrchestrator, AgentRunResult
  from ai_agents.agents import DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER, BQ_TERRAFORM_AGENT
  from ai_agents.integrations.ollama import OllamaLLMClient
  from ai_agents.integrations.vertex import VertexLLMClient
  from ai_agents.workflows.bq_terraform_workflow import run_bq_terraform_workflow
"""

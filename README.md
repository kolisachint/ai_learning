# Agentic Design Automation Framework

This repository now includes a default folder layout for building an agentic AI
framework focused on design automation with LangGraph and Anthropic.

The current Python package remains `ai_agents`, and the recommended direction is:

- `LangGraph` for orchestration and stateful graph execution
- `Anthropic` for model access
- design-specific prompts, tools, and workflows for briefs, UI ideas, reviews,
  and artifact generation

## Default folder layout

```text
ai_learning/
  README.md
  configs/
    agents/
    workflows/
  data/
    raw/
    interim/
    processed/
    external/
  docs/
    architecture.md
  models/
  notebooks/
  scripts/
    run_agents.py
  src/
    ai_agents/
      graphs/
      integrations/
        anthropic/
      design_ops/
      prompts/
      state/
      tools/
      workflows/
      agents.py
      orchestrator.py
  tests/
```

## What goes where

- `src/ai_agents/graphs/`: LangGraph state graphs, node wiring, routing, retries,
  and graph entrypoints.
- `src/ai_agents/integrations/anthropic/`: Anthropic client wrappers, message
  adapters, and model configuration.
- `src/ai_agents/design_ops/`: design automation logic such as brief expansion,
  heuristic review, component generation, and design QA.
- `src/ai_agents/prompts/`: prompt templates for design strategist, researcher,
  critic, generator, and reviewer roles.
- `src/ai_agents/state/`: shared graph state models and typed payloads passed
  between nodes.
- `src/ai_agents/tools/`: local tools, API connectors, file handlers, and design
  system utilities.
- `src/ai_agents/workflows/`: higher-level workflow presets such as landing page
  generation, design critique, or design-system audit.
- `configs/agents/`: YAML or TOML config for agent roles and model behavior.
- `configs/workflows/`: workflow-level configuration such as graph choices,
  tool permissions, and output routing.
- `docs/architecture.md`: the recommended starting place for project structure
  and responsibilities.

## Suggested first build path

1. Add an Anthropic client wrapper in
   `src/ai_agents/integrations/anthropic/`.
2. Create a LangGraph workflow in `src/ai_agents/graphs/` for one design task,
   such as expanding a design brief into wireframe guidance.
3. Store role prompts in `src/ai_agents/prompts/`.
4. Define shared graph state in `src/ai_agents/state/`.
5. Add tests for graph transitions and prompt-tool contracts in `tests/`.

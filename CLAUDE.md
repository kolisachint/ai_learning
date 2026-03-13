# CLAUDE.md — Project Instructions for Claude Code

Loaded automatically every session. Defines conventions, constraints, and
workflow rules for agentic coding in this repository.

## Project Overview

`ai-learning-agents` is an agentic AI framework for design ops and data
engineering automation. Core features:
- **Planner → Researcher → Writer** general-purpose pipeline
- **BQ Terraform agent** — converts CSV/JSON/PDF schema files to Terraform HCL
  using Ollama (offline, local LLM — no API key required)

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Offline LLM | Ollama (`deepseek-coder:6.7b`, `codellama:7b`) |
| Cloud LLM | Vertex AI (Gemini 1.5 pro/flash) |
| Orchestration | LangGraph (in progress — `graphs/`) |
| PDF parsing | pdfplumber |
| Tests | pytest |
| Package manager | pip + setuptools (`pyproject.toml`) |
| Env config | python-dotenv (`.env` from `.env.example`) |

## Optimized Package Layout

```
src/ai_agents/
  base.py                    # LLMClient Protocol + Agent dataclass  ← SOURCE OF TRUTH
  agents.py                  # backward-compat shim (imports from agents/)
  orchestrator.py            # AgentOrchestrator + AgentRunResult
  vertex_llm_client.py       # backward-compat shim (imports from integrations/vertex/)

  agents/                    # ← one file per agent group
    __init__.py
    core.py                  # DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER
    bq_terraform.py          # BQ_TERRAFORM_AGENT, SCHEMA_EXTRACTOR_AGENT

  integrations/              # ← one subpackage per LLM backend
    ollama/
      client.py              # OllamaLLMClient (offline, default: deepseek-coder:6.7b)
    vertex/
      client.py              # VertexLLMClient (Gemini on GCP)
    anthropic/
      __init__.py            # stub — Anthropic SDK integration (TODO)

  tools/
    file_reader.py           # read_schema_file() → CSV/JSON/PDF → schema list or text

  prompts/
    bq_terraform.py          # SCHEMA_EXTRACTION_SYSTEM, BQ_TERRAFORM_SYSTEM + builders

  state/
    bq.py                    # SchemaField, BQTerraformState

  workflows/
    bq_terraform_workflow.py # run_bq_terraform_workflow() — end-to-end pipeline

  graphs/                    # LangGraph StateGraph definitions (TODO)
  design_ops/                # design automation domain logic (TODO)

scripts/
  run_agents.py              # CLI: general Planner→Researcher→Writer pipeline
  run_bq_terraform.py        # CLI: BQ schema file → Terraform HCL

tests/
  test_orchestrator.py
  test_bq_terraform_agent.py

data/
  raw/                       # input schema files (CSV, JSON, PDF)
  interim/                   # intermediate artifacts
  processed/terraform/       # generated .tf files
```

## Coding Conventions

- `from __future__ import annotations` at the top of every module.
- `@dataclass(frozen=True)` for immutable config objects (`Agent`).
- All LLM clients implement `LLMClient` from `base.py` — never call LLM APIs
  directly from business logic.
- New agent groups → new file in `agents/`, exported from `agents/__init__.py`.
- New LLM backends → new subpackage in `integrations/<provider>/client.py`.
- Prompts → `prompts/<workflow>.py`, never inline in orchestrator/workflow code.
- Typed workflow state → `state/<domain>.py`.
- Workflows → `workflows/<name>_workflow.py`, import from graphs/tools/state.

## Testing

```bash
pytest tests/
```

- Use `FakeLLM` pattern (see tests) — never hit real APIs in unit tests.
- One test file per source module, named `test_<module>.py`.
- Monkeypatching is acceptable for isolating file I/O in workflow tests.

## Running Locally

```bash
cp .env.example .env          # fill in GCP credentials (only needed for Vertex)
pip install -e ".[dev]"

# General agent pipeline (requires Vertex AI / GCP):
python scripts/run_agents.py "Your task here" --show-all

# BQ Terraform (requires Ollama for PDF; CSV/JSON work without LLM):
ollama pull deepseek-coder:6.7b     # one-time
ollama serve                          # keep running
python scripts/run_bq_terraform.py data/raw/schema.csv \
    --table orders --dataset sales --project my-gcp-project
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GOOGLE_CLOUD_PROJECT` | Vertex only | GCP project ID |
| `GOOGLE_CLOUD_LOCATION` | No | Default: `us-central1` |
| `VERTEX_MODEL_NAME` | No | Default: `gemini-1.5-pro` |
| `GOOGLE_APPLICATION_CREDENTIALS` | No | Path to service account JSON |
| `ANTHROPIC_API_KEY` | Anthropic only | Claude API key |
| `OLLAMA_MODEL` | No | Default: `deepseek-coder:6.7b` |
| `OLLAMA_HOST` | No | Default: `http://localhost:11434` |

Never commit `.env` or any file containing secrets.

## What NOT to Do

- Do not call LLM APIs outside a class implementing the `LLMClient` Protocol.
- Do not put prompt strings inline in orchestrator/workflow files.
- Do not add new top-level source files — place code in the correct sub-package.
- Do not create helpers for one-off operations; inline them instead.
- Do not add error handling for states that can't happen.
- Do not commit `.env`, service account JSON, or model weights.

## Agent Development Checklist

When adding a new agent:
1. Add it to `agents/<group>.py` as a frozen `Agent` constant.
2. Store its system prompt in `prompts/<workflow>.py`.
3. Add typed state in `state/<domain>.py` if needed.
4. Wire into a workflow in `workflows/<name>_workflow.py`.
5. Write tests with `FakeLLM` in `tests/test_<workflow>.py`.

When adding a new LLM backend:
1. Create `integrations/<provider>/client.py` implementing `LLMClient`.
2. Export from `integrations/<provider>/__init__.py`.
3. Never modify `orchestrator.py` — inject at call sites.

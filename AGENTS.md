# AGENTS.md — Agent Registry and Conventions

Authoritative reference for all agents, orchestration patterns, and the
conventions for adding new ones. Read by both humans and AI coding assistants.

---

## Core Model

Every agent is a frozen dataclass defined in `src/ai_agents/base.py`:

```python
@dataclass(frozen=True)
class Agent:
    name: str           # unique snake_case identifier
    role: str           # one-line human description
    system_prompt: str  # injected as the LLM system message
```

Agents are **stateless config objects**. Runtime state flows through typed
`State` objects in `src/ai_agents/state/`.

---

## Agent Registry

### Group: Core Pipeline (`agents/core.py`)

| Constant | Name | Role |
|---|---|---|
| `DEFAULT_PLANNER` | `planner` | Breaks down goals into numbered actionable steps |
| `DEFAULT_RESEARCHER` | `researcher` | Collects technical details from provided context |
| `DEFAULT_WRITER` | `writer` | Produces the final polished response or deliverable |

**Pipeline**: Task → Planner → Researcher → Writer → `AgentRunResult`

```
User Task
    │
    ▼
┌─────────┐   prompt + task      ┌────────────┐
│ Planner │ ──────────────────► │ LLM Client │
└─────────┘ ◄── plan ─────────── └────────────┘
    │
    ▼ plan
┌────────────┐  task + plan       ┌────────────┐
│ Researcher │ ─────────────────► │ LLM Client │
└────────────┘ ◄── research ───── └────────────┘
    │
    ▼ research
┌────────┐  task + plan + research  ┌────────────┐
│ Writer │ ──────────────────────►  │ LLM Client │
└────────┘ ◄── final output ─────── └────────────┘
```

Implemented in `src/ai_agents/orchestrator.py` → `AgentOrchestrator.run()`.

---

### Group: BQ Terraform (`agents/bq_terraform.py`)

| Constant | Name | Role |
|---|---|---|
| `BQ_TERRAFORM_AGENT` | `bq_terraform` | Converts BQ schema definitions to Terraform HCL |
| `SCHEMA_EXTRACTOR_AGENT` | `schema_extractor` | Extracts structured schema fields from unstructured text (PDF) |

**Recommended LLM backend**: `OllamaLLMClient` with `deepseek-coder:6.7b`
(offline, no API key, optimised for code generation).

**Workflow** (`workflows/bq_terraform_workflow.py`):

```
Input File (.csv / .json / .pdf)
    │
    ▼
FileReader.read_schema_file()
    │
    ├── CSV/JSON ─────────────────────────────────────────────────────►┐
    │   (structured → SchemaField list, no LLM needed)                 │
    │                                                                   │
    └── PDF ──► SCHEMA_EXTRACTOR_AGENT (Ollama) ──► SchemaField list ──►┤
                                                                        │
                                                                        ▼
                                              _generate_hcl_deterministic()   ← default (fast)
                                           or BQ_TERRAFORM_AGENT (Ollama)     ← --llm-hcl flag
                                                                        │
                                                                        ▼
                                               data/processed/terraform/<table>.tf
```

**Input formats**:
- `.json` — BQ schema JSON array `[{name, type, mode, description}, …]`
  or `{schema: […]}` / `{fields: […]}` wrapper
- `.csv` — headers: `name,type,mode,description` (name + type required)
- `.pdf` — any PDF containing schema documentation; Ollama extracts fields

**Output**: valid Terraform HCL with `google_bigquery_table` resource,
`jsonencode()` schema block, variable definitions, lifecycle rules.

**CLI**:
```bash
# Structured input — no LLM needed:
python scripts/run_bq_terraform.py data/raw/schema.csv \
    --table orders --dataset sales --project my-gcp-project

# PDF input — Ollama required:
python scripts/run_bq_terraform.py data/raw/schema.pdf \
    --table events --dataset analytics --project my-gcp-project

# Use Ollama for HCL generation too (richer output):
python scripts/run_bq_terraform.py data/raw/schema.json \
    --table users --dataset core --project my-gcp-project --llm-hcl

# Health check:
python scripts/run_bq_terraform.py any.csv --table t --dataset d \
    --project p --check-ollama
```

---

## LLM Client Contract

All backends implement the `LLMClient` Protocol from `src/ai_agents/base.py`:

```python
class LLMClient(Protocol):
    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str: ...
```

### Available Implementations

| Class | Location | Backend | Use case |
|---|---|---|---|
| `OllamaLLMClient` | `integrations/ollama/client.py` | Local Ollama | Offline, code/schema tasks |
| `VertexLLMClient` | `integrations/vertex/client.py` | Vertex AI / Gemini | Cloud, general tasks |

### Ollama Setup (one-time)

```bash
# Install: https://ollama.com
brew install ollama          # macOS

# Pull recommended model for code/schema tasks:
ollama pull deepseek-coder:6.7b    # best for BQ Terraform (fast + accurate)
# Alternatives:
ollama pull codellama:7b
ollama pull llama3:8b

# Start server (auto-starts on most systems):
ollama serve

# Verify:
python scripts/run_bq_terraform.py --check-ollama any.csv --table t --dataset d --project p
```

Model selection (priority order):
1. `--model` CLI flag
2. `OLLAMA_MODEL` env var
3. Default: `deepseek-coder:6.7b`

---

## Adding a New Agent

1. **Define** in `src/ai_agents/agents/<group>.py`:
   ```python
   MY_AGENT = Agent(
       name="my_agent",
       role="One-line description of what this agent does.",
       system_prompt=MY_AGENT_SYSTEM_PROMPT,  # from prompts/
   )
   ```

2. **Store the system prompt** in `src/ai_agents/prompts/<workflow>.py`:
   ```python
   MY_AGENT_SYSTEM_PROMPT = "You are a ... agent. ..."
   ```

3. **Export** from `src/ai_agents/agents/__init__.py`:
   ```python
   from .<group> import MY_AGENT
   __all__ = [..., "MY_AGENT"]
   ```

4. **Add state** in `src/ai_agents/state/<domain>.py` if the agent
   produces new artifacts that downstream agents need.

5. **Wire into workflow** in `src/ai_agents/workflows/<name>_workflow.py`.

6. **Test** in `tests/test_<workflow>.py` using `FakeLLM`.

---

## Adding a New LLM Backend

1. Create `src/ai_agents/integrations/<provider>/__init__.py` and
   `src/ai_agents/integrations/<provider>/client.py`.
2. Implement the `LLMClient` Protocol (just the `ask` method).
3. Export from the `__init__.py`.
4. Inject at call sites — never modify `orchestrator.py`.

---

## Naming Conventions

| Thing | Convention | Example |
|---|---|---|
| Agent constant | `SCREAMING_SNAKE_CASE` | `BQ_TERRAFORM_AGENT` |
| Agent name field | `snake_case` | `"bq_terraform"` |
| Graph node function | `<name>_node` | `schema_extractor_node` |
| State class | `PascalCase` + `State` | `BQTerraformState` |
| Workflow entrypoint | `run_<workflow>_workflow` | `run_bq_terraform_workflow` |
| System prompt constant | `<NAME>_SYSTEM` | `BQ_TERRAFORM_SYSTEM` |
| Prompt builder function | `<name>_prompt` | `bq_terraform_prompt` |

---

## Data Flow Conventions

```
data/raw/          ← user-supplied input files (schema CSV/JSON/PDF, briefs)
data/interim/      ← extracted intermediate artifacts (parsed schema dicts)
data/processed/    ← final outputs
  terraform/       ← generated .tf files
  wireframes/      ← (future) design outputs
  handoff/         ← (future) design handoff packages
```

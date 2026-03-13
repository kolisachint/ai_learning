# ai-learning-agents

An agentic AI framework for data engineering automation and design ops. Ships with an offline **BigQuery Terraform Agent** that converts table schema files (CSV, JSON, PDF) into production-ready Terraform HCL — no cloud API key required.

<<<<<<< HEAD
    
# ai_learning

This repository is currently a **starter skeleton** and does not yet contain application code.

## Current structure

- `.git/` — Git metadata and history.
- `.gitkeep` — Placeholder file to keep the repository non-empty.

## What a newcomer should know

1. There is no implemented source tree yet (no `src/`, `app/`, tests, or build config files).
2. The repository is ready to be initialized into any stack (Python, JS/TS, Go, Rust, etc.).
3. The first meaningful step is to define a project goal and pick a concrete runtime/toolchain.

## Suggested next learning/build steps

1. Define the project objective in a short architecture/design note.
2. Add a language/tooling scaffold (for example, `pyproject.toml` or `package.json`).
3. Create a minimal runnable app entry point and a basic test.
4. Add developer automation (`Makefile` or scripts for lint/test/run).
5. Set up CI to run tests and lint on each commit.

## Example initial layout to aim for

```
ai_learning/
  README.md
  docs/
    architecture.md
  src/
  tests/
  scripts/
  .gitignore
```
=======
---

## Features

- **BQ Terraform Agent** — reads one or many table schemas from a single file and generates `google_bigquery_table` Terraform resources
- **Offline-first** — uses [Ollama](https://ollama.com) for all LLM inference; no internet or API key needed for CSV/JSON input
- **Multi-format input** — CSV, JSON, and PDF schema files; single-table and multi-table in the same file
- **Planner → Researcher → Writer** pipeline for general-purpose agentic tasks (Vertex AI / Gemini)
- Cleanly layered: agents / integrations / tools / prompts / state / workflows

---

## Quick Start

### 1. Install dependencies

```bash
pip install -e ".[dev]"
```

### 2. Set up Ollama (required for PDF; optional for CSV/JSON)

```bash
# Install: https://ollama.com
brew install ollama            # macOS

# Pull the recommended code model (one-time, ~4 GB):
ollama pull deepseek-coder:6.7b

# Start the server (usually auto-starts after install):
ollama serve
```

### 3. Run the BQ Terraform agent

```bash
# PDF schema with multiple tables → generates one .tf file per table:
python scripts/run_bq_terraform.py data/raw/schema_multi.pdf \
    --dataset analytics --project my-gcp-project

# Check results:
ls data/processed/terraform/
# sessions.tf   page_views.tf   conversions.tf
```

---

## Input Formats

### PDF — one or many tables (Ollama required for extraction)

Put any PDF that contains schema documentation in `data/raw/`. The agent uses Ollama to extract field names, types, modes, and descriptions automatically.

```bash
python scripts/run_bq_terraform.py data/raw/schema_multi.pdf \
    --dataset analytics --project my-gcp-project
```

Sample output:
```
Reading schema from : data/raw/schema_multi.pdf
Dataset             : analytics
Project             : my-gcp-project
Ollama model        : deepseek-coder:6.7b
HCL generation      : deterministic

  [OK]   sessions         12 fields → data/processed/terraform/sessions.tf
  [OK]   page_views        8 fields → data/processed/terraform/page_views.tf
  [OK]   conversions       7 fields → data/processed/terraform/conversions.tf

Done: 3/3 table(s) succeeded.
```

### JSON — multi-table array

```json
[
  {
    "table": "orders",
    "fields": [
      { "name": "order_id",   "type": "STRING",    "mode": "REQUIRED", "description": "Unique order ID" },
      { "name": "amount",     "type": "NUMERIC",   "mode": "NULLABLE", "description": "Order total" },
      { "name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Order timestamp" }
    ]
  },
  {
    "table": "customers",
    "fields": [
      { "name": "customer_id", "type": "STRING", "mode": "REQUIRED", "description": "Unique customer ID" },
      { "name": "email",       "type": "STRING", "mode": "REQUIRED", "description": "Email address" }
    ]
  }
]
```

```bash
python scripts/run_bq_terraform.py data/raw/schema_multi.json \
    --dataset sales --project my-gcp-project
```

Also accepts a dict format `{"table_name": [field, ...], ...}` and the native BQ schema array `[{name, type, mode, description}]` (single-table; requires `--table`).

### CSV — multi-table with `table_name` column

```csv
table_name,name,type,mode,description
orders,order_id,STRING,REQUIRED,Unique order ID
orders,amount,NUMERIC,NULLABLE,Order total
customers,customer_id,STRING,REQUIRED,Unique customer ID
customers,email,STRING,REQUIRED,Email address
```

```bash
python scripts/run_bq_terraform.py data/raw/schema_multi.csv \
    --dataset sales --project my-gcp-project
```

Without a `table_name` column the file is treated as single-table and `--table` is required:

```bash
python scripts/run_bq_terraform.py data/raw/schema.csv \
    --table orders --dataset sales --project my-gcp-project
```

---

## CLI Reference

```
python scripts/run_bq_terraform.py <input> --dataset <ID> --project <ID> [options]

Positional:
  input               Path to .csv, .json, or .pdf schema file

Required:
  --dataset ID        BigQuery dataset ID
  --project ID        GCP project ID (or set GOOGLE_CLOUD_PROJECT)

Optional:
  --table NAME        For single-table files (no embedded table name), or to
                      filter a multi-table file to one specific table
  --output-dir DIR    Output directory for .tf files (default: data/processed/terraform)
  --model TAG         Ollama model tag (default: deepseek-coder:6.7b)
  --ollama-host URL   Ollama server URL (default: http://localhost:11434)
  --timeout SECS      Ollama request timeout in seconds (default: 300)
  --num-ctx TOKENS    Ollama context window — smaller is faster on CPU (default: 2048)
  --llm-hcl           Use Ollama for HCL generation too (richer, slower)
  --stdout            Print HCL to stdout instead of writing a file
  --check-ollama      Check Ollama connectivity and list available models
```

---

## Generated Terraform Output

Each table produces a standalone `.tf` file:

```hcl
variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "my-gcp-project"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "analytics"
}

variable "table_id" {
  description = "BigQuery table ID"
  type        = string
  default     = "sessions"
}

resource "google_bigquery_table" "sessions" {
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = var.table_id

  deletion_protection = false

  schema = jsonencode([
    {
      name        = "session_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique session identifier (UUID v4)"
    },
    ...
  ])

  lifecycle {
    prevent_destroy = true
  }
}
```

---

## Project Layout

```
src/ai_agents/
  base.py                     # LLMClient Protocol + Agent dataclass
  orchestrator.py             # AgentOrchestrator (Planner→Researcher→Writer)
  agents/
    core.py                   # DEFAULT_PLANNER, DEFAULT_RESEARCHER, DEFAULT_WRITER
    bq_terraform.py           # BQ_TERRAFORM_AGENT, SCHEMA_EXTRACTOR_AGENT
  integrations/
    ollama/client.py          # OllamaLLMClient  — offline, no API key
    vertex/client.py          # VertexLLMClient  — Gemini on GCP
  tools/
    file_reader.py            # CSV / JSON / PDF → list[TableSchema]
  prompts/
    bq_terraform.py           # system prompts + prompt builders
  state/
    bq.py                     # SchemaField, TableSchema, BQTerraformState
  workflows/
    bq_terraform_workflow.py  # run_bq_terraform_workflow()

scripts/
  run_agents.py               # General Planner→Researcher→Writer CLI
  run_bq_terraform.py         # BQ schema → Terraform HCL CLI

data/
  raw/                        # Input schema files (CSV, JSON, PDF)
  processed/terraform/        # Generated .tf files

tests/
  test_orchestrator.py
  test_bq_terraform_agent.py
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OLLAMA_MODEL` | No | `deepseek-coder:6.7b` | Ollama model tag |
| `OLLAMA_HOST` | No | `http://localhost:11434` | Ollama server URL |
| `GOOGLE_CLOUD_PROJECT` | Vertex only | — | GCP project ID |
| `GOOGLE_CLOUD_LOCATION` | No | `us-central1` | GCP region |
| `VERTEX_MODEL_NAME` | No | `gemini-1.5-pro` | Vertex AI model |
| `ANTHROPIC_API_KEY` | Anthropic only | — | Claude API key |

Copy `.env.example` to `.env` and fill in values:

```bash
cp .env.example .env
```

---

## Running Tests

```bash
pytest tests/
```

All tests use `FakeLLM` — no Ollama or cloud credentials needed.

---

## Adding a New Agent

See [AGENTS.md](AGENTS.md) for the full registry, naming conventions, and step-by-step instructions for adding agents, LLM backends, and workflows.
>>>>>>> 200d84f (Add multi-table BQ Terraform agent with full README)

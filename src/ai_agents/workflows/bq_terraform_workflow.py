"""BQ Terraform Workflow — end-to-end pipeline (single & multi-table).

Converts a BQ schema file (CSV, JSON, or PDF) into Terraform HCL files.
One ``.tf`` file is written per table found in the input.

Pipeline
────────
  1. FileReader  → reads CSV/JSON → list[TableSchema]
                   reads PDF      → raw text (str)
  2. (PDF only)  → SchemaExtractorAgent (Ollama) → list[TableSchema]
  3. Per table   → HCL Builder (deterministic) or BQTerraformAgent (LLM)
  4. Writer      → saves <table_name>.tf to data/processed/terraform/

Usage
─────
    from ai_agents.workflows.bq_terraform_workflow import run_bq_terraform_workflow
    from ai_agents.integrations.ollama import OllamaLLMClient

    results = run_bq_terraform_workflow(
        input_path="data/raw/schema_multi.json",
        dataset_id="sales",
        project_id="my-gcp-project",
        llm=OllamaLLMClient(),
    )
    for r in results:
        print(r.table_name, "→", r.output_path, "✓" if r.succeeded else r.error)
"""

from __future__ import annotations

import json
from pathlib import Path

from ..base import LLMClient
from ..prompts.bq_terraform import (
    SCHEMA_EXTRACTION_SYSTEM,
    BQ_TERRAFORM_SYSTEM,
    bq_terraform_prompt,
    schema_extraction_prompt,
)
from ..state.bq import BQTerraformState, SchemaField, TableSchema
from ..tools.file_reader import read_schema_file


def run_bq_terraform_workflow(
    input_path: str | Path,
    dataset_id: str,
    project_id: str,
    llm: LLMClient,
    table_name: str | None = None,
    output_dir: str | Path = "data/processed/terraform",
    use_llm_for_hcl: bool = False,
) -> list[BQTerraformState]:
    """Run the full BQ schema → Terraform HCL pipeline.

    Args:
        input_path:       Path to ``.csv``, ``.json``, or ``.pdf`` schema file.
        dataset_id:       BigQuery dataset ID (applied to all tables).
        project_id:       GCP project ID.
        llm:              Any LLMClient (use OllamaLLMClient for offline inference).
        table_name:       For single-table CSV/JSON with no embedded table name.
                          Also acts as a filter for multi-table files: if provided,
                          only the matching table is processed.
        output_dir:       Directory for generated ``.tf`` files.
        use_llm_for_hcl:  Use Ollama to generate HCL (richer, slower). Default:
                          deterministic builder (fast, always correct).

    Returns:
        One ``BQTerraformState`` per table. Check ``.succeeded`` and ``.error``
        on each result.
    """
    input_path = Path(input_path)
    try:
        raw = read_schema_file(input_path, table_name=table_name)
    except Exception as exc:  # noqa: BLE001
        return [_error_state(str(input_path), table_name or "unknown", dataset_id, project_id, str(exc))]

    # ── Step 1: resolve list[TableSchema] ────────────────────────────────────
    if isinstance(raw, str):
        # PDF path — use LLM to extract table schemas
        tables = _extract_tables_from_text(raw, llm)
    else:
        tables = raw  # list[TableSchema] already

    if not tables:
        return [_error_state(str(input_path), table_name or "unknown", dataset_id, project_id,
                             "No table schemas found in the input file.")]

    # ── Step 2: optional filter by --table ───────────────────────────────────
    if table_name and len(tables) > 1:
        matched = [t for t in tables if t.name == table_name]
        if not matched:
            available = ", ".join(t.name for t in tables)
            return [_error_state(str(input_path), table_name, dataset_id, project_id,
                                 f"Table '{table_name}' not found. Available: {available}")]
        tables = matched

    # ── Step 3: generate HCL for each table ──────────────────────────────────
    results: list[BQTerraformState] = []
    for table in tables:
        state = BQTerraformState(
            input_path=str(input_path),
            table_name=table.name,
            dataset_id=dataset_id,
            project_id=project_id,
            schema_fields=table.fields,
        )
        try:
            if use_llm_for_hcl:
                state.terraform_hcl = _generate_hcl_via_llm(state, llm)
            else:
                state.terraform_hcl = _generate_hcl_deterministic(state)
            state.output_path = _write_output(state, output_dir)
        except Exception as exc:  # noqa: BLE001
            state.error = str(exc)
        results.append(state)

    return results


# ──────────────────────────────────────────────────────────────────────────────
# PDF extraction
# ──────────────────────────────────────────────────────────────────────────────

def _extract_tables_from_text(raw_text: str, llm: LLMClient) -> list[TableSchema]:
    """Use LLM to extract one or more BQ table schemas from unstructured text."""
    response = llm.ask(
        prompt=schema_extraction_prompt(raw_text),
        system_prompt=SCHEMA_EXTRACTION_SYSTEM,
        max_tokens=1024,
    )
    cleaned = (
        response.strip()
        .removeprefix("```json")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )
    try:
        raw_tables = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Schema extractor returned invalid JSON.\nModel output:\n{response}\nError: {exc}"
        ) from exc

    if not isinstance(raw_tables, list):
        raise ValueError(f"Schema extractor must return a JSON array; got {type(raw_tables)}")

    tables: list[TableSchema] = []
    for obj in raw_tables:
        tname = obj.get("table") or obj.get("name") or ""
        if not tname:
            raise ValueError(f"Extracted table object is missing 'table' key: {obj}")
        fields_raw = obj.get("fields") or obj.get("schema") or []
        tables.append(TableSchema(
            name=tname,
            fields=[SchemaField.from_dict(f) for f in fields_raw],
        ))
    return tables


# ──────────────────────────────────────────────────────────────────────────────
# HCL generation
# ──────────────────────────────────────────────────────────────────────────────

def _generate_hcl_via_llm(state: BQTerraformState, llm: LLMClient) -> str:
    fields_as_dicts = [f.to_dict() for f in state.schema_fields]
    prompt = bq_terraform_prompt(
        table_name=state.table_name,
        dataset_id=state.dataset_id,
        project_id=state.project_id,
        schema_fields=fields_as_dicts,
    )
    response = llm.ask(prompt=prompt, system_prompt=BQ_TERRAFORM_SYSTEM, max_tokens=4000)
    return (
        response.strip()
        .removeprefix("```hcl")
        .removeprefix("```terraform")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )


def _generate_hcl_deterministic(state: BQTerraformState) -> str:
    """Deterministically produce valid Terraform HCL — no LLM required."""
    resource_label = _tf_label(state.table_name)
    schema_block = _render_schema_jsonencode(state.schema_fields)

    return f'''\
# Generated by ai-learning-agents BQ Terraform workflow
# Source: {state.input_path}

# ── Variables ────────────────────────────────────────────────────────────────

variable "project_id" {{
  description = "GCP project ID"
  type        = string
  default     = "{state.project_id}"
}}

variable "dataset_id" {{
  description = "BigQuery dataset ID"
  type        = string
  default     = "{state.dataset_id}"
}}

variable "table_id" {{
  description = "BigQuery table ID"
  type        = string
  default     = "{state.table_name}"
}}

# ── BigQuery Table ────────────────────────────────────────────────────────────

resource "google_bigquery_table" "{resource_label}" {{
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = var.table_id

  deletion_protection = false

  schema = {schema_block}

  lifecycle {{
    prevent_destroy = true
  }}
}}
'''


def _render_schema_jsonencode(fields: list[SchemaField]) -> str:
    lines = ["jsonencode(["]
    for i, f in enumerate(fields):
        comma = "," if i < len(fields) - 1 else ""
        desc_line = f'      description = "{_tf_escape(f.description)}"' if f.description else ""
        field_block = (
            f'    {{\n'
            f'      name        = "{f.name}"\n'
            f'      type        = "{f.type}"\n'
            f'      mode        = "{f.mode}"'
            + (f'\n{desc_line}' if desc_line else '') +
            f'\n    }}{comma}'
        )
        lines.append(field_block)
    lines.append("  ])")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _tf_label(name: str) -> str:
    return name.lower().replace("-", "_").replace(" ", "_")


def _tf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _write_output(state: BQTerraformState, output_dir: str | Path) -> str:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{state.table_name}.tf"
    out_path.write_text(state.terraform_hcl, encoding="utf-8")
    return str(out_path)


def _error_state(
    input_path: str, table_name: str, dataset_id: str, project_id: str, error: str
) -> BQTerraformState:
    s = BQTerraformState(
        input_path=input_path, table_name=table_name,
        dataset_id=dataset_id, project_id=project_id,
    )
    s.error = error
    return s

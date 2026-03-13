"""BQ Terraform Workflow — end-to-end pipeline.

Converts a BQ schema file (CSV, JSON, or PDF) into a Terraform HCL file
using the Ollama local LLM for intelligent schema processing.

Pipeline:
  1. FileReader  → reads CSV/JSON to structured fields, or PDF to raw text
  2. (PDF only)  → SchemaExtractorAgent (Ollama) extracts structured fields
  3. HCL Builder → deterministically generates Terraform HCL from fields
                   (optionally enriched by BQTerraformAgent via Ollama)
  4. Writer      → saves .tf file to data/processed/terraform/

Usage:
    from ai_agents.workflows.bq_terraform_workflow import run_bq_terraform_workflow
    from ai_agents.integrations.ollama import OllamaLLMClient

    result = run_bq_terraform_workflow(
        input_path="data/raw/schema.csv",
        table_name="orders",
        dataset_id="sales",
        project_id="my-gcp-project",
        llm=OllamaLLMClient(),
    )
    print(result.terraform_hcl)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..agents.bq_terraform import BQ_TERRAFORM_AGENT, SCHEMA_EXTRACTOR_AGENT
from ..base import LLMClient
from ..prompts.bq_terraform import (
    SCHEMA_EXTRACTION_SYSTEM,
    BQ_TERRAFORM_SYSTEM,
    bq_terraform_prompt,
    schema_extraction_prompt,
)
from ..state.bq import BQTerraformState, SchemaField
from ..tools.file_reader import read_schema_file


def run_bq_terraform_workflow(
    input_path: str | Path,
    table_name: str,
    dataset_id: str,
    project_id: str,
    llm: LLMClient,
    output_dir: str | Path = "data/processed/terraform",
    use_llm_for_hcl: bool = False,
) -> BQTerraformState:
    """Run the full BQ schema → Terraform HCL pipeline.

    Args:
        input_path:       Path to .csv, .json, or .pdf schema file.
        table_name:       BigQuery table name (used in the TF resource).
        dataset_id:       BigQuery dataset ID.
        project_id:       GCP project ID.
        llm:              Any LLMClient implementation (use OllamaLLMClient
                          for offline / local inference).
        output_dir:       Directory to write the generated .tf file.
        use_llm_for_hcl:  If True, pass the schema through the BQTerraformAgent
                          (Ollama) for richer HCL generation. If False (default),
                          use the deterministic HCL builder — faster and reliable.

    Returns:
        BQTerraformState with terraform_hcl and output_path populated on success.
    """
    state = BQTerraformState(
        input_path=str(input_path),
        table_name=table_name,
        dataset_id=dataset_id,
        project_id=project_id,
    )

    try:
        # ── Step 1: Read input file ──────────────────────────────────────────
        state.raw_input = read_schema_file(input_path)

        # ── Step 2: Extract schema fields ────────────────────────────────────
        if state.is_pdf_input:
            # PDF path: use Ollama to extract structured schema from raw text
            state.schema_fields = _extract_schema_from_text(
                raw_text=state.raw_input,  # type: ignore[arg-type]
                llm=llm,
            )
        else:
            # CSV / JSON path: already structured — just coerce to SchemaField
            state.schema_fields = [
                SchemaField.from_dict(f) for f in state.raw_input  # type: ignore[arg-type]
            ]

        if not state.schema_fields:
            raise ValueError("No schema fields could be extracted from the input file.")

        # ── Step 3: Generate Terraform HCL ───────────────────────────────────
        if use_llm_for_hcl:
            state.terraform_hcl = _generate_hcl_via_llm(state, llm)
        else:
            state.terraform_hcl = _generate_hcl_deterministic(state)

        # ── Step 4: Write output file ─────────────────────────────────────────
        state.output_path = _write_output(state, output_dir)

    except Exception as exc:  # noqa: BLE001
        state.error = str(exc)

    return state


# ------------------------------------------------------------------
# Step implementations
# ------------------------------------------------------------------

def _extract_schema_from_text(raw_text: str, llm: LLMClient) -> list[SchemaField]:
    """Use Ollama to extract BQ schema fields from unstructured PDF text."""
    response = llm.ask(
        prompt=schema_extraction_prompt(raw_text),
        system_prompt=SCHEMA_EXTRACTION_SYSTEM,
        max_tokens=1024,  # JSON array of fields — rarely exceeds 1k tokens
    )
    # Strip accidental markdown fences the model might add
    cleaned = response.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        fields_raw = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Schema extractor returned invalid JSON. "
            f"Model output:\n{response}\n\nError: {exc}"
        ) from exc

    if not isinstance(fields_raw, list):
        raise ValueError(f"Schema extractor must return a JSON array; got: {type(fields_raw)}")

    return [SchemaField.from_dict(f) for f in fields_raw]


def _generate_hcl_via_llm(state: BQTerraformState, llm: LLMClient) -> str:
    """Use Ollama/LLM to generate Terraform HCL (richer, may include comments)."""
    fields_as_dicts = [f.to_dict() for f in state.schema_fields]
    prompt = bq_terraform_prompt(
        table_name=state.table_name,
        dataset_id=state.dataset_id,
        project_id=state.project_id,
        schema_fields=fields_as_dicts,
    )
    response = llm.ask(
        prompt=prompt,
        system_prompt=BQ_TERRAFORM_SYSTEM,
        max_tokens=4000,
    )
    # Strip accidental markdown fences
    return response.strip().removeprefix("```hcl").removeprefix("```terraform").removeprefix("```").removesuffix("```").strip()


def _generate_hcl_deterministic(state: BQTerraformState) -> str:
    """Deterministically generate valid Terraform HCL from parsed schema fields.

    This is fast, reliable, and requires no LLM — preferred for CSV/JSON inputs.
    """
    resource_label = _tf_label(state.table_name)
    schema_block = _render_schema_jsonencode(state.schema_fields)

    hcl = f'''\
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
    return hcl


def _render_schema_jsonencode(fields: list[SchemaField]) -> str:
    """Render the jsonencode() schema block for Terraform."""
    lines = ["jsonencode(["]
    for i, f in enumerate(fields):
        comma = "," if i < len(fields) - 1 else ""
        desc_line = f'      description = "{_tf_escape(f.description)}"' if f.description else ""
        field_block = f'''\
    {{
      name        = "{f.name}"
      type        = "{f.type}"
      mode        = "{f.mode}"{chr(10) + desc_line if desc_line else ""}
    }}{comma}'''
        lines.append(field_block)
    lines.append("  ])")
    return "\n".join(lines)


def _tf_label(name: str) -> str:
    """Convert a table name to a valid Terraform resource label."""
    return name.lower().replace("-", "_").replace(" ", "_")


def _tf_escape(s: str) -> str:
    """Escape double quotes and backslashes for HCL string values."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _write_output(state: BQTerraformState, output_dir: str | Path) -> str:
    """Write the generated HCL to data/processed/terraform/<table_name>.tf."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{state.table_name}.tf"
    out_path.write_text(state.terraform_hcl, encoding="utf-8")
    return str(out_path)

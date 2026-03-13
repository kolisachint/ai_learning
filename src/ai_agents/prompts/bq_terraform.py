"""Prompt templates for the BQ Terraform workflow."""

from __future__ import annotations


# ------------------------------------------------------------------
# Schema extraction (PDF → structured JSON array)
# ------------------------------------------------------------------

SCHEMA_EXTRACTION_SYSTEM = (
    "You are a data schema analyst. "
    "Given raw text that describes a BigQuery table schema, extract all fields "
    "and return them as a JSON array. Each element must have keys: "
    "name, type, mode, description. "
    "Valid BQ types: STRING, INTEGER, FLOAT, BOOLEAN, RECORD, TIMESTAMP, DATE, "
    "TIME, DATETIME, NUMERIC, BIGNUMERIC, BYTES, JSON, GEOGRAPHY, INTERVAL. "
    "Valid modes: NULLABLE, REQUIRED, REPEATED. "
    "Output ONLY the JSON array — no markdown fences, no explanation."
)


def schema_extraction_prompt(raw_text: str) -> str:
    return (
        f"Extract the BigQuery schema fields from the following text and return "
        f"a JSON array:\n\n{raw_text}"
    )


# ------------------------------------------------------------------
# HCL generation (schema fields → Terraform resource)
# ------------------------------------------------------------------

BQ_TERRAFORM_SYSTEM = (
    "You are a Terraform and BigQuery expert. "
    "Given a BigQuery table schema, produce a complete, valid Terraform HCL file. "
    "Requirements:\n"
    "- Use the google_bigquery_table resource.\n"
    "- Use jsonencode() for the schema block.\n"
    "- Reference project, dataset, and table via variables.\n"
    "- Include a variables.tf block defining project_id, dataset_id, table_id.\n"
    "- Add deletion_protection = false and a lifecycle prevent_destroy block.\n"
    "- Output ONLY valid HCL — no markdown fences, no explanation."
)


def bq_terraform_prompt(
    table_name: str,
    dataset_id: str,
    project_id: str,
    schema_fields: list[dict],
) -> str:
    import json as _json

    schema_json = _json.dumps(schema_fields, indent=2)
    return (
        f"Generate Terraform HCL for a BigQuery table with the following details:\n\n"
        f"Project  : {project_id}\n"
        f"Dataset  : {dataset_id}\n"
        f"Table    : {table_name}\n\n"
        f"Schema fields (JSON):\n{schema_json}\n\n"
        f"Produce the complete Terraform resource and variable definitions."
    )

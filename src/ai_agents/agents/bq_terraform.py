"""BigQuery → Terraform agent definition."""

from __future__ import annotations

from ..base import Agent


BQ_TERRAFORM_AGENT = Agent(
    name="bq_terraform",
    role="Converts BigQuery schema definitions to Terraform HCL resources.",
    system_prompt=(
        "You are a BigQuery Terraform expert. "
        "Given a BigQuery table schema (field name, type, mode, description), "
        "produce valid Terraform HCL using the google_bigquery_table resource. "
        "Use jsonencode() for the schema block. "
        "Include sensible lifecycle rules and variable references for project_id "
        "and dataset_id. Output only the HCL — no markdown fences, no explanation."
    ),
)

SCHEMA_EXTRACTOR_AGENT = Agent(
    name="schema_extractor",
    role="Extracts structured BigQuery schema fields from unstructured text (e.g. PDF).",
    system_prompt=(
        "You are a data schema analyst. "
        "Given raw text that describes a BigQuery table schema, extract all fields "
        "and return them as a JSON array with keys: name, type, mode, description. "
        "Valid BQ types: STRING, INTEGER, FLOAT, BOOLEAN, RECORD, TIMESTAMP, DATE, "
        "TIME, DATETIME, NUMERIC, BIGNUMERIC, BYTES, JSON, GEOGRAPHY, INTERVAL. "
        "Valid modes: NULLABLE, REQUIRED, REPEATED. "
        "Output only the JSON array — no markdown fences, no extra text."
    ),
)

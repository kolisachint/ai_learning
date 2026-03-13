"""Typed state objects for the BQ Terraform workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SchemaField:
    """Single BigQuery table field."""

    name: str
    type: str
    mode: str = "NULLABLE"
    description: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "SchemaField":
        return cls(
            name=d["name"],
            type=d["type"],
            mode=d.get("mode", "NULLABLE"),
            description=d.get("description", ""),
        )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "mode": self.mode,
            "description": self.description,
        }


@dataclass
class BQTerraformState:
    """Mutable state that flows through the BQ Terraform workflow.

    Stages:
      1. raw_input populated by FileReader
      2. schema_fields populated by parser or SchemaExtractorAgent (PDF path)
      3. terraform_hcl populated by BQTerraformAgent
      4. output_path set after writing to disk
    """

    # --- inputs ---
    input_path: str
    table_name: str
    dataset_id: str
    project_id: str

    # --- intermediate ---
    # list[dict] for structured input (CSV/JSON), str for PDF raw text
    raw_input: list[dict] | str = field(default_factory=list)
    schema_fields: list[SchemaField] = field(default_factory=list)

    # --- outputs ---
    terraform_hcl: str = ""
    output_path: str = ""
    error: str = ""

    @property
    def is_pdf_input(self) -> bool:
        return isinstance(self.raw_input, str)

    @property
    def has_schema(self) -> bool:
        return bool(self.schema_fields)

    @property
    def succeeded(self) -> bool:
        return bool(self.terraform_hcl) and not self.error

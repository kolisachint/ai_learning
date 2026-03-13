"""Typed state objects for the BQ Terraform workflow."""

from __future__ import annotations

from dataclasses import dataclass, field


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
class TableSchema:
    """A named table with its schema fields.

    Returned by file_reader for both single-table and multi-table inputs.
    """

    name: str
    fields: list[SchemaField]

    @classmethod
    def from_dict(cls, d: dict) -> "TableSchema":
        """Parse from a multi-table JSON element: {table/name, fields/schema}."""
        name = d.get("table") or d.get("name") or ""
        raw_fields = d.get("fields") or d.get("schema") or []
        return cls(
            name=name,
            fields=[SchemaField.from_dict(f) for f in raw_fields],
        )


@dataclass
class BQTerraformState:
    """Mutable state for one table flowing through the BQ Terraform workflow.

    The workflow returns one BQTerraformState per table found in the input file.

    Stages:
      1. schema_fields populated by file_reader (CSV/JSON) or SchemaExtractorAgent (PDF)
      2. terraform_hcl populated by HCL builder
      3. output_path set after writing to disk
    """

    # --- inputs ---
    input_path: str
    table_name: str
    dataset_id: str
    project_id: str

    # --- intermediate ---
    schema_fields: list[SchemaField] = field(default_factory=list)

    # --- outputs ---
    terraform_hcl: str = ""
    output_path: str = ""
    error: str = ""

    @property
    def has_schema(self) -> bool:
        return bool(self.schema_fields)

    @property
    def succeeded(self) -> bool:
        return bool(self.terraform_hcl) and not self.error

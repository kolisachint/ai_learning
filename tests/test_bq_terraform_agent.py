"""Tests for the BQ Terraform agent and workflow.

All tests use FakeLLM — no real LLM or Ollama required.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from ai_agents.state.bq import SchemaField, BQTerraformState
from ai_agents.tools.file_reader import read_schema_file, _normalise
from ai_agents.workflows.bq_terraform_workflow import (
    run_bq_terraform_workflow,
    _generate_hcl_deterministic,
    _render_schema_jsonencode,
)


# ------------------------------------------------------------------
# Fake LLM
# ------------------------------------------------------------------

class FakeLLM:
    """Test double for OllamaLLMClient / any LLMClient."""

    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self._responses = responses or {}

    def ask(self, prompt: str, system_prompt: str, max_tokens: int = 1000) -> str:
        self.calls.append((prompt, system_prompt))
        for key, response in self._responses.items():
            if key in prompt or key in system_prompt:
                return response
        return '[]'


# ------------------------------------------------------------------
# file_reader — JSON
# ------------------------------------------------------------------

class TestReadJson:
    def test_array_format(self, tmp_path: Path) -> None:
        schema = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED", "description": "PK"},
            {"name": "ts", "type": "TIMESTAMP"},
        ]
        f = tmp_path / "schema.json"
        f.write_text(json.dumps(schema))
        result = read_schema_file(f)
        assert len(result) == 2
        assert result[0]["name"] == "id"
        assert result[0]["mode"] == "REQUIRED"
        assert result[1]["mode"] == "NULLABLE"   # default applied

    def test_dict_with_schema_key(self, tmp_path: Path) -> None:
        data = {"schema": [{"name": "col", "type": "INTEGER"}]}
        f = tmp_path / "schema.json"
        f.write_text(json.dumps(data))
        result = read_schema_file(f)
        assert result[0]["type"] == "INTEGER"

    def test_missing_name_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.json"
        f.write_text(json.dumps([{"type": "STRING"}]))
        with pytest.raises(ValueError, match="missing a 'name'"):
            read_schema_file(f)

    def test_type_alias_normalisation(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.json"
        f.write_text(json.dumps([{"name": "x", "type": "INT64"}]))
        result = read_schema_file(f)
        assert result[0]["type"] == "INTEGER"


# ------------------------------------------------------------------
# file_reader — CSV
# ------------------------------------------------------------------

class TestReadCsv:
    def test_basic_csv(self, tmp_path: Path) -> None:
        content = "name,type,mode,description\nuser_id,STRING,REQUIRED,Primary key\nemail,STRING,NULLABLE,\n"
        f = tmp_path / "schema.csv"
        f.write_text(content)
        result = read_schema_file(f)
        assert len(result) == 2
        assert result[0]["name"] == "user_id"
        assert result[0]["mode"] == "REQUIRED"
        assert result[1]["description"] == ""

    def test_missing_required_column_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.csv"
        f.write_text("name,mode\nfoo,NULLABLE\n")
        with pytest.raises(ValueError, match="missing required columns"):
            read_schema_file(f)

    def test_case_insensitive_headers(self, tmp_path: Path) -> None:
        content = "Name,Type,Mode\nval,FLOAT,REQUIRED\n"
        f = tmp_path / "schema.csv"
        f.write_text(content)
        result = read_schema_file(f)
        assert result[0]["type"] == "FLOAT"

    def test_invalid_mode_defaults_to_nullable(self, tmp_path: Path) -> None:
        content = "name,type,mode\ncol,STRING,WRONG\n"
        f = tmp_path / "schema.csv"
        f.write_text(content)
        result = read_schema_file(f)
        assert result[0]["mode"] == "NULLABLE"


# ------------------------------------------------------------------
# file_reader — edge cases
# ------------------------------------------------------------------

class TestReadSchemaEdgeCases:
    def test_unsupported_extension_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.xlsx"
        f.write_text("dummy")
        with pytest.raises(ValueError, match="Unsupported file type"):
            read_schema_file(f)

    def test_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            read_schema_file("/nonexistent/schema.json")


# ------------------------------------------------------------------
# HCL generation — deterministic
# ------------------------------------------------------------------

class TestDeterministicHCL:
    def _make_state(self, fields: list[SchemaField]) -> BQTerraformState:
        state = BQTerraformState(
            input_path="schema.json",
            table_name="orders",
            dataset_id="sales",
            project_id="my-project",
        )
        state.schema_fields = fields
        return state

    def test_resource_block_present(self) -> None:
        fields = [SchemaField(name="id", type="STRING", mode="REQUIRED")]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert 'resource "google_bigquery_table" "orders"' in hcl

    def test_variable_blocks_present(self) -> None:
        fields = [SchemaField(name="id", type="STRING")]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert 'variable "project_id"' in hcl
        assert 'variable "dataset_id"' in hcl
        assert 'variable "table_id"' in hcl

    def test_schema_contains_field(self) -> None:
        fields = [SchemaField(name="email", type="STRING", mode="NULLABLE", description="User email")]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert '"email"' in hcl
        assert '"STRING"' in hcl

    def test_lifecycle_block_present(self) -> None:
        fields = [SchemaField(name="x", type="INTEGER")]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert "prevent_destroy" in hcl

    def test_deletion_protection_false(self) -> None:
        fields = [SchemaField(name="x", type="INTEGER")]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert "deletion_protection = false" in hcl

    def test_description_escaped(self) -> None:
        fields = [SchemaField(name="x", type="STRING", description='Say "hello"')]
        state = self._make_state(fields)
        hcl = _generate_hcl_deterministic(state)
        assert r'\"hello\"' in hcl


# ------------------------------------------------------------------
# Full workflow — CSV/JSON path (no LLM needed for HCL)
# ------------------------------------------------------------------

class TestBQTerraformWorkflow:
    def test_csv_end_to_end(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.csv"
        schema_file.write_text("name,type,mode,description\nid,STRING,REQUIRED,PK\namt,NUMERIC,NULLABLE,\n")

        fake = FakeLLM()
        state = run_bq_terraform_workflow(
            input_path=schema_file,
            table_name="invoices",
            dataset_id="finance",
            project_id="proj-123",
            llm=fake,
            output_dir=tmp_path / "out",
        )

        assert not state.error, f"Unexpected error: {state.error}"
        assert state.succeeded
        assert len(state.schema_fields) == 2
        assert 'google_bigquery_table' in state.terraform_hcl
        assert Path(state.output_path).exists()
        # CSV/JSON path: LLM should NOT be called for deterministic HCL
        assert len(fake.calls) == 0

    def test_json_end_to_end(self, tmp_path: Path) -> None:
        schema = [{"name": "user_id", "type": "STRING", "mode": "REQUIRED"}]
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(schema))

        fake = FakeLLM()
        state = run_bq_terraform_workflow(
            input_path=schema_file,
            table_name="users",
            dataset_id="core",
            project_id="proj-abc",
            llm=fake,
            output_dir=tmp_path / "out",
        )

        assert state.succeeded
        assert state.schema_fields[0].name == "user_id"
        assert len(fake.calls) == 0

    def test_pdf_path_uses_llm(self, tmp_path: Path) -> None:
        """PDF path should call LLM once for schema extraction."""
        # We can't create a real PDF without pdfplumber, so patch read_schema_file
        import ai_agents.workflows.bq_terraform_workflow as wf_module

        extracted_json = json.dumps([{"name": "ev_id", "type": "STRING", "mode": "REQUIRED", "description": ""}])
        fake = FakeLLM(responses={"Extract the BigQuery schema": extracted_json})

        # Monkeypatch read_schema_file to return raw text (simulating PDF)
        original = wf_module.read_schema_file
        wf_module.read_schema_file = lambda _path: "Table: events\nFields: ev_id STRING REQUIRED"

        try:
            state = run_bq_terraform_workflow(
                input_path="fake.pdf",
                table_name="events",
                dataset_id="analytics",
                project_id="proj-xyz",
                llm=fake,
                output_dir=tmp_path / "out",
            )
        finally:
            wf_module.read_schema_file = original

        assert state.succeeded
        assert len(fake.calls) == 1   # schema extraction call
        assert state.schema_fields[0].name == "ev_id"

    def test_missing_file_returns_error(self, tmp_path: Path) -> None:
        fake = FakeLLM()
        state = run_bq_terraform_workflow(
            input_path="/nonexistent/schema.json",
            table_name="t",
            dataset_id="d",
            project_id="p",
            llm=fake,
            output_dir=tmp_path / "out",
        )
        assert state.error
        assert not state.succeeded

    def test_output_file_written(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps([{"name": "col", "type": "STRING"}]))

        fake = FakeLLM()
        state = run_bq_terraform_workflow(
            input_path=schema_file,
            table_name="my_table",
            dataset_id="ds",
            project_id="proj",
            llm=fake,
            output_dir=tmp_path / "terraform",
        )

        out = Path(state.output_path)
        assert out.name == "my_table.tf"
        assert out.read_text() == state.terraform_hcl

"""Tests for the BQ Terraform agent and workflow.

All tests use FakeLLM — no real LLM or Ollama required.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from ai_agents.state.bq import SchemaField, TableSchema, BQTerraformState
from ai_agents.tools.file_reader import read_schema_file, _normalise_field
from ai_agents.workflows.bq_terraform_workflow import (
    run_bq_terraform_workflow,
    _generate_hcl_deterministic,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fake LLM
# ──────────────────────────────────────────────────────────────────────────────

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
        return "[]"


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — JSON (single-table)
# ──────────────────────────────────────────────────────────────────────────────

class TestReadJsonSingleTable:
    def test_bare_array_with_table_name(self, tmp_path: Path) -> None:
        schema = [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
        f = tmp_path / "schema.json"
        f.write_text(json.dumps(schema))
        result = read_schema_file(f, table_name="users")
        assert len(result) == 1
        assert result[0].name == "users"
        assert result[0].fields[0].name == "id"

    def test_bare_array_without_table_name_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.json"
        f.write_text(json.dumps([{"name": "id", "type": "STRING"}]))
        with pytest.raises(ValueError, match="--table"):
            read_schema_file(f)

    def test_type_alias_normalisation(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.json"
        f.write_text(json.dumps([{"name": "x", "type": "INT64"}]))
        result = read_schema_file(f, table_name="t")
        assert result[0].fields[0].type == "INTEGER"

    def test_missing_name_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.json"
        f.write_text(json.dumps([{"type": "STRING"}]))
        with pytest.raises(ValueError, match="missing a 'name'"):
            read_schema_file(f, table_name="t")


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — JSON (multi-table)
# ──────────────────────────────────────────────────────────────────────────────

class TestReadJsonMultiTable:
    def test_array_of_table_objects(self, tmp_path: Path) -> None:
        data = [
            {"table": "orders", "fields": [{"name": "id", "type": "STRING"}]},
            {"table": "users",  "fields": [{"name": "uid", "type": "STRING"}, {"name": "email", "type": "STRING"}]},
        ]
        f = tmp_path / "multi.json"
        f.write_text(json.dumps(data))
        result = read_schema_file(f)
        assert len(result) == 2
        assert result[0].name == "orders"
        assert result[1].name == "users"
        assert len(result[1].fields) == 2

    def test_dict_format(self, tmp_path: Path) -> None:
        data = {
            "products": [{"name": "sku", "type": "STRING"}],
            "inventory": [{"name": "qty", "type": "INTEGER"}],
        }
        f = tmp_path / "dict.json"
        f.write_text(json.dumps(data))
        result = read_schema_file(f)
        assert {t.name for t in result} == {"products", "inventory"}

    def test_fields_or_schema_key_accepted(self, tmp_path: Path) -> None:
        data = [{"table": "t", "schema": [{"name": "col", "type": "STRING"}]}]
        f = tmp_path / "s.json"
        f.write_text(json.dumps(data))
        result = read_schema_file(f)
        assert result[0].fields[0].name == "col"

    def test_missing_table_key_raises(self, tmp_path: Path) -> None:
        data = [{"fields": [{"name": "x", "type": "STRING"}]}]
        f = tmp_path / "bad.json"
        f.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="missing a 'table' key"):
            read_schema_file(f)


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — CSV (single-table)
# ──────────────────────────────────────────────────────────────────────────────

class TestReadCsvSingleTable:
    def test_basic_csv(self, tmp_path: Path) -> None:
        content = "name,type,mode,description\nuser_id,STRING,REQUIRED,PK\nemail,STRING,NULLABLE,\n"
        f = tmp_path / "schema.csv"
        f.write_text(content)
        result = read_schema_file(f, table_name="users")
        assert len(result) == 1
        assert result[0].name == "users"
        assert len(result[0].fields) == 2

    def test_no_table_col_no_table_name_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.csv"
        f.write_text("name,type\nfoo,STRING\n")
        with pytest.raises(ValueError, match="--table"):
            read_schema_file(f)

    def test_case_insensitive_headers(self, tmp_path: Path) -> None:
        content = "Name,Type,Mode\nval,FLOAT,REQUIRED\n"
        f = tmp_path / "schema.csv"
        f.write_text(content)
        result = read_schema_file(f, table_name="t")
        assert result[0].fields[0].type == "FLOAT"

    def test_invalid_mode_defaults_to_nullable(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.csv"
        f.write_text("name,type,mode\ncol,STRING,WRONG\n")
        result = read_schema_file(f, table_name="t")
        assert result[0].fields[0].mode == "NULLABLE"


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — CSV (multi-table)
# ──────────────────────────────────────────────────────────────────────────────

class TestReadCsvMultiTable:
    def _csv(self, tmp_path: Path, content: str) -> Path:
        f = tmp_path / "multi.csv"
        f.write_text(content)
        return f

    def test_groups_by_table_name_column(self, tmp_path: Path) -> None:
        content = (
            "table_name,name,type,mode,description\n"
            "orders,order_id,STRING,REQUIRED,PK\n"
            "orders,amount,NUMERIC,NULLABLE,\n"
            "users,user_id,STRING,REQUIRED,PK\n"
        )
        result = read_schema_file(self._csv(tmp_path, content))
        assert len(result) == 2
        assert result[0].name == "orders"
        assert len(result[0].fields) == 2
        assert result[1].name == "users"

    def test_table_column_alias(self, tmp_path: Path) -> None:
        """'table' column (no _name suffix) should also be detected."""
        content = "table,name,type\nfoo,x,STRING\nbar,y,INTEGER\n"
        result = read_schema_file(self._csv(tmp_path, content))
        assert {t.name for t in result} == {"foo", "bar"}

    def test_order_preserved(self, tmp_path: Path) -> None:
        content = "table_name,name,type\nalpha,a,STRING\nbeta,b,STRING\nalpha,c,STRING\n"
        result = read_schema_file(self._csv(tmp_path, content))
        # alpha appears first, beta second; order reflects first-seen
        assert result[0].name == "alpha"
        assert result[1].name == "beta"
        # alpha should have both its fields
        assert len(result[0].fields) == 2


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — edge cases
# ──────────────────────────────────────────────────────────────────────────────

class TestReadSchemaEdgeCases:
    def test_unsupported_extension_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.xlsx"
        f.write_text("dummy")
        with pytest.raises(ValueError, match="Unsupported file type"):
            read_schema_file(f)

    def test_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            read_schema_file("/nonexistent/schema.json")


# ──────────────────────────────────────────────────────────────────────────────
# HCL generation — deterministic
# ──────────────────────────────────────────────────────────────────────────────

class TestDeterministicHCL:
    def _state(self, fields: list[SchemaField], table="orders") -> BQTerraformState:
        s = BQTerraformState(
            input_path="schema.json", table_name=table,
            dataset_id="sales", project_id="proj",
        )
        s.schema_fields = fields
        return s

    def test_resource_block_present(self) -> None:
        hcl = _generate_hcl_deterministic(self._state([SchemaField("id", "STRING", "REQUIRED")]))
        assert 'resource "google_bigquery_table" "orders"' in hcl

    def test_variable_blocks_present(self) -> None:
        hcl = _generate_hcl_deterministic(self._state([SchemaField("id", "STRING")]))
        assert 'variable "project_id"' in hcl
        assert 'variable "dataset_id"' in hcl
        assert 'variable "table_id"' in hcl

    def test_schema_field_present(self) -> None:
        hcl = _generate_hcl_deterministic(
            self._state([SchemaField("email", "STRING", "NULLABLE", "User email")])
        )
        assert '"email"' in hcl
        assert '"STRING"' in hcl

    def test_lifecycle_block_present(self) -> None:
        hcl = _generate_hcl_deterministic(self._state([SchemaField("x", "INTEGER")]))
        assert "prevent_destroy" in hcl

    def test_deletion_protection_false(self) -> None:
        hcl = _generate_hcl_deterministic(self._state([SchemaField("x", "INTEGER")]))
        assert "deletion_protection = false" in hcl

    def test_description_escaped(self) -> None:
        hcl = _generate_hcl_deterministic(
            self._state([SchemaField("x", "STRING", description='Say "hello"')])
        )
        assert r'\"hello\"' in hcl


# ──────────────────────────────────────────────────────────────────────────────
# Full workflow — single-table CSV/JSON (no LLM)
# ──────────────────────────────────────────────────────────────────────────────

class TestWorkflowSingleTable:
    def test_csv_end_to_end(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.csv"
        f.write_text("name,type,mode,description\nid,STRING,REQUIRED,PK\namt,NUMERIC,NULLABLE,\n")
        fake = FakeLLM()
        results = run_bq_terraform_workflow(
            input_path=f, dataset_id="fin", project_id="proj",
            llm=fake, table_name="invoices", output_dir=tmp_path / "out",
        )
        assert len(results) == 1
        assert results[0].succeeded
        assert len(results[0].schema_fields) == 2
        assert len(fake.calls) == 0  # no LLM for structured input

    def test_json_end_to_end(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.json"
        f.write_text(json.dumps([{"name": "user_id", "type": "STRING", "mode": "REQUIRED"}]))
        results = run_bq_terraform_workflow(
            input_path=f, dataset_id="core", project_id="proj",
            llm=FakeLLM(), table_name="users", output_dir=tmp_path / "out",
        )
        assert results[0].schema_fields[0].name == "user_id"

    def test_output_file_written(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.json"
        f.write_text(json.dumps([{"name": "col", "type": "STRING"}]))
        results = run_bq_terraform_workflow(
            input_path=f, dataset_id="ds", project_id="proj",
            llm=FakeLLM(), table_name="my_table", output_dir=tmp_path / "tf",
        )
        out = Path(results[0].output_path)
        assert out.name == "my_table.tf"
        assert out.read_text() == results[0].terraform_hcl

    def test_missing_file_returns_error(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path="/nonexistent/schema.json",
            dataset_id="d", project_id="p", llm=FakeLLM(),
            table_name="t", output_dir=tmp_path / "out",
        )
        assert results[0].error
        assert not results[0].succeeded


# ──────────────────────────────────────────────────────────────────────────────
# Full workflow — multi-table
# ──────────────────────────────────────────────────────────────────────────────

class TestWorkflowMultiTable:
    def _multi_json(self, tmp_path: Path) -> Path:
        data = [
            {"table": "orders", "fields": [
                {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "amount",   "type": "NUMERIC"},
            ]},
            {"table": "customers", "fields": [
                {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "email",       "type": "STRING"},
                {"name": "created_at",  "type": "TIMESTAMP"},
            ]},
            {"table": "products", "fields": [
                {"name": "sku",   "type": "STRING"},
                {"name": "price", "type": "NUMERIC"},
            ]},
        ]
        f = tmp_path / "multi.json"
        f.write_text(json.dumps(data))
        return f

    def test_all_tables_returned(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), output_dir=tmp_path / "tf",
        )
        assert len(results) == 3
        names = {r.table_name for r in results}
        assert names == {"orders", "customers", "products"}

    def test_all_tables_succeed(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), output_dir=tmp_path / "tf",
        )
        assert all(r.succeeded for r in results)

    def test_separate_tf_files_written(self, tmp_path: Path) -> None:
        out_dir = tmp_path / "tf"
        run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), output_dir=out_dir,
        )
        tf_files = {p.name for p in out_dir.iterdir() if p.suffix == ".tf"}
        assert tf_files == {"orders.tf", "customers.tf", "products.tf"}

    def test_table_filter(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), table_name="orders", output_dir=tmp_path / "tf",
        )
        assert len(results) == 1
        assert results[0].table_name == "orders"

    def test_table_filter_not_found_returns_error(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), table_name="nonexistent", output_dir=tmp_path / "tf",
        )
        assert len(results) == 1
        assert results[0].error
        assert "nonexistent" in results[0].error

    def test_multi_csv_end_to_end(self, tmp_path: Path) -> None:
        content = (
            "table_name,name,type,mode\n"
            "orders,order_id,STRING,REQUIRED\n"
            "orders,amount,NUMERIC,NULLABLE\n"
            "users,user_id,STRING,REQUIRED\n"
        )
        f = tmp_path / "multi.csv"
        f.write_text(content)
        results = run_bq_terraform_workflow(
            input_path=f, dataset_id="core", project_id="proj",
            llm=FakeLLM(), output_dir=tmp_path / "tf",
        )
        assert len(results) == 2
        assert all(r.succeeded for r in results)

    def test_field_counts_correct(self, tmp_path: Path) -> None:
        results = run_bq_terraform_workflow(
            input_path=self._multi_json(tmp_path),
            dataset_id="sales", project_id="proj",
            llm=FakeLLM(), output_dir=tmp_path / "tf",
        )
        counts = {r.table_name: len(r.schema_fields) for r in results}
        assert counts == {"orders": 2, "customers": 3, "products": 2}


# ──────────────────────────────────────────────────────────────────────────────
# Full workflow — PDF path (LLM extraction, multi-table)
# ──────────────────────────────────────────────────────────────────────────────

class TestWorkflowPdf:
    def test_pdf_calls_llm_once(self, tmp_path: Path) -> None:
        import ai_agents.workflows.bq_terraform_workflow as wf_module

        multi_json = json.dumps([
            {"table": "sessions", "fields": [{"name": "session_id", "type": "STRING", "mode": "REQUIRED", "description": ""}]},
            {"table": "events",   "fields": [{"name": "event_id",   "type": "STRING", "mode": "REQUIRED", "description": ""}]},
        ])
        fake = FakeLLM(responses={"Extract all BigQuery": multi_json})
        original = wf_module.read_schema_file
        wf_module.read_schema_file = lambda _path, **kw: "raw pdf text about sessions and events"

        try:
            results = run_bq_terraform_workflow(
                input_path="schema.pdf", dataset_id="analytics",
                project_id="proj", llm=fake, output_dir=tmp_path / "tf",
            )
        finally:
            wf_module.read_schema_file = original

        assert len(fake.calls) == 1
        assert len(results) == 2
        assert all(r.succeeded for r in results)

    def test_pdf_multi_table_names(self, tmp_path: Path) -> None:
        import ai_agents.workflows.bq_terraform_workflow as wf_module

        multi_json = json.dumps([
            {"table": "sessions",    "fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED", "description": ""}]},
            {"table": "page_views",  "fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED", "description": ""}]},
            {"table": "conversions", "fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED", "description": ""}]},
        ])
        fake = FakeLLM(responses={"Extract all BigQuery": multi_json})
        original = wf_module.read_schema_file
        wf_module.read_schema_file = lambda _path, **kw: "raw text"

        try:
            results = run_bq_terraform_workflow(
                input_path="schema.pdf", dataset_id="d",
                project_id="p", llm=fake, output_dir=tmp_path / "tf",
            )
        finally:
            wf_module.read_schema_file = original

        assert {r.table_name for r in results} == {"sessions", "page_views", "conversions"}


# ──────────────────────────────────────────────────────────────────────────────
# file_reader — HTML (structured schema tables)
# ──────────────────────────────────────────────────────────────────────────────

class TestReadHtmlStructured:
    def _html(self, tmp_path: Path, body: str, title: str = "Schema") -> Path:
        f = tmp_path / "schema.html"
        f.write_text(f"<html><head><title>{title}</title></head><body>{body}</body></html>")
        return f

    def _table(self, rows: list[tuple], caption: str = "", table_id: str = "") -> str:
        cap = f"<caption>{caption}</caption>" if caption else ""
        id_attr = f' id="{table_id}"' if table_id else ""
        header = "<tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr>"
        data = "".join(
            f"<tr>{''.join(f'<td>{c}</td>' for c in row)}</tr>" for row in rows
        )
        return f"<table{id_attr}>{cap}<thead>{header}</thead><tbody>{data}</tbody></table>"

    def test_single_table_from_caption(self, tmp_path: Path) -> None:
        html = self._table([("user_id", "STRING", "REQUIRED", "PK")], caption="users")
        result = read_schema_file(self._html(tmp_path, html))
        assert isinstance(result, list)
        assert result[0].name == "users"
        assert result[0].fields[0].name == "user_id"

    def test_single_table_from_id_attr(self, tmp_path: Path) -> None:
        html = self._table([("col", "STRING", "NULLABLE", "")], table_id="orders")
        result = read_schema_file(self._html(tmp_path, html))
        assert result[0].name == "orders"

    def test_table_name_from_preceding_heading(self, tmp_path: Path) -> None:
        body = "<h2>payments</h2>" + self._table([("amount", "NUMERIC", "REQUIRED", "Total")])
        result = read_schema_file(self._html(tmp_path, body))
        assert result[0].name == "payments"

    def test_table_prefix_stripped_from_heading(self, tmp_path: Path) -> None:
        body = "<h3>Table: transactions</h3>" + self._table([("tx_id", "STRING", "REQUIRED", "")])
        result = read_schema_file(self._html(tmp_path, body))
        assert result[0].name == "transactions"

    def test_table_name_from_argument_when_no_metadata(self, tmp_path: Path) -> None:
        html = self._table([("id", "STRING", "REQUIRED", "")])
        result = read_schema_file(self._html(tmp_path, html), table_name="events")
        assert result[0].name == "events"

    def test_auto_name_when_no_metadata_and_no_arg(self, tmp_path: Path) -> None:
        html = self._table([("id", "STRING", "REQUIRED", "")])
        result = read_schema_file(self._html(tmp_path, html))
        assert result[0].name == "table_1"

    def test_field_count_correct(self, tmp_path: Path) -> None:
        rows = [("a", "STRING", "REQUIRED", ""), ("b", "INTEGER", "NULLABLE", ""), ("c", "BOOLEAN", "NULLABLE", "")]
        html = self._table(rows, caption="t")
        result = read_schema_file(self._html(tmp_path, html))
        assert len(result[0].fields) == 3

    def test_type_normalisation(self, tmp_path: Path) -> None:
        html = self._table([("x", "INT64", "NULLABLE", "")], caption="t")
        result = read_schema_file(self._html(tmp_path, html))
        assert result[0].fields[0].type == "INTEGER"

    def test_invalid_mode_defaults_to_nullable(self, tmp_path: Path) -> None:
        html = self._table([("x", "STRING", "OOPS", "")], caption="t")
        result = read_schema_file(self._html(tmp_path, html))
        assert result[0].fields[0].mode == "NULLABLE"

    def test_no_name_type_columns_falls_back_to_text(self, tmp_path: Path) -> None:
        body = "<table><tr><td>foo</td><td>bar</td></tr></table><p>Some schema info here</p>"
        result = read_schema_file(self._html(tmp_path, body))
        assert isinstance(result, str)

    def test_htm_extension_supported(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.htm"
        html = self._table([("id", "STRING", "REQUIRED", "")], caption="t")
        f.write_text(f"<html><body>{html}</body></html>")
        result = read_schema_file(f)
        assert isinstance(result, list)


class TestReadHtmlMultiTable:
    def _multi_html(self, tmp_path: Path) -> Path:
        f = tmp_path / "multi.html"
        f.write_text("""
        <html><body>
          <h2>campaigns</h2>
          <table>
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody>
              <tr><td>campaign_id</td><td>STRING</td><td>REQUIRED</td><td>Unique ID</td></tr>
              <tr><td>budget</td><td>NUMERIC</td><td>NULLABLE</td><td>Budget USD</td></tr>
            </tbody>
          </table>
          <h2>Table: ad_impressions</h2>
          <table>
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody>
              <tr><td>impression_id</td><td>STRING</td><td>REQUIRED</td><td>Unique impression ID</td></tr>
              <tr><td>campaign_id</td><td>STRING</td><td>REQUIRED</td><td>FK to campaigns</td></tr>
              <tr><td>cost_usd</td><td>NUMERIC</td><td>NULLABLE</td><td>Cost per impression</td></tr>
            </tbody>
          </table>
          <table id="conversions">
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody>
              <tr><td>conversion_id</td><td>STRING</td><td>REQUIRED</td><td>Unique ID</td></tr>
            </tbody>
          </table>
        </body></html>
        """)
        return f

    def test_all_tables_parsed(self, tmp_path: Path) -> None:
        result = read_schema_file(self._multi_html(tmp_path))
        assert isinstance(result, list)
        assert len(result) == 3

    def test_table_names_resolved(self, tmp_path: Path) -> None:
        result = read_schema_file(self._multi_html(tmp_path))
        names = {t.name for t in result}
        assert names == {"campaigns", "ad_impressions", "conversions"}

    def test_field_counts(self, tmp_path: Path) -> None:
        result = read_schema_file(self._multi_html(tmp_path))
        counts = {t.name: len(t.fields) for t in result}
        assert counts == {"campaigns": 2, "ad_impressions": 3, "conversions": 1}


# ──────────────────────────────────────────────────────────────────────────────
# Full workflow — HTML structured (no LLM)
# ──────────────────────────────────────────────────────────────────────────────

class TestWorkflowHtml:
    def test_structured_html_no_llm_calls(self, tmp_path: Path) -> None:
        f = tmp_path / "schema.html"
        f.write_text("""<html><body>
          <h2>payments</h2>
          <table>
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody>
              <tr><td>payment_id</td><td>STRING</td><td>REQUIRED</td><td>PK</td></tr>
              <tr><td>amount</td><td>NUMERIC</td><td>NULLABLE</td><td>Amount</td></tr>
            </tbody>
          </table>
        </body></html>""")
        fake = FakeLLM()
        results = run_bq_terraform_workflow(
            input_path=f, dataset_id="finance", project_id="proj",
            llm=fake, output_dir=tmp_path / "tf",
        )
        assert results[0].succeeded
        assert results[0].table_name == "payments"
        assert len(results[0].schema_fields) == 2
        assert len(fake.calls) == 0  # no LLM for structured HTML

    def test_multi_table_html_produces_multiple_tf_files(self, tmp_path: Path) -> None:
        f = tmp_path / "multi.html"
        f.write_text("""<html><body>
          <table id="orders">
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody><tr><td>order_id</td><td>STRING</td><td>REQUIRED</td><td>PK</td></tr></tbody>
          </table>
          <table id="customers">
            <thead><tr><th>name</th><th>type</th><th>mode</th><th>description</th></tr></thead>
            <tbody><tr><td>customer_id</td><td>STRING</td><td>REQUIRED</td><td>PK</td></tr></tbody>
          </table>
        </body></html>""")
        out_dir = tmp_path / "tf"
        run_bq_terraform_workflow(
            input_path=f, dataset_id="sales", project_id="proj",
            llm=FakeLLM(), output_dir=out_dir,
        )
        tf_files = {p.name for p in out_dir.iterdir() if p.suffix == ".tf"}
        assert tf_files == {"orders.tf", "customers.tf"}

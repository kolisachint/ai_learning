"""file_reader — parse one or more BQ table schemas from CSV, JSON, or PDF.

Always returns ``list[TableSchema]`` for structured files so callers handle
both single-table and multi-table inputs uniformly. PDF returns raw ``str``
since schema extraction requires LLM assistance.

Multi-table detection rules
───────────────────────────
JSON
  • Array of objects each having a ``table`` or ``name`` key alongside
    ``fields`` or ``schema`` → multi-table
  • Dict mapping table-name → field-array  → multi-table
  • Bare array of field objects (``{name, type, …}``)  → single-table
    (requires ``table_name`` argument)

CSV
  • Has a ``table_name`` or ``table`` column → multi-table (rows grouped
    by that column; remaining columns are field attributes)
  • No such column → single-table (requires ``table_name`` argument)

PDF
  • Returns raw extracted text as ``str``; caller passes to LLM.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Union

from ..state.bq import SchemaField, TableSchema

# read_schema_file return type
ParsedInput = Union[list[TableSchema], str]

_VALID_TYPES = {
    "STRING", "INTEGER", "INT64", "FLOAT", "FLOAT64", "BOOLEAN", "BOOL",
    "RECORD", "STRUCT", "TIMESTAMP", "DATE", "TIME", "DATETIME",
    "NUMERIC", "BIGNUMERIC", "BYTES", "JSON", "GEOGRAPHY", "INTERVAL",
}
_VALID_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}
_TABLE_COL_NAMES = {"table_name", "table"}


def read_schema_file(
    path: str | Path,
    table_name: str | None = None,
) -> ParsedInput:
    """Read a BQ schema file.

    Args:
        path:       Path to a ``.csv``, ``.json``, or ``.pdf`` file.
        table_name: Required when the file contains no table-name information
                    (single-table bare CSV/JSON). Ignored for multi-table files
                    and PDFs.

    Returns:
        ``list[TableSchema]``  for CSV/JSON (one element per table found).
        ``str``                for PDF (raw text for LLM extraction).

    Raises:
        FileNotFoundError: File does not exist.
        ValueError:        Unsupported format, missing columns, or missing
                           ``table_name`` for bare single-table files.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        return _read_json(path, table_name)
    if suffix == ".csv":
        return _read_csv(path, table_name)
    if suffix == ".pdf":
        return _read_pdf(path)
    raise ValueError(f"Unsupported file type '{suffix}'. Expected .csv, .json, or .pdf.")


# ──────────────────────────────────────────────────────────────────────────────
# JSON reader
# ──────────────────────────────────────────────────────────────────────────────

def _read_json(path: Path, table_name: str | None) -> list[TableSchema]:
    raw = json.loads(path.read_text(encoding="utf-8"))

    # ── dict: {"orders": [...], "users": [...]} ───────────────────────────
    if isinstance(raw, dict):
        tables = []
        for tname, fields_raw in raw.items():
            if not isinstance(fields_raw, list):
                raise ValueError(
                    f"In multi-table JSON dict, value for '{tname}' must be a "
                    f"field array; got {type(fields_raw).__name__}."
                )
            tables.append(TableSchema(
                name=tname,
                fields=[_normalise_field(f) for f in fields_raw],
            ))
        return tables

    if not isinstance(raw, list) or not raw:
        raise ValueError("JSON must be a non-empty array or a table-name → fields dict.")

    first = raw[0]

    # ── array of table objects: [{table/name, fields/schema}, ...] ────────
    # Detect by: has "table" key, OR has "fields"/"schema" key (schema container),
    # OR has "name" without "type" (table-name object, not a field row).
    _is_table_object = (
        "table" in first
        or "fields" in first
        or "schema" in first
        or ("name" in first and "type" not in first)
    )
    if isinstance(first, dict) and _is_table_object:
        tables = []
        for obj in raw:
            tname = obj.get("table") or obj.get("name") or ""
            if not tname:
                raise ValueError(
                    f"Multi-table JSON element is missing a 'table' key: {obj}"
                )
            fields_raw = obj.get("fields") or obj.get("schema") or []
            tables.append(TableSchema(
                name=tname,
                fields=[_normalise_field(f) for f in fields_raw],
            ))
        return tables

    # ── bare field array: [{name, type, ...}, ...] — single-table ─────────
    if not table_name:
        raise ValueError(
            "JSON contains a bare field array (single-table format). "
            "Provide --table <name> so the table can be named."
        )
    return [TableSchema(
        name=table_name,
        fields=[_normalise_field(f) for f in raw],
    )]


# ──────────────────────────────────────────────────────────────────────────────
# CSV reader
# ──────────────────────────────────────────────────────────────────────────────

def _read_csv(path: Path, table_name: str | None) -> list[TableSchema]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("CSV file is empty or has no header row.")

        lower_headers = {h.lower(): h for h in reader.fieldnames}
        table_col = next((lower_headers[k] for k in _TABLE_COL_NAMES if k in lower_headers), None)

        if table_col:
            # ── multi-table: group by table_col ───────────────────────────
            buckets: dict[str, list[SchemaField]] = defaultdict(list)
            order: list[str] = []
            for row in reader:
                tname = (row.get(table_col) or "").strip()
                if not tname:
                    continue
                if tname not in buckets:
                    order.append(tname)
                row_lower = {k.lower(): v for k, v in row.items()}
                buckets[tname].append(_normalise_field({
                    "name":        row_lower.get("name", ""),
                    "type":        row_lower.get("type", "STRING"),
                    "mode":        row_lower.get("mode", "NULLABLE"),
                    "description": row_lower.get("description", ""),
                }))
            return [TableSchema(name=t, fields=buckets[t]) for t in order]

        # ── single-table: no table column ─────────────────────────────────
        required_cols = {"name", "type"}
        missing = required_cols - lower_headers.keys()
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {', '.join(sorted(missing))}. "
                f"Found: {', '.join(reader.fieldnames)}"
            )
        if not table_name:
            raise ValueError(
                "CSV has no 'table_name' column (single-table format). "
                "Provide --table <name> so the table can be named."
            )
        fields = []
        for row in reader:
            row_lower = {k.lower(): v for k, v in row.items()}
            fields.append(_normalise_field({
                "name":        row_lower.get("name", ""),
                "type":        row_lower.get("type", "STRING"),
                "mode":        row_lower.get("mode", "NULLABLE"),
                "description": row_lower.get("description", ""),
            }))
        return [TableSchema(name=table_name, fields=fields)]


# ──────────────────────────────────────────────────────────────────────────────
# PDF reader
# ──────────────────────────────────────────────────────────────────────────────

def _read_pdf(path: Path) -> str:
    """Extract text from a PDF; returned as raw string for LLM processing."""
    try:
        import pdfplumber  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF support. Install: pip install pdfplumber"
        )

    pages: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)

    if not pages:
        raise ValueError(f"No extractable text found in PDF: {path}")
    return "\n\n".join(pages)


# ──────────────────────────────────────────────────────────────────────────────
# Field normalisation
# ──────────────────────────────────────────────────────────────────────────────

def _normalise_field(field: dict) -> SchemaField:
    name = str(field.get("name") or "").strip()
    if not name:
        raise ValueError(f"Schema field is missing a 'name': {field}")

    raw_type = str(field.get("type") or "STRING").strip().upper()
    _aliases = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN", "STRUCT": "RECORD"}
    field_type = _aliases.get(raw_type, raw_type)

    raw_mode = str(field.get("mode") or "NULLABLE").strip().upper()
    mode = raw_mode if raw_mode in _VALID_MODES else "NULLABLE"

    return SchemaField(
        name=name,
        type=field_type,
        mode=mode,
        description=str(field.get("description") or "").strip(),
    )

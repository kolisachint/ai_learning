"""file_reader — parse one or more BQ table schemas from CSV, JSON, PDF, or HTML.

Always returns ``list[TableSchema]`` for structured files so callers handle
both single-table and multi-table inputs uniformly.  Unstructured files (PDF,
and HTML with no recognisable schema tables) return raw ``str`` for LLM
extraction.

Multi-table detection rules
───────────────────────────
JSON
  • Array of objects each having a ``table`` or ``name`` key alongside
    ``fields`` or ``schema`` → multi-table
  • Dict mapping table-name → field-array → multi-table
  • Bare array of field objects (``{name, type, …}``) → single-table
    (requires ``table_name`` argument)

CSV
  • Has a ``table_name`` or ``table`` column → multi-table (rows grouped
    by that column; remaining columns are field attributes)
  • No such column → single-table (requires ``table_name`` argument)

HTML  (.html / .htm)
  • ``<table>`` elements whose headers include ``name`` and ``type`` columns
    → parsed deterministically; table name resolved from ``<caption>``,
    ``id``/``data-table`` attribute, or the nearest preceding heading
  • No such tables found → full text extracted for LLM (like PDF)

PDF
  • Returns raw extracted text as ``str``; caller passes to LLM.
"""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Union

from ..state.bq import SchemaField, TableSchema

ParsedInput = Union[list[TableSchema], str]

_VALID_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}
_TABLE_COL_NAMES = {"table_name", "table"}
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN", "STRUCT": "RECORD"}


def read_schema_file(
    path: str | Path,
    table_name: str | None = None,
) -> ParsedInput:
    """Read a BQ schema file.

    Args:
        path:       Path to a ``.csv``, ``.json``, ``.html``, ``.htm``, or
                    ``.pdf`` file.
        table_name: Required when the file contains no table-name information
                    (single-table bare CSV/JSON). Ignored for multi-table files,
                    PDFs, and HTML files with embedded table names.

    Returns:
        ``list[TableSchema]``  for CSV/JSON and structured HTML.
        ``str``                for PDF and unstructured HTML (raw text for LLM).

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
    if suffix in (".html", ".htm"):
        return _read_html(path, table_name)
    if suffix == ".pdf":
        return _read_pdf(path)
    raise ValueError(
        f"Unsupported file type '{suffix}'. Expected .csv, .json, .html, .htm, or .pdf."
    )


# ──────────────────────────────────────────────────────────────────────────────
# JSON
# ──────────────────────────────────────────────────────────────────────────────

def _read_json(path: Path, table_name: str | None) -> list[TableSchema]:
    raw = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(raw, dict):
        tables = []
        for tname, fields_raw in raw.items():
            if not isinstance(fields_raw, list):
                raise ValueError(
                    f"In multi-table JSON dict, value for '{tname}' must be a "
                    f"field array; got {type(fields_raw).__name__}."
                )
            tables.append(TableSchema(name=tname, fields=[_normalise_field(f) for f in fields_raw]))
        return tables

    if not isinstance(raw, list) or not raw:
        raise ValueError("JSON must be a non-empty array or a table-name → fields dict.")

    first = raw[0]
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
                raise ValueError(f"Multi-table JSON element is missing a 'table' key: {obj}")
            fields_raw = obj.get("fields") or obj.get("schema") or []
            tables.append(TableSchema(name=tname, fields=[_normalise_field(f) for f in fields_raw]))
        return tables

    if not table_name:
        raise ValueError(
            "JSON contains a bare field array (single-table format). "
            "Provide --table <name> so the table can be named."
        )
    return [TableSchema(name=table_name, fields=[_normalise_field(f) for f in raw])]


# ──────────────────────────────────────────────────────────────────────────────
# CSV
# ──────────────────────────────────────────────────────────────────────────────

def _read_csv(path: Path, table_name: str | None) -> list[TableSchema]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("CSV file is empty or has no header row.")

        lower_headers = {h.lower(): h for h in reader.fieldnames}
        table_col = next((lower_headers[k] for k in _TABLE_COL_NAMES if k in lower_headers), None)

        if table_col:
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
                    "name": row_lower.get("name", ""),
                    "type": row_lower.get("type", "STRING"),
                    "mode": row_lower.get("mode", "NULLABLE"),
                    "description": row_lower.get("description", ""),
                }))
            return [TableSchema(name=t, fields=buckets[t]) for t in order]

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
                "name": row_lower.get("name", ""),
                "type": row_lower.get("type", "STRING"),
                "mode": row_lower.get("mode", "NULLABLE"),
                "description": row_lower.get("description", ""),
            }))
        return [TableSchema(name=table_name, fields=fields)]


# ──────────────────────────────────────────────────────────────────────────────
# HTML
# ──────────────────────────────────────────────────────────────────────────────

def _read_html(path: Path, table_name: str | None) -> ParsedInput:
    """Parse BQ schema from an HTML file.

    Strategy
    ────────
    1. Find every ``<table>`` whose header row contains at least ``name`` and
       ``type`` columns.  Parse those rows deterministically — no LLM needed.
       Table name resolved from (priority order):
         a. ``<caption>`` element inside the table
         b. ``id`` or ``data-table`` attribute on ``<table>``
         c. Nearest preceding ``<h1>``–``<h5>`` sibling (strips common
            prefixes such as "Table:" or "Schema:")
         d. ``table_name`` argument (single structured table only)
         e. Auto-generated ``table_1``, ``table_2``, …

    2. If no schema tables are found, extract all visible text and return it
       as a ``str`` for LLM-assisted extraction (same path as PDF).

    Requires ``beautifulsoup4``: ``pip install beautifulsoup4``
    """
    try:
        from bs4 import BeautifulSoup, Tag  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "beautifulsoup4 is required for HTML support. "
            "Install it: pip install beautifulsoup4"
        )

    html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    schema_tables = _parse_html_schema_tables(soup, table_name)
    if schema_tables:
        return schema_tables

    # No structured schema tables found — extract text for LLM
    text = soup.get_text(separator="\n", strip=True)
    if not text.strip():
        raise ValueError(f"No extractable content found in HTML file: {path}")
    return text


def _parse_html_schema_tables(soup, override_name: str | None) -> list[TableSchema]:
    """Find and deterministically parse HTML <table> elements with schema columns."""
    _REQUIRED_HEADERS = {"name", "type"}
    results: list[TableSchema] = []

    for table_el in soup.find_all("table"):
        # Locate the header row (first <tr> containing <th> or the first <tr>)
        header_cells = table_el.find_all("th")
        if not header_cells:
            first_row = table_el.find("tr")
            header_cells = first_row.find_all("td") if first_row else []

        headers = [c.get_text(strip=True).lower() for c in header_cells]
        if not _REQUIRED_HEADERS.issubset(set(headers)):
            continue  # not a schema table

        # Parse data rows (skip the header row itself)
        all_rows = table_el.find_all("tr")
        data_rows = all_rows[1:] if header_cells and all_rows else all_rows

        fields: list[SchemaField] = []
        for row in data_rows:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            row_dict = {header: (cells[i] if i < len(cells) else "") for i, header in enumerate(headers)}
            if row_dict.get("name"):
                try:
                    fields.append(_normalise_field(row_dict))
                except ValueError:
                    pass  # skip malformed rows

        if not fields:
            continue

        # Resolve table name
        tname = (
            _html_caption(table_el)
            or _html_attr_name(table_el)
            or _html_heading_before(table_el)
            or (override_name if len(results) == 0 else None)
            or f"table_{len(results) + 1}"
        )
        results.append(TableSchema(name=tname, fields=fields))

    return results


def _html_caption(table_el) -> str:
    caption = table_el.find("caption")
    return _slug(caption.get_text(strip=True)) if caption else ""


def _html_attr_name(table_el) -> str:
    for attr in ("data-table", "data-name", "id"):
        val = (table_el.get(attr) or "").strip()
        if val:
            return _slug(val)
    return ""


def _html_heading_before(table_el) -> str:
    """Walk backwards through siblings to find the nearest heading element."""
    _HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5"}
    _STRIP_PREFIXES = ("table:", "schema:", "table -", "schema -", "table—", "schema—")

    for sibling in table_el.previous_siblings:
        tag = getattr(sibling, "name", None)
        if tag in _HEADING_TAGS:
            text = sibling.get_text(strip=True)
            for prefix in _STRIP_PREFIXES:
                if text.lower().startswith(prefix):
                    text = text[len(prefix):].strip()
                    break
            return _slug(text)
    return ""


def _slug(text: str) -> str:
    """Convert arbitrary text to a snake_case table name."""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


# ──────────────────────────────────────────────────────────────────────────────
# PDF
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
    field_type = _TYPE_ALIASES.get(raw_type, raw_type)

    raw_mode = str(field.get("mode") or "NULLABLE").strip().upper()
    mode = raw_mode if raw_mode in _VALID_MODES else "NULLABLE"

    return SchemaField(
        name=name,
        type=field_type,
        mode=mode,
        description=str(field.get("description") or "").strip(),
    )

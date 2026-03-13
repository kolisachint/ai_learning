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
# HTML — column-name synonyms for flexible header matching
# ──────────────────────────────────────────────────────────────────────────────

# Canonical key → set of accepted header strings (lowercased, spaces OK)
_HTML_COL_SYNONYMS: dict[str, set[str]] = {
    "name": {
        "name", "field", "field name", "fieldname", "column", "col",
        "column name", "columnname", "attribute", "property",
        "param", "parameter", "key", "field_name", "column_name",
    },
    "type": {
        "type", "data type", "datatype", "data_type", "field type",
        "fieldtype", "field_type", "column type", "columntype",
        "column_type", "dtype", "bq type", "bq_type",
        "bigquery type", "bigquery_type",
    },
    "mode": {
        "mode", "nullable", "null", "nullability", "is nullable",
        "is_nullable", "required", "is required", "is_required",
        "constraint", "optional", "cardinality",
    },
    "description": {
        "description", "desc", "comment", "remarks", "notes",
        "detail", "details", "info", "information",
        "meaning", "definition", "purpose", "summary",
    },
}

# HTML tags that are pure noise and should be stripped before any parsing
_HTML_NOISE_TAGS = {
    "script", "style", "nav", "footer", "header", "head",
    "noscript", "iframe", "aside", "form", "button",
}

# Tags that strongly suggest a layout / structural table rather than data
_LAYOUT_ROLES = {"presentation", "none"}


def _read_html(path: Path, table_name: str | None) -> ParsedInput:
    """Parse BQ schema from an HTML file.

    Strategy
    ────────
    1. Strip all noise tags (scripts, styles, nav, …).
    2. Find every ``<table>`` with at least 2 columns.  Skip layout tables
       (role="presentation", single-column, <2 data rows).
    3. Map each table's header row to canonical column names using a broad
       synonym dictionary — handles "Field Name", "Column", "Data Type",
       "Remarks", etc. without requiring exact strings.
    4. If at least a ``name``-like and ``type``-like column are found,
       parse that table deterministically — no LLM required.
       Table name is resolved from ``<caption>`` → ``id``/``data-table``
       attr → nearest preceding heading → ``--table`` arg → auto-index.
    5. If no schema tables are found, convert the remaining HTML to a
       compact structured text (headings + pipe-separated table rows) and
       return that string for LLM-assisted extraction.

    Requires ``beautifulsoup4``: ``pip install beautifulsoup4``
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "beautifulsoup4 is required for HTML support. "
            "Install it: pip install beautifulsoup4"
        )

    html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    # Strip noise before any processing
    for tag in soup.find_all(_HTML_NOISE_TAGS):
        tag.decompose()

    schema_tables = _parse_html_schema_tables(soup, table_name)
    if schema_tables:
        return schema_tables

    # ── LLM fallback: convert to clean structured text ────────────────────────
    # Send headings + table rows (pipe-separated) — far cleaner than get_text()
    text = _html_to_structured_text(soup)
    if not text.strip():
        raise ValueError(f"No extractable content found in HTML file: {path}")
    return text


# ──────────────────────────────────────────────────────────────────────────────
# Structured HTML table parsing
# ──────────────────────────────────────────────────────────────────────────────

def _parse_html_schema_tables(soup, override_name: str | None) -> list[TableSchema]:
    """Scan every <table> and parse those that look like BQ schema definitions."""
    results: list[TableSchema] = []

    for table_el in soup.find_all("table"):
        if _is_layout_table(table_el):
            continue

        all_rows = table_el.find_all("tr")
        if len(all_rows) < 2:
            continue

        # Find the header row: prefer a row with <th> elements, else first row
        header_row_idx, col_map = _detect_schema_columns(all_rows)
        if header_row_idx is None or "name" not in col_map or "type" not in col_map:
            continue

        # Parse data rows (everything after the header row)
        fields: list[SchemaField] = []
        for row in all_rows[header_row_idx + 1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or all(c == "" for c in cells):
                continue

            field_dict = {
                canon: cells[idx] if idx < len(cells) else ""
                for canon, idx in col_map.items()
            }
            if field_dict.get("name"):
                try:
                    fields.append(_normalise_field(field_dict))
                except ValueError:
                    pass  # skip rows missing required values

        if not fields:
            continue

        tname = (
            _html_caption(table_el)
            or _html_attr_name(table_el)
            or _html_heading_before(table_el)
            or (override_name if len(results) == 0 else None)
            or f"table_{len(results) + 1}"
        )
        results.append(TableSchema(name=tname, fields=fields))

    return results


def _detect_schema_columns(rows) -> tuple[int | None, dict[str, int]]:
    """Return (header_row_index, {canonical_name: col_index}) or (None, {})."""
    # Check the first few rows for a header row
    for i, row in enumerate(rows[:4]):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        col_map = _map_headers(cells)
        if "name" in col_map and "type" in col_map:
            return i, col_map
    return None, {}


def _map_headers(header_cells) -> dict[str, int]:
    """Map each cell's text to a canonical column name using synonym sets."""
    col_map: dict[str, int] = {}
    for i, cell in enumerate(header_cells):
        raw = cell.get_text(strip=True).lower()
        # Normalise to space-separated for lookup
        normalised = re.sub(r"[\s_\-]+", " ", raw).strip()
        for canon, synonyms in _HTML_COL_SYNONYMS.items():
            if canon not in col_map and (raw in synonyms or normalised in synonyms):
                col_map[canon] = i
                break
    return col_map


def _is_layout_table(table_el) -> bool:
    """Return True for tables used as page layout rather than data containers."""
    role = (table_el.get("role") or "").lower()
    if role in _LAYOUT_ROLES:
        return True
    rows = table_el.find_all("tr")
    if not rows:
        return True
    max_cols = max((len(r.find_all(["td", "th"])) for r in rows), default=0)
    if max_cols < 2:
        return True
    # Tables with only one data row (header + 0 or 1 rows) are not schema tables
    if len(rows) < 2:
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Table name helpers
# ──────────────────────────────────────────────────────────────────────────────

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
    _HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
    _STRIP_PREFIXES = (
        "table:", "schema:", "table -", "schema -",
        "table—", "schema—", "entity:", "object:",
    )
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
    """Convert arbitrary text to a snake_case identifier."""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


# ──────────────────────────────────────────────────────────────────────────────
# LLM fallback: convert remaining HTML to structured text
# ──────────────────────────────────────────────────────────────────────────────

def _html_to_structured_text(soup) -> str:
    """Convert HTML to compact structured text for LLM extraction.

    Produces headings and pipe-separated table rows — far cleaner than
    raw ``get_text()`` output, giving the LLM recognisable structure.
    """
    _HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
    parts: list[str] = []
    visited_tables: set[int] = set()

    body = soup.find("body") or soup
    for element in body.descendants:
        tag = getattr(element, "name", None)
        if tag is None:
            continue

        if tag in _HEADING_TAGS:
            text = element.get_text(strip=True)
            if text:
                parts.append(f"\n### {text}")

        elif tag == "table" and id(element) not in visited_tables:
            visited_tables.add(id(element))
            table_text = _table_to_pipe_text(element)
            if table_text:
                parts.append(table_text)

    raw = "\n".join(parts)
    # Collapse excessive blank lines
    return re.sub(r"\n{3,}", "\n\n", raw).strip()


def _table_to_pipe_text(table_el) -> str:
    """Render an HTML table as pipe-separated rows for LLM readability."""
    lines: list[str] = []
    for row in table_el.find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if any(cells):
            lines.append(" | ".join(cells))
    return "\n".join(lines)


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

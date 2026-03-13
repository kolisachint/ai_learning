"""file_reader — parse BQ schema from CSV, JSON, or PDF.

Returns a list of canonical schema dicts for CSV/JSON, or raw extracted
text for PDF (which is then passed to the schema extractor agent).

Canonical schema dict keys:
  name        (str)  field name
  type        (str)  BQ type: STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, …
  mode        (str)  NULLABLE | REQUIRED | REPEATED  (default: NULLABLE)
  description (str)  human-readable description       (default: "")
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Union

SchemaField = dict[str, str]
SchemaOrText = Union[list[SchemaField], str]

_VALID_TYPES = {
    "STRING", "INTEGER", "INT64", "FLOAT", "FLOAT64", "BOOLEAN", "BOOL",
    "RECORD", "STRUCT", "TIMESTAMP", "DATE", "TIME", "DATETIME",
    "NUMERIC", "BIGNUMERIC", "BYTES", "JSON", "GEOGRAPHY", "INTERVAL",
}
_VALID_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}


def read_schema_file(path: str | Path) -> SchemaOrText:
    """Read a BQ schema file and return structured fields or raw PDF text.

    Args:
        path: Path to a .csv, .json, or .pdf file.

    Returns:
        list[SchemaField] for CSV/JSON, or str (raw text) for PDF.

    Raises:
        ValueError: Unsupported file extension.
        FileNotFoundError: File does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        return _read_json(path)
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".pdf":
        return _read_pdf(path)
    raise ValueError(f"Unsupported file type '{suffix}'. Expected .csv, .json, or .pdf.")


# ------------------------------------------------------------------
# Format-specific readers
# ------------------------------------------------------------------

def _read_json(path: Path) -> list[SchemaField]:
    """Parse a BQ schema JSON file.

    Accepts both the native BQ schema format (array of field objects) and
    a dict with a top-level ``schema`` or ``fields`` key.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(raw, list):
        fields = raw
    elif isinstance(raw, dict):
        fields = raw.get("schema") or raw.get("fields") or []
        if not fields:
            raise ValueError(
                "JSON file must be an array of field objects, or a dict with "
                "a 'schema' or 'fields' key."
            )
    else:
        raise ValueError("JSON file must contain an array or object at the top level.")

    return [_normalise(f) for f in fields]


def _read_csv(path: Path) -> list[SchemaField]:
    """Parse a CSV file where each row is one schema field.

    Expected columns (case-insensitive): name, type, mode, description.
    Only ``name`` and ``type`` are required; others default gracefully.
    """
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("CSV file is empty or has no header row.")

        # Normalise header names to lowercase for flexible matching
        lower_headers = {h.lower(): h for h in reader.fieldnames}
        required = {"name", "type"}
        missing = required - lower_headers.keys()
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {', '.join(sorted(missing))}. "
                f"Found: {', '.join(reader.fieldnames)}"
            )

        fields: list[SchemaField] = []
        for i, row in enumerate(reader, start=2):
            # Re-key using the original header names
            normalised_row = {k.lower(): v for k, v in row.items()}
            fields.append(
                _normalise(
                    {
                        "name": normalised_row.get("name", ""),
                        "type": normalised_row.get("type", "STRING"),
                        "mode": normalised_row.get("mode", "NULLABLE"),
                        "description": normalised_row.get("description", ""),
                    }
                )
            )
        return fields


def _read_pdf(path: Path) -> str:
    """Extract text from a PDF file using pdfplumber.

    The raw text is returned as-is and should be passed to the
    schema_extractor agent for structured parsing.

    Raises:
        ImportError: If pdfplumber is not installed.
    """
    try:
        import pdfplumber  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF support. "
            "Install it: pip install pdfplumber"
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


# ------------------------------------------------------------------
# Normalisation
# ------------------------------------------------------------------

def _normalise(field: dict) -> SchemaField:
    """Coerce a raw field dict into canonical form with validated values."""
    name = str(field.get("name") or "").strip()
    if not name:
        raise ValueError(f"Schema field is missing a 'name': {field}")

    raw_type = str(field.get("type") or "STRING").strip().upper()
    # Map BQ aliases to canonical names
    _type_aliases = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN", "STRUCT": "RECORD"}
    field_type = _type_aliases.get(raw_type, raw_type)
    if field_type not in _VALID_TYPES:
        # Accept unknown types without crashing — BQ may add new ones
        pass

    raw_mode = str(field.get("mode") or "NULLABLE").strip().upper()
    mode = raw_mode if raw_mode in _VALID_MODES else "NULLABLE"

    description = str(field.get("description") or "").strip()

    return {"name": name, "type": field_type, "mode": mode, "description": description}

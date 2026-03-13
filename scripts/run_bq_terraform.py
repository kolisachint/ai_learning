#!/usr/bin/env python3
"""CLI — convert BQ schema file(s) to Terraform HCL using Ollama.

Supports single-table and multi-table inputs in CSV, JSON, or PDF format.

Usage examples
──────────────

  # Multi-table JSON (no --table needed):
  python scripts/run_bq_terraform.py data/raw/schema_multi.json \\
      --dataset sales --project my-gcp-project

  # Single-table bare JSON (--table required):
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table users --dataset core --project my-gcp-project

  # Multi-table CSV:
  python scripts/run_bq_terraform.py data/raw/schema_multi.csv \\
      --dataset sales --project my-gcp-project

  # PDF (one or many tables, Ollama required for extraction):
  python scripts/run_bq_terraform.py data/raw/schema_multi.pdf \\
      --dataset analytics --project my-gcp-project

  # Filter multi-table file to one table only:
  python scripts/run_bq_terraform.py data/raw/schema_multi.json \\
      --table orders --dataset sales --project my-gcp-project

  # Print HCL to stdout (single-table result only):
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table users --dataset core --project my-gcp-project --stdout

  # Use Ollama for HCL generation (richer output):
  python scripts/run_bq_terraform.py data/raw/schema_multi.json \\
      --dataset sales --project my-gcp-project --llm-hcl

  # Health check:
  python scripts/run_bq_terraform.py any.csv --dataset d --project p --check-ollama
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from dotenv import load_dotenv

from ai_agents.integrations.ollama import OllamaLLMClient
from ai_agents.workflows.bq_terraform_workflow import run_bq_terraform_workflow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert BQ schema (CSV/JSON/PDF) to Terraform HCL via Ollama.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input", help="Path to schema file (.csv, .json, or .pdf).")
    parser.add_argument(
        "--table", default=None, metavar="NAME",
        help=(
            "Table name. Required for bare single-table CSV/JSON (no embedded table name). "
            "For multi-table files, acts as a filter (omit to process all tables)."
        ),
    )
    parser.add_argument("--dataset", required=True, metavar="ID", help="BigQuery dataset ID.")
    parser.add_argument(
        "--project", default=None, metavar="ID",
        help="GCP project ID. Falls back to GOOGLE_CLOUD_PROJECT env var.",
    )
    parser.add_argument(
        "--output-dir", default="data/processed/terraform", metavar="DIR",
        help="Directory for generated .tf files (default: data/processed/terraform).",
    )
    parser.add_argument(
        "--model", default=None, metavar="TAG",
        help="Ollama model tag (default: deepseek-coder:6.7b or OLLAMA_MODEL env var).",
    )
    parser.add_argument(
        "--ollama-host", default=None, metavar="URL",
        help="Ollama server URL (default: http://localhost:11434 or OLLAMA_HOST env var).",
    )
    parser.add_argument(
        "--timeout", type=int, default=300, metavar="SECS",
        help="Ollama HTTP request timeout in seconds (default: 300).",
    )
    parser.add_argument(
        "--num-ctx", type=int, default=2048, metavar="TOKENS",
        help="Ollama context window (default: 2048). Smaller = faster on CPU.",
    )
    parser.add_argument(
        "--llm-hcl", action="store_true",
        help="Use Ollama for HCL generation (richer, slower). Default: deterministic.",
    )
    parser.add_argument(
        "--stdout", action="store_true",
        help="Print generated HCL to stdout (only works when a single table is produced).",
    )
    parser.add_argument(
        "--check-ollama", action="store_true",
        help="Check Ollama connectivity and list models, then exit.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    llm = OllamaLLMClient(
        model=args.model,
        host=args.ollama_host,
        timeout=args.timeout,
        num_ctx=args.num_ctx,
    )

    # ── Health check ─────────────────────────────────────────────────────────
    if args.check_ollama:
        if llm.is_available():
            models = llm.list_models()
            print(f"Ollama is running at {llm._host}")
            print(f"Model in use : {llm.model}")
            print(f"Available    : {', '.join(models) or '(none pulled yet)'}")
            print("\nTo pull the recommended model:")
            print("  ollama pull deepseek-coder:6.7b")
        else:
            print(f"Ollama is NOT reachable at {llm._host}", file=sys.stderr)
            print("Start it with: ollama serve", file=sys.stderr)
            return 1
        return 0

    # ── Project ID ───────────────────────────────────────────────────────────
    project_id = args.project or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print("Error: --project is required (or set GOOGLE_CLOUD_PROJECT).", file=sys.stderr)
        return 1

    # ── PDF requires Ollama ───────────────────────────────────────────────────
    input_path = Path(args.input)
    if input_path.suffix.lower() == ".pdf" and not llm.is_available():
        print(
            f"Error: PDF input requires Ollama (schema extraction). "
            f"Cannot reach Ollama at {llm._host}.\nStart it with: ollama serve",
            file=sys.stderr,
        )
        return 1

    # ── Run workflow ──────────────────────────────────────────────────────────
    print(f"Reading schema from : {input_path}")
    print(f"Dataset             : {args.dataset}")
    print(f"Project             : {project_id}")
    print(f"Ollama model        : {llm.model}")
    print(f"HCL generation      : {'LLM (Ollama)' if args.llm_hcl else 'deterministic'}")
    if args.table:
        print(f"Table filter        : {args.table}")
    print()

    results = run_bq_terraform_workflow(
        input_path=input_path,
        dataset_id=args.dataset,
        project_id=project_id,
        llm=llm,
        table_name=args.table,
        output_dir=args.output_dir,
        use_llm_for_hcl=args.llm_hcl,
    )

    # ── Report results ────────────────────────────────────────────────────────
    if args.stdout:
        if len(results) > 1:
            print(
                f"Error: --stdout requires a single table; got {len(results)}. "
                "Use --table to filter.",
                file=sys.stderr,
            )
            return 1
        if results[0].error:
            print(f"Error: {results[0].error}", file=sys.stderr)
            return 1
        print(results[0].terraform_hcl)
        return 0

    failed = 0
    for state in results:
        if state.error:
            print(f"  [FAIL] {state.table_name}: {state.error}", file=sys.stderr)
            failed += 1
        else:
            fields_count = len(state.schema_fields)
            print(f"  [OK]   {state.table_name:30s} {fields_count:3d} fields → {state.output_path}")

    print()
    total = len(results)
    ok = total - failed
    print(f"Done: {ok}/{total} table(s) succeeded.")
    if failed:
        print(f"      {failed} table(s) failed — see errors above.", file=sys.stderr)

    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())

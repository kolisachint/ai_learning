#!/usr/bin/env python3
"""CLI — convert a BQ schema file to Terraform HCL using Ollama.

Usage examples:

  # From JSON schema file (deterministic HCL, no LLM needed):
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table orders --dataset sales --project my-gcp-project

  # From CSV:
  python scripts/run_bq_terraform.py data/raw/schema.csv \\
      --table users --dataset core --project my-gcp-project

  # From PDF (requires Ollama for schema extraction):
  python scripts/run_bq_terraform.py data/raw/schema.pdf \\
      --table events --dataset analytics --project my-gcp-project

  # Use Ollama for HCL generation too (richer output):
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table orders --dataset sales --project my-gcp-project \\
      --llm-hcl

  # Specify Ollama model and host:
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table orders --dataset sales --project my-gcp-project \\
      --model codellama:7b --ollama-host http://localhost:11434

  # Print to stdout instead of writing a file:
  python scripts/run_bq_terraform.py data/raw/schema.json \\
      --table orders --dataset sales --project my-gcp-project \\
      --stdout
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Ensure src/ is on the path when run as a script
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from dotenv import load_dotenv

from ai_agents.integrations.ollama import OllamaLLMClient
from ai_agents.workflows.bq_terraform_workflow import run_bq_terraform_workflow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a BQ schema (CSV/JSON/PDF) to Terraform HCL via Ollama.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "input",
        help="Path to schema file (.csv, .json, or .pdf).",
    )
    parser.add_argument(
        "--table",
        required=True,
        metavar="NAME",
        help="BigQuery table name.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        metavar="ID",
        help="BigQuery dataset ID.",
    )
    parser.add_argument(
        "--project",
        default=None,
        metavar="ID",
        help="GCP project ID. Falls back to GOOGLE_CLOUD_PROJECT env var.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed/terraform",
        metavar="DIR",
        help="Directory to write the .tf file (default: data/processed/terraform).",
    )
    parser.add_argument(
        "--model",
        default=None,
        metavar="TAG",
        help="Ollama model tag (default: deepseek-coder:6.7b or OLLAMA_MODEL env var).",
    )
    parser.add_argument(
        "--ollama-host",
        default=None,
        metavar="URL",
        help="Ollama server URL (default: http://localhost:11434 or OLLAMA_HOST env var).",
    )
    parser.add_argument(
        "--llm-hcl",
        action="store_true",
        help="Use Ollama to generate HCL (slower but richer). "
             "Default: deterministic generation (fast, no LLM needed for CSV/JSON).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        metavar="SECS",
        help="Ollama HTTP request timeout in seconds (default: 300).",
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=2048,
        metavar="TOKENS",
        help="Ollama context window size (default: 2048). "
             "Smaller = faster on CPU. Increase if schema is very large.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print generated HCL to stdout instead of writing a file.",
    )
    parser.add_argument(
        "--check-ollama",
        action="store_true",
        help="Check if Ollama is reachable and list available models, then exit.",
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

    # ── Optional health check ────────────────────────────────────────────────
    if args.check_ollama:
        if llm.is_available():
            models = llm.list_models()
            print(f"Ollama is running at {llm._host}")
            print(f"Model in use : {llm.model}")
            print(f"Available    : {', '.join(models) if models else '(none pulled yet)'}")
            print("\nTo pull the recommended model:")
            print("  ollama pull deepseek-coder:6.7b")
        else:
            print(f"Ollama is NOT reachable at {llm._host}", file=sys.stderr)
            print("Start it with: ollama serve", file=sys.stderr)
            return 1
        return 0

    # ── Project ID ──────────────────────────────────────────────────────────
    project_id = args.project or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print(
            "Error: --project is required (or set GOOGLE_CLOUD_PROJECT).",
            file=sys.stderr,
        )
        return 1

    # ── PDF requires Ollama ──────────────────────────────────────────────────
    input_path = Path(args.input)
    if input_path.suffix.lower() == ".pdf":
        if not llm.is_available():
            print(
                f"Error: PDF input requires Ollama (schema extraction). "
                f"Cannot reach Ollama at {llm._host}.\n"
                "Start it with: ollama serve",
                file=sys.stderr,
            )
            return 1

    # ── Run workflow ─────────────────────────────────────────────────────────
    print(f"Reading schema from : {input_path}")
    print(f"Table               : {args.table}")
    print(f"Dataset             : {args.dataset}")
    print(f"Project             : {project_id}")
    print(f"Ollama model        : {llm.model}")
    print(f"HCL generation      : {'LLM (Ollama)' if args.llm_hcl else 'deterministic'}")
    print()

    state = run_bq_terraform_workflow(
        input_path=input_path,
        table_name=args.table,
        dataset_id=args.dataset,
        project_id=project_id,
        llm=llm,
        output_dir=args.output_dir,
        use_llm_for_hcl=args.llm_hcl,
    )

    if state.error:
        print(f"Error: {state.error}", file=sys.stderr)
        return 1

    if args.stdout:
        print(state.terraform_hcl)
    else:
        print(f"Schema fields : {len(state.schema_fields)} fields parsed")
        print(f"Output written: {state.output_path}")
        print()
        print("Preview (first 20 lines):")
        print("─" * 60)
        lines = state.terraform_hcl.splitlines()
        print("\n".join(lines[:20]))
        if len(lines) > 20:
            print(f"  ... ({len(lines) - 20} more lines)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

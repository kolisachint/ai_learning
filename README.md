
python3 scripts/run_bq_terraform.py data/raw/schema.pdf \
    --table events --dataset analytics --project my-gcp-project \
    --timeout 600 --num-ctx 1024

    
# ai_learning

This repository is currently a **starter skeleton** and does not yet contain application code.

## Current structure

- `.git/` — Git metadata and history.
- `.gitkeep` — Placeholder file to keep the repository non-empty.

## What a newcomer should know

1. There is no implemented source tree yet (no `src/`, `app/`, tests, or build config files).
2. The repository is ready to be initialized into any stack (Python, JS/TS, Go, Rust, etc.).
3. The first meaningful step is to define a project goal and pick a concrete runtime/toolchain.

## Suggested next learning/build steps

1. Define the project objective in a short architecture/design note.
2. Add a language/tooling scaffold (for example, `pyproject.toml` or `package.json`).
3. Create a minimal runnable app entry point and a basic test.
4. Add developer automation (`Makefile` or scripts for lint/test/run).
5. Set up CI to run tests and lint on each commit.

## Example initial layout to aim for

```
ai_learning/
  README.md
  docs/
    architecture.md
  src/
  tests/
  scripts/
  .gitignore
```

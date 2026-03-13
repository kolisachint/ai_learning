# Architecture Notes

This project is set up as an agentic design automation framework.

## Core idea

Use LangGraph to coordinate a set of specialized design agents backed by
Anthropic models. Each workflow should take a design input, move it through a
graph of specialized steps, and produce structured design outputs.

## Recommended package ownership

- `graphs/`: graph definitions and node transitions
- `integrations/anthropic/`: Anthropic SDK access and request shaping
- `design_ops/`: domain logic for design tasks
- `prompts/`: prompt text and prompt builders
- `state/`: typed shared state and artifact schemas
- `tools/`: callable tools and utility adapters
- `workflows/`: reusable end-to-end workflow entrypoints

## Example design workflow

1. Intake a product or feature brief
2. Expand requirements and constraints
3. Generate design directions
4. Critique against heuristics or design system rules
5. Refine outputs
6. Export a final brief, rationale, or implementation-ready handoff

## Output strategy

Keep generated content separated by purpose:

- raw inputs in `data/raw/`
- transformed intermediate artifacts in `data/interim/`
- final structured outputs in `data/processed/`

If you later add image or UI exports, create subfolders under `data/processed/`
for `wireframes/`, `copy/`, `reviews/`, and `handoff/`.

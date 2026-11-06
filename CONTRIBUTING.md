# Contributing and git workflow

## Branches

- `main` is always green and deployable. Never commit to it directly.
- One branch per phase or feature: `phase-1-capture`, `fix/dlq-retry`.
- Open a PR into `main`; merge only when CI is green.
- After merging a phase, tag the merge commit: `phase-1`, `phase-2`, ...

## Commits

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`,
`chore:`, `ci:`, `refactor:`. Keep each commit to one logical change.

## Local checks (the same ones CI runs)

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1      macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"

ruff check .
ruff format --check .
mypy
pytest
```

## Decisions

Anything that changes the architecture needs an ADR in `docs/adr/` in the same PR.

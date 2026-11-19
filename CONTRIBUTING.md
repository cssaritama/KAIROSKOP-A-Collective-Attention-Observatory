# Contributing

## How this repository is run

Profile `research`. Data class Public. State Active. Owner Carlos Saritama.
README.md states what is deployed and what is not.

Before starting work, read:

1. `README.md`
2. `CURRENT_STATE.md`
3. `docs/standards/PROJECT_STANDARDS.md` and `docs/standards/PROFILE.md`

Then follow links only as far as the change needs. Do not read `archive/`, `past_version/` and raw stream dumps
unless the change needs that evidence.

Rules:

- Schema, snapshot specification and CI workflow changes need recorded evidence: a content hash, the test run and a green CI run.
- Ideas go to `IDEAS.md`; external data sources and references to `REFERENCES.md`.
- `docs/standards/` is a verified copy of the Systems Studio Engineering Standard. Do not edit it; start specifications, decisions and verification records from `docs/standards/templates/`.
- Work on a focused branch and open a pull request. Keep `main` working.

Verify:

```bash
make test
make audit
```

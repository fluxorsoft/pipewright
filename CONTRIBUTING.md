# Contributing to Pipewright

Thank you for helping. Pipewright is maintained by Fluxor Software and welcomes issues, discussions and pull
requests.

## Before you start

- **Read [CONTEXT.md](CONTEXT.md).** It holds the requirements, design decisions and conventions, each with a stable
  ID (`FR-`, `NFR-`, `D-`). A pull request that changes behaviour should say which IDs it touches, and update
  CONTEXT.md in the same change if it alters one.
- **For anything larger than a fix, open an issue first** so we can agree the approach before you spend time on it.
- **Google Cloud only.** Pipewright deliberately targets one cloud (CONTEXT.md, C-01); multi-cloud abstractions are
  out of scope.

## Development setup

```bash
git clone https://github.com/fluxorsoft/pipewright.git
cd pipewright
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run --no-project --with-requirements examples/churn/tests/requirements.txt python -m pytest examples/churn/tests
```

Python 3.11, [uv](https://docs.astral.sh/uv/). Behind a TLS-inspecting proxy set `UV_NATIVE_TLS=1` and
`REQUESTS_CA_BUNDLE`.

## Pull requests

- **Tests accompany changes.** Unit tests run offline, with fakes for Google Cloud; nothing in `tests/` needs
  credentials. A behaviour change visible to users also updates the README, `docs/ONBOARDING.md` or the example.
- **Match the surrounding code:** naming, comment density, idiom. Line length 120 (`ruff`).
- **Configuration changes are schema changes.** New keys go in the JSON Schemas under
  `src/pipewright/config/schemas/` with a description, defaults in `config/ml.py` or `config/resolve.py`, and a
  cross-check if they interact with other keys. The manifest schema is a contract with platform teams: only add
  optional fields (CONTEXT.md, R-05).
- **Writing style for documentation:** plain English, short sentences, no em-dashes. Explain a term where it first
  appears.
- **Never include credentials, project IDs, real email addresses or customer data** in code, tests, examples or
  issues. Use `example.com`, `acme-ml-dev` and similar placeholders.

## Licensing of contributions

Pipewright is licensed under the [Apache License 2.0](LICENSE). By submitting a contribution you agree that it is
licensed under the same terms (section 5 of the license), and you confirm that you have the right to submit it.
Please sign off your commits (`git commit -s`) to certify the
[Developer Certificate of Origin](https://developercertificate.org/).

## Reporting security issues

Please do not open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md).

# PipelineAPI (FastAPI)

Best-practice starter layout for a FastAPI service.

## Project structure

```text
.
├── app/
│   ├── api/
│   │   ├── dependencies.py
│   │   └── v1/
│   │       ├── api.py
│   │       └── endpoints/
│   │           └── health.py
│   ├── core/
│   │   ├── config.py
│   │   └── logging.py
│   ├── db/
│   ├── models/
│   ├── schemas/
│   │   └── health.py
│   ├── services/
│   └── main.py
├── tests/
│   └── test_health.py
├── .env.example
├── Makefile
└── pyproject.toml
```

## Quick start

```bash
make setup
cp .env.example .env
make run
```

Open: <http://127.0.0.1:8000/docs>

## Commands

```bash
make run      # Start dev server
make test     # Run tests
make lint     # Lint
make format   # Format
```

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
UVICORN := $(VENV)/bin/uvicorn

.PHONY: setup run test lint format

setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

run:
	$(UVICORN) app.main:app --reload --host 0.0.0.0 --port 8000

test:
	$(PYTEST)

lint:
	$(RUFF) check .

format:
	$(RUFF) format .

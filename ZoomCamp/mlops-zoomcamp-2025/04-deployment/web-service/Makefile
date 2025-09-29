VENV_NAME := mlops_venv_396
VENV_ACTIVATE := source $(VENV_NAME)/bin/activate
PYTHON := $(VENV_NAME)/bin/python

setup:
    python -m venv $(VENV_NAME)
    $(VENV_ACTIVATE) && pip install -r requirements.txt

clean:
    rm -rf __pycache__
    rm -rf $(VENV_NAME)

train:
    $(VENV_ACTIVATE) && $(PYTHON) train.py

predict:
    $(VENV_ACTIVATE) && $(PYTHON) predict.py

test:
    $(VENV_ACTIVATE) && pytest

lint:
    $(VENV_ACTIVATE) && black .
    $(VENV_ACTIVATE) && isort .
    $(VENV_ACTIVATE) && flake8

.PHONY: setup clean train predict test lint
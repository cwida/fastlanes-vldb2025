# Makefile

# Name of the venv folder
VENV := .venv

# Python and pip inside the venv
PYTHON := $(VENV)/bin/python
PIP     := $(VENV)/bin/pip

# Your entry‐point script
SCRIPT  := master_script.py

.PHONY: all venv install run clean

# Default target: create venv, install deps, then run
all: run

# Create the virtual environment
venv:
	python3 -m venv $(VENV)

# Install dependencies into the venv
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install pandas

# Run your main script using the venv’s python
run: install
	$(PYTHON) $(SCRIPT)

# Remove the venv (and start fresh)
clean:
	rm -rf $(VENV)

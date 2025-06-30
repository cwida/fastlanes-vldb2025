# Name of the venv folder
VENV    := .venv

# Python and pip inside the venv
PYTHON  := $(VENV)/bin/python
PIP     := $(VENV)/bin/pip

# Your entry‐point script
SCRIPT  := master_script.py

.PHONY: all venv install run clean

# Default target: create venv, install deps, then run
all: run

# Create the virtual environment
venv:
	python3 -m venv $(VENV)

# Install dependencies into the venv, pinning duckdb to 1.2.x (>=1.2.0,<1.3.0)
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install pandas duckdb~=1.2.0

# Run your main script using the venv’s python (with venv “activated” so that
# any 'python3' in subprocesses also refers to the same interpreter)
run: install
	. $(VENV)/bin/activate && python3 $(SCRIPT)

# Remove the venv (and start fresh)
clean:
	rm -rf $(VENV)

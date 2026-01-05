chemicals
==========

A minimal Python library managed by `uv`.

Getting started
---------------
1) Ensure Python 3.13+ is available.  
2) Install dependencies with `uv sync`.  
3) Run the extractor: `uv run python extractor.py`

Project structure
-----------------
- `src/chemicals/`: library code (currently `hello()` helper).
- `pyproject.toml`: project metadata and dependencies.

Common commands
---------------
- `uv sync`: install dependencies into the virtualenv.
- `uv run <cmd>`: execute a command with project deps available.
- `uv add <package>`: add a runtime dependency.



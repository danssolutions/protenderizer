# protenderizer
Tool for analyzing EU public procurement data

## Installation

In the root directory, run from CMD or VSCode:
`pip install -e .`

Then, you can run from any directory:
`protenderizer <command>`

For example:
`protenderizer --help`

After making changes to any dependencies, just rerun `pip install -e .` again before testing.

## Running tests
pytest

### Run only unit tests
pytest tests/unit

### Run only integration tests
pytest tests/integration

### Run only performance tests
pytest tests/performance

### Show detailed output
pytest -v

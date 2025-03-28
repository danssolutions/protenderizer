# protenderizer
Tool for analyzing EU public procurement data

For the rest of the group: 

Install the dependencies first:

pip install pytest pytest-mock requests-mock pytest-benchmark click scikit-learn 

Then run tests:

# Run all tests
pytest

# Run only unit tests
pytest tests/unit

# Run only integration tests
pytest -m integration

# Run performance tests
pytest -m performance

# Show detailed output
pytest -v

# With coverage (optional)
pytest --cov=myapp tests/
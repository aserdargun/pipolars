Contributing
============

Thank you for your interest in contributing to PIPolars! This guide will help
you get started.

Development Setup
-----------------

Prerequisites
~~~~~~~~~~~~~

- Windows 10/11 or Windows Server 2016+
- Python 3.10 or later
- OSIsoft PI AF SDK 2.x
- `uv <https://github.com/astral-sh/uv>`_ (recommended) or pip

Clone and Install
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/pipolars/pipolars.git
   cd pipolars

   # Install with all development dependencies
   uv sync --all-extras

   # Or using pip
   pip install -e ".[dev,docs]"

Running Tests
~~~~~~~~~~~~~

.. code-block:: bash

   # Run all tests
   uv run pytest

   # Run unit tests only
   uv run pytest tests/unit

   # Run with coverage
   uv run pytest --cov=pipolars --cov-report=html

   # Run integration tests (requires PI connection)
   PI_SERVER=my-server uv run pytest -m integration

Code Quality
~~~~~~~~~~~~

.. code-block:: bash

   # Type checking
   uv run mypy src

   # Linting
   uv run ruff check src

   # Format code
   uv run ruff format src

Building Documentation
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Install docs dependencies
   uv sync --extra docs

   # Build HTML docs
   cd docs
   make html

   # Or on Windows
   make.bat html

   # View docs (Windows)
   start _build/html/index.html

Code Style
----------

PIPolars follows these conventions:

Python Style
~~~~~~~~~~~~

- **Formatting**: Ruff formatter (line length 100)
- **Imports**: isort (via Ruff), first-party imports grouped
- **Docstrings**: Google style
- **Type hints**: Required for all public APIs

Example:

.. code-block:: python

   def recorded_values(
       self,
       tags: str | list[str],
       start: PITimestamp,
       end: PITimestamp,
       max_count: int = 0,
       include_quality: bool = False,
   ) -> pl.DataFrame:
       """Get recorded values for one or more tags.

       Args:
           tags: Single tag or list of tags
           start: Start time (datetime, string, or AFTime)
           end: End time
           max_count: Maximum values per tag (0 = no limit)
           include_quality: Include quality column

       Returns:
           DataFrame with recorded values

       Raises:
           PIPointNotFoundError: If tag doesn't exist
           PIConnectionError: If not connected

       Example:
           >>> df = client.recorded_values("SINUSOID", "*-1d", "*")
       """

Commit Messages
~~~~~~~~~~~~~~~

Use conventional commits:

- ``feat:`` New feature
- ``fix:`` Bug fix
- ``docs:`` Documentation
- ``test:`` Tests
- ``refactor:`` Code refactoring
- ``chore:`` Maintenance

Example:

.. code-block:: text

   feat: add bulk snapshot retrieval

   - Add snapshots() method to PIClient
   - Implement parallel fetching in BulkExtractor
   - Add tests for bulk operations

Pull Request Process
--------------------

1. **Fork** the repository
2. **Create** a feature branch (``git checkout -b feature/my-feature``)
3. **Write** tests for new functionality
4. **Ensure** all tests pass (``uv run pytest``)
5. **Check** types (``uv run mypy src``)
6. **Check** linting (``uv run ruff check src``)
7. **Commit** with meaningful messages
8. **Push** to your fork
9. **Open** a Pull Request

PR Checklist
~~~~~~~~~~~~

- [ ] Tests pass locally
- [ ] New code has tests
- [ ] Type hints added
- [ ] Docstrings added
- [ ] Documentation updated (if needed)
- [ ] CHANGELOG updated (for features/fixes)
- [ ] No linting errors

Project Structure
-----------------

.. code-block:: text

   pipolars/
   ├── src/pipolars/
   │   ├── api/           # User-facing API
   │   │   ├── client.py  # PIClient
   │   │   ├── query.py   # PIQuery
   │   │   └── lazy.py    # LazyFrame support
   │   ├── connection/    # PI connectivity
   │   │   ├── server.py  # PI Server connection
   │   │   ├── af_database.py
   │   │   ├── sdk.py     # AF SDK wrapper
   │   │   └── auth.py    # Authentication
   │   ├── extraction/    # Data retrieval
   │   │   ├── points.py  # Single point extraction
   │   │   ├── bulk.py    # Bulk operations
   │   │   └── ...
   │   ├── transform/     # Data conversion
   │   │   ├── converters.py
   │   │   └── ...
   │   ├── cache/         # Caching
   │   │   ├── storage.py
   │   │   └── strategies.py
   │   └── core/          # Core types
   │       ├── config.py
   │       ├── types.py
   │       └── exceptions.py
   ├── tests/
   │   ├── unit/
   │   └── integration/
   ├── docs/
   └── examples/

Testing Guidelines
------------------

Unit Tests
~~~~~~~~~~

- Test individual functions/methods in isolation
- Mock external dependencies (PI SDK)
- Located in ``tests/unit/``

Integration Tests
~~~~~~~~~~~~~~~~~

- Test against actual PI Server
- Marked with ``@pytest.mark.integration``
- Located in ``tests/integration/``
- Require ``PI_SERVER`` environment variable

.. code-block:: python

   import pytest
   from pipolars import PIClient

   @pytest.mark.integration
   def test_recorded_values_real_server():
       server = os.environ.get("PI_SERVER")
       with PIClient(server) as client:
           df = client.recorded_values("SINUSOID", "*-1h", "*")
           assert len(df) > 0

Test Fixtures
~~~~~~~~~~~~~

Common fixtures are in ``tests/conftest.py``:

.. code-block:: python

   import pytest
   from pipolars import PIConfig
   from pipolars.core.config import PIServerConfig

   @pytest.fixture
   def pi_config():
       return PIConfig(
           server=PIServerConfig(host="test-server"),
       )

Reporting Issues
----------------

Bug Reports
~~~~~~~~~~~

Include:

1. Python version (``python --version``)
2. PIPolars version (``pip show pipolars``)
3. PI AF SDK version
4. Windows version
5. Minimal reproducible example
6. Full error traceback

Feature Requests
~~~~~~~~~~~~~~~~

Include:

1. Use case description
2. Expected behavior
3. Example API (if applicable)

License
-------

By contributing to PIPolars, you agree that your contributions will be
licensed under the MIT License.

Questions?
----------

- Open a GitHub Issue
- Check existing issues and discussions

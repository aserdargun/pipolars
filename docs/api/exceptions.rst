Exceptions
==========

.. module:: pipolars.core.exceptions

This module defines the exception hierarchy for PIPolars. All exceptions
inherit from ``PIPolarsError``, making it easy to catch any library-related
error.

Exception Hierarchy
-------------------

.. code-block:: text

   PIPolarsError
   ├── PIConnectionError
   │   └── PIAuthenticationError
   ├── PIDataError
   │   ├── PIPointNotFoundError
   │   └── PIBulkOperationError
   ├── PIQueryError
   │   └── PITimeParseError
   ├── PIConfigurationError
   ├── PIAFSDKError
   ├── PICacheError
   └── PITransformError

Base Exception
--------------

PIPolarsError
~~~~~~~~~~~~~

.. autoclass:: pipolars.PIPolarsError
   :members:
   :undoc-members:
   :show-inheritance:

   Base exception for all PIPolars errors.

   .. attribute:: message
      :type: str

      Human-readable error message.

   .. attribute:: details
      :type: dict[str, Any]

      Optional dictionary with additional error context.

Usage:

.. code-block:: python

   from pipolars.core.exceptions import PIPolarsError

   try:
       df = client.recorded_values("TAG", "*-1h", "*")
   except PIPolarsError as e:
       print(f"Error: {e.message}")
       print(f"Details: {e.details}")

Connection Exceptions
---------------------

PIConnectionError
~~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.PIConnectionError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when connection to PI System fails.

   .. attribute:: server
      :type: str | None

      The server that failed to connect.

   Raised when:

   - PI Server is unreachable
   - Authentication fails
   - AF Database connection fails
   - Network timeout occurs

PIAuthenticationError
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PIAuthenticationError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when PI System authentication fails.

   Raised when:

   - Invalid credentials provided
   - User lacks permissions
   - Kerberos/NTLM authentication fails

Usage:

.. code-block:: python

   from pipolars.core.exceptions import PIConnectionError, PIAuthenticationError

   try:
       with PIClient("my-server") as client:
           df = client.snapshot("TAG")
   except PIAuthenticationError as e:
       print(f"Auth failed: {e}")
   except PIConnectionError as e:
       print(f"Connection failed to {e.server}: {e}")

Data Exceptions
---------------

PIDataError
~~~~~~~~~~~

.. autoclass:: pipolars.PIDataError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when data retrieval or conversion fails.

   .. attribute:: tag
      :type: str | None

      The tag that caused the error.

   Raised when:

   - Requested tag doesn't exist
   - Data type conversion fails
   - Invalid time range specified
   - Bulk operation partially fails

PIPointNotFoundError
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PIPointNotFoundError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when a PI Point (tag) cannot be found.

   Raised when:

   - Tag name doesn't exist in the PI Data Archive
   - Tag was deleted or renamed
   - User lacks access to the tag

Usage:

.. code-block:: python

   from pipolars.core.exceptions import PIPointNotFoundError

   try:
       df = client.snapshot("NONEXISTENT_TAG")
   except PIPointNotFoundError as e:
       print(f"Tag not found: {e.tag}")

PIBulkOperationError
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PIBulkOperationError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when a bulk operation partially fails.

   .. attribute:: succeeded
      :type: list[str]

      List of tags that succeeded.

   .. attribute:: failed
      :type: dict[str, str]

      Dictionary mapping failed tags to their error messages.

Usage:

.. code-block:: python

   from pipolars.core.exceptions import PIBulkOperationError

   try:
       df = client.recorded_values(
           ["TAG1", "TAG2", "INVALID", "TAG3"],
           "*-1h", "*"
       )
   except PIBulkOperationError as e:
       print(f"Succeeded: {e.succeeded}")
       print(f"Failed: {e.failed}")

       # Process successful results
       for tag in e.succeeded:
           print(f"Got data for {tag}")

Query Exceptions
----------------

PIQueryError
~~~~~~~~~~~~

.. autoclass:: pipolars.PIQueryError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when a PI query is invalid or fails.

   .. attribute:: query
      :type: str | None

      The query that failed.

   Raised when:

   - Invalid time expression provided
   - Query syntax error
   - Query timeout
   - Too many results requested

PITimeParseError
~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PITimeParseError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when a time expression cannot be parsed.

   Raised when:

   - Invalid relative time expression (e.g., ``"*-invalid"``)
   - Malformed absolute timestamp
   - Unsupported time format

Usage:

.. code-block:: python

   from pipolars.core.exceptions import PITimeParseError

   try:
       df = client.recorded_values("TAG", "invalid-time", "*")
   except PITimeParseError as e:
       print(f"Invalid time: {e}")

Other Exceptions
----------------

PIConfigurationError
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PIConfigurationError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when configuration is invalid.

   Raised when:

   - Required configuration missing
   - Invalid configuration value
   - Configuration file parse error

PIAFSDKError
~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PIAFSDKError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when the AF SDK encounters an error.

   .. attribute:: sdk_error_code
      :type: int | None

      The error code from AF SDK if available.

   .. attribute:: sdk_message
      :type: str | None

      The original error message from AF SDK.

PICacheError
~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PICacheError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when cache operations fail.

   Raised when:

   - Cache read/write fails
   - Cache corruption detected
   - Cache storage is full

PITransformError
~~~~~~~~~~~~~~~~

.. autoclass:: pipolars.core.exceptions.PITransformError
   :members:
   :undoc-members:
   :show-inheritance:

   Raised when data transformation fails.

   Raised when:

   - Cannot convert PI type to Polars type
   - Schema mismatch
   - Data validation fails

Error Handling Patterns
-----------------------

Catch All PIPolars Errors
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars.core.exceptions import PIPolarsError

   try:
       with PIClient("my-server") as client:
           df = client.recorded_values("TAG", "*-1h", "*")
   except PIPolarsError as e:
       print(f"PIPolars error: {e}")
       print(f"Details: {e.details}")

Specific Error Handling
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars.core.exceptions import (
       PIConnectionError,
       PIPointNotFoundError,
       PITimeParseError,
       PIPolarsError,
   )

   def safe_query(client, tag, start, end):
       try:
           return client.recorded_values(tag, start, end)

       except PIPointNotFoundError:
           print(f"Tag '{tag}' not found, skipping...")
           return None

       except PITimeParseError as e:
           print(f"Invalid time expression: {e}")
           raise ValueError("Invalid time range") from e

       except PIConnectionError as e:
           print(f"Connection lost to {e.server}")
           raise

       except PIPolarsError as e:
           print(f"Unexpected error: {e}")
           raise

Retry Pattern
~~~~~~~~~~~~~

.. code-block:: python

   import time
   from pipolars.core.exceptions import PIConnectionError

   def query_with_retry(client, tag, start, end, max_retries=3):
       for attempt in range(max_retries):
           try:
               return client.recorded_values(tag, start, end)
           except PIConnectionError as e:
               if attempt < max_retries - 1:
                   print(f"Retry {attempt + 1}/{max_retries}...")
                   time.sleep(2 ** attempt)
               else:
                   raise

See Also
--------

- :doc:`../user_guide/advanced` - Error handling patterns
- :doc:`client` - PIClient methods that raise exceptions

Configuration
=============

.. module:: pipolars.core.config

This module provides configuration classes using Pydantic for validation
and environment variable support.

PIConfig
--------

.. autoclass:: pipolars.PIConfig
   :members:
   :undoc-members:
   :show-inheritance:

   The main configuration class that aggregates all settings.

   .. rubric:: Class Methods

   .. automethod:: from_file
   .. automethod:: to_dict

Usage:

.. code-block:: python

   from pipolars import PIConfig
   from pipolars.core.config import PIServerConfig, CacheConfig, CacheBackend

   # Programmatic configuration
   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(backend=CacheBackend.SQLITE),
       debug=True,
   )

   # From file
   config = PIConfig.from_file("pipolars.toml")

PIServerConfig
--------------

.. autoclass:: pipolars.core.config.PIServerConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for PI Data Archive connection.

   .. attribute:: host
      :type: str

      PI Server hostname or IP address. Required.

   .. attribute:: port
      :type: int
      :value: 5450

      PI Server port.

   .. attribute:: timeout
      :type: int
      :value: 30

      Connection timeout in seconds.

   .. attribute:: auth_method
      :type: AuthMethod
      :value: AuthMethod.WINDOWS

      Authentication method.

   .. attribute:: username
      :type: str | None
      :value: None

      Username for explicit authentication.

   .. attribute:: password
      :type: SecretStr | None
      :value: None

      Password for explicit authentication.

Environment variables:

.. code-block:: bash

   PI_SERVER_HOST=my-pi-server
   PI_SERVER_PORT=5450
   PI_SERVER_TIMEOUT=30
   PI_SERVER_AUTH_METHOD=windows

AFServerConfig
--------------

.. autoclass:: pipolars.core.config.AFServerConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for AF Server connection.

   .. attribute:: host
      :type: str | None
      :value: None

      AF Server hostname (defaults to PI Server).

   .. attribute:: database
      :type: str | None
      :value: None

      Default AF Database name.

   .. attribute:: timeout
      :type: int
      :value: 30

      Connection timeout in seconds.

Environment variables:

.. code-block:: bash

   AF_SERVER_HOST=my-af-server
   AF_SERVER_DATABASE=MyDatabase

CacheConfig
-----------

.. autoclass:: pipolars.core.config.CacheConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for data caching.

   .. attribute:: backend
      :type: CacheBackend
      :value: CacheBackend.NONE

      Cache storage backend.

   .. attribute:: path
      :type: Path
      :value: ~/.pipolars/cache

      Path for file-based cache backends.

   .. attribute:: max_size_mb
      :type: int
      :value: 1024

      Maximum cache size in megabytes.

   .. attribute:: ttl_hours
      :type: int
      :value: 24

      Time-to-live for cached data in hours.

   .. attribute:: compression
      :type: bool
      :value: True

      Enable compression for cached data.

   .. autoproperty:: ttl

Environment variables:

.. code-block:: bash

   PIPOLARS_CACHE_BACKEND=sqlite
   PIPOLARS_CACHE_PATH=~/.pipolars/cache
   PIPOLARS_CACHE_MAX_SIZE_MB=1024
   PIPOLARS_CACHE_TTL_HOURS=24

QueryConfig
-----------

.. autoclass:: pipolars.core.config.QueryConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for PI queries.

   .. attribute:: max_points_per_query
      :type: int
      :value: 1000

      Maximum number of points in a single query.

   .. attribute:: default_page_size
      :type: int
      :value: 10000

      Default page size for paginated queries.

   .. attribute:: max_values_per_request
      :type: int
      :value: 150000

      Maximum values per request.

   .. attribute:: parallel_requests
      :type: int
      :value: 4

      Number of parallel requests for bulk operations.

   .. attribute:: retry_attempts
      :type: int
      :value: 3

      Number of retry attempts for failed requests.

   .. attribute:: retry_delay
      :type: float
      :value: 1.0

      Delay between retries in seconds.

Environment variables:

.. code-block:: bash

   PIPOLARS_QUERY_MAX_POINTS_PER_QUERY=1000
   PIPOLARS_QUERY_PARALLEL_REQUESTS=4

PolarsConfig
------------

.. autoclass:: pipolars.core.config.PolarsConfig
   :members:
   :undoc-members:
   :show-inheritance:

   Configuration for Polars DataFrame output.

   .. attribute:: timestamp_column
      :type: str
      :value: "timestamp"

      Name of the timestamp column.

   .. attribute:: value_column
      :type: str
      :value: "value"

      Name of the value column.

   .. attribute:: quality_column
      :type: str
      :value: "quality"

      Name of the quality column.

   .. attribute:: tag_column
      :type: str
      :value: "tag"

      Name of the tag column (for multi-tag queries).

   .. attribute:: include_quality
      :type: bool
      :value: False

      Include quality column by default.

   .. attribute:: timezone
      :type: str
      :value: "UTC"

      Default timezone for timestamps.

Environment variables:

.. code-block:: bash

   PIPOLARS_POLARS_TIMEZONE=America/New_York
   PIPOLARS_POLARS_INCLUDE_QUALITY=true

Enumerations
------------

AuthMethod
~~~~~~~~~~

.. autoclass:: pipolars.core.config.AuthMethod
   :members:
   :undoc-members:
   :show-inheritance:

   Authentication methods for PI System connection.

   .. attribute:: WINDOWS
      :value: "windows"

      Use Windows integrated authentication (NTLM/Kerberos).

   .. attribute:: EXPLICIT
      :value: "explicit"

      Use explicit username/password authentication.

CacheBackend
~~~~~~~~~~~~

.. autoclass:: pipolars.core.config.CacheBackend
   :members:
   :undoc-members:
   :show-inheritance:

   Cache storage backends.

   .. attribute:: NONE
      :value: "none"

      No caching.

   .. attribute:: MEMORY
      :value: "memory"

      In-memory cache (lost on restart).

   .. attribute:: SQLITE
      :value: "sqlite"

      SQLite database cache.

   .. attribute:: ARROW
      :value: "arrow"

      Apache Arrow IPC file cache.

Configuration File Format
-------------------------

TOML Format
~~~~~~~~~~~

.. code-block:: toml

   [server]
   host = "my-pi-server"
   port = 5450
   timeout = 30
   auth_method = "windows"

   [af]
   database = "MyDatabase"

   [cache]
   backend = "sqlite"
   max_size_mb = 1024
   ttl_hours = 24

   [query]
   parallel_requests = 4
   retry_attempts = 3

   [polars]
   timezone = "UTC"

   debug = false
   log_level = "INFO"

JSON Format
~~~~~~~~~~~

.. code-block:: json

   {
       "server": {
           "host": "my-pi-server",
           "port": 5450
       },
       "cache": {
           "backend": "sqlite"
       },
       "debug": false
   }

See Also
--------

- :doc:`../user_guide/configuration` - Configuration guide
- :doc:`../user_guide/caching` - Caching guide

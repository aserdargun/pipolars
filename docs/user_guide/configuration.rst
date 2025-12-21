Configuration
=============

PIPolars uses Pydantic for configuration management, supporting environment
variables, configuration files, and programmatic configuration.

Configuration Classes
---------------------

PIPolars has a hierarchical configuration structure:

.. code-block:: text

   PIConfig
   ├── server: PIServerConfig    # PI Server connection settings
   ├── af: AFServerConfig        # AF Server settings
   ├── cache: CacheConfig        # Caching configuration
   ├── query: QueryConfig        # Query behavior settings
   ├── polars: PolarsConfig      # DataFrame output settings
   ├── debug: bool               # Debug mode
   └── log_level: str            # Logging level

PIConfig
--------

The main configuration class that aggregates all settings:

.. code-block:: python

   from pipolars import PIConfig
   from pipolars.core.config import (
       PIServerConfig, AFServerConfig, CacheConfig,
       QueryConfig, PolarsConfig, CacheBackend
   )

   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(backend=CacheBackend.SQLITE),
       debug=True,
       log_level="DEBUG",
   )

   with PIClient(config=config) as client:
       df = client.snapshot("SINUSOID")

PIServerConfig
--------------

PI Data Archive connection settings:

.. code-block:: python

   from pipolars.core.config import PIServerConfig, AuthMethod

   server_config = PIServerConfig(
       host="my-pi-server",    # Required: hostname or IP
       port=5450,              # Optional: default 5450
       timeout=30,             # Optional: connection timeout (seconds)
       auth_method=AuthMethod.WINDOWS,  # Authentication method
       username=None,          # For explicit auth
       password=None,          # For explicit auth
   )

Environment variables:

.. code-block:: bash

   PI_SERVER_HOST=my-pi-server
   PI_SERVER_PORT=5450
   PI_SERVER_TIMEOUT=30
   PI_SERVER_AUTH_METHOD=windows

AFServerConfig
--------------

AF Server connection settings:

.. code-block:: python

   from pipolars.core.config import AFServerConfig

   af_config = AFServerConfig(
       host="my-af-server",    # Optional: defaults to PI server
       database="MyDatabase",  # Optional: default AF database
       timeout=30,             # Connection timeout
   )

Environment variables:

.. code-block:: bash

   AF_SERVER_HOST=my-af-server
   AF_SERVER_DATABASE=MyDatabase
   AF_SERVER_TIMEOUT=30

CacheConfig
-----------

Caching behavior settings:

.. code-block:: python

   from pathlib import Path
   from pipolars.core.config import CacheConfig, CacheBackend

   cache_config = CacheConfig(
       backend=CacheBackend.SQLITE,  # none, memory, sqlite, arrow
       path=Path("~/.pipolars/cache").expanduser(),
       max_size_mb=1024,             # Maximum cache size
       ttl_hours=24,                 # Cache time-to-live
       compression=True,             # Compress cached data
   )

Environment variables:

.. code-block:: bash

   PIPOLARS_CACHE_BACKEND=sqlite
   PIPOLARS_CACHE_PATH=~/.pipolars/cache
   PIPOLARS_CACHE_MAX_SIZE_MB=1024
   PIPOLARS_CACHE_TTL_HOURS=24
   PIPOLARS_CACHE_COMPRESSION=true

QueryConfig
-----------

Query behavior settings:

.. code-block:: python

   from pipolars.core.config import QueryConfig

   query_config = QueryConfig(
       max_points_per_query=1000,     # Max tags per query
       default_page_size=10000,       # Pagination size
       max_values_per_request=150000, # Max values per request
       parallel_requests=4,           # Parallel bulk operations
       retry_attempts=3,              # Retry on failure
       retry_delay=1.0,               # Delay between retries
   )

Environment variables:

.. code-block:: bash

   PIPOLARS_QUERY_MAX_POINTS_PER_QUERY=1000
   PIPOLARS_QUERY_PARALLEL_REQUESTS=4
   PIPOLARS_QUERY_RETRY_ATTEMPTS=3

PolarsConfig
------------

DataFrame output settings:

.. code-block:: python

   from pipolars.core.config import PolarsConfig

   polars_config = PolarsConfig(
       timestamp_column="timestamp",  # Name of timestamp column
       value_column="value",          # Name of value column
       quality_column="quality",      # Name of quality column
       tag_column="tag",              # Name of tag column
       include_quality=False,         # Include quality by default
       timezone="UTC",                # Output timezone
   )

Environment variables:

.. code-block:: bash

   PIPOLARS_POLARS_TIMESTAMP_COLUMN=timestamp
   PIPOLARS_POLARS_VALUE_COLUMN=value
   PIPOLARS_POLARS_TIMEZONE=America/New_York
   PIPOLARS_POLARS_INCLUDE_QUALITY=false

Configuration Files
-------------------

TOML Configuration
~~~~~~~~~~~~~~~~~~

Create a ``pipolars.toml`` file:

.. code-block:: toml

   [server]
   host = "my-pi-server"
   port = 5450
   timeout = 30
   auth_method = "windows"

   [af]
   host = "my-af-server"
   database = "MyDatabase"

   [cache]
   backend = "sqlite"
   path = "~/.pipolars/cache"
   max_size_mb = 1024
   ttl_hours = 24

   [query]
   parallel_requests = 4
   retry_attempts = 3

   [polars]
   timezone = "UTC"
   include_quality = false

   [general]
   debug = false
   log_level = "INFO"

Load from file:

.. code-block:: python

   from pipolars import PIConfig

   config = PIConfig.from_file("pipolars.toml")
   with PIClient(config=config) as client:
       df = client.snapshot("SINUSOID")

JSON Configuration
~~~~~~~~~~~~~~~~~~

You can also use JSON:

.. code-block:: json

   {
       "server": {
           "host": "my-pi-server",
           "port": 5450,
           "timeout": 30
       },
       "cache": {
           "backend": "sqlite",
           "max_size_mb": 1024
       },
       "debug": false
   }

.. code-block:: python

   config = PIConfig.from_file("pipolars.json")

Environment File (.env)
-----------------------

PIPolars can load from ``.env`` files:

.. code-block:: text

   # .env file
   PI_SERVER_HOST=my-pi-server
   PI_SERVER_PORT=5450
   PIPOLARS_CACHE_BACKEND=sqlite
   PIPOLARS_CACHE_TTL_HOURS=24

PIPolars automatically loads ``.env`` from the current directory.

Configuration Precedence
------------------------

Configuration values are resolved in this order (highest priority first):

1. Programmatic configuration (passed to constructors)
2. Environment variables
3. ``.env`` file
4. Configuration file (if loaded)
5. Default values

.. code-block:: python

   # Environment: PI_SERVER_PORT=5451

   config = PIServerConfig(
       host="my-server",
       port=5450,  # This takes precedence over env var
   )
   print(config.port)  # 5450

   config2 = PIServerConfig(
       host="my-server",
       # port not specified, uses env var
   )
   print(config2.port)  # 5451

Viewing Configuration
---------------------

.. code-block:: python

   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(backend=CacheBackend.SQLITE),
   )

   # View as dictionary (sensitive values masked)
   print(config.to_dict())

   # Access individual settings
   print(config.server.host)
   print(config.cache.backend)
   print(config.cache.ttl)  # Returns timedelta

Validation
----------

Configuration is validated using Pydantic:

.. code-block:: python

   from pydantic import ValidationError

   try:
       config = PIServerConfig(
           host="my-server",
           port=99999,  # Invalid port
       )
   except ValidationError as e:
       print(e)
       # port: Input should be less than or equal to 65535

   try:
       config = PIServerConfig(
           host="my-server",
           auth_method="explicit",
           # Missing username/password for explicit auth
       )
   except ValidationError as e:
       print(e)
       # Username and password are required for explicit authentication

Complete Example
----------------

.. code-block:: python

   from pipolars import PIClient, PIConfig
   from pipolars.core.config import (
       PIServerConfig, AFServerConfig, CacheConfig,
       QueryConfig, PolarsConfig, CacheBackend, AuthMethod
   )
   from pathlib import Path

   # Full configuration
   config = PIConfig(
       server=PIServerConfig(
           host="prod-pi-server",
           port=5450,
           timeout=60,
           auth_method=AuthMethod.WINDOWS,
       ),
       af=AFServerConfig(
           host="prod-af-server",
           database="Production",
       ),
       cache=CacheConfig(
           backend=CacheBackend.SQLITE,
           path=Path("/data/pipolars_cache"),
           max_size_mb=4096,
           ttl_hours=48,
           compression=True,
       ),
       query=QueryConfig(
           parallel_requests=8,
           retry_attempts=3,
           retry_delay=2.0,
       ),
       polars=PolarsConfig(
           timezone="America/New_York",
           include_quality=True,
       ),
       debug=False,
       log_level="WARNING",
   )

   with PIClient(config=config) as client:
       df = client.recorded_values("SINUSOID", "*-1d", "*")

Next Steps
----------

- :doc:`caching` - Detailed caching configuration
- :doc:`advanced` - Advanced usage patterns
- :doc:`../api/config` - Configuration API reference

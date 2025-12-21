Time Expressions
================

PIPolars supports flexible time specifications using PI time expressions,
Python datetime objects, or the ``AFTime`` helper class.

PI Time Expression Syntax
-------------------------

PI time expressions are strings that specify absolute or relative times.
PIPolars passes these to the PI AF SDK for parsing.

Relative Time Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Expression
     - Description
   * - ``*``
     - Now (current time)
   * - ``*-1h``
     - 1 hour ago
   * - ``*-1d``
     - 1 day ago
   * - ``*-7d``
     - 7 days ago
   * - ``*-30d``
     - 30 days ago
   * - ``*-1w``
     - 1 week ago
   * - ``*-2M``
     - 2 months ago
   * - ``*-1y``
     - 1 year ago
   * - ``*+1h``
     - 1 hour from now
   * - ``*-1d+6h``
     - 1 day ago plus 6 hours

Named Time Expressions
~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Expression
     - Description
   * - ``t``
     - Today at midnight (start of day)
   * - ``y``
     - Yesterday at midnight
   * - ``t+8h``
     - Today at 8:00 AM
   * - ``y+17h``
     - Yesterday at 5:00 PM
   * - ``t-1d``
     - Start of yesterday (same as ``y``)

Absolute Time Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 50

   * - Expression
     - Description
   * - ``2024-01-15``
     - January 15, 2024 at midnight
   * - ``2024-01-15T10:30:00``
     - January 15, 2024 at 10:30:00
   * - ``2024-01-15 10:30:00``
     - Same (with space separator)
   * - ``15-Jan-2024``
     - Alternative date format
   * - ``01/15/2024``
     - US date format

Using Time Expressions
----------------------

In Direct Methods
~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Relative expressions
       df = client.recorded_values("SINUSOID", "*-1d", "*")
       df = client.recorded_values("SINUSOID", "*-7d", "*")

       # Named expressions
       df = client.recorded_values("SINUSOID", "t", "*")  # Today
       df = client.recorded_values("SINUSOID", "y", "t")  # Yesterday

       # Absolute expressions
       df = client.recorded_values(
           "SINUSOID",
           "2024-01-01",
           "2024-01-31"
       )

       # Combination
       df = client.recorded_values(
           "SINUSOID",
           "2024-01-15T08:00:00",
           "*"
       )

In Query Builder
~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Using time_range
       df = (
           client.query("SINUSOID")
           .time_range("*-1d", "*")
           .recorded()
           .to_dataframe()
       )

       # Using convenience methods
       df = (
           client.query("SINUSOID")
           .last(hours=24)
           .recorded()
           .to_dataframe()
       )

       df = (
           client.query("SINUSOID")
           .today()
           .recorded()
           .to_dataframe()
       )

The AFTime Class
----------------

PIPolars provides the ``AFTime`` class for programmatic time construction:

.. code-block:: python

   from pipolars import AFTime

   # Create from expression
   now = AFTime("*")
   yesterday = AFTime("y")

   # Class methods
   now = AFTime.now()           # Current time
   today = AFTime.today()       # Today at midnight
   yesterday = AFTime.yesterday()  # Yesterday at midnight

   # Relative time
   one_day_ago = AFTime.ago(days=1)
   two_hours_ago = AFTime.ago(hours=2)
   complex_ago = AFTime.ago(days=1, hours=6, minutes=30)

Using AFTime in Queries
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, AFTime

   with PIClient("my-pi-server") as client:
       start = AFTime.ago(days=7)
       end = AFTime.now()

       df = client.recorded_values(
           "SINUSOID",
           start=start,
           end=end
       )

Python datetime Objects
-----------------------

You can also use Python datetime objects:

.. code-block:: python

   from datetime import datetime, timedelta
   from pipolars import PIClient

   with PIClient("my-pi-server") as client:
       now = datetime.now()
       one_day_ago = now - timedelta(days=1)

       df = client.recorded_values(
           "SINUSOID",
           start=one_day_ago,
           end=now
       )

Timezone Handling
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from datetime import datetime
   from zoneinfo import ZoneInfo

   # Create timezone-aware datetime
   tz = ZoneInfo("America/New_York")
   start = datetime(2024, 1, 15, 8, 0, tzinfo=tz)
   end = datetime(2024, 1, 15, 17, 0, tzinfo=tz)

   df = client.recorded_values("SINUSOID", start=start, end=end)

TimeRange Class
---------------

The ``TimeRange`` class encapsulates a start and end time:

.. code-block:: python

   from pipolars.core.types import TimeRange, AFTime

   # Create from explicit times
   time_range = TimeRange(
       start=AFTime("*-1d"),
       end=AFTime("*")
   )

   # Use convenience methods
   last_week = TimeRange.last(days=7)
   today = TimeRange.today()

Interval Specifications
-----------------------

For interpolated values and summaries, specify intervals:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Interval
     - Description
   * - ``1h``
     - 1 hour
   * - ``30m``
     - 30 minutes
   * - ``15m``
     - 15 minutes
   * - ``1d``
     - 1 day
   * - ``6h``
     - 6 hours
   * - ``1w``
     - 1 week

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Hourly interpolation
       df = client.interpolated_values(
           "SINUSOID", "*-1d", "*", interval="1h"
       )

       # 15-minute interpolation
       df = client.interpolated_values(
           "SINUSOID", "*-4h", "*", interval="15m"
       )

       # Daily summaries
       df = client.summaries(
           "SINUSOID", "*-30d", "*",
           interval="1d",
           summary_types=SummaryType.AVERAGE
       )

Best Practices
--------------

1. **Use relative expressions** for scripts that run regularly:

   .. code-block:: python

      # Good - always gets last 24 hours
      df = client.recorded_values("SINUSOID", "*-1d", "*")

2. **Use absolute times** for historical analysis:

   .. code-block:: python

      # Good - specific date range
      df = client.recorded_values(
          "SINUSOID",
          "2024-01-01",
          "2024-01-31"
      )

3. **Consider timezone** when using datetime objects:

   .. code-block:: python

      from zoneinfo import ZoneInfo

      # Always specify timezone for clarity
      tz = ZoneInfo("UTC")
      start = datetime(2024, 1, 15, tzinfo=tz)

4. **Use appropriate intervals** for interpolation:

   - Short ranges (< 1 day): Use minutes (5m, 15m)
   - Medium ranges (1-7 days): Use hours (1h, 4h)
   - Long ranges (> 7 days): Use days (1d)

Error Handling
--------------

Invalid time expressions raise ``PITimeParseError``:

.. code-block:: python

   from pipolars.core.exceptions import PITimeParseError

   try:
       df = client.recorded_values("SINUSOID", "invalid", "*")
   except PITimeParseError as e:
       print(f"Invalid time expression: {e}")

Next Steps
----------

- :doc:`dataframes` - Working with result DataFrames
- :doc:`../api/types` - AFTime and TimeRange API reference

Types
=====

.. module:: pipolars.core.types

This module contains type definitions, enums, and data classes used
throughout PIPolars.

Type Aliases
------------

.. data:: PITimestamp

   Type alias for timestamps: ``Union[datetime, str, AFTime]``

   Accepts:
   - Python ``datetime`` objects
   - PI time expression strings (e.g., ``"*-1d"``, ``"t"``)
   - ``AFTime`` objects

.. data:: TagName

   Type alias for tag names: ``str``

.. data:: TagPath

   Type alias for AF attribute paths: ``str``

AFTime Class
------------

.. autoclass:: pipolars.AFTime
   :members:
   :undoc-members:
   :show-inheritance:

   .. rubric:: Class Methods

   .. automethod:: now
   .. automethod:: today
   .. automethod:: yesterday
   .. automethod:: ago
   .. automethod:: from_datetime

Usage:

.. code-block:: python

   from pipolars import AFTime

   # Create from expression
   now = AFTime("*")
   yesterday = AFTime("y")

   # Use class methods
   now = AFTime.now()
   today = AFTime.today()
   one_hour_ago = AFTime.ago(hours=1)
   complex_ago = AFTime.ago(days=1, hours=6, minutes=30)

   # From datetime
   from datetime import datetime
   dt = datetime(2024, 1, 15, 12, 0)
   af_time = AFTime.from_datetime(dt)

PIValue Class
-------------

.. autoclass:: pipolars.PIValue
   :members:
   :undoc-members:
   :show-inheritance:

TimeRange Class
---------------

.. autoclass:: pipolars.core.types.TimeRange
   :members:
   :undoc-members:
   :show-inheritance:

   .. rubric:: Class Methods

   .. automethod:: last
   .. automethod:: today

Usage:

.. code-block:: python

   from pipolars.core.types import TimeRange, AFTime

   # Create time range
   time_range = TimeRange(
       start=AFTime("*-1d"),
       end=AFTime("*")
   )

   # Use convenience methods
   last_week = TimeRange.last(days=7)
   today = TimeRange.today()

PointConfig Class
-----------------

.. autoclass:: pipolars.core.types.PointConfig
   :members:
   :undoc-members:
   :show-inheritance:

SummaryResult Class
-------------------

.. autoclass:: pipolars.core.types.SummaryResult
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

RetrievalMode
~~~~~~~~~~~~~

.. autoclass:: pipolars.RetrievalMode
   :members:
   :undoc-members:
   :show-inheritance:

   Data retrieval modes for PI queries.

   .. attribute:: RECORDED
      :value: "recorded"

      Return actual recorded values as stored in the archive.

   .. attribute:: INTERPOLATED
      :value: "interpolated"

      Return interpolated values at regular intervals.

   .. attribute:: PLOT
      :value: "plot"

      Return values optimized for plotting (reduced data density).

   .. attribute:: SUMMARY
      :value: "summary"

      Return summary statistics (min, max, avg, etc.).

   .. attribute:: COMPRESSED
      :value: "compressed"

      Return compressed data using exception/compression settings.

SummaryType
~~~~~~~~~~~

.. autoclass:: pipolars.SummaryType
   :members:
   :undoc-members:
   :show-inheritance:

   Summary calculation types for PI data.

   .. attribute:: TOTAL
      :value: 1

      Sum of values.

   .. attribute:: AVERAGE
      :value: 2

      Time-weighted average.

   .. attribute:: MINIMUM
      :value: 4

      Minimum value.

   .. attribute:: MAXIMUM
      :value: 8

      Maximum value.

   .. attribute:: RANGE
      :value: 16

      Range (max - min).

   .. attribute:: STD_DEV
      :value: 32

      Standard deviation.

   .. attribute:: POP_STD_DEV
      :value: 64

      Population standard deviation.

   .. attribute:: COUNT
      :value: 128

      Number of values.

   .. attribute:: PERCENT_GOOD
      :value: 8192

      Percentage of good values.

Usage:

.. code-block:: python

   from pipolars import SummaryType

   # Single summary type
   df = client.summary("TAG", "*-1d", "*", summary_types=SummaryType.AVERAGE)

   # Multiple summary types
   df = client.summary(
       "TAG", "*-1d", "*",
       summary_types=[
           SummaryType.AVERAGE,
           SummaryType.MINIMUM,
           SummaryType.MAXIMUM,
           SummaryType.STD_DEV
       ]
   )

TimestampMode
~~~~~~~~~~~~~

.. autoclass:: pipolars.TimestampMode
   :members:
   :undoc-members:
   :show-inheritance:

   Timestamp handling modes for summary calculations.

   .. attribute:: AUTO
      :value: "auto"

      Automatically determine timestamp placement.

   .. attribute:: START
      :value: "start"

      Use interval start time.

   .. attribute:: END
      :value: "end"

      Use interval end time.

   .. attribute:: MIDDLE
      :value: "middle"

      Use interval midpoint.

DataQuality
~~~~~~~~~~~

.. autoclass:: pipolars.DataQuality
   :members:
   :undoc-members:
   :show-inheritance:

   PI data quality flags.

   .. attribute:: GOOD
      :value: 0

      Value is good and reliable.

   .. attribute:: SUBSTITUTED
      :value: 1

      Value was manually substituted.

   .. attribute:: QUESTIONABLE
      :value: 2

      Value quality is questionable.

   .. attribute:: BAD
      :value: 3

      Value is bad or unreliable.

   .. attribute:: NO_DATA
      :value: 4

      No data available for the requested time.

   .. attribute:: CALC_FAILED
      :value: 5

      Calculation failed to produce a value.

DigitalState
~~~~~~~~~~~~

.. autoclass:: pipolars.core.types.DigitalState
   :members:
   :undoc-members:
   :show-inheritance:

PointType
~~~~~~~~~

.. autoclass:: pipolars.core.types.PointType
   :members:
   :undoc-members:
   :show-inheritance:

BoundaryType
~~~~~~~~~~~~

.. autoclass:: pipolars.core.types.BoundaryType
   :members:
   :undoc-members:
   :show-inheritance:

Schema Definitions
------------------

PIPolars defines Polars schemas for consistent DataFrame structures:

.. data:: PI_VALUE_SCHEMA

   Schema for single-tag value DataFrames:

   .. code-block:: python

      {
          "timestamp": pl.Datetime("us", "UTC"),
          "value": pl.Float64(),
          "quality": pl.Int8(),
      }

.. data:: PI_VALUE_WITH_TAG_SCHEMA

   Schema for multi-tag value DataFrames:

   .. code-block:: python

      {
          "tag": pl.Utf8(),
          "timestamp": pl.Datetime("us", "UTC"),
          "value": pl.Float64(),
          "quality": pl.Int8(),
      }

.. data:: SUMMARY_SCHEMA

   Schema for summary DataFrames:

   .. code-block:: python

      {
          "tag": pl.Utf8(),
          "start": pl.Datetime("us", "UTC"),
          "end": pl.Datetime("us", "UTC"),
          "average": pl.Float64(),
          "minimum": pl.Float64(),
          "maximum": pl.Float64(),
          "total": pl.Float64(),
          "count": pl.Int64(),
          "std_dev": pl.Float64(),
          "percent_good": pl.Float64(),
      }

See Also
--------

- :doc:`../user_guide/time_expressions` - Time expression guide
- :doc:`../user_guide/dataframes` - DataFrame output guide

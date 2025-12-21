API Reference
=============

This section provides complete API documentation for all public classes,
functions, and modules in PIPolars.

Overview
--------

PIPolars is organized into several modules:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Module
     - Description
   * - :mod:`pipolars.api`
     - User-facing API (PIClient, PIQuery)
   * - :mod:`pipolars.core`
     - Core types, configuration, and exceptions
   * - :mod:`pipolars.cache`
     - Caching backends and strategies
   * - :mod:`pipolars.connection`
     - PI Server connectivity
   * - :mod:`pipolars.extraction`
     - Data extraction from PI
   * - :mod:`pipolars.transform`
     - Data conversion to Polars

Quick Reference
---------------

Main Classes
~~~~~~~~~~~~

.. autosummary::
   :toctree: _autosummary
   :nosignatures:

   pipolars.PIClient
   pipolars.PIQuery
   pipolars.PIConfig

Types
~~~~~

.. autosummary::
   :toctree: _autosummary
   :nosignatures:

   pipolars.AFTime
   pipolars.PIValue
   pipolars.RetrievalMode
   pipolars.SummaryType
   pipolars.TimestampMode
   pipolars.DataQuality

Exceptions
~~~~~~~~~~

.. autosummary::
   :toctree: _autosummary
   :nosignatures:

   pipolars.PIPolarsError
   pipolars.PIConnectionError
   pipolars.PIDataError
   pipolars.PIQueryError

Detailed API
------------

.. toctree::
   :maxdepth: 2

   client
   query
   types
   config
   exceptions
   cache

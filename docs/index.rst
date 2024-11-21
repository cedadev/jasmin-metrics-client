.. Jasmin Metrics Client documentation master file, created by
   sphinx-quickstart on Wed Jul 24 15:04:46 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Jasmin Metrics Client documentation
==============================================================================

The Jasmin Metrics Client is a Python library designed to interact with the Elasticsearch of the JASMIN metrics system.
It provides methods to retrieve unique metric names, metric labels, and metric data within specified time ranges and filters. 
To be able to use this library you are supposed to have a token to be able to connect to Elasticsearch.

.. _features:

Features
--------

- Retrieve all unique metric names.
- Retrieve labels associated with a specific metric.
- Retrieve metric data for a specific metric name, optionally filtered by labels and time range.

Methods
-------

``get_all_metrics(size: int = 1000)``
    Retrieves all unique metric names from the Elasticsearch index.

``get_metric_labels(metric_name: str)``
    Retrieves all labels associated with a specific metric name.

``get_metric(metric_name: str, filters: Optional[dict[str, dict[str, str]]] = None, size: int = 10000)``
    Retrieves metric data for a specific metric name, optionally filtered by labels and time range.

Installation
------------

Prerequisites
~~~~~~~~~~~~~

- Python 3.7 or higher

Poetry
~~~~~~

This project uses `Poetry <https://python-poetry.org/>`_ for dependency management and packaging. Follow the steps below to install Poetry and set up the project.

1. **Install Poetry**:

   Follow the `official installation guide <https://python-poetry.org/docs/#installation>`_ for more options.

2. **Clone the repository**:

   .. code-block:: sh

      git clone https://github.com/your-username/jasmin-metrics-client.git
      cd jasmin-metrics-client

3. **Install dependencies**:

   .. code-block:: sh

      poetry install

Usage
-----

Below are examples of how to use the Jasmin Metrics Client.

Initialize the Client
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from jasmin_metrics_client import MetricsClient

   client = MetricsClient("your-api-token")

Retrieve All Metrics
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   metrics = client.get_all_metrics()
   print(metrics)

Retrieve Metric Labels
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   labels = client.get_metric_labels("power_total_inst")
   print(labels)

Retrieve Metric Data
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   filters = {
       "labels": {"rack": "12"},
       "time": {"start": "2024-10-28T05:03:14", "end": "2024-10-28T05:03:14"},
   }
   data = client.get_metric("power_total_inst", filters)
   print(data)

Error Handling
~~~~~~~~~~~~~~

It's important to handle potential errors when using the client. Here's an example:

.. code-block:: python

   try:
       metrics = client.get_all_metrics()
       print(metrics)
   except Exception as e:
       print(f"An error occurred: {e}")

Contributing
------------

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

License
-------

This project is licensed under the MIT License. See the `LICENSE <LICENSE>`_ file for details.

.. mdinclude:: ../README.md

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   security
   contributing
   code_of_conduct
   license
   changelog


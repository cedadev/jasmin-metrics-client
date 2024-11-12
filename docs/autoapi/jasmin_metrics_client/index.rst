jasmin_metrics_client
=====================

.. py:module:: jasmin_metrics_client

.. autoapi-nested-parse::

   jasmin-metrics-client

   A library to do requests to elastic search metric data



Submodules
----------

.. toctree::
   :maxdepth: 1

   /autoapi/jasmin_metrics_client/main/index


Classes
-------

.. autoapisummary::

   jasmin_metrics_client.MetricsClient


Package Contents
----------------

.. py:class:: MetricsClient(token: str)

   .. py:method:: get_all_metrics() -> Optional[List[str]]

      Retrieve all unique metric names from the Elasticsearch index.

      Returns:
          Optional[List[str]]: A list of unique metric names, or empty list if no results.



   .. py:method:: get_metric_labels(metric_name: str) -> Optional[List[str]]

      Retrieve all labels associated with a specific metric name.

      Args:
          metric_name (str): The name of the metric.

      Returns:
          Optional[List[str]]: A list of labels for the metric, or None if an error occurs.



   .. py:method:: get_metric(metric_name: str, filters: Optional[dict[str, dict[str, str]]] = None, size: int = 10000) -> Optional[pandas.DataFrame]

      Retrieve metric data for a specific metric name, optionally filtered by labels and time range.

      Args:
          metric_name (str): The name of the metric.
          filters (Optional[Dict[str, Any]]): Optional filters for labels and time range. Defaults to None.
          size (int): The number of results to retrieve. Defaults to 10000.

      Returns:
          Optional[pd.DataFrame]: A DataFrame containing the metric data, or None if an error occurs.



   .. py:method:: _build_query(s: elasticsearch_dsl.Search, metric_name: str, filters: Optional[dict[str, dict[str, str]]] = None) -> elasticsearch_dsl.Search
      :staticmethod:


      Helper function to build Elasticsearch query.




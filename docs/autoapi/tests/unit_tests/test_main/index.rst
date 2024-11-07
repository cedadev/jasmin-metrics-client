tests.unit_tests.test_main
==========================

.. py:module:: tests.unit_tests.test_main


Classes
-------

.. autoapisummary::

   tests.unit_tests.test_main.TestMetricsClient


Module Contents
---------------

.. py:class:: TestMetricsClient(methodName='runTest')

   Bases: :py:obj:`unittest.TestCase`


   Test class for metrics client


   .. py:method:: test_get_all_metrics(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_all_metrics` to retrieve unique metric names.

      Mocks the Elasticsearch `search` method to return a simulated response with
      a list of metric names. Verifies that:
      - The returned metrics are in the expected list format.
      - Specific metric names are included in the response.
      - The search query was called with expected parameters.



   .. py:method:: test_get_metric_labels(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_metric_labels` to retrieve labels for a specified metric.

      Mocks the Elasticsearch `search` method to return a response containing
      a metric with associated labels. Verifies that:
      - The returned list contains expected labels.
      - The response format is a non-empty list.
      - The search query was called with the correct query structure.



   .. py:method:: test_get_metric(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_metric` to retrieve metric values within a time range and filter.

      Mocks Elasticsearch `search` to return several metric instances for a
      specified metric name and filters. Verifies that:
      - The returned data is a pandas DataFrame with the expected timestamped values.
      - The DataFrame contents match the mock response data.
      - The search query was called with expected parameters for date filtering.



   .. py:method:: test_build_query_invalid_dates() -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient._build_query` method for handling invalid date conditions.

      Tests different scenarios with improperly formatted or invalid dates,
      including:
      - Missing end date.
      - Invalid date format.
      - Start date after the end date.
      - End date in the future.

      Verifies that each invalid condition raises an appropriate error with a
      descriptive message.



   .. py:method:: test_get_all_metrics_no_results(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_all_metrics` handling of no results case.

      Mocks Elasticsearch `search` to return an empty aggregation bucket, simulating
      no metrics found. Verifies that:
      - The returned list is empty when no metrics are available.



   .. py:method:: test_get_metric_labels_no_labels(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_metric_labels` behavior when no labels are available for a metric.

      Mocks Elasticsearch `search` to return an empty list for a non-existent metric,
      simulating no labels. Verifies that:
      - The returned list is empty.



   .. py:method:: test_get_metric_missing_keys(MockElasticsearch: Any) -> None

      Test :meth:`~jasmin_metrics_client.main.MetricsClient.get_metric` when expected metric data is missing from the response.

      Mocks Elasticsearch `search` to return a response with missing metric keys.
      Verifies that:
      - The returned DataFrame is empty when metric values are missing.




jasmin_metrics_client.main
==========================

.. py:module:: jasmin_metrics_client.main


Classes
-------

.. autoapisummary::

   jasmin_metrics_client.main.MetricsClient


Module Contents
---------------

.. py:class:: MetricsClient(token: Optional[str] = None)

   .. py:attribute:: kwargs


   .. py:method:: get_all_metrics() -> Optional[List[str]]


   .. py:method:: get_metric_labels(metric_name: str) -> Optional[List[str]]


   .. py:method:: get_metric(metric_name: str, filters: Optional[Dict[str, Any]] = None, size: int = 10000) -> Optional[pandas.DataFrame]



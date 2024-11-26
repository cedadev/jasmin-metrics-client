import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
from elastic_transport import ObjectApiResponse
from elasticsearch_dsl import Search

from jasmin_metrics_client import MetricsClient


class TestMetricsClient(unittest.TestCase):
    """Test class for metrics client"""

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_all_metrics(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_all_metrics` to retrieve unique metric names.

        Mocks the Elasticsearch `search` method to return a simulated response with
        a list of metric names. Verifies that:
        - The returned metrics are in the expected list format.
        - Specific metric names are included in the response.
        - The search query was called with expected parameters.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={
                "hits": {
                    "total": {"value": 10000, "relation": "gte"},
                    "max_score": None,
                    "hits": [],
                },
                "aggregations": {
                    "unique_metrics": {
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                        "buckets": [
                            {"key": "power_total_inst", "doc_count": 2931142},
                            {"key": "power_total_hour_avg", "doc_count": 2930399},
                            {"key": "storage_SOF_provisioned", "doc_count": 147355},
                            {"key": "storage_SOF_used", "doc_count": 146829},
                        ],
                    },
                },
            },
        )

        client = MetricsClient("token")
        metrics = client.get_all_metrics()
        self.assertIsNotNone(metrics)
        self.assertIsInstance(metrics, list)
        if metrics is not None:
            self.assertIn("power_total_inst", metrics)
            self.assertIn("power_total_hour_avg", metrics)
            self.assertIn("storage_SOF_provisioned", metrics)
            self.assertIn("storage_SOF_used", metrics)

        mock_search.assert_called_once_with(
            index=["jasmin-metrics-production"],
            body={
                "aggs": {
                    "unique_metrics": {
                        "terms": {
                            "field": "prometheus.labels.metric_name.keyword",
                            "size": 1000,
                        },
                    },
                },
            },
        )

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_metric_labels(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_metric_labels` to retrieve labels for a specified metric.

        Mocks the Elasticsearch `search` method to return a response containing
        a metric with associated labels. Verifies that:
        - The returned list contains expected labels.
        - The response format is a non-empty list.
        - The search query was called with the correct query structure.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={
                "hits": {
                    "total": {"value": 10000, "relation": "gte"},
                    "max_score": 0.84420943,
                    "hits": [
                        {
                            "_index": "jasmin-metrics-production",
                            "_type": "_doc",
                            "_id": "D_BKzpIBbfepjNJtMfZ5",
                            "_score": 0.84420943,
                            "_source": {
                                "@timestamp": "2024-10-27T14:03:13Z",
                                "prometheus": {
                                    "metrics": {"power_total_inst": 5200.0},
                                    "labels": {
                                        "metric_name": "power_total_inst",
                                        "pdu": "pdu071.jasmin",
                                        "rack": "12",
                                        "rack_phase": "7",
                                        "rack_role": "Storage",
                                        "rack_tag": "['storage_type:sof']",
                                    },
                                },
                            },
                        },
                    ],
                },
            },
        )

        client = MetricsClient("token")
        labels = client.get_metric_labels("power_total_inst")
        expected_labels = [
            "metric_name",
            "pdu",
            "rack",
            "rack_phase",
            "rack_role",
            "rack_tag",
        ]
        self.assertIsNotNone(labels)
        self.assertIsInstance(labels, list)
        if labels is not None:
            for label in expected_labels:
                self.assertIn(label, labels)
        mock_search.assert_called_once_with(
            index=["jasmin-metrics-production"],
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "match": {
                                    "prometheus.labels.metric_name.keyword": "power_total_inst",
                                },
                            },
                        ],
                    },
                },
                "from": 0,
                "size": 1,
            },
        )

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_metric(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_metric` to retrieve metric values within a time range and filter.

        Mocks Elasticsearch `search` to return several metric instances for a
        specified metric name and filters. Verifies that:
        - The returned data is a pandas DataFrame with the expected timestamped values.
        - The DataFrame contents match the mock response data.
        - The search query was called with expected parameters for date filtering.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={
                "hits": {
                    "total": {"value": 105, "relation": "eq"},
                    "max_score": 4.956992,
                    "hits": [
                        {
                            "_source": {
                                "@timestamp": "2024-10-28T05:03:14Z",
                                "prometheus": {
                                    "metrics": {"power_total_inst": 5100.0},
                                    "labels": {
                                        "metric_name": "power_total_inst",
                                        "rack": "12",
                                        "pdu": "pdu071.jasmin",
                                    },
                                },
                            },
                        },
                        {
                            "_source": {
                                "@timestamp": "2024-10-28T05:03:14Z",
                                "prometheus": {
                                    "metrics": {"power_total_inst": 2670.0},
                                    "labels": {
                                        "metric_name": "power_total_inst",
                                        "rack": "12",
                                        "pdu": "pdu072.jasmin",
                                    },
                                },
                            },
                        },
                        {
                            "_source": {
                                "@timestamp": "2024-10-28T05:03:14Z",
                                "prometheus": {
                                    "metrics": {"power_total_inst": 200.0},
                                    "labels": {
                                        "metric_name": "power_total_inst",
                                        "rack": "12",
                                        "pdu": "pdu073.jasmin",
                                    },
                                },
                            },
                        },
                    ],
                },
            },
        )

        client = MetricsClient("token")
        metric_name = "power_total_inst"
        filters = {
            "labels": {"rack": "12"},
            "time": {"start": "2024-10-28T05:03:14", "end": "2024-10-28T05:03:14"},
        }

        result = client.get_metric(metric_name, filters)
        expected_data = pd.DataFrame(
            [
                {"timestamp": "2024-10-28T05:03:14Z", "value": 5100.0},
                {"timestamp": "2024-10-28T05:03:14Z", "value": 2670.0},
                {"timestamp": "2024-10-28T05:03:14Z", "value": 200.0},
            ],
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, expected_data)
        mock_search.assert_called_once_with(
            index=["jasmin-metrics-production"],
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "prometheus.labels.metric_name.keyword": "power_total_inst",
                                },
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "2024-10-28T05:03:14",
                                        "lte": "2024-10-28T05:03:14",
                                    },
                                },
                            },
                        ],
                        "must": [{"term": {"prometheus.labels.rack.keyword": "12"}}],
                    },
                },
                "size": 10000,
            },
        )

    # Tests for edge cases
    def test_build_query_invalid_dates(self) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient._build_query` method for handling invalid date conditions.

        Tests different scenarios with improperly formatted or invalid dates,
        including:
        - Missing end date .
        - Invalid date format.
        - Start date after the end date.
        - End date in the future.

        Verifies that each invalid condition raises an appropriate error with a
        descriptive message.
        """
        s = Search()
        # Test case 1: Missing 'end' date
        try:
            MetricsClient._build_query(
                s,
                "power_total_inst",
                {
                    "time": {"start": "2024-10-28T05:03:14"},
                },
            )
        except Exception as e:
            self.assertTrue(
                str(e).__contains__(
                    "Both 'start' and 'end' ISO-formatted dates must be provided in 'time' filter",
                ),
            )

        # Test case 2: Invalid date format
        try:
            MetricsClient._build_query(
                s,
                "power_total_inst",
                {
                    "time": {"start": "invalid-date", "end": "2024-10-28T05:03:14"},
                },
            )
        except Exception as e:
            self.assertTrue(
                str(e).__contains__(
                    "Dates must be in ISO format (YYYY-MM-DDTHH:MM:SS) for 'start' and 'end'",
                ),
            )

        # Test case 3: Start date is after end date
        try:
            MetricsClient._build_query(
                s,
                "power_total_inst",
                {
                    "time": {
                        "start": "2024-10-28T05:03:15",
                        "end": "2024-10-28T05:03:14",
                    },
                },
            )
        except Exception as e:
            self.assertTrue(
                str(e).__contains__(
                    "The 'start' date must be before the 'end' date or be the same",
                ),
            )

        # Test case 4: End date is in the future
        future_date = (datetime.now() + timedelta(days=1)).isoformat()
        try:
            MetricsClient._build_query(
                s,
                "power_total_inst",
                {
                    "time": {"start": "2024-10-28T05:03:14", "end": future_date},
                },
            )
        except Exception as e:
            self.assertTrue(
                str(e).__contains__("The 'end' date cannot be in the future"),
            )

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_all_metrics_no_results(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_all_metrics` handling of no results case.

        Mocks Elasticsearch `search` to return an empty aggregation bucket, simulating
        no metrics found. Verifies that:
        - The returned list is empty when no metrics are available.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={
                "hits": {
                    "total": {"value": 10000, "relation": "gte"},
                    "max_score": None,
                    "hits": [],
                },
                "aggregations": {"unique_metrics": {"buckets": []}},
            },
        )
        client = MetricsClient("token")
        metrics = client.get_all_metrics()
        self.assertEqual(metrics, [])

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_metric_labels_no_labels(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_metric_labels` behavior when no labels are available for a metric.

        Mocks Elasticsearch `search` to return an empty list for a non-existent metric,
        simulating no labels. Verifies that:
        - The returned list is empty.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={"hits": {"hits": []}},
        )

        client = MetricsClient("token")
        labels = client.get_metric_labels("non_existent_metric")
        self.assertEqual(labels, [])

    @patch("jasmin_metrics_client.main.Elasticsearch")
    def test_get_metric_missing_keys(self, mock_elasticsearch: Mock) -> None:
        """Test py:meth:`~jasmin_metrics_client.main.MetricsClient.get_metric` when expected metric data is missing from the response.

        Mocks Elasticsearch `search` to return a response with missing metric keys.
        Verifies that:
        - The returned DataFrame is empty when metric values are missing.
        """
        mock_search = mock_elasticsearch.return_value.search
        mock_search.return_value = ObjectApiResponse(
            meta=None,
            body={"hits": {"hits": [{"_source": {"prometheus": {"metrics": {}}}}]}},
        )

        client = MetricsClient("token")
        result = client.get_metric("power_total_inst")
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        if result is not None:
            self.assertTrue(result.empty)

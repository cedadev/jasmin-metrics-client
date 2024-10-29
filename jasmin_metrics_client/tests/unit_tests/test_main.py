import unittest
from typing import Any, List
from unittest.mock import patch

import pandas as pd

from jasmin_metrics_client.main import MetricsClient


class TestMetricsClient(unittest.TestCase):
    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient.search")
    def test_get_all_metrics(self, mock_search: Any) -> None:
        mock_search.return_value = {
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
                }
            }
        }

        client = MetricsClient()
        metrics = client.get_all_metrics()

        metrics = metrics or []
        self.assertIsInstance(metrics, list)

        self.assertIn("power_total_inst", metrics)
        self.assertIn("power_total_hour_avg", metrics)
        self.assertIn("storage_SOF_provisioned", metrics)
        self.assertIn("storage_SOF_used", metrics)
        mock_search.assert_called_once_with(
            index="jasmin-metrics-production",
            query={
                "aggs": {
                    "unique_metrics": {
                        "terms": {
                            "field": "prometheus.labels.metric_name.keyword",
                            "size": 1000,
                        }
                    }
                },
                "size": 0,
            },
        )

    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient.search")
    def test_get_metric_labels(self, mock_search: Any) -> None:
        mock_search.return_value = {
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
                    }
                ],
            }
        }

        client = MetricsClient()
        labels: List[str] = client.get_metric_labels("power_total_inst") or []
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
        for label in expected_labels:
            self.assertIn(label, labels)
        mock_search.assert_called_once_with(
            index="jasmin-metrics-production",
            query={
                "query": {
                    "match": {
                        "prometheus.labels.metric_name.keyword": "power_total_inst"
                    }
                },
                "size": 1,
            },
        )

    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient.search")
    def test_get_metric(self, mock_search: Any) -> None:
        mock_search.return_value = {
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
            }
        }

        client = MetricsClient()
        metric_name = "power_total_inst"
        filters = {
            "labels": {"rack": "12"},
            "time": {"start": "now-1d/d", "end": "now"},
        }

        result = client.get_metric(metric_name, filters)
        expected_data = pd.DataFrame(
            [
                {"timestamp": "2024-10-28T05:03:14Z", "value": 5100.0},
                {"timestamp": "2024-10-28T05:03:14Z", "value": 2670.0},
                {"timestamp": "2024-10-28T05:03:14Z", "value": 200.0},
            ]
        )
        self.assertIsNotNone(result)
        pd.testing.assert_frame_equal(result, expected_data)
        mock_search.assert_called_once_with(
            index="jasmin-metrics-production",
            query={
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "prometheus.labels.metric_name.keyword": "power_total_inst"
                                }
                            },
                            {"match": {"prometheus.labels.rack.keyword": "12"}},
                        ],
                        "filter": [
                            {"range": {"@timestamp": {"gte": "now-1d/d", "lte": "now"}}}
                        ],
                    }
                },
            },
        )

    if __name__ == "__main__":
        unittest.main()

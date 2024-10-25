import unittest
from unittest.mock import patch

import pandas as pd

from jasmin_metrics_client.main import MetricsClient


class TestMetricsClient(unittest.TestCase):

    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient")
    def test_get_all_metrics(self, mock_es_client):
        mock_es_client().search.return_value = {
            "aggregations": {
                "unique_metrics": {
                    "buckets": [{"key": "metric_1"}, {"key": "metric_2"}]
                }
            }
        }

        client = MetricsClient()
        metrics = client.get_all_metrics()

        self.assertIsInstance(metrics, list)
        self.assertIn("metric_1", metrics)
        self.assertIn("metric_2", metrics)

    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient")
    def test_get_metric_labels(self, mock_es_client):
        mock_es_client().search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "prometheus": {
                                "labels": {"label_1": "val_1", "label_2": "val_2"}
                            }
                        }
                    },
                    {"_source": {"prometheus": {"labels": {"label_3": "val_3"}}}},
                ]
            }
        }

        client = MetricsClient()
        labels = client.get_metric_labels("storage_tape_provisioned")

        self.assertIsInstance(labels, list)
        self.assertIn("label_1", labels)
        self.assertIn("label_2", labels)
        self.assertIn("label_3", labels)

    @patch("ceda_elasticsearch_tools.CEDAElasticsearchClient")
    def test_get_metric(self, mock_es_client):
        mock_es_client().search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "@timestamp": "2024-09-01T00:00:00Z",
                            "prometheus": {
                                "metrics": {"storage_tape_provisioned": 100}
                            },
                        }
                    },
                    {
                        "_source": {
                            "@timestamp": "2024-09-02T00:00:00Z",
                            "prometheus": {
                                "metrics": {"storage_tape_provisioned": 200}
                            },
                        }
                    },
                ]
            }
        }

        client = MetricsClient()
        df = client.get_metric("storage_tape_provisioned")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df["value"].iloc[0], 100)
        self.assertEqual(df["value"].iloc[1], 200)


if __name__ == "__main__":
    unittest.main()

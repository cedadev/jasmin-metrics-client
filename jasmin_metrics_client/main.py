import logging

import pandas as pd
from ceda_elasticsearch_tools import CEDAElasticsearchClient
from elasticsearch import ElasticsearchException


class MetricsClient:
    def __init__(self, token=None, hosts=None):
        try:
            self.es = CEDAElasticsearchClient(
                headers={"x-api-key": token} if token else {}, hosts=hosts
            )
            logging.info("Elasticsearch client initialized successfully.")
        except ElasticsearchException as e:
            logging.error(f"Error initializing Elasticsearch client: {str(e)}")
            raise e

    def get_all_metrics(self):
        try:
            query = {
                "aggs": {
                    "unique_metrics": {
                        "terms": {
                            "field": "prometheus.labels.metric_name.keyword",
                            "size": 1000,
                        }
                    }
                },
                "size": 0,
            }
            response = self.es.search(index="jasmin-metrics-production", body=query)
            return [
                bucket["key"]
                for bucket in response["aggregations"]["unique_metrics"]["buckets"]
            ]
        except ElasticsearchException as e:
            logging.error(f"Error fetching all metrics: {str(e)}")
            return None

    def get_metric_labels(self, metric_name):
        try:
            # Get labels for a specific metric
            query = {
                "query": {
                    "match": {"prometheus.labels.metric_name.keyword": metric_name}
                }
            }
            response = self.es.search(index="jasmin-metrics-production", body=query)
            if not response["hits"]["hits"]:
                logging.info(f"No labels found for metric: {metric_name}")
                return []
            # Extract unique labels from the hits
            labels = set()
            for hit in response["hits"]["hits"]:
                labels.update(hit["_source"]["prometheus"]["labels"].keys())
            return list(labels)
        except ElasticsearchException as e:
            logging.error(f"Error fetching metric labels for {metric_name}: {str(e)}")
            return None

    def get_metric(self, metric_name, filters=None):
        try:
            # Construct the base query
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "prometheus.labels.metric_name.keyword": metric_name
                                }
                            }
                        ]
                    }
                }
            }

            # Apply filters if provided
            if filters:
                if "labels" in filters:
                    for key, value in filters["labels"].items():
                        query["query"]["bool"]["must"].append(
                            {"match": {f"prometheus.labels.{key}.keyword": value}}
                        )
                if "time" in filters:
                    time_range = filters["time"]
                    query["query"]["bool"]["filter"] = [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": time_range.get("start", "now-1d/d"),
                                    "lte": time_range.get("end", "now/d"),
                                }
                            }
                        }
                    ]

            response = self.es.search(index="jasmin-metrics-production", body=query)

            # Convert the response to a Pandas DataFrame
            data = []
            for hit in response["hits"]["hits"]:
                timestamp = hit["_source"]["@timestamp"]
                value = hit["_source"]["prometheus"]["metrics"].get(metric_name)
                data.append({"timestamp": timestamp, "value": value})

            return pd.DataFrame(data)
        except ElasticsearchException as e:
            logging.error(f"Error fetching metric {metric_name}: {str(e)}")
            return None


# Example usage (just for demonstration, not part of the client):
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    metrics_client = MetricsClient(token="your_api_key")

    # Get all metrics
    metrics = metrics_client.get_all_metrics()
    if metrics:
        print("Available Metrics:", metrics)

    # Get labels for a specific metric
    labels = metrics_client.get_metric_labels("storage_tape_provisioned")
    if labels:
        print("Labels for storage_tape_provisioned:", labels)

    # Get metric data with filters
    df = metrics_client.get_metric(
        "storage_tape_provisioned",
        filters={
            "labels": {"consortium": "atmos"},
            "time": {"start": "2024-09-01T00:00:00Z", "end": "latest"},
        },
    )
    if df is not None:
        print(df)

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from ceda_elasticsearch_tools import CEDAElasticsearchClient
from elasticsearch import ElasticsearchException


class MetricsClient:
    def __init__(self, token: Optional[str] = None) -> None:
        kwargs = {}
        if token:
            kwargs["headers"] = {"Authorization": f"Bearer {token}"}
        try:
            self.es = CEDAElasticsearchClient(**kwargs)
            logging.info("Elasticsearch client initialized successfully.")
        except ElasticsearchException as e:
            logging.error(f"Error initializing Elasticsearch client: {str(e)}")
            raise e

    def get_all_metrics(self) -> Optional[List[str]]:
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
            response = self.es.search(index="jasmin-metrics-production", query=query)
            return [
                bucket["key"]
                for bucket in response["aggregations"]["unique_metrics"]["buckets"]
            ]
        except ElasticsearchException as e:
            logging.error(f"Error fetching all metrics: {str(e)}")
            return None

    def get_metric_labels(self, metric_name: str) -> Optional[List[str]]:
        try:
            query = {
                "query": {
                    "match": {"prometheus.labels.metric_name.keyword": metric_name}
                },
                "size": 1,
            }
            response = self.es.search(index="jasmin-metrics-production", query=query)
            if not response["hits"]["hits"]:
                logging.info(f"No labels found for metric: {metric_name}")
                return []

            labels = set()
            for hit in response["hits"]["hits"]:
                labels.update(hit["_source"]["prometheus"]["labels"].keys())
            return list(labels)
        except ElasticsearchException as e:
            logging.error(f"Error fetching metric labels for {metric_name}: {str(e)}")
            return None

    def get_metric(
        self,
        metric_name: str,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 10000,
    ) -> Optional[pd.DataFrame]:
        try:

            query: Dict[str, Any] = {
                "size": size,
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
                },
            }

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

            response = self.es.search(index="jasmin-metrics-production", query=query)

            data: List[Dict[str, Union[str, float]]] = []
            for hit in response["hits"]["hits"]:
                timestamp = hit["_source"]["@timestamp"]
                value = hit["_source"]["prometheus"]["metrics"].get(metric_name)
                data.append({"timestamp": timestamp, "value": value})

            return pd.DataFrame(data)
        except ElasticsearchException as e:
            logging.error(f"Error fetching metric {metric_name}: {str(e)}")
            return None

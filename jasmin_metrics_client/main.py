import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from elasticsearch import Elasticsearch, ElasticsearchWarning


class MetricsClient:
    def __init__(self, token: Optional[str] = None) -> None:
        """
        Initialize the MetricsClient with an optional token for authentication.

        Args:
            token (Optional[str]): The authentication token. Defaults to None.
        """
        hosts = [f"es{i}.ceda.ac.uk:9200" for i in range(1, 9)]

        # Define headers if token is provided
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        if not token:
            logging.error("No authentication token provided.")

        try:
            # Initialize the Elasticsearch client without the `use_ssl` parameter
            self.es = Elasticsearch(
                hosts=hosts,
                headers=headers,
                # ca_certs="path/to/CA_ROOT"
            )
            logging.info("Elasticsearch client initialized successfully.")
        except ElasticsearchWarning as e:
            logging.error(f"Error initializing Elasticsearch client: {str(e)}")
            raise e
        except Exception as e:
            logging.error(
                f"Unexpected error initializing Elasticsearch client: {str(e)}"
            )
            raise e

    def get_all_metrics(self) -> Optional[List[str]]:
        """
        Retrieve all unique metric names from the Elasticsearch index.

        Returns:
            Optional[List[str]]: A list of unique metric names, or None if an error occurs.
        """
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
        response = None
        try:
            response = self.es.search(index="jasmin-metrics-production", query=query)
        except ElasticsearchWarning as e:
            logging.error(f"Error fetching all metrics: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching all metrics: {str(e)}")
            return None
        if (
            "aggregations" not in response
            or "unique_metrics" not in response["aggregations"]
        ):
            logging.error(
                "Unexpected response structure: missing 'aggregations' or 'unique_metrics'"
            )
            return None
        return [
            bucket["key"]
            for bucket in response["aggregations"]["unique_metrics"]["buckets"]
        ]

    def get_metric_labels(self, metric_name: str) -> Optional[List[str]]:
        """
        Retrieve all labels associated with a specific metric name.

        Args:
            metric_name (str): The name of the metric.

        Returns:
            Optional[List[str]]: A list of labels for the metric, or None if an error occurs.
        """
        response = None
        query = {
            "query": {"match": {"prometheus.labels.metric_name.keyword": metric_name}},
            "size": 1,
        }
        try:
            response = self.es.search(index="jasmin-metrics-production", query=query)

        except ElasticsearchWarning as e:
            logging.error(f"Error fetching metric labels for {metric_name}: {str(e)}")
            return None

        if "hits" not in response or "hits" not in response["hits"]:
            logging.error("Unexpected response structure: missing 'hits'")
            return None
        if not response["hits"]["hits"]:
            logging.info(f"No labels found for metric: {metric_name}")
            return []
        labels = set()
        for hit in response["hits"]["hits"]:
            labels.update(hit["_source"]["prometheus"]["labels"].keys())
        return list(labels)

    def get_metric(
        self,
        metric_name: str,
        filters: Optional[dict[str, dict[str, str]]] = None,
        size: int = 10000,
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve metric data for a specific metric name, optionally filtered by labels and time range.

        Args:
            metric_name (str): The name of the metric.
            filters (Optional[Dict[str, Any]]): Optional filters for labels and time range. Defaults to None.
            size (int): The number of results to retrieve. Defaults to 10000.

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the metric data, or None if an error occurs.
        """
        query = self._build_query(metric_name, filters, size)
        response = None
        try:
            response = self.es.search(index="jasmin-metrics-production", query=query)
        except ElasticsearchWarning as e:
            logging.error(f"Error fetching metric {metric_name}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching metric {metric_name}: {str(e)}")
            return None
        if "hits" not in response or "hits" not in response["hits"]:
            logging.error("Unexpected response structure: missing 'hits'")
            return None
        data: List[Dict[str, Union[str, float]]] = []
        for hit in response["hits"]["hits"]:
            if "_source" not in hit or "@timestamp" not in hit["_source"]:
                logging.error(
                    "Unexpected response structure: missing '_source' or '@timestamp' in a hit"
                )
                return None
            timestamp = hit["_source"]["@timestamp"]
            value = hit["_source"]["prometheus"]["metrics"].get(metric_name)
            data.append({"timestamp": timestamp, "value": value})
        return pd.DataFrame(data)

    @staticmethod
    def _build_query(
        metric_name: str,
        filters: Optional[dict[str, dict[str, str]]] = None,
        size: int = 10000,
    ) -> Dict[str, Any]:
        """Helper function to build Elasticsearch query."""
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
                    ],
                    "filter": [],
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
                if "start" not in time_range or "end" not in time_range:
                    raise ValueError(
                        "Both 'start' and 'end' ISO-formatted dates must be provided in 'time' filter."
                    )
                try:
                    start_date = datetime.fromisoformat(
                        time_range["start"].replace("Z", "")
                    )
                    end_date = datetime.fromisoformat(
                        time_range["end"].replace("Z", "")
                    )
                except ValueError as e:
                    raise ValueError(
                        "Dates must be in ISO format (YYYY-MM-DDTHH:MM:SS) for 'start' and 'end'"
                    ) from e
                if start_date > end_date:
                    raise ValueError(
                        "The 'start' date must be before the 'end' date or be the same."
                    )

                if end_date > datetime.now():
                    raise ValueError("The 'end' date cannot be in the future.")

                query["query"]["bool"]["filter"].append(
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_range["start"],
                                "lte": time_range["end"],
                            }
                        }
                    }
                )
        return query

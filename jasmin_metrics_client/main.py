import logging
from datetime import datetime
from typing import Dict, List, Mapping, Optional

import pandas as pd
from elastic_transport import ApiError
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Response, Search
from elasticsearch_dsl.response import Hit


class MetricsClient:
    def __init__(self, token: str) -> None:
        """
        Initialize the MetricsClient with an optional token for authentication.

        Args:
            token (Optional[str]): The authentication token.
        """
        hosts = ["https://elasticsearch.ceda.ac.uk"]
        headers: Mapping[str, str] = {"x-api-key": token}
        headers = {key: value for key, value in headers.items() if value is not None}

        try:
            self.es = Elasticsearch(
                hosts=hosts,
                headers=headers,
                timeout=30,
                max_retries=5,
                retry_on_timeout=True,
            )
            self.search = Search(using=self.es)
            logging.info("Elasticsearch client initialized successfully.")
        except ApiError as e:
            raise ApiError(
                message=f"Unexpected error initializing Elasticsearch client: {str(e)}",
                meta=e.meta,
                body=e.body,
            )

    def get_all_metrics(self) -> Optional[List[str]]:
        """
        Retrieve all unique metric names from the Elasticsearch index.

        Returns:
            Optional[List[str]]: A list of unique metric names, or empty list if no results.
        """
        response: Response[Hit]
        try:
            s = self.search.index("jasmin-metrics-production")
            s.aggs.bucket(
                "unique_metrics",
                "terms",
                field="prometheus.labels.metric_name.keyword",
                size=1000,
            )
            response = s.execute()
        except ApiError as e:
            raise ApiError(
                message=f"Unexpected error fetching all metrics: {str(e)}",
                meta=e.meta,
                body=e.body,
            )
        if not response.aggregations.unique_metrics.buckets:
            logging.info("No matrics found")
            return []

        res = [buckets.key for buckets in response.aggregations.unique_metrics.buckets]
        return res

    def get_metric_labels(self, metric_name: str) -> Optional[List[str]]:
        """
        Retrieve all labels associated with a specific metric name.

        Args:
            metric_name (str): The name of the metric.

        Returns:
            Optional[List[str]]: A list of labels for the metric, or None if an error occurs.
        """
        response: Response[Hit]

        try:
            s = self.search.index("jasmin-metrics-production")
            s = s.filter(
                "match", **{"prometheus.labels.metric_name.keyword": metric_name}
            )
            response = s[0:1].execute()
        except ApiError as e:
            raise ApiError(
                message=f"Unexpected error fetching metric {metric_name}: {str(e)}",
                meta=e.meta,
                body=e.body,
            )
        if not response.hits:
            logging.info(f"No labels found for metric: {metric_name}")
            return []
        labels = set()
        for hit in response:
            labels.update(hit.prometheus.labels.keys())
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

        response: Response[Hit]
        try:
            s = self.search.index("jasmin-metrics-production")
            s = self._build_query(s, metric_name, filters)
            s = s[:size]
            response = s.execute()
        except ApiError as e:
            raise ApiError(
                message=f"Unexpected error fetching metric {metric_name}: {str(e)}",
                meta=e.meta,
                body=e.body,
            )
        if not response.hits:
            logging.info(f"No data found for metric: {metric_name}")
            return pd.DataFrame()

        data: List[Dict[str, float]] = []
        for hit in response.hits:
            if "@timestamp" not in hit or "prometheus" not in hit:
                logging.error(
                    "Unexpected response structure: missing '@timestamp' or metric data"
                )
                return pd.DataFrame()

            timestamp = hit["@timestamp"]
            value = float(hit["prometheus"]["metrics"][metric_name])
            data.append({"timestamp": timestamp, "value": value})

        return pd.DataFrame(data)

    @staticmethod
    def _build_query(
        s: Search, metric_name: str, filters: Optional[dict[str, dict[str, str]]] = None
    ) -> Search:
        """Helper function to build Elasticsearch query."""
        s = s.filter("term", **{"prometheus.labels.metric_name.keyword": metric_name})
        if filters:
            if "labels" in filters:
                for key, value in filters["labels"].items():
                    s = s.query("term", **{f"prometheus.labels.{key}.keyword": value})

            if "time" in filters:
                time_range = filters["time"]
                if "start" not in time_range or "end" not in time_range:
                    raise ValueError(
                        "Both 'start' and 'end' ISO-formatted dates must be provided in 'time' filter"
                    )
                try:
                    start_date = datetime.fromisoformat(time_range["start"])
                    end_date = datetime.fromisoformat(time_range["end"])
                except ValueError as e:
                    raise ValueError(
                        "Dates must be in ISO format (YYYY-MM-DDTHH:MM:SS) for 'start' and 'end'"
                    ) from e
                if start_date > end_date:
                    raise ValueError(
                        "The 'start' date must be before the 'end' date or be the same"
                    )

                if end_date > datetime.now():
                    raise ValueError("The 'end' date cannot be in the future")

                s = s.filter(
                    "range",
                    **{
                        "@timestamp": {
                            "gte": time_range["start"],
                            "lte": time_range["end"],
                        }
                    },
                )

        return s

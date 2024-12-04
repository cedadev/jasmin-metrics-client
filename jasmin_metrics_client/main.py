"""Main module for the Jasmin Metrics Client."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

from datetime import datetime, timezone

import pandas as pd
from elastic_transport import ApiError
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Response, Search

if TYPE_CHECKING:
    from elasticsearch_dsl.response import Hit


class MetricsClient:
    """A client for fetching metrics data from Elasticsearch."""

    def __init__(self, token: str) -> None:
        """Initialize the MetricsClient with an optional token for authentication.

        Args:
        ----
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
            error_message = "Elasticsearch client initialized successfully."
            logging.debug(error_message)
        except ApiError as err:
            raise ApiError(
                message=f"Unexpected error initializing Elasticsearch client: {err!s}",
                meta=err.meta,
                body=err.body,
            ) from err

    def get_all_metrics(self, size: int = 1000) -> list[str]:
        """Retrieve all unique metric names from the Elasticsearch index.

        Returns
        -------
            Optional[List[str]]: A list of unique metric names, or empty list if no results.

        """
        response: Response[Hit]
        try:
            search = self.search.index("jasmin-metrics-production")
            search.aggs.bucket(
                "unique_metrics",
                "terms",
                field="prometheus.labels.metric_name.keyword",
                size=size,
            )
            response = search.execute()
        except ApiError as err:
            raise ApiError(
                message=f"Unexpected error fetching all metrics: {err!s}",
                meta=err.meta,
                body=err.body,
            ) from err
        if not response.aggregations.unique_metrics.buckets:
            error_message = "No metrics found"
            logging.debug(error_message)
            return []

        return [buckets.key for buckets in response.aggregations.unique_metrics.buckets]

    def get_metric_labels(self, metric_name: str) -> list[str]:
        """Retrieve all labels associated with a specific metric name.

        Args:
        ----
            metric_name (str): The name of the metric.

        Returns:
        -------
            Optional[List[str]]: A list of labels for the metric, or None if an error occurs.

        """
        response: Response[Hit]

        try:
            search = self.search.index("jasmin-metrics-production")
            search = search.filter(
                "match",
                **{"prometheus.labels.metric_name.keyword": metric_name},
            )
            response = search[0:1].execute()
        except ApiError as err:
            raise ApiError(
                message=f"Unexpected error fetching metric {metric_name}: {err!s}",
                meta=err.meta,
                body=err.body,
            ) from err
        if not response.hits:
            error_message = f"No data found for metric: {metric_name}"
            logging.debug(error_message)
            return []
        labels = set()
        for hit in response:
            labels.update(hit.prometheus.labels.keys())
        return list(labels)

    def get_metric(
        self,
        metric_name: str,
        filters: dict[str, dict[str, str]] | None = None,
        size: int = 10000,
    ) -> pd.DataFrame:
        """Retrieve metric data for a specific metric name, optionally filtered by labels and time range.

        Args:
        ----
            metric_name (str): The name of the metric.
            filters (Optional[Dict[str, Any]]): Optional filters for labels and time range. Defaults to None.
            size (int): The number of results to retrieve. Defaults to 10000.

        Returns:
        -------
            Optional[pd.DataFrame]: A DataFrame containing the metric data, or None if an error occurs.

        """
        response: Response[Hit]
        try:
            search = self.search.index("jasmin-metrics-production")
            # noinspection PyTypeChecker
            search = self._build_query(search, metric_name, filters)
            search = search[:size]
            response = search.execute()
        except ApiError as err:
            raise ApiError(
                message=f"Unexpected error fetching metric {metric_name}: {err!s}",
                meta=err.meta,
                body=err.body,
            ) from err
        if not response.hits:
            error_message = f"No data found for metric: {metric_name}"
            logging.debug(error_message)
            return pd.DataFrame()

        data: list[dict[str, float]] = []
        for hit in response.hits:
            if "@timestamp" not in hit or "prometheus" not in hit:
                error_message = (
                    "Unexpected response structure: missing '@timestamp' or metric data"
                )
                logging.debug(error_message)
                return pd.DataFrame()

            timestamp = hit["@timestamp"]
            value = float(hit["prometheus"]["metrics"][metric_name])
            data.append({"timestamp": timestamp, "value": value})

        return pd.DataFrame(data)

    @staticmethod
    def _build_query(
        search: Search,
        metric_name: str,
        filters: dict[str, dict[str, str]] | None = None,
    ) -> Search:
        """Build an Elasticsearch query."""
        search = search.filter(
            "term",
            **{"prometheus.labels.metric_name.keyword": metric_name},
        )
        if filters:
            if "labels" in filters:
                for key, value in filters["labels"].items():
                    search = search.query(
                        "term",
                        **{f"prometheus.labels.{key}.keyword": value},
                    )

            if "time" in filters:
                time_range = filters["time"]
                if "start" not in time_range or "end" not in time_range:
                    error_message = "Both 'start' and 'end' ISO-formatted dates must be provided in 'time' filter"
                    raise ValueError(error_message)
                try:
                    start_date = datetime.fromisoformat(time_range["start"])
                    end_date = datetime.fromisoformat(time_range["end"])
                except ValueError as err:
                    error_message = "Dates must be in ISO format (YYYY-MM-DDTHH:MM:SS) for 'start' and 'end'"
                    raise ValueError(error_message) from err
                if start_date > end_date:
                    error_message = (
                        "The 'start' date must be before the 'end' date or be the same"
                    )
                    raise ValueError(error_message)

                if end_date > datetime.now(timezone.utc).replace(tzinfo=None):
                    error_message = "The 'end' date cannot be in the future"
                    raise ValueError(error_message)

                search = search.filter(
                    "range",
                    **{
                        "@timestamp": {
                            "gte": time_range["start"],
                            "lte": time_range["end"],
                        },
                    },
                )

        return search

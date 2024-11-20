# Jasmin Metrics Client

The Jasmin Metrics Client is a Python library designed to interact with the Elasticsearch  of the JASMIN metrics system. It provides methods to retrieve unique metric names, metric labels, and metric data within specified time ranges and filters. To be able to use this library you are supposed to have a token to be able to connect to Elasticsearch.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Initialize the Client](#initialize-the-client)
  - [Retrieve All Metrics](#retrieve-all-metrics)
  - [Retrieve Metric Labels](#retrieve-metric-labels)
  - [Retrieve Metric Data](#retrieve-metric-data)
- [Error Handling](#error-handling)
- [Contributing](#contributing)
- [License](#license)

## Features
- Retrieve all unique metric names.
- Retrieve labels associated with a specific metric.
- Retrieve metric data for a specific metric name, optionally filtered  by labels and time range.

## Methods

### `get_all_metrics(size: int = 1000)`
Retrieves all unique metric names from the Elasticsearch index.

### `get_metric_labels(metric_name: str)`
Retrieves all labels associated with a specific metric name.

### `get_metric(metric_name: str, filters: Optional[dict[str, dict[str, str]]] = None, size: int = 10000)`
Retrieves metric data for a specific metric name, optionally filtered by labels and time range.

## Installation

### Prerequisites

- Python 3.7 or higher

### Poetry

This project uses [Poetry](https://python-poetry.org/) for dependency management and packaging. Follow the steps below to install Poetry and set up the project.

1. **Install Poetry**:

   Follow the [official installation guide](https://python-poetry.org/docs/#installation) for more options.

2. **Clone the repository**:

    ```sh
    git clone https://github.com/your-username/jasmin-metrics-client.git
    cd jasmin-metrics-client
    ```

3. **Install dependencies**:

    ```sh
    poetry install
    ```

## Usage

Below are examples of how to use the Jasmin Metrics Client.

### Initialize the Client

   ```python
   from jasmin_metrics_client import MetricsClient

   client = MetricsClient("your-api-token")
   ```

### Retrieve All Metrics
   ```python
   metrics = client.get_all_metrics()
   print(metrics)
   ```

### Retrieve Metric Labels

   ```python
   labels = client.get_metric_labels("power_total_inst")
   print(labels)
   ```
### Retrieve Metric Data

   ```python
   filters = {
    "labels": {"rack": "12"},
    "time": {"start": "2024-10-28T05:03:14", "end": "2024-10-28T05:03:14"},
   }
   data = client.get_metric("power_total_inst", filters)
   print(data)
   ```

### Error Handling

   It's important to handle potential errors when using the client. Here's an example:

   ```python
   try:
       metrics = client.get_all_metrics()
       print(metrics)
   except Exception as e:
       print(f"An error occurred: {e}")
   ```

### Contributing

   Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

### License

   This project is licensed under the MIT License. See the LICENSE file for details.
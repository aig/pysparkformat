# PySpark Data Source Formats

This project provides a collection of custom data source formats for Apache Spark 4.0+ and Databricks,
leveraging the new V2 data source PySpark API.

---

<p>
    <a href="https://pysparkformat.readthedocs.io/en/latest/?badge=latest">
        <img src="https://img.shields.io/readthedocs/pysparkformat?style=for-the-badge" alt="Documentation Status"/>
    </a>
    <a href="https://pypi.org/project/pysparkformat/">
        <img src="https://img.shields.io/pypi/v/pysparkformat?color=green&amp;style=for-the-badge" alt="Latest Python Release"/>
    </a>
</p>

---


## Formats

Currently, the following formats are supported:

| Format      | Read | Write | Description                                          |
|-------------|------|-------|------------------------------------------------------|
| `http-csv`  | Yes  | No    | Reads CSV files in parallel directly from a URL.     |
| `http-json` | Yes  | No    | Reads JSON Lines in parallel directly from a URL.    |
| `http-page` | Yes  | No    | Parses HTML page, extracts links, fetches headers.   |

📖 **[Full Documentation](https://pysparkformat.readthedocs.io/en/latest/)** — detailed options, examples, and API reference.

## Installation

```bash
# Install PySpark 4.0.0
pip install pyspark==4.0.0

# Install the package using pip
pip install pysparkformat
```

For Databricks, install within a Databricks notebook using:

```bash
%pip install pysparkformat
```
This has been tested with Databricks Runtime 15.4 LTS and later.


## Quick Start

### `http-csv`

```python
from pyspark.sql import SparkSession
from pysparkformat.http.csv import HTTPCSVDataSource

spark = SparkSession.builder.appName("example").getOrCreate()
spark.dataSource.register(HTTPCSVDataSource)

url = "https://example.com/data.csv"
df = spark.read.format("http-csv").option("header", True).load(url)
df.show()
```

### `http-json`

```python
from pysparkformat.http.json import HTTPJSONDataSource

spark.dataSource.register(HTTPJSONDataSource)

url = "https://example.com/data.jsonl"
df = spark.read.format("http-json").schema("name string, value int").load(url)
df.show()
```

### `http-page`

```python
from pysparkformat.http.page import HTTPPageDataSource

spark.dataSource.register(HTTPPageDataSource)

# Parse page for all links and get headers via HEAD requests
df = spark.read.format("http-page").load("https://example.com/")
df.show()  # columns: page_request_url, page_request_datetime, page_request_error, page_response_http_status, page_response_headers, page_backlink_url
```

## Contribute

Contributions are welcome!
We encourage the addition of new custom data source formats and improvements to existing ones.

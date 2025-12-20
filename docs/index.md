# pysparkformat

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


## `http-csv`

The `http-csv` format reads CSV files in parallel directly from a URL using HTTP range requests.

### Options

| Name            | Description                                           | Type    | Default   |
|-----------------|-------------------------------------------------------|---------|-----------|
| `header`        | Indicates whether the CSV file contains a header row. | boolean | `false`   |
| `sep`           | The field delimiter character.                        | string  | `,`       |
| `encoding`      | The character encoding of the file.                   | string  | `utf-8`   |
| `quote`         | The quote character.                                  | string  | `"`       |
| `escape`        | The escape character.                                 | string  | `\`       |
| `maxLineSize`   | The maximum length of a line (in bytes).              | integer | `10000`   |
| `partitionSize` | The size of each data partition (in bytes).           | integer | `1048576` |


### Example

```python
from pyspark.sql import SparkSession
from pysparkformat.http.csv import HTTPCSVDataSource

# Initialize SparkSession (only needed if not running in Databricks)
spark = SparkSession.builder.appName("http-csv-example").getOrCreate()

# You may need to disable format checking depending on your cluster configuration
spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

# Register the custom data source
spark.dataSource.register(HTTPCSVDataSource)

# URL of the CSV file
url = "https://raw.githubusercontent.com/aig/pysparkformat/refs/heads/main/tests/data/valid-with-header.csv"

# Read the data
df = spark.read.format("http-csv").option("header", True).load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```

## `http-json`

The `http-json` format reads JSON Lines files in parallel directly from a URL using HTTP range requests.

### Options

| Name            | Description                                 | Type    | Default   |
|-----------------|---------------------------------------------|---------|-----------|
| `maxLineSize`   | The maximum length of a line (in bytes).    | integer | `10000`   |
| `partitionSize` | The size of each data partition (in bytes). | integer | `1048576` |

### Example

```python
from pyspark.sql import SparkSession
from pysparkformat.http.json import HTTPJSONDataSource

# Initialize SparkSession (only needed if not running in Databricks)
spark = SparkSession.builder.appName("http-json-example").getOrCreate()

# You may need to disable format checking depending on your cluster configuration
spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

# Register the custom data source
spark.dataSource.register(HTTPJSONDataSource)

# URL of the JSON file
url = "https://raw.githubusercontent.com/aig/pysparkformat/refs/heads/main/tests/data/valid-nested.jsonl"

# Read the data (you must specify the schema at the moment)
json_schema = "name string, wins array<array<string>>"
df = spark.read.format("http-json").schema(json_schema).load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```

## `http-page`

The `http-page` format fetches an HTML page, parses all links (`<a href="...">` tags),
and performs HEAD requests on each link to retrieve response headers. Each link becomes
a separate Spark partition, enabling parallel header fetching across the cluster.

### Schema

The output DataFrame has the following fixed schema:

| Column                      | Type              | Nullable | Description                              |
|-----------------------------|-------------------|----------|------------------------------------------|
| `page_request_url`          | StringType        | No       | The URL being checked (HEAD request)     |
| `page_request_datetime`     | TimestampType     | No       | When the request was made (UTC)          |
| `page_request_error`        | StringType        | Yes      | Error message if request failed          |
| `page_response_http_status` | IntegerType       | Yes      | HTTP response status code                |
| `page_response_headers`     | ArrayType(String) | Yes      | Response headers as "key: value" strings |
| `page_backlink_url`         | StringType        | No       | The page where this link was found       |

### Options

| Name              | Description                                    | Type    | Default            |
|-------------------|------------------------------------------------|---------|--------------------| 
| `timeout`         | Request timeout in seconds.                    | integer | `30`               |
| `userAgent`       | User agent string for HTTP requests.           | string  | `pysparkformat/1.0`|
| `followRedirects` | Follow HTTP redirects.                         | boolean | `true`             |
| `dnsServer`       | Custom DNS server for resolving hostnames.     | string  | (system DNS)       |

### Example: Get Headers for All Links

```python
from pyspark.sql import SparkSession
from pysparkformat.http.page import HTTPPageDataSource

# Initialize SparkSession
spark = SparkSession.builder.appName("http-page-example").getOrCreate()

# Register the custom data source
spark.dataSource.register(HTTPPageDataSource)

# Parse page and get headers for all links
df = spark.read.format("http-page").load("https://example.com/")

# Display results
df.show(truncate=False)
```

### Example: Extract Content-Length from Headers

```python
from pyspark.sql.functions import expr

# Get headers and extract Content-Length
df = spark.read.format("http-page").load("https://example.com/documents/")

# Filter headers to find Content-Length
df.select(
    "page_request_url",
    expr("filter(page_response_headers, h -> h like 'Content-Length%')").alias("content_length")
).show(truncate=False)
```

### Example: Custom Timeout and User Agent

```python
# Use custom timeout and user agent
df = spark.read.format("http-page") \
    .option("timeout", 60) \
    .option("userAgent", "MyBot/1.0") \
    .load("https://example.com/")

df.show()
```

### Use Cases

- **Link discovery**: Find all links on a webpage
- **File size checking**: Get Content-Length headers without downloading files
- **Content-Type detection**: Determine MIME types of linked resources
- **Link validation**: Check which links are accessible (have headers)
- **Last-Modified tracking**: Monitor when linked resources were last updated

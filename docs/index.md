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
| `http-page` | Yes  | No    | Fetches HTML page, extracts links, downloads content.|

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

The `http-page` format fetches an HTML page. By default, it only fetches the main page.
If a `linkPattern` is specified, it also parses the HTML to extract matching links
(`<a href="...">` tags) and fetches their content. Each URL becomes a separate Spark partition,
enabling parallel downloads across the cluster.

### Schema

The output DataFrame has the following fixed schema:

| Column      | Type          | Nullable | Description                           |
|-------------|---------------|----------|---------------------------------------|
| `url`       | StringType    | No       | The URL of the fetched resource       |
| `timestamp` | TimestampType | No       | When the content was fetched (UTC)    |
| `content`   | BinaryType    | Yes      | Binary content of the fetched resource|

### Options

| Name              | Description                                           | Type    | Default            |
|-------------------|-------------------------------------------------------|---------|--------------------| 
| `timeout`         | Request timeout in seconds.                           | integer | `30`               |
| `userAgent`       | User agent string for HTTP requests.                  | string  | `pysparkformat/1.0`|
| `followRedirects` | Follow HTTP redirects.                                | boolean | `true`             |
| `linkPattern`     | Regex pattern to match links to also fetch (optional).| string  | (none)             |

### Example: Fetch Main Page Only

```python
from pyspark.sql import SparkSession
from pysparkformat.http.page import HTTPPageDataSource

# Initialize SparkSession
spark = SparkSession.builder.appName("http-page-example").getOrCreate()

# Register the custom data source
spark.dataSource.register(HTTPPageDataSource)

# Fetch only the main page
df = spark.read.format("http-page").load("https://example.com/")

# Display results (1 row with the main page content)
df.show()
```

### Example: Fetch Main Page Plus Matching Links

```python
# Fetch main page and all PDF files linked from it
df = spark.read.format("http-page") \
    .option("linkPattern", r"\.pdf$") \
    .load("https://example.com/documents/")

df.show()
```

### Example: Fetch Main Page Plus All Links

```python
# Fetch main page and all http/https links (use .* to match everything)
df = spark.read.format("http-page") \
    .option("linkPattern", r".*") \
    .load("https://example.com/")

df.show()
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

- **Single page fetch**: Download a single web page as binary content
- **Document collection**: Fetch main page plus all PDF, CSV, or other files linked from it
- **Data pipeline ingestion**: Automatically discover and download new files from a web directory
- **Content archival**: Save binary content of the main page and selected linked resources

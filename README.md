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

| Format      | Read | Write | Description                                       |
|-------------|------|-------|---------------------------------------------------|
| `http-csv`  | Yes  | No    | Reads CSV files in parallel directly from a URL.  |
| `http-json` | Yes  | No    | Reads JSON Lines in parallel directly from a URL. |

## Installation

```bash
# Install PySpark 4.0.0.dev2
pip install pyspark==4.0.0.dev2

# Install the package using pip
pip install pysparkformat
```

For Databricks, install within a Databricks notebook using:

```bash
%pip install pysparkformat
```
This has been tested with Databricks Runtime 15.4 LTS and later.


## `http-csv`

The following options can be specified when using the `http-csv` format:

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
url = "https://raw.githubusercontent.com/aig/pysparkformat/refs/heads/master/tests/data/valid-with-header.csv"

# Read the data
df = spark.read.format("http-csv").option("header", True).load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```

## `http-json`

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
url = "https://raw.githubusercontent.com/aig/pysparkformat/refs/heads/master/tests/data/valid-nested.jsonl"

# Read the data (you must specify the schema at the moment)
json_schema = "name string, wins array<array<string>>"
df = spark.read.format("http-json").schema(json_schema).load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```

## Contribute

Contributions are welcome! 
We encourage the addition of new custom data source formats and improvements to existing ones.

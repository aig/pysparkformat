# pysparkformat: PySpark Data Source Formats

This project provides a collection of custom data source formats for Apache Spark 4.0+ and Databricks, 
leveraging the new V2 data source PySpark API.  

---

<p>
    <a href="https://pypi.org/project/pysparkformat/">
        <img src="https://img.shields.io/pypi/v/pysparkformat?color=green&amp;style=for-the-badge" alt="Latest Python Release"/>
    </a>
</p>

---

## Supported Formats

Currently, the following formats are supported:

### `http-csv`

This format reads in parallel CSV directly from a URL.

#### Options

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


### `http-json`
This format reads in parallel JSON Lines directly from a URL. You must specify the schema when using this format.

#### Options
| Name            | Description                                 | Type    | Default   |
|-----------------|---------------------------------------------|---------|-----------|
| `maxLineSize`   | The maximum length of a line (in bytes).    | integer | `10000`   |
| `partitionSize` | The size of each data partition (in bytes). | integer | `1048576` |

## Installation

This requires PySpark 4.0 or later to be installed:

```bash
pip install pyspark==4.0.0.dev2
```

Install the package using pip:

```bash
pip install pysparkformat
```


**For Databricks:**

Install within a Databricks notebook using:

```python
%pip install pysparkformat
```
This has been tested with Databricks Runtime 15.4 LTS and later.


## Usage Example: `http-csv`

This example demonstrates reading a CSV file from a URL using the `http-csv` format.

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
df = spark.read.format("http-csv") \
             .option("header", True) \
             .load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```

## Usage Example: `http-json`
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
df = spark.read.format("http-json") \
             .schema("name string, wins array<array<string>>") \
             .load(url)

# Display the DataFrame (use `display(df)` in Databricks)
df.show()
```
## Contributing

Contributions are welcome! 
We encourage the addition of new custom data source formats and improvements to existing ones.

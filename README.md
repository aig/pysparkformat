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

**For Databricks:**

Install within a Databricks notebook using:

```shell
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

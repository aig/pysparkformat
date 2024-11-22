# pysparkformat

Apache Spark 4.0 introduces a new data source API called V2 and even more now we can use python to create custom data sources. 
This is a great feature that allows us to create custom data sources that can be used in any pyspark projects.

This project is intended to collect all custom pyspark formats that I have created for my projects.

## http-csv format
| Name          | Description                           | Default |
|---------------|---------------------------------------|:-------:|
| header        | Whether the file has a header or not  |  false  |
| sep           | The delimiter used in the file        |    ,    |
| encoding      | The encoding of the file              |  UTF-8  |
| quote         | The quote character                   |    "    |
| escape        | The escape character                  |    \    |
| maxLineSize   | The maximum length of a line in bytes |  10000  |
| partitionSize | The size of each partition in bytes   | 1048576 |

You are welcome to contribute with new formats or improvements in the existing ones.

## Usage

```bash
pip install pyspark==4.0.0.dev2
pip install pysparkformat
```

You also can use this package in Databricks notebooks. Tested with Databricks Runtime 15.4 LTS.
Just install it using the following command to general-purpose cluster:
```bash
%pip install pysparkformat
```

```python
from pyspark.sql import SparkSession
from pysparkformat.http.csv import HTTPCSVDataSource

# you can comment the following line if you are running this code in Databricks
spark = SparkSession.builder.appName("custom-datasource-example").getOrCreate()

# uncomment to disable format check for Databricks Runtime
# spark.conf.set("spark.databricks.delta.formatCheck.enabled", False)

spark.dataSource.register(HTTPCSVDataSource)

url = "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv"
df = spark.read.format("http-csv").option("header", True).load(url)
df.show() # or use display(df) in Databricks
```

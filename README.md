# pysparkformat

Apache Spark 4.0 introduces a new data source API called V2 and even more now we can use python to create custom data sources. 
This is a great feature that allows us to create custom data sources that can be used in any pyspark projects.

This project is intended to collect all custom pyspark formats that I have created for my projects.

Here is what we have so far:
 * http-csv : A custom data source that reads CSV files from HTTP.

You are welcome to contribute with new formats or improvements in the existing ones.

Usage:
```bash
pip install pyspark==4.0.0.dev2
pip install pysparkformat
```

You also can use this package in Databricks notebooks, just install it using the following command:
```bash
%pip install pysparkformat
```

```python
from pyspark.sql import SparkSession
from pysparkformat.http.csv import HTTPCSVDataSource

spark = SparkSession.builder.appName("custom-datasource-example").getOrCreate()
spark.dataSource.register(HTTPCSVDataSource)

url = "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv"
df = spark.read.format("http-csv").option("url", url).load()
df.show()
```

import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pysparkformat.http.csv import HTTPCSVDataSource


class TestHttpCsv(unittest.TestCase):
    def test_http_csv(self):
        os.environ["PYSPARK_PYTHON"] = sys.executable

        if sys.platform == "win32":
            hadoop_home = Path(__file__).parent.parent / "tools" / "windows" / "hadoop"
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["PATH"] = os.environ["PATH"] + ";" + str(hadoop_home / "bin")

        spark = SparkSession.builder.appName("custom-datasource-example").getOrCreate()

        spark.dataSource.register(HTTPCSVDataSource)

        url = (
            "https://raw.githubusercontent.com/aig/pysparkformat/"
            + "refs/heads/master/tests/data/valid-csv-with-header.csv"
        )

        result = (
            spark.read.format("http-csv")
            .option("header", True)
            .option("maxLineSize", 10000)
            .load(url)
            .localCheckpoint()
        )

        assert result.count() == 50985


if __name__ == "__main__":
    unittest.main()

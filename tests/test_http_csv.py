import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pysparkformat.http.csv import HTTPCSVDataSource


class TestHttpCsv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable

        if sys.platform == "win32":
            hadoop_home = Path(__file__).parent.parent / "tools" / "windows" / "hadoop"
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["PATH"] += ";" + str(hadoop_home / "bin")

        cls.spark = SparkSession.builder.appName("http-csv-test-app").getOrCreate()
        cls.spark.dataSource.register(HTTPCSVDataSource)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_valid_csv_with_header(self):
        url = (
            "https://raw.githubusercontent.com/aig/pysparkformat/"
            + "refs/heads/master/tests/data/valid-csv-with-header.csv"
        )

        result = (
            self.spark.read.format("http-csv")
            .option("header", True)
            .load(url)
            .localCheckpoint()
        )

        self.assertEqual(result.count(), 50985)

    def test_valid_csv_without_header(self):
        url = (
            "https://raw.githubusercontent.com/aig/pysparkformat/"
            + "refs/heads/master/tests/data/valid-csv-with-header.csv"
        )

        result = (
            self.spark.read.format("http-csv")
            .option("header", False)
            .load(url)
            .localCheckpoint()
        )

        self.assertEqual(result.count(), 50986)


if __name__ == "__main__":
    unittest.main()

if __name__ == "__main__":
    unittest.main()

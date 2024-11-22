import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pysparkformat.http.csv import HTTPCSVDataSource


class TestHttpCsv(unittest.TestCase):
    TEST_DATA_URL = (
        "https://raw.githubusercontent.com/aig/pysparkformat/"
        + "refs/heads/master/tests/data/"
    )
    VALID_WITH_HEADER = "valid-with-header.csv"
    VALID_WITHOUT_HEADER = "valid-without-header.csv"
    VALID_WITH_HEADER_NO_DATA = "valid-with-header-no-data.csv"

    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable

        if sys.platform == "win32":
            hadoop_home = Path(__file__).parent.parent / "tools" / "windows" / "hadoop"
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["PATH"] += ";" + str(hadoop_home / "bin")

        cls.spark = SparkSession.builder.appName("http-csv-test-app").getOrCreate()
        cls.spark.dataSource.register(HTTPCSVDataSource)

        cls.data_path = Path(__file__).resolve().parent / "data"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_csv_valid_with_header(self):
        remote_result = (
            self.spark.read.format("http-csv")
            .option("header", True)
            .load(self.TEST_DATA_URL + self.VALID_WITH_HEADER)
        )

        local_result = self.spark.read.option("header", True).csv(
            str(self.data_path / self.VALID_WITH_HEADER)
        )
        self.assertEqual(remote_result.exceptAll(local_result).count(), 0)
        self.assertEqual(local_result.exceptAll(remote_result).count(), 0)

    def test_csv_valid_without_header(self):
        remote_result = (
            self.spark.read.format("http-csv")
            .option("header", False)
            .load(self.TEST_DATA_URL + self.VALID_WITHOUT_HEADER)
            .localCheckpoint()
        )

        local_result = self.spark.read.option("header", False).csv(
            str(self.data_path / self.VALID_WITHOUT_HEADER)
        )
        self.assertEqual(remote_result.exceptAll(local_result).count(), 0)
        self.assertEqual(local_result.exceptAll(remote_result).count(), 0)

    def test_csv_valid_with_header_no_data(self):
        remote_result = (
            self.spark.read.format("http-csv")
            .option("header", True)
            .load(self.TEST_DATA_URL + self.VALID_WITH_HEADER_NO_DATA)
            .localCheckpoint()
        )

        local_result = self.spark.read.option("header", True).csv(
            str(self.data_path / self.VALID_WITH_HEADER_NO_DATA)
        )
        self.assertEqual(remote_result.exceptAll(local_result).count(), 0)
        self.assertEqual(local_result.exceptAll(remote_result).count(), 0)


if __name__ == "__main__":
    unittest.main()

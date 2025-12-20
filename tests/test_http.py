import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pysparkformat.http.csv import HTTPCSVDataSource
from pysparkformat.http.json import HTTPJSONDataSource


class TestHTTP(unittest.TestCase):
    TEST_DATA_URL = (
        "https://raw.githubusercontent.com/aig/pysparkformat/"
        + "refs/heads/main/tests/data/"
    )
    VALID_CSV_WITH_HEADER = "valid-with-header.csv"
    VALID_CSV_WITHOUT_HEADER = "valid-without-header.csv"
    VALID_CSV_WITH_HEADER_NO_DATA = "valid-with-header-no-data.csv"

    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable

        if sys.platform == "win32":
            hadoop_home = Path(__file__).parent.parent / "tools" / "win32" / "hadoop"
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["PATH"] += ";" + str(hadoop_home / "bin")

        cls.spark = SparkSession.builder.appName("http-test-app").getOrCreate()
        cls.spark.dataSource.register(HTTPCSVDataSource)
        cls.spark.dataSource.register(HTTPJSONDataSource)

        cls.data_path = Path(__file__).resolve().parent / "data"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_csv_valid_with_header(self):
        options = {"header": "true"}
        self._check_csv(self.VALID_CSV_WITH_HEADER, options)

    def test_csv_valid_without_header(self):
        options = {"header": "false"}
        self._check_csv(self.VALID_CSV_WITHOUT_HEADER, options)

    def test_csv_valid_with_header_no_data(self):
        options = {"header": "true"}
        self._check_csv(self.VALID_CSV_WITH_HEADER_NO_DATA, options)

    def test_json_valid_nested(self):
        options = {}
        self._check_json("valid-nested.jsonl", options)

    def _check_json(self, name: str, options):
        local_result = self.spark.read.options(**options).json(
            str(self.data_path / name)
        )
        remote_result = (
            self.spark.read.format("http-json")
            .options(**options)
            .schema(local_result.schema)
            .load(self.TEST_DATA_URL + name)
            .localCheckpoint()
        )
        self.assertEqual(remote_result.schema, local_result.schema)
        self.assertEqual(remote_result.exceptAll(local_result).count(), 0)
        self.assertEqual(local_result.exceptAll(remote_result).count(), 0)

    def _check_csv(self, name: str, options: dict):
        remote_result = (
            self.spark.read.format("http-csv")
            .options(**options)
            .load(self.TEST_DATA_URL + name)
            .localCheckpoint()
        )
        local_result = self.spark.read.options(**options).csv(
            str(self.data_path / name)
        )
        self.assertEqual(remote_result.schema, local_result.schema)
        self.assertEqual(remote_result.exceptAll(local_result).count(), 0)
        self.assertEqual(local_result.exceptAll(remote_result).count(), 0)


if __name__ == "__main__":
    unittest.main()

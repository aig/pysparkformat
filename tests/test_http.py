import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from pysparkformat.http.csv import HTTPCSVDataSource
from pysparkformat.http.json import HTTPJSONDataSource
from pysparkformat.http.page import HTTPPageDataSource


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
        cls.spark.dataSource.register(HTTPPageDataSource)

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

    def test_page_fetch_main_page(self):
        """Test that http-page fetches main page by default."""
        base_url = "https://github.com/aig/pysparkformat"
        result = self.spark.read.format("http-page").load(base_url).localCheckpoint()

        # Check schema
        self.assertEqual(len(result.schema.fields), 3)
        self.assertEqual(result.schema.fields[0].name, "url")
        self.assertEqual(result.schema.fields[1].name, "timestamp")
        self.assertEqual(result.schema.fields[2].name, "content")

        # Should have fetched exactly one page (the main page)
        self.assertEqual(result.count(), 1)

        # The URL should be the base URL
        urls = result.select("url").collect()
        self.assertEqual(urls[0].url, base_url)

    def test_page_with_link_pattern(self):
        """Test that linkPattern option adds matching links."""
        base_url = "https://github.com/aig/pysparkformat"
        # Use GitHub repository tree page that has links to files
        result = (
            self.spark.read.format("http-page")
            .option("linkPattern", r"/blob/")
            .load(base_url)
            .localCheckpoint()
        )

        # Should have the main page plus matching links
        self.assertGreater(result.count(), 1)

        # First URL should be the base URL
        urls = [row.url for row in result.select("url").collect()]
        self.assertEqual(urls[0], base_url)

        # Other URLs should match the pattern
        for url in urls[1:]:
            self.assertIn("/blob/", url)


if __name__ == "__main__":
    unittest.main()

import json

from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from pysparkformat.http.file import HTTPFile, HTTPTextReader


class Parameters:
    DEFAULT_MAX_LINE_SIZE = 10000

    def __init__(self, options: dict):
        self.options = options
        self.path = str(options.get("path", ""))
        if not self.path:
            raise ValueError("path is required")

        self.max_line_size = max(
            int(options.get("maxLineSize", self.DEFAULT_MAX_LINE_SIZE)), 1
        )


class HTTPJSONLDataSource(DataSource):
    def __init__(self, options: dict):
        super().__init__(options)
        self.options = options

        params = Parameters(options)
        self.file = HTTPFile(params.path)
        file_reader = HTTPTextReader(self.file)
        data = file_reader.read_first_line(params.max_line_size)
        parsed = json.loads(data)
        print(parsed)


    @classmethod
    def name(cls):
        return "http-jsonl"

    def schema(self):
        return self.schema

    def reader(self, schema: StructType):
        return JSONLDataSourceReader(schema, self.options, self.file)


class JSONLDataSourceReader:
    def __init__(self, schema: StructType, options: dict, file: HTTPFile):
        self.schema = schema
        self.options = options
        self.file = file

    def read(self):
        raise NotImplementedError

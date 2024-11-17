import csv
import math

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from requests.structures import CaseInsensitiveDict


class HTTPHeader:
    def __init__(self, headers: CaseInsensitiveDict):
        self._headers = headers

    @property
    def content_length(self):
        return int(self._headers.get("Content-Length", 0))


class Parameters:
    DEFAULT_PARTITION_SIZE = 1024 * 1024
    DEFAULT_MAX_LINE_SIZE = 10000

    def __init__(self, options: dict):
        self.options = options

        self.path = str(options.get("path", ""))
        if not self.path:
            raise ValueError("path is required")

        self.header = str(options.get("header", "false")).lower() == "true"
        self.max_line_size = max(
            int(options.get("maxLineSize", self.DEFAULT_MAX_LINE_SIZE)), 1
        )
        self.partition_size = max(
            int(options.get("partitionSize", self.DEFAULT_PARTITION_SIZE)), 1
        )


class HTTPCSVDataSource(DataSource):
    def __init__(self, options: dict):
        import requests

        super().__init__(options)

        params = Parameters(options)

        response = requests.head(params.path)
        if response.status_code != 200:
            raise ValueError("path is not accessible")

        self.header = HTTPHeader(response.headers)

        if self.header.content_length == 0:
            raise ValueError("Content-Length is not available")

        http_range_start = 0

        chunks = []
        while True:
            http_range_end = min(
                http_range_start + params.max_line_size, self.header.content_length - 1
            )

            headers = {"Range": f"bytes={http_range_start}-{http_range_end}"}

            response = requests.get(params.path, headers=headers)
            if response.status_code != 206:
                raise ValueError("HTTP range request failed")

            chunk = response.content
            chunks.append(chunk)

            if chunk.find(10) != -1:
                break

            http_range_start = http_range_end + 1

            if http_range_start == self.header.content_length:
                break

        reader = csv.reader(b"".join(chunks).decode("utf-8").splitlines())
        row = next(reader)

        if params.header:
            self.columns = row
        else:
            self.columns = [f"c{i}" for i in range(len(row))]

    @classmethod
    def name(cls):
        return "http-csv"

    def schema(self):
        return StructType(
            [StructField(column, StringType(), True) for column in self.columns]
        )

    def reader(self, schema: StructType):
        return CSVDataSourceReader(schema, self.options, self.header)


class CSVDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict, header: HTTPHeader):
        self.schema = schema
        self.options = options
        self.header = header
        self.params = Parameters(options)

    def partitions(self):
        n = math.ceil(self.header.content_length / self.params.partition_size)
        return [InputPartition(i + 1) for i in range(n)]

    def read(self, partition):
        import requests

        block_start = (partition.value - 1) * self.params.partition_size
        block_size = partition.value * self.params.partition_size

        http_range_start = block_start
        http_range_end = min(
            (block_size - 1) + self.params.max_line_size, self.header.content_length - 1
        )

        if http_range_end > self.header.content_length:
            http_range_end = self.header.content_length - 1

        headers = {"Range": f"bytes={http_range_start}-{http_range_end}"}

        response = requests.get(self.params.path, headers=headers)
        if response.status_code != 206:
            raise ValueError("HTTP range request failed")

        content = response.content
        index = content.find(10, self.params.partition_size)
        if index != -1:
            content = content[:index]
        else:
            if http_range_end != self.header.content_length - 1:
                raise ValueError("Line is too long. Increase maxLineSize")

        # if not first partition, skip first line, we read it in previous partition
        if partition.value != 1:
            index = content.find(10)
            if index != -1:
                content = content[index + 1 :]

        reader = csv.reader(content.decode("utf-8").splitlines())

        if partition.value == 1 and self.params.header:
            next(reader)

        for row in reader:
            yield tuple(row)

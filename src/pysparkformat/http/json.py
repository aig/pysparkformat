import json
import math

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from pysparkformat.http.file import HTTPFile, HTTPTextPartitionReader


class Parameters:
    DEFAULT_MAX_LINE_SIZE = 10000
    DEFAULT_PARTITION_SIZE = 1024 * 1024

    def __init__(self, options: dict):
        self.options = options
        self.path = str(options.get("path", ""))
        if not self.path:
            raise ValueError("path is required")

        self.max_line_size = max(
            int(options.get("maxLineSize", self.DEFAULT_MAX_LINE_SIZE)), 1
        )

        self.partition_size = max(
            int(options.get("partitionSize", self.DEFAULT_PARTITION_SIZE)),
            self.max_line_size,
        )


class HTTPJSONDataSource(DataSource):
    def __init__(self, options: dict):
        super().__init__(options)
        self.options = options

        params = Parameters(options)
        self.file = HTTPFile(params.path)

    @classmethod
    def name(cls):
        return "http-json"

    def schema(self):
        raise NotImplementedError

    def reader(self, schema: StructType):
        return JSONDataSourceReader(schema, self.options, self.file)


class JSONDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict, file: HTTPFile):
        self.schema = schema
        self.options = options
        self.file = file
        self.params = Parameters(options)

    def partitions(self):
        n = math.ceil(self.file.content_length / self.params.partition_size)
        return [InputPartition(i + 1) for i in range(n)]

    def read(self, partition: InputPartition):
        file_reader = HTTPTextPartitionReader(
            self.file, self.params.partition_size, self.params.max_line_size
        )

        content = file_reader.read_partition(partition.value)

        # if not first partition, skip first line, we read it in previous partition
        if partition.value != 1:
            index = content.find(10)
            if index != -1:
                content = content[index + 1 :]

        for line in content.splitlines():
            yield tuple(json.loads(line).values())

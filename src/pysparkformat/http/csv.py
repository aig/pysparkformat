import csv
import math

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StringType, StructField, StructType

from pysparkformat.http.file import HTTPFile, HTTPTextPartitionReader, HTTPTextReader


class Parameters:
    DEFAULT_PARTITION_SIZE = 1024 * 1024
    DEFAULT_MAX_LINE_SIZE = 10000
    DEFAULT_ENCODING = "utf-8"
    DEFAULT_SEP = ","
    DEFAULT_QUOTE = '"'
    DEFAULT_ESCAPE = "\\"
    DEFAULT_HEADER = "false"

    def __init__(self, options: dict):
        self.options = options

        self.path = str(options.get("path", ""))
        if not self.path:
            raise ValueError("path is required")

        self.header = str(options.get("header", self.DEFAULT_HEADER)).lower() == "true"
        self.max_line_size = max(
            int(options.get("maxLineSize", self.DEFAULT_MAX_LINE_SIZE)), 1
        )
        self.partition_size = max(
            int(options.get("partitionSize", self.DEFAULT_PARTITION_SIZE)), 1
        )

        if self.partition_size < self.max_line_size:
            raise ValueError("partitionSize must be greater than maxLineSize")

        self.quote = str(options.get("quote", self.DEFAULT_QUOTE))
        self.sep = str(options.get("sep", self.DEFAULT_SEP))
        self.encoding = str(options.get("encoding", self.DEFAULT_ENCODING))
        self.escape = str(options.get("escape", self.DEFAULT_ESCAPE))


class HTTPCSVDataSource(DataSource):
    def __init__(self, options: dict):
        super().__init__(options)

        params = Parameters(options)
        self.file = HTTPFile(params.path)

        file_reader = HTTPTextReader(self.file)
        data = file_reader.read_first_line(params.max_line_size)

        csv_reader = csv.reader(
            data.decode(params.encoding).splitlines(),
            delimiter=params.sep,
            quotechar=params.quote,
            escapechar=params.escape,
        )
        row = next(csv_reader)

        if params.header:
            self.columns = row
        else:
            self.columns = [f"_c{i}" for i in range(len(row))]

    @classmethod
    def name(cls):
        return "http-csv"

    def schema(self):
        return StructType(
            [StructField(column, StringType(), True) for column in self.columns]
        )

    def reader(self, schema: StructType):
        return CSVDataSourceReader(schema, self.options, self.file)


class CSVDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict, file: HTTPFile):
        self.schema = schema
        self.options = options
        self.file = file
        self.params = Parameters(options)

    def partitions(self):
        n = math.ceil(self.file.content_length / self.params.partition_size)
        return [InputPartition(i + 1) for i in range(n)]

    def read(self, partition):
        file_reader = HTTPTextPartitionReader(
            self.file, self.params.partition_size, self.params.max_line_size
        )

        content = file_reader.read_partition(partition.value)

        # if not first partition, skip first line, we read it in previous partition
        if partition.value != 1:
            index = content.find(10)
            if index != -1:
                content = content[index + 1 :]

        csv_reader = csv.reader(
            content.decode(self.params.encoding).splitlines(),
            delimiter=self.params.sep,
            quotechar=self.params.quote,
            escapechar=self.params.escape,
        )

        if partition.value == 1 and self.params.header:
            next(csv_reader)

        for row in csv_reader:
            yield tuple(row)

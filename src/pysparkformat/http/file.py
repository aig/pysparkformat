import requests
from pyspark.sql.datasource import InputPartition
from requests.structures import CaseInsensitiveDict


DEFAULT_REQUEST_HEADERS = {"Accept-Encoding": "none"}


class ResponseHeader:
    def __init__(self, headers: CaseInsensitiveDict):
        self.headers = headers

    @property
    def content_length(self):
        return int(self.headers.get("Content-Length", 0))


class HTTPFile:
    def __init__(self, url: str):
        self.url = url

        response = requests.head(self.url, headers=DEFAULT_REQUEST_HEADERS)
        if response.status_code != 200:
            raise ValueError("path is not accessible")

        self.header = ResponseHeader(response.headers)

        if self.header.content_length == 0:
            raise ValueError("Content-Length is not available")

        del response

    @property
    def content_length(self):
        return self.header.content_length


class HTTPTextReader:
    def __init__(self, file: HTTPFile):
        self.file = file

    def read_line(self, max_line_size: int) -> bytes:
        http_range_start = 0

        chunks = []
        while True:
            http_range_end = min(
                http_range_start + max_line_size, self.file.content_length - 1
            )

            headers = {
                "Range": f"bytes={http_range_start}-{http_range_end}",
                **DEFAULT_REQUEST_HEADERS,
            }

            response = requests.get(self.file.url, headers=headers)
            if response.status_code != 206:
                raise ValueError("HTTP range request failed")

            chunk = response.content
            chunks.append(chunk)

            if chunk.find(10) != -1:
                break

            http_range_start = http_range_end + 1

            if http_range_start == self.file.content_length:
                break

        return b"".join(chunks)


class HTTPTextPartitionReader:
    def __init__(self, file: HTTPFile, partition_size: int, max_line_size: int):
        self.file = file
        self.partition_size = partition_size
        self.max_line_size = max_line_size

    def read_partition(self, partition_number: int) -> bytes:
        """
        Read partition from HTTP file
        :param partition_number: partition number starting from 1
        :return: returns partition content
        """
        import requests

        if partition_number < 1:
            raise ValueError("Partition number should be greater than 0")

        block_start = (partition_number - 1) * self.partition_size
        block_size = partition_number * self.partition_size

        http_range_start = block_start
        http_range_end = min(
            (block_size - 1) + self.max_line_size, self.file.content_length - 1
        )

        if http_range_end > self.file.content_length:
            http_range_end = self.file.content_length - 1

        headers = {
            "Range": f"bytes={http_range_start}-{http_range_end}",
            **DEFAULT_REQUEST_HEADERS,
        }

        response = requests.get(self.file.url, headers=headers)
        if response.status_code != 206:
            raise ValueError("HTTP range request failed")

        content = response.content
        index = content.find(10, self.partition_size)
        if index != -1:
            return content[:index]

        if http_range_end != self.file.content_length - 1:
            raise ValueError("Line is too long. Increase maxLineSize")

        return content

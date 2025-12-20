"""
HTTP Page DataSource for PySpark

This module provides a PySpark DataSource that fetches an HTML page.
By default, it only fetches the main page. If a linkPattern is specified,
it also parses the HTML to extract matching links and fetches their content.
Each URL becomes a partition, and the result contains url, timestamp,
and binary content.
"""

from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    BinaryType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class LinkExtractor(HTMLParser):
    """HTML parser that extracts all href links from anchor tags."""

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag: str, attrs: list):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href" and value:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(self.base_url, value)
                    # Only include http/https links
                    parsed = urlparse(absolute_url)
                    if parsed.scheme in ("http", "https"):
                        self.links.append(absolute_url)


class Parameters:
    """Parameters for HTTP Page DataSource."""

    DEFAULT_TIMEOUT = 30
    DEFAULT_USER_AGENT = "pysparkformat/1.0"
    DEFAULT_FOLLOW_REDIRECTS = "true"

    def __init__(self, options: dict):
        self.options = options

        self.path = str(options.get("path", ""))
        if not self.path:
            raise ValueError("path is required")

        self.timeout = int(options.get("timeout", self.DEFAULT_TIMEOUT))
        self.user_agent = str(options.get("userAgent", self.DEFAULT_USER_AGENT))
        self.follow_redirects = (
            str(options.get("followRedirects", self.DEFAULT_FOLLOW_REDIRECTS)).lower()
            == "true"
        )

        # Optional pattern to extract and fetch links from the page
        self.link_pattern = options.get("linkPattern")


class HTTPPageDataSource(DataSource):
    """
    PySpark DataSource that fetches an HTML page and optionally its links.

    By default, only the main page is fetched. If linkPattern is specified,
    the HTML is parsed, matching links are extracted, and their content is
    also fetched. Each URL becomes a separate partition.

    Options:
        - path: URL of the HTML page to fetch (required)
        - timeout: Request timeout in seconds (default: 30)
        - userAgent: User agent string for requests (default: pysparkformat/1.0)
        - followRedirects: Follow HTTP redirects (default: true)
        - linkPattern: Regex pattern to match links to also fetch (optional)
    """

    def __init__(self, options: dict):
        super().__init__(options)
        self.params = Parameters(options)

    @classmethod
    def name(cls):
        return "http-page"

    def schema(self):
        return StructType(
            [
                StructField("url", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("content", BinaryType(), True),
            ]
        )

    def reader(self, schema: StructType):
        return HTTPPageDataSourceReader(schema, self.options)


class HTTPPageDataSourceReader(DataSourceReader):
    """Reader that creates partitions for each link found in the HTML page."""

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.params = Parameters(options)

    def partitions(self):
        """
        Create partitions: main page always included, matching links if pattern set.
        """
        import re

        headers = {"User-Agent": self.params.user_agent}

        response = requests.get(
            self.params.path,
            headers=headers,
            timeout=self.params.timeout,
            allow_redirects=self.params.follow_redirects,
        )

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch base page: HTTP {response.status_code}")

        # Always start with the main page
        urls = [self.params.path]

        # If linkPattern is specified, parse HTML and add matching links
        if self.params.link_pattern:
            parser = LinkExtractor(self.params.path)
            parser.feed(response.text)

            # Get unique links
            links = list(dict.fromkeys(parser.links))

            # Filter by pattern
            pattern = re.compile(self.params.link_pattern)
            matching_links = [link for link in links if pattern.search(link)]

            # Add matching links (excluding duplicates of main page)
            for link in matching_links:
                if link not in urls:
                    urls.append(link)

        # Create a partition for each URL
        return [InputPartition(url) for url in urls]

    def read(self, partition: InputPartition):
        """
        Fetch the content of the URL in this partition and yield the result.
        """
        url = partition.value
        headers = {"User-Agent": self.params.user_agent}

        timestamp = datetime.now(timezone.utc)

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=self.params.timeout,
                allow_redirects=self.params.follow_redirects,
            )

            content = response.content if response.status_code == 200 else None

        except requests.RequestException:
            content = None

        yield (url, timestamp, content)

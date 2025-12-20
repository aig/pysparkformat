"""
HTTP Page DataSource for PySpark

This module provides a PySpark DataSource that fetches an HTML page,
parses all links from it, and performs HEAD requests on each link to get headers.
Each link becomes a partition, and the result contains url, timestamp,
and response headers as an array.
"""

import ipaddress
import socket
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Hostnames that are always blocked (case-insensitive)
BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
}


def is_private_ip(hostname: str) -> bool:
    """
    Check if a hostname resolves to a private/internal IP address.

    Returns True if the IP is private, loopback, link-local, or reserved.
    """
    try:
        # Try to parse as IP address directly
        ip = ipaddress.ip_address(hostname)
        return (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
        )
    except ValueError:
        # Not an IP address, try to resolve hostname
        try:
            resolved_ip = socket.gethostbyname(hostname)
            ip = ipaddress.ip_address(resolved_ip)
            return (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_multicast
            )
        except (socket.gaierror, ValueError):
            # Cannot resolve - treat as unsafe
            return True


def is_safe_url(url: str) -> bool:
    """
    Check if a URL is safe to request (not pointing to internal resources).

    Blocks:
    - localhost and similar hostnames
    - Private IP ranges (10.x.x.x, 172.16-31.x.x, 192.168.x.x)
    - Loopback addresses (127.x.x.x, ::1)
    - Link-local addresses (169.254.x.x, fe80::)
    - Reserved/multicast addresses
    """
    parsed = urlparse(url)
    hostname = parsed.hostname

    if not hostname:
        return False

    # Check blocked hostnames
    if hostname.lower() in BLOCKED_HOSTNAMES:
        return False

    # Check if IP is private/internal
    return not is_private_ip(hostname)


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
                    # Only include http/https links that are safe
                    parsed = urlparse(absolute_url)
                    if parsed.scheme in ("http", "https") and is_safe_url(
                        absolute_url
                    ):
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


class HTTPPageDataSource(DataSource):
    """
    PySpark DataSource that parses an HTML page for links and fetches headers.

    Fetches the HTML page, extracts all links, creates a partition per link,
    and performs HEAD requests to get response headers for each link.

    Options:
        - path: URL of the HTML page to parse (required)
        - timeout: Request timeout in seconds (default: 30)
        - userAgent: User agent string for requests (default: pysparkformat/1.0)
        - followRedirects: Follow HTTP redirects (default: true)
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
                StructField("page_request_url", StringType(), False),
                StructField("page_request_datetime", TimestampType(), False),
                StructField("page_request_error", StringType(), True),
                StructField("page_response_http_status", IntegerType(), True),
                StructField("page_response_headers", ArrayType(StringType()), True),
                StructField("page_backlink_url", StringType(), False),
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
        Fetch HTML page, parse all links, create a partition per link.
        """
        headers = {"User-Agent": self.params.user_agent}

        response = requests.get(
            self.params.path,
            headers=headers,
            timeout=self.params.timeout,
            allow_redirects=self.params.follow_redirects,
        )

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch base page: HTTP {response.status_code}")

        # Parse HTML to extract all links
        parser = LinkExtractor(self.params.path)
        parser.feed(response.text)

        # Get unique links
        links = list(dict.fromkeys(parser.links))

        # Create a partition for each link (tuple of request_url, backlink_url)
        page_backlink_url = self.params.path
        return [InputPartition((link, page_backlink_url)) for link in links]

    def read(self, partition: InputPartition):
        """
        Perform HEAD request on the URL and yield request/response fields.
        """
        page_request_url, page_backlink_url = partition.value
        request_headers = {"User-Agent": self.params.user_agent}

        page_request_datetime = datetime.now(timezone.utc)
        page_request_error = None

        try:
            response = requests.head(
                page_request_url,
                headers=request_headers,
                timeout=self.params.timeout,
                allow_redirects=self.params.follow_redirects,
            )

            page_response_http_status = response.status_code
            # Convert headers to array of "key: value" strings
            page_response_headers = [
                f"{key}: {value}" for key, value in response.headers.items()
            ]

        except requests.RequestException as e:
            page_response_http_status = None
            page_response_headers = None
            page_request_error = str(e)

        yield (
            page_request_url,
            page_request_datetime,
            page_request_error,
            page_response_http_status,
            page_response_headers,
            page_backlink_url,
        )

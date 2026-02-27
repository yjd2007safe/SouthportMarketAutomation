import unittest
from unittest.mock import patch

from src.requests import open_ingest_url


class TestOpenIngestUrl(unittest.TestCase):
    @patch("src.requests.urlopen")
    def test_allows_http(self, mock_urlopen):
        sentinel = object()
        mock_urlopen.return_value = sentinel

        result = open_ingest_url("http://example.com/data.csv")

        self.assertIs(result, sentinel)
        mock_urlopen.assert_called_once_with("http://example.com/data.csv")

    @patch("src.requests.urlopen")
    def test_allows_https(self, mock_urlopen):
        sentinel = object()
        mock_urlopen.return_value = sentinel

        result = open_ingest_url("https://example.com/data.csv")

        self.assertIs(result, sentinel)
        mock_urlopen.assert_called_once_with("https://example.com/data.csv")

    @patch("src.requests.urlopen")
    def test_rejects_file_scheme(self, mock_urlopen):
        with self.assertRaisesRegex(
            ValueError,
            "Only http and https URLs are allowed",
        ):
            open_ingest_url("file:///tmp/local.csv")

        mock_urlopen.assert_not_called()

    @patch("src.requests.urlopen")
    def test_rejects_custom_scheme(self, mock_urlopen):
        with self.assertRaisesRegex(
            ValueError,
            "Unsupported URL scheme 's3'",
        ):
            open_ingest_url("s3://bucket/key.csv")

        mock_urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()

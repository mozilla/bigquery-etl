"""Common exception types."""


class BigQueryInsertError(Exception):
    """Wrap errors returned by BigQuery insertAll requests in an Exception."""

    @staticmethod
    def raise_if_present(errors):
        """Raise this exception if errors is truthy."""
        if errors:
            raise BigQueryInsertError(errors)

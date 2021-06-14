"""Import Apple Detailed Subscriber report version 1_3 from App Store Connect API."""

from bigquery_etl.subplat.apple import apple_import

if __name__ == "__main__":
    apple_import()

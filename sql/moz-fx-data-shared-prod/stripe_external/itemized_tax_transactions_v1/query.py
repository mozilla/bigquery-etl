"""Import itemized tax transactions report from the Stripe API."""

from bigquery_etl.stripe import stripe_import

if __name__ == "__main__":
    stripe_import()

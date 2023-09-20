print("Look at me! I'm running a Python command! WOW!")

import argparse
import pandas as pd

from datetime import date, timedelta
from collections import namedtuple

from data_validation import retrieve_data_validation_metrics, record_validation_results

print("Dependencies successfully imported!")

parser = argparse.ArgumentParser(
    description="Validate Recent Search Input Against Historical Norms",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--data_validation_origin", help="Origin table for data validation metrics"
)
parser.add_argument(
    "--data_validation_reporting_destination",
    help="Table to store data validation metric test results",
)
print("Parser successfully instantiated!")

args = parser.parse_args()
print("Args successfully parsed!")
print(f"Data Validation Origin: {args.data_validation_origin}")
print(f"Data Validation Reporting Destination: {args.data_validation_reporting_destination}")

print("Retrieving Data Validation Metrics Now...")
validation_df = retrieve_data_validation_metrics(args.data_validation_origin)
print(f"Input Dataframe Shape: {validation_df.shape}")

print("Recording validation results...")
record_validation_results(validation_df, args.data_validation_reporting_destination)


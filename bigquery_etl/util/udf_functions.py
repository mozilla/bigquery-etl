"""Utility functions related to UDF parsing and documentation."""

import re


def get_parameters(sql_text):
    """Extract input and output parameters from UDF sql code."""
    # TODO: Extract the input an output from sql_text and replace in the next 2 lines.
    inputs = sql_text
    outputs = ""

    input_parameters = re.sub(
        r"CREATE OR REPLACE FUNCTION [a-z,A-Z,_,.]+\s*\(\s*|\s*AS\s*|\(|\s*\)\s*",
        "",
        inputs,
    )
    input_parameters = re.sub(r"\s{2,}", " ", input_parameters)

    # TODO: Correct the following code to extract output_parameters.
    output_parameters = re.sub(
        r"RETURNS [a-z,A-Z,_,.]+\s*\(\s*|\s*AS\s*|\(|\s*\)\s*", "", outputs
    )
    output_parameters = re.sub(r"\s{2,}", " ", output_parameters)

    return [input_parameters, output_parameters]

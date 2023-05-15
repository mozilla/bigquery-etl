"""Utility functions related to UDF parsing and documentation."""

import re


def find_input(sql_text):
    """Extract input parameters from UDF sql code."""
    sql_text = re.sub(r"\s{2,}|\\n", " ", sql_text)
    return (
        re.search(r"FUNCTION [a-z,0-9,_,.]+\s*\(\s*(.+?)\s*\)", sql_text)
        .groups()[0]
        .strip()
    )


def find_output(sql_text):
    """Extract output parameters from UDF sql code."""
    sql_text = re.sub(r"\s{2,}", " ", sql_text)
    res = re.search(r"RETURNS (.+?) AS", sql_text)
    if res is None:
        return None
    return res.groups()[0]


def get_udf_parameters(sql_text):
    """Extract input and output parameters from UDF sql code."""
    input_part = find_input(sql_text)
    output_part = find_output(sql_text)
    return {"input": input_part, "output": output_part}

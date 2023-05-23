"""Utility functions related to UDF parsing and documentation."""

import re
from typing import Optional, Tuple


def get_input(sql_text: str) -> Optional[str]:
    """Extract input parameters from UDF sql code."""
    cleaned_sql_text = re.sub(r"\s{2,}|\n", " ", sql_text)
    input_groups = re.search(
        r"(?:FUNCTION|PROCEDURE) [a-z,0-9,_,.]+\s?\(\s?(.+?)(?:\s?\)|, OUT)",
        cleaned_sql_text,
    )
    if input_groups is None:
        return None
    input_string = input_groups.groups()[0]
    return input_string


def get_output(sql_text: str) -> Optional[str]:
    """Extract output parameters from UDF sql code."""
    cleaned_sql_text = re.sub(r"\s{2,}|\n", " ", sql_text)
    output_groups = re.search(
        r"(?:RETURNS |CAST\([a-z,A-Z,0-9,_,.]+ AS |, OUT )(.+?)(?: AS|\s?\)|\s?LANGUAGE)",
        cleaned_sql_text,
    )
    if output_groups is None:
        return None
    return output_groups.groups()[0]


def get_udf_parameters(sql_text: str) -> Tuple:
    """Extract input and output parameters from UDF sql code.

    The sql_text parameter can include UDF's header, body, tests & comments.
    """
    input_part = get_input(sql_text)
    output_part = get_output(sql_text)
    return input_part, output_part

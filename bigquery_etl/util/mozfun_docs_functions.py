"""Utility functions related to UDF parsing and documentation."""

import re
from typing import Optional, Tuple


def clean_sql_text(sql_text: str) -> str:
    """Remove line breaks, comments and multiple whitespaces from sql_text."""
    sql_text_without_comments = re.sub(r"--.+$", "", sql_text, flags=re.MULTILINE)
    return re.sub(r"\s{2,}|\n", " ", sql_text_without_comments).strip()


def clean_result(result_string: str) -> str:
    """Remove white spaces around brackets."""
    return re.sub(r"(?<=\<|\>)\s+|\s+(?=\<|\>)", "", result_string)


def get_input(sql_text: str) -> Optional[str]:
    """Extract input parameters from UDF sql code."""
    cleaned_sql_text = clean_sql_text(sql_text)
    input_groups = re.search(
        r"(?:FUNCTION|PROCEDURE) [a-z,0-9,_,.]+\s?\(\s?(.+?)(?:\s?\)|, OUT)",
        cleaned_sql_text,
    )
    if input_groups is None:
        return None
    input_string = input_groups.groups()[0]
    return clean_result(input_string)


def get_output(sql_text: str) -> Optional[str]:
    """Extract output parameters from UDF sql code."""
    cleaned_sql_text = clean_sql_text(sql_text)
    output_groups = re.search(
        r"(?:RETURNS |CAST\([a-z,A-Z,0-9,_,.,\-,\s,&,\(\),>]+ AS |, OUT )(.+?)(?: AS|\s?\)|\s?LANGUAGE)",
        cleaned_sql_text,
    )
    if output_groups is None:
        return None
    output_string = output_groups.groups()[0]
    return clean_result(output_string)


def get_mozfun_parameters(sql_text: str) -> Tuple:
    """Extract input and output parameters from UDF sql code and stored procedure sql code.

    The sql_text parameter can include sql codes header, body, tests & comments.
    """
    input_part = get_input(sql_text)
    output_part = get_output(sql_text)
    return input_part, output_part

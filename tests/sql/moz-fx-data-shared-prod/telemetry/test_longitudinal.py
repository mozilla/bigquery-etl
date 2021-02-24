import subprocess
import sys
import textwrap

import pytest


def test_basic():
    results = subprocess.check_output(
        [
            sys.executable,
            "sql/moz-fx-data-shared-prod/telemetry/longitudinal.sql.py",
            "--tablename",
            "foo",
            "--to",
            "20190401",
        ]
    ).decode("ascii")
    assert results == textwrap.dedent(
        """
      WITH aggregated AS (
        SELECT
         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date_s3) AS _n,
         client_id,
         ARRAY_AGG(STRUCT(*)) OVER (PARTITION BY client_id ORDER BY submission_date_s3 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS aggcol
        FROM
          foo
        WHERE
          submission_date_s3 > '2018-10-01'
          AND submission_date_s3 <= '2019-04-01')
      SELECT
        * except (_n)
      FROM
        aggregated
      WHERE
        _n = 1

        """  # noqa: E501
    )


def test_everything():

    results = subprocess.check_output(
        [
            sys.executable,
            "sql/moz-fx-data-shared-prod/telemetry/longitudinal.sql.py",
            "--tablename",
            "foo",
            "--to",
            "20190401",
            "--submission-date-col",
            "submission_date_baz",
            "--select",
            "* EXCEPT locale, active_addons",
            "--where",
            "normalized_channel = 'beta'",
            "--grouping-column",
            "os",
            "--ordering-columns",
            "submission_date,app_name",
            "--max-array-length",
            "10000",
        ]
    ).decode("ascii")
    assert results == textwrap.dedent(
        """
      WITH aggregated AS (
        SELECT
         ROW_NUMBER() OVER (PARTITION BY os ORDER BY submission_date, app_name) AS _n,
         os,
         ARRAY_AGG(STRUCT(* EXCEPT locale, active_addons)) OVER (PARTITION BY os ORDER BY submission_date, app_name ROWS BETWEEN CURRENT ROW AND 10000 FOLLOWING) AS aggcol
        FROM
          foo
        WHERE
          submission_date_baz > '2018-10-01'
          AND submission_date_baz <= '2019-04-01'
          AND normalized_channel = 'beta')
      SELECT
        * except (_n)
      FROM
        aggregated
      WHERE
        _n = 1

        """  # noqa: E501
    )


def test_column_conflict():
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            [
                sys.executable,
                "sql/moz-fx-data-shared-prod/telemetry/longitudinal.sql.py",
                "--tablename",
                "foo",
                "--to",
                "20190401",
                "--grouping-column",
                "os",
                "--ordering-columns",
                "os,app_name",
            ],
            stderr=subprocess.STDOUT,
        )
    assert e.value.output.decode("ascii").endswith(
        "'os' can't be used as both a grouping and ordering column\n"
    )

#!/usr/bin/env python3
import argparse
import datetime
import sys
import textwrap

def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%Y%m%d")


def comma_separated(names):
    return [x.strip() for x in names.split(',')]


p = argparse.ArgumentParser()
p.add_argument('--tablename', type=str, help='Table to pull data from', required=True)
p.add_argument('--from', type=parse_date,
               help='From submission date. Defaults to 6 months before `to`.')
p.add_argument('--to', type=parse_date, help='To submission date', required=True)
p.add_argument(
    '--submission-date-col',
    type=str,
    help='Name of the submission date column. Defaults to submission_date_s3',
    default='submission_date_s3'
)
p.add_argument(
    '--select',
    type=str,
    help='Select statement to retrieve data with; e.g. '
    '"substr(subsession_start_date, 0, 10) as activity_date". If none given, '
    'defaults to all input columns',
    default='*'
)
p.add_argument(
    '--where',
    type=str,
    help='Where SQL clause, filtering input data. E.g. '
    '"normalized_channel = \'nightly\'"'
)
p.add_argument(
    '--grouping-column',
    type=str,
    help='Column to group data by. Defaults to client_id',
    default='client_id'
)
p.add_argument(
    '--ordering-columns',
    type=comma_separated,
    help='Columns to order the arrays by (comma separated). '
    'Defaults to submission-date-col'
)
p.add_argument(
    '--max-array-length',
    type=int,
    help='Max length of any groups history. Defaults to no limit. '
    'Negatives are ignored'
)


def generateSql(baseOpts):
    opts = vars(baseOpts).copy()
    opts.update({
        'from': datetime.datetime.strftime(
            opts['from'] or
            opts['to'] - datetime.timedelta(days=180),
        "'%Y-%m-%d'"),
        'to': datetime.datetime.strftime(opts['to'], "'%Y-%m-%d'"),
        'where': '\n{}AND {}'.format(' ' * 10, opts['where'])
        if opts['where'] else '',
        'ordering_columns': ', '.join(opts['ordering_columns'] or
                                      [opts['submission_date_col']]),
        'limit': '\n{}LIMIT\n{}{}'.format(
            ' ' * 8,
            ' ' * 10,
            opts['max_array_length'])
        if opts['max_array_length'] else ''
    })
    if opts['grouping_column'] in opts['ordering_columns']:
        raise ValueError(
            "{} can't be used as both a grouping and ordering column"
            .format(opts['grouping_column'], opts['ordering_columns']))

    return textwrap.dedent("""
      WITH aggregated AS (
        SELECT
         ROW_NUMBER() OVER (PARTITION BY {grouping_column} ORDER BY {ordering_columns}) AS _n,
         {grouping_column},
         ARRAY_AGG(STRUCT({select})) OVER (PARTITION BY {grouping_column} ORDER BY {ordering_columns} ROWS BETWEEN CURRENT ROW AND {limit} FOLLOWING) AS aggcol
        FROM
          {tablename}
        WHERE
          {submission_date_col} > {from}
          AND {submission_date_col} <= {to}{where})
      SELECT
        * except (_n)
      FROM
        aggregated
      WHERE
        _n = 1
    """.format(**opts))


def main(argv):
    opts = p.parse_args(argv[1:])
    print(generateSql(opts))


if __name__ == '__main__':
    main(sys.argv)

"""Identifies and records abnormalities in daily search metrics at country level"""

import click
import datetime as dt
import holidays
import numpy as np
import pandas as pd
from google.cloud import bigquery


# check whether the day +/- 1 day is holiday
def is_it_holiday(ds, country):
	isHoliday = False
	ds_range = [pd.to_datetime(ds) + dt.timedelta(days = x) for x in range(-2,2,1)] # get clever about the holiday if it's close to weekend
	try:
		isHoliday_range = [pd.to_datetime(ds) in holidays.CountryHoliday(country) for ds in ds_range]
		isHoliday = max(isHoliday_range)
	except:
		pass
	if country == 'CN':
		isHoliday = max([True if (x.month == 10) & (x.day == 1) else False for x in ds_range])
	if isHoliday == False:
		isHoliday = max([True if (x.month == 12) & (x.day >= 25) else False for x in ds_range]) # Christmax
	if isHoliday == False:
		isHoliday = max([True if (x.month == 1) & (x.day == 1) else False for x in ds_range]) # NewYear
	if isHoliday == False:
		isHoliday = max([True if (x.month == 5) & (x.day == 1) else False for x in ds_range]) # National Labor Day
	return isHoliday


def get_days_since_1970(adate):
   return (pd.to_datetime(adate)-pd.to_datetime(1970,1,1)).dt.days


@click.command()
@click.option("--project_id", required=True)
@click.option("--submission_date", required=True)
@click.option('--dry_run', is_flag=True, default=False)
def main(project_id, submission_date, dry_run):

    query_statement = """
        WITH
        new_data AS (
        SELECT
            submission_date,
            country,
            SUM(sap) AS sap,
            SUM(tagged_sap) AS tagged_sap,
            SUM(tagged_follow_on) AS tagged_follow_on,
            SUM(search_with_ads) AS search_with_ads,
            SUM(ad_click) AS ad_click
        FROM
            `mozdata.search.search_aggregates`
        WHERE
            submission_date = @submission_date
        GROUP BY
            1,
            2 ),
        long_data AS (
        SELECT
            submission_date,
            country,
            metric,
            value
        FROM (
            SELECT
            submission_date,
            country,
            'sap' AS metric,
            sap AS value
            FROM
            new_data)
        UNION ALL (
            SELECT
            submission_date,
            country,
            'tagged_sap' AS metric,
            tagged_sap AS value
            FROM
            new_data)
        UNION ALL (
            SELECT
            submission_date,
            country,
            'tagged_follow_on' AS metric,
            tagged_follow_on AS value
            FROM
            new_data)
        UNION ALL (
            SELECT
            submission_date,
            country,
            'search_with_ads' AS metric,
            search_with_ads AS value
            FROM
            new_data)
        UNION ALL (
            SELECT
            submission_date,
            country,
            'ad_click' AS metric,
            ad_click AS value
            FROM
            new_data) ),
        extended_new_data AS (
        SELECT
            submission_date,
            country,
            new_data_index,
            metric,
            value
        FROM (
            SELECT
            DATE(submission_date) AS submission_date,
            country,
            FALSE AS new_data_index,
            metric,
            value
            FROM
            `mozdata.analysis.desktop_search_alert_historical_data`
            WHERE
            DATE(submission_date) >= DATE_SUB((
                SELECT
                MAX(DATE(submission_date))
                FROM
                `mozdata.analysis.desktop_search_alert_historical_data`), INTERVAL 30 DAY) )
        UNION ALL (
            SELECT
            DATE(submission_date) AS submission_date,
            country,
            TRUE AS new_data_index,
            metric,
            value
            FROM
            long_data)
        ORDER BY
            submission_date,
            country )
        SELECT
        *
        FROM
        extended_new_data
    """

    client = bigquery.Client(project = project_id)

    query_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("submission_date", "DATE", submission_date),
        ]
    )

    query_job = client.query(query_statement, job_config=query_job_config)
    print("Running query:", query_job.job_id)
    search_data = (query_job.result().to_dataframe())

    search_data = search_data.sort_values(by=['submission_date', 'country', 'metric'])
    search_data['submission_date'] = pd.to_datetime(search_data['submission_date'])

    # today as day 0, what's the value in day -1, -2, -7, -14, -21, -28
    search_data['value_prev1d'] = search_data.groupby(['country', 'metric']).value.shift(1)
    search_data['value_prev2d'] = search_data.groupby(['country', 'metric']).value.shift(2)
    search_data['value_prev1w'] = search_data.groupby(['country', 'metric']).value.shift(7)
    search_data['value_prev2w'] = search_data.groupby(['country', 'metric']).value.shift(14)
    search_data['value_prev3w'] = search_data.groupby(['country', 'metric']).value.shift(21)
    search_data['value_prev4w'] = search_data.groupby(['country', 'metric']).value.shift(28)

    # today as day 0, what's today's value over day -1, -2, -7, -14, -21, -28
    search_data['dod'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(1)
    search_data['do2d'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(2)
    search_data['wow'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(7)
    search_data['wo2w'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(14)
    search_data['wo3w'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(21)
    search_data['wo4w'] = search_data['value']/search_data.groupby(['country', 'metric']).value.shift(28)

    # today as day 0, what's today's value contribution over global; and how did it look like day -1, -2, -7, -8 
    search_data['pcnt_value'] = search_data['value']/search_data.groupby(['submission_date', 'metric']).value.transform(np.sum)
    search_data['pcnt_value_prevd'] = search_data.groupby(['country', 'metric']).pcnt_value.shift(1)
    search_data['pcnt_value_prev2d'] = search_data.groupby(['country', 'metric']).pcnt_value.shift(2)
    search_data['pcnt_value_prev1w'] = search_data.groupby(['country', 'metric']).pcnt_value.shift(7)
    search_data['pcnt_value_prevd_prev1w'] = search_data.groupby(['country', 'metric']).pcnt_value.shift(8)

    # in terms of dod, how did it look like for day -1, -2
    search_data['dod_prevd'] = search_data.groupby(['country', 'metric']).dod.shift(1)
    search_data['dod_prev2d'] = search_data.groupby(['country', 'metric']).dod.shift(2)

    # in terms of wow, how did it look like for day -1, -2
    search_data['wow_prevd'] = search_data.groupby(['country', 'metric']).wow.shift(1)
    search_data['wow_prev2d'] = search_data.groupby(['country', 'metric']).wow.shift(2)

    # how did it look like for dod today, and dod same day last week? 
    search_data['wow_in_dod'] = search_data['dod']/search_data.groupby(['country', 'metric']).dod.shift(7)

    # how did it look like for wow today, and wow yesterday, and the day before yesterday?
    search_data['dod_in_wow'] = search_data['wow']/search_data.groupby(['country', 'metric']).wow.shift(1)
    search_data['do2d_in_wow'] = search_data['wow']/search_data.groupby(['country', 'metric']).wow.shift(2)

    # Only grab the data after the latest_date and add to the table
    search_data = search_data.loc[search_data.new_data_index == True]
    search_data = search_data.drop(columns= ['new_data_index'], axis=1)

    if dry_run:
        print("Dry-run mode, will not write to 'mozdata.analysis.desktop_search_alert_historical_data'")
    else:
        print("Updating 'mozdata.analysis.desktop_search_alert_historical_data'")
        job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
        job = client.load_table_from_dataframe(search_data, 'mozdata.analysis.desktop_search_alert_historical_data', job_config = job_config)
        job.result()

    """### General Rule without estimate to identify outlier"""

    # can we order the conditions in descending severity?
    # dow: Sat - 5, Sun - 6, Mon - 0, Tue - 1, Wed - 2, Thu - 3, Fri - 4

    search_data['dayofweek'] = search_data['submission_date'].dt.dayofweek

    # Note, the checking quit after the first satisfying condition is met, so should aim to add criterion only if they won't meet by with previous conditions
    conditions = [    
        # Rule1: drop we want to capture on day1 
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.001) & (search_data['dod'] < 0.1), # decrease (-2)
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.005) & (search_data['dod'] < 0.3), # decrease (-2)
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.015) & (search_data['dod'] < 0.6) & (search_data['dayofweek'] < 5), # decrease (-2)  

        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.002) & (search_data['dod'] < 0.25), # decrease (-1)
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.005) & (search_data['dod'] < 0.6) & (search_data['dayofweek'] < 5), # decrease (-1)
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.025) & (search_data['wow'] < 0.8) & (search_data['dod_in_wow'] < 0.8), # decrease (-1)

        # increase we want to capture on day1 -- less sensitive than deal with drop
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.01) & (search_data['dod'] > 3) & (search_data['dayofweek'] != 0), # increase (1)
        (search_data.value_prev1d > 1000) & (search_data['pcnt_value_prevd'] > 0.01) & (search_data['wow'] > 2) & (search_data['dod_in_wow'] > 2), # increase (1)

        # Rule2: We aim to capture the drop on the completion of the 2nd day if we didn't capture it on the 1st day 
        # if (1) do2d dropped to < 40%, and (2) wow < 60% and (3) the contribution > 0.1% 
        (search_data.value_prev2d > 1000) & (search_data['pcnt_value_prev2d'] > 0.002) & (search_data['wow'] < 0.5) \
        & (search_data['dod'] < 0.9) & (search_data['do2d'] < 0.6) & (search_data['dayofweek'] < 5), 
        # if (1) wow dropped to <60% two days in a roll
        (search_data.pcnt_value_prevd_prev1w >= 0.05) & (search_data['wow'] < 0.75) & (search_data['wow_prevd'] < 0.75),
        # increase
        (search_data.value> 1000) & (search_data['pcnt_value'] > 0.003) & (search_data['wow'] > 1.5*1.0/0.6) & (search_data['do2d'] > 1.5*1.0/0.4) 

    ]

    choices = [-2, -2, -2, -1,  -1, -1, 1, 1, -1, -1, 1]
    search_data['abnormal'] = np.select(conditions, choices, default=0)
    abnormality_data = search_data.loc[(search_data.abnormal != 0)]
    abnormality_data['is_holiday'] = [is_it_holiday(abnormality_data.iloc[i]['submission_date'], abnormality_data.iloc[i]['country']) for i in range(abnormality_data.shape[0])]
    abnormality_data['latest_abnormality_in_days'] =   (abnormality_data['submission_date']-pd.to_datetime('1970-01-01')).dt.days

    # if there is newly added abnormality data, then add it to the alert records
    if(abnormality_data.shape[0] > 0):
        if dry_run:
            print("Dry-run mode, will not write to 'mozdata.analysis.desktop_search_alert_records'")
        else:
            print("Updating 'mozdata.analysis.desktop_search_alert_records'")
            job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
            job = client.load_table_from_dataframe(abnormality_data, 'mozdata.analysis.desktop_search_alert_records', job_config = job_config)
            job.result()

    # Append to 'mozdata.analysis.desktop_search_alert_latest_daily' daily the latest abnormality date so we can properly trigger the Looker alert
    # In Looker, time series alert check the last 2 rows in the data table (so we need to append daily even there is no abnormality o/t the alert won't work
    query_statement = """
        WITH
        no_holiday_update AS (
        SELECT
            @submission_date AS asof,
            "No" AS is_holiday,
            DATE(MAX(submission_date)) AS latest_abnormality_date,
            MAX(latest_abnormality_in_days) AS latest_abnormality_date_int
        FROM
            `mozdata.analysis.desktop_search_alert_records`
        WHERE
            is_holiday IS FALSE
        GROUP BY
            1,
            2 ),
        holiday_update AS (
        SELECT
            @submission_date AS asof,
            "Yes" AS is_holiday,
            DATE(MAX(submission_date)) AS latest_abnormality_date,
            MAX(latest_abnormality_in_days) AS latest_abnormality_date_int
        FROM
            `mozdata.analysis.desktop_search_alert_records`
        WHERE
            is_holiday IS TRUE
        GROUP BY
            1,
            2 ),
        all_update AS (
        SELECT
            @submission_date AS asof,
            "All" AS is_holiday,
            DATE(MAX(submission_date)) AS latest_abnormality_date,
            MAX(latest_abnormality_in_days) AS latest_abnormality_date_int
        FROM
            `mozdata.analysis.desktop_search_alert_records`
        GROUP BY
            1,
            2 )
        SELECT
        asof,
        is_holiday,
        latest_abnormality_date,
        latest_abnormality_date_int
        FROM
        no_holiday_update
        UNION ALL (
        SELECT
            asof,
            is_holiday,
            latest_abnormality_date,
            latest_abnormality_date_int
        FROM
            holiday_update)
        UNION ALL (
        SELECT
            asof,
            is_holiday,
            latest_abnormality_date,
            latest_abnormality_date_int
        FROM
            all_update)
    """

    query_job = client.query(query_statement, job_config=query_job_config)
    print("Running query:", query_job.job_id)
    latest_alert_data = (query_job.result().to_dataframe())
    
    if dry_run:
        print("Dry-run mode, will not write to 'mozdata.analysis.desktop_search_alert_latest_daily'")
    else:
        print("Updating 'mozdata.analysis.desktop_search_alert_latest_daily'")
        job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
        job = client.load_table_from_dataframe(latest_alert_data, 'mozdata.analysis.desktop_search_alert_latest_daily', job_config = job_config)
        job.result()


if __name__ == "__main__":
    main()

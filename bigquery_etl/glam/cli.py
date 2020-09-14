import click
from google.cloud import bigquery


@click.group()
def glam():
    pass


@glam.group()
def glean():
    pass


@glean.command()
@click.option("--project", default="glam-fenix-dev")
@click.option("--dataset", default="glam_etl_dev")
def list_daily(project, dataset):
    client = bigquery.Client()
    app_df = client.query(
        f"""
        WITH
        extracted AS (
            SELECT
                DISTINCT REGEXP_EXTRACT(table_name, "(.*)__") AS app_id,
            FROM
                `{project}`.{dataset}.INFORMATION_SCHEMA.TABLES
            WHERE
                table_name LIKE "%clients_daily%" )
        SELECT
            app_id,
            (app_id LIKE "%glam%") AS is_logical
        FROM
            extracted
        ORDER BY
            is_logical,
            app_id
    """
    ).to_dataframe()

    query = []
    for row in app_df.itertuples():
        query += [
            f"""
            SELECT
                "{row.app_id}" as app_id,
                {row.is_logical} as is_logical,
                date(min(submission_date)) as earliest,
                date(max(submission_date)) as latest
            FROM
                `{project}`.{dataset}.{row.app_id}__view_clients_daily_scalar_aggregates_v1
            """
        ]

    range_df = (
        client.query("\nUNION ALL\n".join(query))
        .to_dataframe()
        .sort_values(["is_logical", "app_id"])
    )
    click.echo(range_df)


#
def backfill():
    pass


if __name__ == "__main__":
    glam()

from pathlib import Path

import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

ROOT_DIR = Path(__file__).parent
SQL_DIR = ROOT_DIR.parent / "sql_queries"


def fetch_data(config: dict):
    target = config["target"].lower()

    valid_datasets = ["desktop", "mobile", "pocket"]
    assert (
        target in valid_datasets
    ), f"dataset must be one of desktop, mobile or pocket, you requested {target}"

    query_name = SQL_DIR / config["query_name"]

    with open(query_name, "r") as query_filestream:
        query = query_filestream.readlines()
    query = "".join(query)

    project = "moz-fx-data-shared-prod"
    # project = "moz-fx-data-bq-data-science"
    bq_client = bigquery.Client(project=project)
    #bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()

    dataset = (
        bq_client.query(query).result().to_dataframe()#bqstorage_client=bq_storage_client)
    )

    if target == "desktop":
        dataset = desktop_preprocessing(dataset, config["columns"])
        return dataset

    dataset = dataset[dataset[config["columns"]]]

    renames = {"submission_date": "ds", "cdou": "y"}

    dataset.rename(columns=renames, inplace=True)

    return dataset, bq_client


def desktop_preprocessing(dataset: pd.DataFrame, columns: list) -> pd.DataFrame:
    dataset = dataset[columns]
    dataset.sort_values(by=["submission_date"], inplace=True)

    second_half_2021 = dataset[dataset["submission_date"] >= "2021-07-01"]
    second_half_2021["cdou"] = second_half_2021["uri_dau_either_at"].cumsum()
    dataset = dataset[dataset["submission_date"] < "2021-07-01"]

    dataset["difference"] = dataset["uri_dau_either_at"] - dataset["uri_at_dau_cd"]

    dataset["concat"] = dataset.apply(
        lambda x: x["uri_at_dau_cd"]
        if x["submission_date"] < "2020-12-18"
        else x["uri_dau_either_at"],
        axis=1,
    )
    dataset["regressor_00"] = dataset.apply(
        lambda x: 0 if x["submission_date"] < "2020-12-18" else 1, axis=1
    )

    dataset = dataset[["submission_date", "concat", "regressor_00"]]

    renames = {"submission_date": "ds", "concat": "y"}

    dataset.rename(columns=renames, inplace=True)

    return dataset

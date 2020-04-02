"""Helper methods to fetch BigQuery tables."""

from fnmatch import fnmatchcase

from google.cloud import bigquery


def _uses_wildcards(pattern: str) -> bool:
    return bool(set("*?[]") & set(pattern))


def get_tables_matching_patterns(client: bigquery.Client, patterns):
    """Get BigQuery tables matching the provided patterns."""
    all_projects = None
    all_datasets = {}
    all_tables = {}
    matching_tables = []

    for pattern in patterns:
        project, _, dataset_table = pattern.partition(":")
        dataset, _, table = dataset_table.partition(".")
        projects = [project or client.project]
        dataset = dataset or "*"
        table = table or "*"
        if _uses_wildcards(project):
            if all_projects is None:
                all_projects = [p.project_id for p in client.list_projects()]
            projects = [p for p in all_projects if fnmatchcase(project, p)]
        for project in projects:
            datasets = [dataset]
            if _uses_wildcards(dataset):
                if project not in all_datasets:
                    all_datasets[project] = [
                        d.dataset_id for d in client.list_datasets(project)
                    ]
                datasets = [d for d in all_datasets[project] if fnmatchcase(d, dataset)]
            for dataset in datasets:
                dataset = f"{project}.{dataset}"
                tables = [(f"{dataset}.{table}", None)]
                if _uses_wildcards(table):
                    if dataset not in all_tables:
                        all_tables[dataset] = list(client.list_tables(dataset))
                    tables = [
                        f"{dataset}.{t.table_id}"
                        for t in all_tables[dataset]
                        if fnmatchcase(t.table_id, table)
                    ]
                    matching_tables += tables

    return matching_tables

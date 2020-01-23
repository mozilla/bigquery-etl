"""Get the standard sql format fully qualified id for a table."""


def sql_table_id(table):
    """Get the standard sql format fully qualified id for a table."""
    return f"{table.project}.{table.dataset_id}.{table.table_id}"

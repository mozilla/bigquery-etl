"""Functions that retrieve allow or block lists of probes."""


def get_etl_excluded_probes_quickfix():
    """Provide a static list of probes that must be excluded from aggregation.

    TODO: Change the function name and doc when final implementation is done.
    When so, this function will prob be turned into something like 'get_allowed_probes'
    and retrieve the list from an api call.
    """
    # See https://github.com/mozilla/glam/issues/1865
    return {"sqlite_store_open", "sqlite_store_query"}

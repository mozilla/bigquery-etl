"""Functions that retrieve allow or block lists of probes."""


def get_etl_excluded_probes_quickfix(product):
    """Provide a static list of probes that must be excluded from aggregation.

    TODO: Change the function name and doc when final implementation is done.
    When so, this function will prob be turned into something like 'get_allowed_probes'
    and retrieve the list from an api call.
    """
    # See https://github.com/mozilla/glam/issues/1865
    forbidden_probes_by_product = {
        "fenix": {},
        "desktop": {"sqlite_store_open", "sqlite_store_query"},
    }
    return forbidden_probes_by_product[product]

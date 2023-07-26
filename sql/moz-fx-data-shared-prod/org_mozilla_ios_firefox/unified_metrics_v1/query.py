"""Generate a query for unifying legacy metrics and glean metrics.

This approach is required because the order of columns is significant in order
to be able to union two tables, even if the set of columns are the same."""

import io
import json

import requests
from google.cloud import bigquery, exceptions

from bigquery_etl.format_sql.formatter import reformat


def get_columns(schema):
    """Return a list of columns corresponding to the schema.

    Modified from https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/c2dae92a7ed73e4774a897bc4d2a6a8608875339/mozilla_pipeline_schemas/utils.py#L30-L43
    """

    def traverse(prefix, columns):
        res = []
        for node in columns:
            name = node["name"]
            dtype = node["type"]
            # we consider a repeated field a leaf node, and sorted for our purposes
            if dtype == "RECORD" and node["mode"] != "REPEATED":
                res += traverse(f"{prefix}.{name}", node["fields"])
            else:
                res += [f"{prefix}.{name} {dtype}"]
        return res

    res = traverse("root", schema)
    return sorted(res)


def generate_query(columns, table, replacements: dict[str, str] = None):
    """Generate a SQL query given column names.

    We construct a query that selects columns into nested structs. Naive
    selection of all the columns will strip the namespace from the columns.

    The legacy core and legacy event tables are converted as subsets of the
    metrics glean ping. There may be more than one row per client, but this
    matches the existing semantics of the metrics ping. We use this method over
    joining the core and legacy pings because of the non-overlapping nature of
    these two pings and difficulty in using coalesce with a deeply nested
    structure.
    """

    # Build a string that contains the selected columns. We take the set of
    # columns and split them up by namespace. Each namespace is put inside of a
    # STRUCT call. For example, foo.a and foo.b will be translated into a
    # `STRUCT(foo.a, foo.b) as foo` nested column.
    acc = ""

    # Maintain the last element in the columns to determine when a transition
    # must be made.
    prev = []

    # Iterate over the sorted set of columns. This ensures that columns are
    # grouped together correctly. Every time the column goes into a namespace,
    # we push an opening struct statement onto the string. Every time we
    # complete nested struct, we close out the string by aliasing the struct to
    # the namespace.
    for col in sorted(columns):
        split = col.split(".")
        # check if we go deeper
        if len(split) > 1 and len(split) > len(prev):
            # the number of times to start nesting
            if len(prev) == 0:
                k = len(split) - 1
            else:
                k = len(split) - len(prev)
            acc += "struct(" * k
        # the two structs are different now, figure out how much we need to pop
        # off before we continue
        if len(split) > 1 and len(split) == len(prev):
            # find the common ancestor
            depth = 0
            for a, b in list(zip(split[:-1], prev[:-1])):
                if a != b:
                    break
                depth += 1
            # now pop off until we reach the ancestor
            for alias in reversed(prev[depth:-1]):
                acc = acc.rstrip(",")
                acc += f") as {alias},"
            # now enter the new struct
            acc += "struct(" * (len(split) - 1 - depth)
        # pop out of the struct
        if len(split) < len(prev):
            diff = len(prev) - len(split)
            # ignore the leaf
            prev.pop()
            for _ in range(diff):
                c = prev.pop()
                acc = acc.rstrip(",")
                acc += f") as {c},"
        acc += (replacements.get(col, col) if replacements else col) + ","
        prev = split
    # clean up any columns
    if len(prev) > 1:
        prev.pop()
        for c in reversed(prev):
            acc = acc.rstrip(",")
            acc += f") as {c},"
    acc = acc.rstrip(",")

    return reformat(f"select {acc} from `{table}`")


def update_schema(bq, table_id, schema):
    """Update the schema and return the table"""
    table = bq.get_table(table_id)
    table.schema = bq.schema_from_json(io.StringIO(json.dumps(schema)))
    bq.update_table(table, ["schema"])


def main():
    # get the most schema deploy (to the nearest 15 minutes)
    bq = bigquery.Client()
    label = bq.get_dataset("moz-fx-data-shared-prod.telemetry").labels.get(
        "schemas_build_id"
    )
    print(f"last deploy: {label}")

    # get the schema corresponding to the last commit
    commit_hash = label.split("_")[-1]
    schema_url = (
        "https://raw.githubusercontent.com/mozilla-services/mozilla-pipeline-schemas/"
        f"{commit_hash}/schemas/org-mozilla-ios-firefox/metrics/metrics.1.bq"
    )
    resp = requests.get(schema_url)
    schema = resp.json()
    column_summary = get_columns(schema)

    print(json.dumps(column_summary, indent=2))
    """
    The columns take on the following form:

    "root.additional_properties STRING",
    "root.client_info.android_sdk_version STRING",
    "root.client_info.app_build STRING",
    ...

    This will need to be processed yet again so we can query via bigquery
    """

    bq = bigquery.Client()
    legacy_core = (
        "moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.legacy_mobile_core_v2"
    )
    legacy_event = (
        "moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.legacy_mobile_event_counts_v2"
    )
    update_schema(bq, legacy_core, schema)
    update_schema(bq, legacy_event, schema)

    # these columns needs to be excluded due to a change in view generation (metrics)
    # for more details, see: https://github.com/mozilla/bigquery-etl/pull/4029
    # and https://bugzilla.mozilla.org/show_bug.cgi?id=1741487
    columns_to_exclude = ("root.metrics.text RECORD", "root.metrics.url RECORD", "root.metrics.jwe RECORD", "root.metrics.labeled_rate RECORD",)
    stripped = [c.split()[0].lstrip("root.") for c in column_summary if c not in columns_to_exclude]

    query_glean = generate_query(
        ['"glean" as telemetry_system', *stripped],
        "moz-fx-data-shared-prod.org_mozilla_ios_firefox.metrics",
    )
    query_legacy_replacements = {
        "submission_date": "DATE(_PARTITIONTIME) AS submission_date",
        "metrics.datetime.glean_validation_first_run_hour": (
            "mozfun.glean.parse_datetime(metrics.datetime.glean_validation_first_run_hour)"
            " AS glean_validation_first_run_hour"
        ),
    }
    query_legacy_core = generate_query(
        ['"legacy" as telemetry_system', *stripped],
        legacy_core,
        replacements=query_legacy_replacements,
    )
    query_legacy_events = generate_query(
        ['"legacy" as telemetry_system', *stripped],
        legacy_event,
        replacements=query_legacy_replacements,
    )

    view_body = reformat(" UNION ALL ".join([query_glean, query_legacy_core, query_legacy_events]))
    print(view_body)
    view_id = "moz-fx-data-shared-prod.org_mozilla_ios_firefox.unified_metrics"
    try:
        bq.delete_table(bq.get_table(view_id))
    except exceptions.NotFound:
        pass
    view = bigquery.Table(view_id)
    view.view_query = view_body
    bq.create_table(view)
    print(f"updated view at {view_id}")


def test_generate_query_simple():
    columns = ["a", "b"]
    res = generate_query(columns, "test")
    expect = reformat("select a, b from `test`")
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested():
    columns = ["a", "b.c", "b.d"]
    res = generate_query(columns, "test")
    expect = reformat("select a, struct(b.c, b.d) as b from `test`")
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested_deep_skip():
    columns = ["b.c.e", "b.d.f"]
    res = generate_query(columns, "test")
    expect = reformat(
        """
    select struct(
        struct(
            b.c.e
        ) as c,
        struct(
            b.d.f
        ) as d
    ) as b
    from `test`
    """
    )
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested_deep_uneven():
    columns = ["a.b.c.d", "a.b.e"]
    res = generate_query(columns, "test")
    expect = reformat(
        """
    select struct(struct(
            struct(
                a.b.c.d
            ) as c,
            a.b.e
        ) as b
    ) as a
    from `test`
    """
    )
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested_deep_anscestor():
    columns = ["a.b.c.d", "a.e.f.g"]
    res = generate_query(columns, "test")
    expect = reformat(
        """
    select
    struct(
        struct(struct(
            a.b.c.d
        ) as c) as b,
        struct(struct(
            a.e.f.g
        ) as f) as e
    ) as a
    from `test`
    """
    )
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested_deep_anscestor_shared_descendent_names():
    columns = ["a.b.c.d", "a.f.c.g"]
    res = generate_query(columns, "test")
    expect = reformat(
        """
    select
    struct(
        struct(struct(
            a.b.c.d
        ) as c) as b,
        struct(struct(
            a.f.c.g
        ) as c) as f
    ) as a
    from `test`
    """
    )
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


def test_generate_query_nested_deep():
    columns = ["a.b", "a.c", "a.d.x.y.e", "a.d.x.y.f", "g"]
    res = generate_query(columns, "test")
    expect = reformat(
        """
        select struct(
            a.b,
            a.c,
            struct(
                struct(
                    struct(
                        a.d.x.y.e,
                        a.d.x.y.f
                    ) as y
                ) as x
            ) as d
        ) as a,
        g
        from `test`
    """
    )
    assert res == expect, f"expected:\n{expect}\ngot:\n{res}"


if __name__ == "__main__":
    test_generate_query_simple()
    test_generate_query_nested()
    test_generate_query_nested_deep_skip()
    test_generate_query_nested_deep_uneven()
    test_generate_query_nested_deep_anscestor()
    test_generate_query_nested_deep_anscestor_shared_descendent_names()
    test_generate_query_nested_deep()
    main()

from .utils import get_custom_distribution_metadata
from itertools import combinations


def clients_scalar_aggregates(**kwargs):
    """Glean/Fenix specific variables."""
    attributes_list = [
        "client_id",
        "ping_type",
        "os",
        "app_version",
        "app_build_id",
        "channel",
    ]
    attributes_type_list = ["STRING", "STRING", "STRING", "INT64", "STRING", "STRING"]
    user_data_attributes_list = ["metric", "metric_type", "key"]
    return dict(
        attributes=",".join(attributes_list),
        attributes_list=attributes_list,
        attributes_type=",".join(
            f"{name} {dtype}"
            for name, dtype in zip(attributes_list, attributes_type_list)
        ),
        user_data_attributes=",".join(user_data_attributes_list),
        user_data_attributes_list=user_data_attributes_list,
        user_data_type="""
            ARRAY<
                STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                agg_type STRING,
                value FLOAT64
                >
            >
        """,
        **kwargs,
    )


def clients_histogram_aggregates(**kwargs):
    """Variables for templated SQL."""
    attributes_list = [
        "sample_id",
        "client_id",
        "ping_type",
        "os",
        "app_version",
        "app_build_id",
        "channel",
    ]
    return dict(
        attributes_list=attributes_list,
        attributes=",".join(attributes_list),
        metric_attributes="""
            latest_version,
            metric,
            metric_type,
            key,
            agg_type
        """,
        **kwargs,
    )


def clients_scalar_bucket_counts(**kwargs):
    """Variables for bucket_counts."""
    attributes_list = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    return dict(
        attributes=",".join(attributes_list),
        scalar_metric_types="""
            "counter",
            "quantity",
            "labeled_counter"
        """,
        boolean_metric_types="""
            "boolean"
        """,
        aggregate_attributes="""
            metric,
            metric_type,
            key
        """,
        aggregate_attributes_type="""
            metric STRING,
            metric_type STRING,
            key STRING
        """,
        **kwargs,
    )


def clients_histogram_bucket_counts(**kwargs):
    """Variables for templated SQL."""
    attributes_list = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    metric_attributes_list = [
        "latest_version",
        "metric",
        "metric_type",
        "key",
        "agg_type",
    ]

    return dict(
        attributes_list=attributes_list,
        attributes=",".join(attributes_list),
        metric_attributes_list=metric_attributes_list,
        metric_attributes=",".join(metric_attributes_list),
        custom_distribution_metadata_list=get_custom_distribution_metadata("fenix"),
        **kwargs,
    )


def probe_counts(**kwargs):
    """Variables for probe_counts."""

    attributes = ["ping_type", "os", "app_version", "app_build_id", "channel"]

    # If the set of attributes grows, the max_combinations can be set only
    # compute a shallow set for less query complexity
    max_combinations = len(attributes)
    attribute_combinations = []
    for subset_size in reversed(range(max_combinations + 1)):
        for grouping in combinations(attributes, subset_size):
            # channel and app_version are required in the GLAM frontend
            if "channel" not in grouping or "app_version" not in grouping:
                continue
            select_expr = []
            for attribute in attributes:
                select_expr.append((attribute, attribute in grouping))
            attribute_combinations.append(select_expr)

    return dict(
        attributes=attributes,
        attribute_combinations=attribute_combinations,
        aggregate_attributes="""
            metric,
            metric_type,
            key
        """,
        aggregate_grouping="""
            client_agg_type,
            agg_type
        """,
        # not boolean
        scalar_metric_types="""
            "counter",
            "quantity",
            "labeled_counter"
        """,
        boolean_metric_types="""
            "boolean"
        """,
        **kwargs,
    )


def scalar_percentiles(**kwargs):
    """Variables for bucket_counts."""
    attributes = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    max_combinations = len(attributes) + 1
    attribute_combinations = []
    for subset_size in reversed(range(max_combinations)):
        for grouping in combinations(attributes, subset_size):
            select_expr = []
            for attribute in attributes:
                select_expr.append((attribute, attribute in grouping))
            attribute_combinations.append(select_expr)

    return dict(
        attributes=attributes,
        attribute_combinations=attribute_combinations,
        aggregate_attributes="""
            metric,
            metric_type,
            key
        """,
        **kwargs,
    )

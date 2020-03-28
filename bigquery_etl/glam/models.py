from .utils import get_custom_distribution_metadata


def clients_scalar_aggregates():
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
    )


def clients_scalar_bucket_counts():
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
    )


def clients_histogram_bucket_counts():
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
    )

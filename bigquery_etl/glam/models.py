"""Variables for templated SQL."""
from .utils import get_custom_distribution_metadata, compute_datacube_groupings


def clients_scalar_aggregates(**kwargs):
    """Variables for clients scalar aggregation."""
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
    """Variables for histogram aggregates."""
    attributes_list = [
        "sample_id",
        "client_id",
        "ping_type",
        "os",
        "app_version",
        "app_build_id",
        "channel",
    ]
    fixed_attributes = ["app_version", "channel"]
    cubed_attributes = [x for x in attributes_list if x not in fixed_attributes]
    return dict(
        attributes_list=attributes_list,
        attributes=",".join(attributes_list),
        cubed_attributes=cubed_attributes,
        attribute_combinations=compute_datacube_groupings(cubed_attributes),
        metric_attributes="""
            latest_version,
            metric,
            metric_type,
            key,
            agg_type
        """,
        **kwargs,
    )


def scalar_bucket_counts(**kwargs):
    """Variables for scalar bucket_counts."""
    attributes_list = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    fixed_attributes = ["app_version", "channel"]
    cubed_attributes = [x for x in attributes_list if x not in fixed_attributes]
    return dict(
        attributes=",".join(attributes_list),
        cubed_attributes=cubed_attributes,
        attribute_combinations=compute_datacube_groupings(cubed_attributes),
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
        **{
            # re-use variables from previous query
            key: clients_scalar_aggregates()[key]
            for key in ["user_data_attributes", "user_data_type"]
        },
        **kwargs,
    )


def histogram_bucket_counts(**kwargs):
    """Variables for clients histogram bucket counts."""
    attributes_list = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    metric_attributes_list = [
        "latest_version",
        "metric",
        "metric_type",
        "key",
        "agg_type",
    ]
    fixed_attributes = ["app_version", "channel"]
    cubed_attributes = [x for x in attributes_list if x not in fixed_attributes]
    return dict(
        attributes_list=attributes_list,
        attributes=",".join(attributes_list),
        cubed_attributes=cubed_attributes,
        attribute_combinations=compute_datacube_groupings(cubed_attributes),
        metric_attributes_list=metric_attributes_list,
        metric_attributes=",".join(metric_attributes_list),
        custom_distribution_metadata_list=get_custom_distribution_metadata("fenix"),
        **kwargs,
    )


def probe_counts(**kwargs):
    """Variables for probe counts."""
    attributes = ["ping_type", "os", "app_version", "app_build_id", "channel"]

    return dict(
        attributes=",".join(attributes),
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
    """Variables for scalar percentiles."""
    attributes = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    fixed_attributes = ["app_version", "channel"]
    cubed_attributes = [x for x in attributes if x not in fixed_attributes]

    return dict(
        # TODO: be consistent with naming of attributes (e.g. attributes_list)
        attributes=attributes,
        cubed_attributes=cubed_attributes,
        attribute_combinations=compute_datacube_groupings(cubed_attributes),
        aggregate_attributes="""
            metric,
            metric_type,
            key
        """,
        **kwargs,
    )


def user_counts(**kwargs):
    """Variables for user counts."""
    attributes = ["ping_type", "os", "app_version", "app_build_id", "channel"]
    fixed_attributes = ["app_version", "channel"]
    cubed_attributes = [x for x in attributes if x not in fixed_attributes]

    return dict(
        attributes=",".join(attributes),
        cubed_attributes=cubed_attributes,
        attribute_combinations=compute_datacube_groupings(cubed_attributes),
        **kwargs,
    )

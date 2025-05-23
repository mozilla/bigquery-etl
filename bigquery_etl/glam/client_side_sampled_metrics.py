"""Client-side sampled metrics.

This module defines client-side sampled metrics and provides a function
to retrieve metrics by type.
"""

cs_sampled_metrics = {
    "distributions": [
        "glam_experiment_async_sheet_load",
        "glam_experiment_http_content_html5parser_ondatafinished_to_onstop_delay",
        "glam_experiment_largest_contentful_paint",
        "glam_experiment_protect_time",
        "glam_experiment_sub_complete_load_net",
        "glam_experiment_time",
    ],
    "counters": [
        "glam_experiment_active_ticks",
        "glam_experiment_panel_shown",
        "glam_experiment_os_socket_limit_reached",
        "glam_experiment_used",
        "glam_experiment_cpu_time_bogus_values",
        "glam_experiment_total_cpu_time_ms",
    ],
}


def get(metric_type: str) -> list:
    """Get client-side sampled metrics by type."""
    return cs_sampled_metrics[metric_type]

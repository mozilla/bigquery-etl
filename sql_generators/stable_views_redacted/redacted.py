import pandas as pd
from common import (
    build_select_clause,
    get_sensitive_metrics_for_ping,
    resolve_v1_name,
    write_redacted_view,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def main():
    bq_dataset_family = "firefox_desktop"
    ping_name = "microsurvey"
    # field_name = "metrics"
    sensitivity_categories = [
        "highly_sensitive",
        # "stored_content",
    ]

    v1_name = resolve_v1_name(bq_dataset_family)
    df, keep_bools_df = get_sensitive_metrics_for_ping(
        v1_name, ping_name, sensitivity_categories
    )

    full_sql = build_select_clause(keep_bools_df, df, bq_dataset_family, ping_name)
    print(full_sql)


if __name__ == "__main__":
    main()

--macro checks

#fail
{{ not_null(["braze_subscription_name", "description", "mozilla_subscription_id", "firefox_subscription_id", "mozilla_dev_subscription_id", "basket_slug"]) }}

#fail
{{ min_row_count(1) }}

#fail
{{ is_unique(["mozilla_subscription_id", "firefox_subscription_id", "mozilla_dev_subscription_id"]) }}

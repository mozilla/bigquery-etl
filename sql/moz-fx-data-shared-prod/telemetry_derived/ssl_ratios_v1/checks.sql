{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}

{{ min_rows(1) }}

{{ is_unique(["submission_date", "os", "country"])}}

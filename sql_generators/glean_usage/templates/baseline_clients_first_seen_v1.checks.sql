{{ header }}

#fail
{#
   We use raw here b/c the first pass is rendered to create the checks.sql
   files, and the second pass is rendering of the checks themselves.

   For example, the header above is rendered for every checks file
   when we create the checks file, when `bqetl generate glean_usage`
   is called.

   However the second part, where we render the check is_unique() below,
   is rendered when we _run_ the check, during `bqetl query backfill`
   (you can also run them locally with `bqetl check run`).
#}
{% raw -%}
  {{ is_unique(["client_id"]) }}
{% endraw %}


<!--
This is a short README for your routine, you can add any extra
documentation or examples that a user might want to see when
viewing the documentation at https://mozilla.github.io/bigquery-etl

You can embed an SQL file into your README using the following
syntax:

@sql(../examples/fenix_app_info.sql)
-->
## Scheduled Surface Id UDF

This function takes a country and locale to compute the Content teams identifier for the surface the article is
published.

Note:
The UDF tries to mimic the backend merino implementation
(https://github.com/mozilla-services/merino-py/blob/main/merino/curated_recommendations/provider.py#L66-L103) for
the Scheduled_Surface_Id.
The UDF is meant to be used temporarily until the backend computed value gets added to the newtab ping


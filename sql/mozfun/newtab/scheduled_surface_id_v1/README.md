## Scheduled Surface Id UDF

This function takes a country and locale to compute the Content teams identifier for the surface the article is
published.

Note:
The UDF tries to mimic the backend merino implementation
(https://github.com/mozilla-services/merino-py/blob/main/merino/curated_recommendations/provider.py#L66-L103) for
the Scheduled_Surface_Id.
The UDF is meant to be used temporarily until the backend computed value gets added to the newtab ping


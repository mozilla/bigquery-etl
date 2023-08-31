Determine if a given Addon ID is for an adblocker.

As an example, this query will give the number of users who
have an adblocker installed.
```
SELECT
    submission_date,
    COUNT(DISTINCT client_id) AS dau,
FROM
    mozdata.telemetry.addons
WHERE
    mozfun.addons.is_adblocker(addon_id)
    AND submission_date >= "2023-01-01"
GROUP BY
    submission_date
```

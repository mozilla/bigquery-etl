WITH mobile AS
  (SELECT submission_date,
          sum(dau) AS dau
   FROM telemetry.mobile_usage_2021
   WHERE submission_date BETWEEN DATE(2020, 1, 1) AND CURRENT_DATE
     AND app_name IN ('firefox_ios',
                      'fennec',
                      'fenix',
                      'focus_android',
                      'focus_ios')
   GROUP BY submission_date
   )
SELECT *,
       sum(dau) OVER (PARTITION BY extract(YEAR
                                           FROM submission_date)
                      ORDER BY submission_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cdou
FROM mobile
ORDER BY submission_date
WITH new_data AS (
  SELECT
    client_id,
    sample_id,
    MAX(submission_date) AS as_of_date,
    MAX_BY(ltv_states_v1, submission_date).* EXCEPT (client_id, sample_id, submission_date),
  FROM
    moz-fx-data-shared-prod.fenix_derived.ltv_states_v1
  WHERE
    {% if is_init() %}
      submission_date >= "2020-01-01"
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    client_id,
    sample_id
),
historic_data AS (
  SELECT
    *
  FROM
    fenix_derived.client_ltv_v1
)
SELECT
  (
    CASE
      WHEN new_data.as_of_date IS NULL
        THEN historic_data
      WHEN historic_data.as_of_date IS NULL
        THEN new_data
      WHEN new_data.as_of_date > historic_data.as_of_date
        THEN new_data
      ELSE historic_data
    END
  ).*
FROM
  historic_data
FULL OUTER JOIN
  new_data
USING
  (sample_id, client_id)

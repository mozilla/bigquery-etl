SELECT
  -- limit fields in stripe_derived so as not to expose sensitive data
  id,
  billing_reason,
  customer,
  status,
  status_transitions,
  ARRAY(
    SELECT
      STRUCT(
        line.id,
        line.amount,
        line.currency,
        STRUCT(line.period.start, line.period.`end`) AS period,
        line.subscription,
        STRUCT(line.plan.id, line.plan.amount, line.plan.currency) AS plan
      ) AS lines
    FROM
      UNNEST(lines) AS line
  ) AS lines
FROM
  stripe_external.invoices_v1

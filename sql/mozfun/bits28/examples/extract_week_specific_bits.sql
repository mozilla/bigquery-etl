SELECT
  -- Signature is bits28.range(offset_to_day_0, start_bit, number_of_bits)
  mozfun.bits28.range(days_seen_bits, -13 + 0, 7) AS week_0_bits,
  mozfun.bits28.range(days_seen_bits, -13 + 7, 7) AS week_1_bits
FROM
  telemetry.clients_last_seen
WHERE
  submission_date > '2020-01-01'

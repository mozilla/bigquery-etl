# bits28

The `bits28` functions provide an API for working with "bit pattern" INT64
fields, as used in the [`clients_last_seen` dataset](https://docs.telemetry.mozilla.org/datasets/bigquery/clients_last_seen/reference.html)
for desktop Firefox and similar datasets for other applications.

A powerful feature of the `clients_last_seen` methodology is that it doesn't
record specific metrics like MAU and WAU directly, but rather each row stores
a history of the discrete days on which a client was active in the past 28 days.
We could calculate active users in a 10 day or 25 day window just as efficiently
as a 7 day (WAU) or 28 day (MAU) window. But we can also define completely new
metrics based on these usage histories, such as various retention definitions.

The usage history is encoded as a "bit pattern" where the physical
type of the field is a BigQuery INT64, but logically the integer
represents an array of bits, with each 1 indicating a day where the given clients
was active and each 0 indicating a day where the client was inactive.


## to_string (UDF)

Convert an INT64 field into a 28-character string representing
the individual bits.

Implementation based on <https://stackoverflow.com/a/51600210/1260237>

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>


```sql
SELECT
  [mozfun.bits28.to_string(1), mozfun.bits28.to_string(2), mozfun.bits28.to_string(3)]
-- >>> ['0000000000000000000000000001',
--      '0000000000000000000000000010',
--      '0000000000000000000000000011']
```


## days_since_seen (UDF)

Return the position of the rightmost set bit in an INT64 bit pattern.

To determine this position, we take a bitwise AND of the bit pattern and
its complement, then we determine the position of the bit via base-2
logarithm; see <https://stackoverflow.com/a/42747608/1260237>

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>


```sql
SELECT
  mozfun.bits28.days_since_seen(18)
-- >> 1
```


## retention (UDF)

Return a nested struct providing booleans indicating whether a given client was active various time periods based on the passed bit pattern.



## active_in_range (UDF)

Return a boolean indicating if any bits are set in the specified range
of a bit pattern. The `start_offset` must be zero or a negative number
indicating an offset from the rightmost bit in the pattern. n_bits is
the number of bits to consider, counting right from the bit at
`start_offset`.

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>




## to_dates (UDF)

Convert a bit pattern into an array of the dates is represents.

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>




## range (UDF)

Return an INT64 representing a range of bits from a source bit pattern.

The start_offset must be zero or a negative number indicating an offset from
the rightmost bit in the pattern.

n_bits is the number of bits to consider, counting right from the bit at
start_offset.

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>


```sql
SELECT
  -- Signature is bits28.range(offset_to_day_0, start_bit, number_of_bits)
  mozfun.bits28.range(days_seen_bits, -13 + 0, 7) AS week_0_bits,
  mozfun.bits28.range(days_seen_bits, -13 + 7, 7) AS week_1_bits
FROM
  telemetry.clients_last_seen
WHERE
  submission_date > '2020-01-01'
```


## from_string (UDF)

Convert a string representing individual bits into an INT64.

Implementation based on <https://stackoverflow.com/a/51600210/1260237>

See detailed docs for the bits28 suite of functions:
<https://docs.telemetry.mozilla.org/cookbooks/clients_last_seen_bits.html#udf-reference>



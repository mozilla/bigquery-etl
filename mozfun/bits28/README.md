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

/*

Takes in a DATE type and returns a `YYYY-MM-DD` formatted string. This
function does _not_ attempt to replicate the entirety of the
functionality of Presto/Athena's `to_iso8601()` (which takes in a DATE,
DATETIME or TIMESTAMP type and returns either an iso8601-formatted date
string or a date-time string depending on the input type) since all use
cases found in legacy scheduled queries were for the DATE case.

*/
CREATE TEMP FUNCTION
  udf_legacy_to_iso8601(x DATE)
  RETURNS STRING
  AS (
    FORMAT_DATE('%F', x)
  );

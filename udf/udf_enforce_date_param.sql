/*

Returns true if the two passed dates are identical or if either of them is
equal to the special flag date 0001-01-01.

This is useful for queries that have a @submission_date parameter and are
generally executed incrementally, but occasionally need to be run over all
time to recreate the table. A query can be written with a WHERE clause like:

    WHERE udf_enforce_date_param(submission_date, @submission_date)

Under normal operation, this clause is equivalent to:

    WHERE submission_date = @submission_date

But if you execute the query with `--parameter submission_date:DATE:1111-11-11`,
this essentially removes the WHERE clause.

Proper use of this function requires that your query not reference the
`@submission_date` parameter anywhere other than in the call to this function.
It also requires that each day is independent, enforced by having your query
group by submission_date or partition by submission_date in window functions
(such as in clients_daily).

*/

CREATE TEMP FUNCTION
  udf_enforce_date_param(p1 DATE,
    p2 DATE)
  RETURNS BOOLEAN AS ( (p1 = p2)
    OR (p1 = DATE('1111-11-11'))
    OR (p2 = DATE('1111-11-11')) );
--
SELECT
  assert_true(udf_enforce_date_param(DATE('1111-11-11'), DATE('2019-06-02'))),
  assert_true(udf_enforce_date_param(DATE('2019-06-02'), DATE('1111-11-11'))),
  assert_true(udf_enforce_date_param(DATE('1111-11-11'), DATE('1111-11-11'))),
  assert_true(udf_enforce_date_param(DATE('2019-06-02'), DATE('2019-06-02'))),
  assert_false(udf_enforce_date_param(DATE('2019-06-02'), DATE('2019-06-03')))

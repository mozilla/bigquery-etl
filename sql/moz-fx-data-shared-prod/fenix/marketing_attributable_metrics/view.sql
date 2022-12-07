CREATE OR REPLACE VIEW
  fenix.marketing_attributable_metrics
AS
SELECT
  * EXCEPT (searches),
  searches AS search_count,
FROM
  fenix.attributable_clients

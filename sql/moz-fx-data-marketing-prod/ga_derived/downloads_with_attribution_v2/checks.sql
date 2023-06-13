ASSERT((SELECT COUNT(*) FROM `..` WHERE download_date = @download_date) > 250000)
AS
  'ETL Data Check Failed: Table .. contains less than 250,000 rows for date: .'

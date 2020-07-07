SELECT
  mozfun.hist.extract(
    '{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}'
  ).sum
-- 1

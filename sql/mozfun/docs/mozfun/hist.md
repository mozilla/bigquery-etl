# hist

Functions for working with string encodings of histograms from desktop telemetry.


## mean (UDF)

Given histogram h, return floor(mean) of the measurements in the bucket.
That is, the histogram sum divided by the number of measurements taken.

<https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L292-L307>




## merge (UDF)

Merge an array of histograms into a single histogram.

- The histogram values will be summed per-bucket
- The count will be summed
- Other fields will take the mode_last




## normalize (UDF)

Normalize a histogram. Set sum to 1, and normalize to 1 the histogram bucket counts.



## percentiles (UDF)

Given histogram and list of percentiles,calculate what those percentiles are for the histogram. If the histogram is empty, returns NULL.



## threshold_count (UDF)

Return the number of recorded observations greater than threshold for
the histogram.  CAUTION: Does not count any buckets that have any values
less than the threshold. For example, a bucket with range (1, 10) will
not be counted for a threshold of 2. Use threshold that are not bucket
boundaries with caution.

<https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L213-L239>




## extract (UDF)

Return a parsed struct from a string-encoded histogram.

We support a variety of compact encodings as well as the classic JSON
representation as sent in main pings.

The built-in BigQuery JSON parsing functions are not powerful enough to handle
all the logic here, so we resort to some string processing. This function could
behave unexpectedly on poorly-formatted histogram JSON, but we expect that
payload validation in the data pipeline should ensure that histograms are well
formed, which gives us some flexibility.

For more on desktop telemetry histogram structure, see:

- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/collection/histograms.html

The compact encodings were originally proposed in:

- https://docs.google.com/document/d/1k_ji_1DB6htgtXnPpMpa7gX0klm-DGV5NMY7KkvVB00/edit#

```sql
SELECT
  mozfun.hist.extract(
    '{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}'
  ).sum
-- 1
```

```sql
SELECT
  mozfun.hist.extract('5').sum
-- 5
```


## string_to_json (UDF)

Convert a histogram string (in JSON or compact format) to a full histogram JSON blob.



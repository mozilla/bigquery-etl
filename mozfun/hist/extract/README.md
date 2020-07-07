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

@sql(../examples/full_hist_extract.sql)

@sql(../examples/use_counter_hist_extract.sql)

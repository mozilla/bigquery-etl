## Parse Ad Group Name UDF

This function takes a ad group name and parses out known segments.
These segments are things like country, language, or audience; multiple
ad groups can share segments.

Currently supported formats are `gads_v2` (9 segments, parsed into
`ad_network, version, product, initiative, region, country_code, placement,
audience, targeting`) and `meta_v2` (5 segments, parsed into `ad_network,
version, audience, placement, targeting`).

We use versioned ad group names to define segments, where the ad network
(e.g. gads) and the version (e.g. v1, v2) correspond to certain available segments
in the ad group name. We track the versions in [this spreadsheet](https://docs.google.com/spreadsheets/d/1hkK8-IKbgjsHRQq_XNm-Hr6bA8657CcpnChSywlYUMU/edit#gid=635816846).

For a history of this naming scheme, see the [original proposal](https://docs.google.com/document/d/1lnZ6iMT091fq37SmnbpD9IcREMpGupae19Euqu94Low/edit).

See also: `marketing.parse_campaign_name`, which does the same, but for campaign names.

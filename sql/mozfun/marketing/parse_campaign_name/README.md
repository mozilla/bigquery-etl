## Parse Campaign Name UDF

This function takes a campaign name and parses out known segments.
These segments are things like country, language, or audience; multiple
campaigns can share segments.

We use versioned campaign names to define segments, where the ad network
(e.g. gads) and the version (e.g. v1, v2) correspond to certain available segments
in the campaign name. We track the versions in [this spreadsheet](https://docs.google.com/spreadsheets/d/1hkK8-IKbgjsHRQq_XNm-Hr6bA8657CcpnChSywlYUMU/edit#gid=635816846).

For a history of this naming scheme, see the [original proposal](https://docs.google.com/document/d/1lnZ6iMT091fq37SmnbpD9IcREMpGupae19Euqu94Low/edit).

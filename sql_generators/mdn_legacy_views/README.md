# MDN legacy views

Generates `mdn.legacy_action` and `mdn.legacy_page`, which union Yari pings
(`mdn_yari.action`, `mdn_yari.page`) with Fred events (`mdn_fred.events_stream`)
reshaped to match the Yari ping schema.

`templates/fred_events.query.sql` reshapes Fred events and only lists the fields that Fred
provides. At generation time, the schemas of the Yari tables and of the Fred query
are read with dry runs, and `Schema.generate_compatible_select_expression` selects
every Yari field explicitly on both sides of the `UNION ALL`. Yari fields missing
from the Fred query are set to `NULL`.

Because no side uses `SELECT *`, fields that Glean adds upstream (e.g. to
`ping_info`) can't break the deployed views. New fields appear in the views after
the next regeneration.

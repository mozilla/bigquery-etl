-- The addon_id is hashed due to string size limitations in events;
-- we create a "unhashing" lookup table here from the DAU table, which should
-- include all the same addon_ids that we see in install_stats events.
WITH addon_id_lookup AS (
  SELECT DISTINCT
    addon_id,
    TO_HEX(SHA256(addon_id)) AS hashed_addon_id
  FROM
    amo_dev.amo_stats_dau_v2
)
SELECT
  installs.*
FROM
  amo_prod.amo_stats_installs_v3 AS installs
-- This join will filter out all addon_ids that are not already present
-- in the dev dau table, so we don't need to duplicate the list of addons
-- approved for dev here.
JOIN
  addon_id_lookup
  USING (hashed_addon_id)
WHERE
  submission_date = @submission_date

CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.nimbus_feature_monitoring` AS (
    SELECT
      *,
      'address-autofill-feature' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_address_autofill_feature_v1`
    UNION ALL
    SELECT
      *,
      'newtabTrainhopAddon' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabtrainhopaddon_v1`
    UNION ALL
    SELECT
      *,
      'newtabSponsoredContent' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabsponsoredcontent_v1`
    UNION ALL
    SELECT
      *,
      'newTabSectionsExperiment' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabsectionsexperiment_v1`
  )

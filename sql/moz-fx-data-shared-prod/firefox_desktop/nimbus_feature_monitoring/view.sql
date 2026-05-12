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
    UNION ALL
    SELECT
      *,
      'newtabTrainhop' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabtrainhop_v1`
    UNION ALL
    SELECT
      *,
      'pocketNewtab' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_pocketnewtab_v1`
    UNION ALL
    SELECT
      *,
      'newtabPrivatePing' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabprivateping_v1`
    UNION ALL
    SELECT
      *,
      'newtabMerinoOhttp' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabmerinoohttp_v1`
    UNION ALL
    SELECT
      *,
      'newtabAdSizingExperiment' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabadsizingexperiment_v1`
    UNION ALL
    SELECT
      *,
      'newtabPromoCard' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabpromocard_v1`
    UNION ALL
    SELECT
      *,
      'newtabInferredPersonalization' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_newtabinferredpersonalization_v1`
    UNION ALL
    SELECT
      *,
      'urlbar' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_urlbar_v1`
    UNION ALL
    SELECT
      *,
      'infobar' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_infobar_v1`
    UNION ALL
    SELECT
      *,
      'aboutwelcome' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_aboutwelcome_v1`
    UNION ALL
    SELECT
      *,
      'featureCallout' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_featurecallout_v1`
    UNION ALL
    SELECT
      *,
      'fxms-bmb-button' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_bmb_button_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-22' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_22_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-1' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_1_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-20' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_20_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-7' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_7_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-16' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_16_v1`
    UNION ALL
    SELECT
      *,
      'fxms-message-21' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_fxms_message_21_v1`
    UNION ALL
    SELECT
      *,
      'backgroundTaskMessage' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_backgroundtaskmessage_v1`
    UNION ALL
    SELECT
      *,
      'preonboarding' AS feature,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_feature_monitoring_preonboarding_v1`
  )

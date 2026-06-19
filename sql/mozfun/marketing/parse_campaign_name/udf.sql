CREATE OR REPLACE FUNCTION marketing.parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    -- Campaign Artifact Schema - Google Ads - version 2 or 3 (and Reddit Ads v2,
    -- which shares the identical layout).
    -- Supports two formats:
    --   (a) Legacy 18-segment: ad_gap_id and po are single tokens at positions 17-18.
    --   (b) New variable-length: literal `_adgapid_` separator with compound
    --       ad_gap_id (may contain underscores); po is the final segment.
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gads_v[2-3]|reddit_v2)")
      THEN
        CASE
          -- New format: literal `_adgapid` separator, compound ad_gap_id. The value
          -- after `_adgapid` is optional: a bare trailing `_adgapid` parses with
          -- ad_gap_id and po both NULL. `audience` is included only when 16 positional
          -- fields precede the marker.
          WHEN REGEXP_CONTAINS(campaign_name, r"_adgapid(_|$)")
            AND ARRAY_LENGTH(
              SPLIT(REGEXP_EXTRACT(campaign_name, r"^(.*?)_adgapid(?:_|$)"), "_")
            ) IN (15, 16)
            THEN mozfun.map.from_lists(
                ARRAY_CONCAT(
                  [
                    'ad_network',
                    'version',
                    'product',
                    'initiative',
                    'region',
                    'country_code',
                    'city'
                  ],
                  IF(
                    ARRAY_LENGTH(
                      SPLIT(REGEXP_EXTRACT(campaign_name, r"^(.*?)_adgapid(?:_|$)"), "_")
                    ) = 16,
                    ['audience'],
                    CAST([] AS ARRAY<STRING>)
                  ),
                  [
                    'language',
                    'device',
                    'operating_system',
                    'campaign_type',
                    'campaign_goal',
                    'campaign_group',
                    'bidding_type',
                    'optimization_goal',
                    'ad_gap_id',
                    'po'
                  ]
                ),
                ARRAY_CONCAT(
                  SPLIT(REGEXP_EXTRACT(campaign_name, r"^(.*?)_adgapid(?:_|$)"), "_"),
                  [
                    REGEXP_EXTRACT(campaign_name, r"_adgapid_(.+?)_[^_]+$"),
                    REGEXP_EXTRACT(campaign_name, r"_adgapid_.+_([^_]+)$")
                  ]
                )
              )
          -- Legacy format: 18 segments, single-token ad_gap_id and po
          WHEN ARRAY_LENGTH(
              SPLIT(REGEXP_REPLACE(campaign_name, r"_(f[i]?rst_open)_", "_firstopen_"), "_")
            ) = 18
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'audience',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal',
                  'ad_gap_id',
                  'po'
                ],
                SPLIT(REGEXP_REPLACE(campaign_name, r"_(f[i]?rst_open)_", "_firstopen_"), "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Google Ads - version 1
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gds_v1)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 15
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Facebook Ads - version 1 or 2.
    -- Conformed names use a lowercase `meta_v[12]` prefix with 17 positional
    -- fields. This branch also accepts an older 15-field variant (no
    -- `optimization_goal` / `ad_gap_id`; `po` is the final segment), all observed on
    -- legacy Meta conversion-test campaigns. Any other segment count is NULL.
    WHEN REGEXP_CONTAINS(campaign_name, r"(?i)^(new)?meta_v[12]")
      THEN (
          SELECT
            CASE
              ARRAY_LENGTH(t)
              WHEN 17
                THEN mozfun.map.from_lists(
                    [
                      'ad_network',
                      'version',
                      'product',
                      'initiative',
                      'region',
                      'country_code',
                      'city',
                      'language',
                      'device',
                      'operating_system',
                      'campaign_type',
                      'campaign_goal',
                      'campaign_group',
                      'bidding_type',
                      'optimization_goal',
                      'ad_gap_id',
                      'po'
                    ],
                    t
                  )
              WHEN 15
                THEN mozfun.map.from_lists(
                    [
                      'ad_network',
                      'version',
                      'product',
                      'initiative',
                      'region',
                      'country_code',
                      'city',
                      'language',
                      'device',
                      'operating_system',
                      'campaign_type',
                      'campaign_goal',
                      'campaign_group',
                      'bidding_type',
                      'po'
                    ],
                    t
                  )
              ELSE NULL
            END
          FROM
            (SELECT SPLIT(REGEXP_REPLACE(campaign_name, r"(?i)^new", ""), "_") AS t)
        )
    -- Campaign Artifact Schema - Microsoft Ads v1 / Google Ads v1
    -- (`ms_v1` and `gads_v1` share the same paid-search taxonomy.)
    -- Take the first 15 underscore-separated segments, or 16 when `audience` is
    -- present. `audience` presence is detected by inspecting position 7: a locale
    -- code there means position 7 is `language` (no audience), anything else (e.g.
    -- `all`) means it is `audience`. "Locale code" includes a single locale (`en`,
    -- `pt-br`) and a run of concatenated locale codes for multi-language campaigns
    -- (`ende` = en+de, `frnl` = fr+nl, `enfrde` = en+fr+de) -- these are valid
    -- `language` values, not audiences. Inspecting position 7 (vs the trailing
    -- `_adgap`/`_adgapid` marker) survives Microsoft's 100-char truncation, since
    -- position 7 is always intact. Names with fewer than 15 segments return NULL.
    --
    -- NOTE: this branch is a workaround, not a clean parse. It exists because the
    -- upstream campaign names are inconsistent: Microsoft caps names at 100 chars,
    -- truncating the tail (and the `_adgap`/`_adgapid` marker) mid-token; the
    -- `audience` segment is inconsistently present; and some names carry free-text
    -- suffixes. The position-7 heuristic compensates for these. (Caveat: a 4/6-char
    -- even-length lowercase audience value would be misread as a multi-locale
    -- `language`; the only audience value seen in practice is `all`, which is
    -- correctly detected.) Once the campaign names are normalized upstream
    -- (consistent field set, no truncation, correct delimiters), this branch should
    -- be replaced with a straightforward positional parse like the networks above.
    WHEN REGEXP_CONTAINS(campaign_name, r"^(ms_v1|gads_v1)")
      THEN (
          SELECT
            IF(
              ARRAY_LENGTH(t) >= 15,
              mozfun.map.from_lists(
                ARRAY_CONCAT(
                  [
                    'ad_network',
                    'version',
                    'product',
                    'initiative',
                    'region',
                    'country_code',
                    'city'
                  ],
                  IF(has_audience, ['audience'], CAST([] AS ARRAY<STRING>)),
                  [
                    'language',
                    'device',
                    'operating_system',
                    'campaign_type',
                    'campaign_goal',
                    'campaign_group',
                    'bidding_type',
                    'optimization_goal'
                  ]
                ),
                -- from_lists stops at the shorter list, and keys is exactly 15 or
                -- 16 here, so it takes the first 15/16 segments. Surplus is dropped.
                t
              ),
              NULL
            )
          FROM
            (
              SELECT
                t,
                NOT REGEXP_CONTAINS(
                  t[SAFE_OFFSET(7)],
                  r"(?i)^([a-z]{2}(-[a-z]{2,3})?|([a-z]{2}){2,})$"
                ) AS has_audience
              FROM
                (
                  SELECT
                    SPLIT(REGEXP_REPLACE(campaign_name, "national_test", "national-test"), "_") AS t
                )
            )
        )
    -- Campaign Artifact Schema - Apple Search Ads - version 1
    WHEN REGEXP_CONTAINS(campaign_name, r"^(asa_v1)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 14
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Reddit Ads - version 1
    -- Names are inconsistent (13-18 segments observed)
    -- The leading segments map to the canonical order; when the final
    -- segment is a `PO...` order code it is pulled into `po` (so 16-segment names
    -- map cleanly to ad_network .. optimization_goal + po); otherwise the first 17
    -- segments map positionally and any trailing tag is dropped.
    -- (Reddit v2 is handled by the Google Ads v2/3 branch above.)
    WHEN REGEXP_CONTAINS(campaign_name, r"^reddit_v1")
      THEN (
          SELECT
            CASE
              WHEN ARRAY_LENGTH(t) < 13
                THEN NULL
              WHEN REGEXP_CONTAINS(t[SAFE_OFFSET(ARRAY_LENGTH(t) - 1)], r"(?i)^po")
                AND ARRAY_LENGTH(t) <= 17
                THEN mozfun.map.from_lists(
                    ARRAY_CONCAT(
                      ARRAY(
                        SELECT
                          k
                        FROM
                          UNNEST(k17) AS k
                          WITH OFFSET o
                        WHERE
                          o < ARRAY_LENGTH(t) - 1
                        ORDER BY
                          o
                      ),
                      ['po']
                    ),
                    t
                  )
              -- from_lists pairs k17[o] with t[o] and stops at the shorter list,
              -- so this maps the first min(len(t), 17) segments positionally.
              ELSE mozfun.map.from_lists(k17, t)
            END
          FROM
            (
              SELECT
                SPLIT(campaign_name, "_") AS t,
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal',
                  'ad_gap_id',
                  'po'
                ] AS k17
            )
        )
    -- pre Campaign Artifact Schema - Apple Search Ads
    WHEN REGEXP_CONTAINS(campaign_name, r"^Mozilla_Firefox_ASA_")
      THEN [
          STRUCT(
            "region" AS key,
            CASE
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("CA", "US")
                THEN "NA"
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("AT", "BE", "CH", "DE", "ES", "FR", "IT", "NL", "PL", "UK")
                THEN "EU"
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("AU", "JP")
                THEN "Expansion"
              ELSE NULL
            END
          ),
          STRUCT(
            "country_code" AS key,
            REGEXP_EXTRACT(
              campaign_name,
              r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})"
            ) AS value
          )
        ]
    -- pre Campaign Artifact Schema - Google Ads
    WHEN mozfun.google_ads.extract_segments_from_campaign_name(campaign_name) IS NOT NULL
      THEN [
          STRUCT(
            "region" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_region AS value
          ),
          STRUCT(
            "country_code" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_country_code AS value
          ),
          STRUCT(
            "language" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_language AS value
          )
        ]
    WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 16
      THEN [
          STRUCT("ad_network" AS key, SPLIT(campaign_name, "_")[0] AS value),
          STRUCT("version" AS key, SPLIT(campaign_name, "_")[1] AS value),
          STRUCT("product" AS key, SPLIT(campaign_name, "_")[2] AS value),
          STRUCT("initiative" AS key, SPLIT(campaign_name, "_")[3] AS value),
          STRUCT("region" AS key, SPLIT(campaign_name, "_")[4] AS value),
          STRUCT("country_code" AS key, SPLIT(campaign_name, "_")[5] AS value)
        ]
    ELSE NULL
  END
);

SELECT
  -- Test - Campaign Artifact Schema - Google Ads - version 2
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
      )
    ),
    18
  ),
  mozfun.assert.null(marketing.parse_campaign_name('gads_v2_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'asa_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
    )
  ),
  -- Test - Campaign Artifact Schema - Google Ads - version 3
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v3_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
      )
    ),
    18
  ),
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v3_firefox_challengeTheDefault_na_us_all_all_en_mobile_iOS_appCampaign_conversion_uac_tcpa_install_adGap_POUS2003046'
      )
    ),
    18
  ),
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v3_firefox_challengeTheDefault_apac_in_all_all_en_mobile_android_appCampaign_conversion_uac_tcpa_frst_open_adGap_po#2002280'  -- misspelled frst_open
      )
    ),
    18
  ),
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v3_firefox_challengeTheDefault_apac_id_all_all_id_mobile_android_appCampaign_conversion_uac_tcpa_first_open_adGap_po#2002280'
      )
    ),
    18
  ),
  mozfun.assert.null(marketing.parse_campaign_name('gads_v3_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'asa_v3_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
    )
  ),
  -- Test - Campaign Artifact Schema - Google Ads - version 1
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        "gds_v1_firefox_ctd_EU_DE_all_DE_multiscreen_all_gdn_consideration_brand_cpc_something"
      )
    ),
    15
  ),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      "gds_v1_firefox_ctd_EU_DE_all_DE_multiscreen_all_gdn_consideration_brand_cpc_ctr_something"
    )
  ),
  -- Test - Campaign Artifact Schema - Facebook Ads - version 2
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'meta_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
      )
    ),
    17
  ),
  mozfun.assert.null(marketing.parse_campaign_name('meta_v2_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
    )
  ),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'gads_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
    )
  ),
  -- Test - Campaign Artifact Schema - Facebook Ads - version 1 (same 17-field layout as v2)
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'meta_v1_firefox_performance-2026_na_us_all_en_desktop_all_na_conversion_social_cpi_install_na_pous2003939'
      )
    ),
    17
  ),
  -- Test - Facebook Ads - older 15-field CamelCase variant (no optimization_goal /
  -- ad_gap_id; `po` last). Case-insensitive prefix; `po` recovered at position 15.
  mozfun.assert.map_equals(
    marketing.parse_campaign_name(
      'Meta_V1_Firefox_Meta-Conversion-Test_NA_US_National_English_All_All_PaidSocial_Sales_Conversions_Conversions_POUS2003921'
    ),
    [
      STRUCT('ad_network' AS key, 'Meta' AS value),
      STRUCT('version' AS key, 'V1' AS value),
      STRUCT('product' AS key, 'Firefox' AS value),
      STRUCT('initiative' AS key, 'Meta-Conversion-Test' AS value),
      STRUCT('region' AS key, 'NA' AS value),
      STRUCT('country_code' AS key, 'US' AS value),
      STRUCT('city' AS key, 'National' AS value),
      STRUCT('language' AS key, 'English' AS value),
      STRUCT('device' AS key, 'All' AS value),
      STRUCT('operating_system' AS key, 'All' AS value),
      STRUCT('campaign_type' AS key, 'PaidSocial' AS value),
      STRUCT('campaign_goal' AS key, 'Sales' AS value),
      STRUCT('campaign_group' AS key, 'Conversions' AS value),
      STRUCT('bidding_type' AS key, 'Conversions' AS value),
      STRUCT('po' AS key, 'POUS2003921' AS value)
    ]
  ),
  -- Stray leading `NEW` QA prefix is stripped and parses identically.
  mozfun.assert.map_equals(
    marketing.parse_campaign_name(
      'NEWMeta_V1_Firefox_Meta-Conversion-Test_NA_US_National_English_All_All_PaidSocial_Sales_Conversions_Conversions_POUS2003921'
    ),
    marketing.parse_campaign_name(
      'Meta_V1_Firefox_Meta-Conversion-Test_NA_US_National_English_All_All_PaidSocial_Sales_Conversions_Conversions_POUS2003921'
    )
  ),
  -- A meta name with an unsupported segment count still returns NULL.
  mozfun.assert.null(marketing.parse_campaign_name('meta_v1_123')),
  -- Test - Campaign Artifact Schema - Google Ads - version 2 or 3 (new variable-length format)
  -- Audience present (24-seg total): 16 positional + ad_gap_id + po
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_en_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_id3_id4_id5_id6_po#789'
      )
    ),
    18
  ),
  -- Audience omitted, new format: 15 positional + ad_gap_id + po
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_po#789'
      )
    ),
    17
  ),  -- Bare trailing `_adgapid` (value optional): parses with ad_gap_id and po NULL.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_eu_fr_all_fr_desktop_all_search_conversion_features_tcpa_install_adgapid'
      )[15],
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_eu_fr_all_fr_desktop_all_search_conversion_features_tcpa_install_adgapid'
      )[16]
    ],
    [
      STRUCT('ad_gap_id' AS key, CAST(NULL AS STRING) AS value),
      STRUCT('po' AS key, CAST(NULL AS STRING) AS value)
    ]
  ),
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_po#789'
      )[15],
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_po#789'
      )[16]
    ],
    [STRUCT('ad_gap_id' AS key, 'id1_id2' AS value), STRUCT('po' AS key, 'po#789' AS value)]
  ),
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_en_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_id3_id4_id5_id6_po#789'
      )[16],
      marketing.parse_campaign_name(
        'gads_v2_firefox_challengeTheDefault_expansion_pl_all_all_en_desktop_all_search_conversion_brand_cpc_install_adgapid_id1_id2_id3_id4_id5_id6_po#789'
      )[17]
    ],
    [
      STRUCT('ad_gap_id' AS key, 'id1_id2_id3_id4_id5_id6' AS value),
      STRUCT('po' AS key, 'po#789' AS value)
    ]
  ),
  -- Test - Campaign Artifact Schema - Microsoft Ads - version 1
  -- First 15 segments, or 16 with audience. Audience presence is read from position 7
  -- (a locale code => language/no audience; anything else => audience). The malformed
  -- city `national_test` is normalized to `national-test` up front.
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_expansion_in_national-test_en_desktop_all_pmax_conversion_all_cpc_install_adgap'
      )
    ),
    15
  ),
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_expansion_in_national-test_en_desktop_all_pmax_conversion_all_cpc_install_adgap'
      )[6],
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_expansion_in_national-test_en_desktop_all_pmax_conversion_all_cpc_install_adgap'
      )[13],
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_expansion_in_national-test_en_desktop_all_pmax_conversion_all_cpc_install_adgap'
      )[14]
    ],
    [
      STRUCT('city' AS key, 'national-test' AS value),
      STRUCT('bidding_type' AS key, 'cpc' AS value),
      STRUCT('optimization_goal' AS key, 'install' AS value)
    ]
  ),
  -- Malformed city `national_test` (underscore) is normalized to `national-test`.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_apac_id_national_test_id_desktop_all_search_conversion_comp_cpc_install_adgap'
      )[6]
    ],
    [STRUCT('city' AS key, 'national-test' AS value)]
  ),
  -- Truncated name (tail cut at 100 chars) still yields the first 15 segments.
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'ms_v1_firefox_test_sa_br_national-test_pt-br_desktop_all_search_conversion_nonbrand_cpc_install_adga'
      )
    ),
    15
  ),
  -- Fewer than 15 segments → NULL.
  mozfun.assert.null(marketing.parse_campaign_name('ms_v1_123')),
  -- Audience present (position 7 = `all`, not a locale): 16 fields, audience at [7].
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'ms_v1_firefox_challengeTheDefault_expansion_pl_all_all_pl_desktop_all_search_conversion_brand_cpc_install_adgapid_02-G123456789'
      )
    ),
    16
  ),
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'ms_v1_firefox_challengeTheDefault_expansion_pl_all_all_pl_desktop_all_search_conversion_brand_cpc_install_adgapid_02-G123456789'
      )[7],
      marketing.parse_campaign_name(
        'ms_v1_firefox_challengeTheDefault_expansion_pl_all_all_pl_desktop_all_search_conversion_brand_cpc_install_adgapid_02-G123456789'
      )[8]
    ],
    [STRUCT('audience' AS key, 'all' AS value), STRUCT('language' AS key, 'pl' AS value)]
  ),
  -- Truncated audience name (marker cut): audience still recovered via position 7.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'ms_v1_firefox_challengeTheDefault_eu_de_all_all_de_desktop_all_search_conversion_pdfeditor_tcpa_inst'
      )[7]
    ],
    [STRUCT('audience' AS key, 'all' AS value)]
  ),
  -- Test - Campaign Artifact Schema - Apple Search Ads - version 1
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'asa_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
      )
    ),
    14
  ),
  mozfun.assert.null(marketing.parse_campaign_name('asa_v1_123')),
  -- Test - pre Campaign Artifact Schema - Apple Search Ads
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_CA_SearchTab"),
    [STRUCT("region" AS key, "NA" AS value), STRUCT("country_code" AS key, "CA" AS value)]
  ),
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_iOSGeoTest_EN_Test2"),
    [
      STRUCT("region" AS key, CAST(NULL AS string) AS value),
      STRUCT("country_code" AS key, "EN" AS value)
    ]
  ),
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_iOSGeoTest_UK_Test3"),
    [STRUCT("region" AS key, "EU" AS value), STRUCT("country_code" AS key, "UK" AS value)]
  ),
  -- Test - pre Campaign Artifact Schema - Google Ads
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    [
      STRUCT("region" AS key, "EU" AS value),
      STRUCT("country_code" AS key, "AT" AS value),
      STRUCT("language" AS key, "EN" AS value)
    ]
  ),
  -- Test - NULL
  mozfun.assert.null(marketing.parse_campaign_name(NULL)),
  -- Test - Campaign Artifact Schema - Google Ads - version 1 (shares ms_v1 logic)
  -- 16 segments, position 7 (`en`) is a locale => no audience => 15 fields, marker dropped.
  mozfun.assert.map_equals(
    marketing.parse_campaign_name(
      "gads_v1_firefox_test_na_us_national-test_en_desktop_all_search_conversion_nonbrand_cpc_install_adgap"
    ),
    [
      STRUCT("ad_network" AS key, "gads" AS value),
      STRUCT("version" AS key, "v1" AS value),
      STRUCT("product" AS key, "firefox" AS value),
      STRUCT("initiative" AS key, "test" AS value),
      STRUCT("region" AS key, "na" AS value),
      STRUCT("country_code" AS key, "us" AS value),
      STRUCT("city" AS key, "national-test" AS value),
      STRUCT("language" AS key, "en" AS value),
      STRUCT("device" AS key, "desktop" AS value),
      STRUCT("operating_system" AS key, "all" AS value),
      STRUCT("campaign_type" AS key, "search" AS value),
      STRUCT("campaign_goal" AS key, "conversion" AS value),
      STRUCT("campaign_group" AS key, "nonbrand" AS value),
      STRUCT("bidding_type" AS key, "cpc" AS value),
      STRUCT("optimization_goal" AS key, "install" AS value)
    ]
  ),
  -- gads_v1 15-field variant with no marker (e.g. `nothingpersonal` campaigns).
  mozfun.assert.map_equals(
    marketing.parse_campaign_name(
      "gads_v1_firefox_nothingpersonal_na_us_Chicago_en_all_all_search_conversion_non-brand_tcpa_install"
    ),
    [
      STRUCT("ad_network" AS key, "gads" AS value),
      STRUCT("version" AS key, "v1" AS value),
      STRUCT("product" AS key, "firefox" AS value),
      STRUCT("initiative" AS key, "nothingpersonal" AS value),
      STRUCT("region" AS key, "na" AS value),
      STRUCT("country_code" AS key, "us" AS value),
      STRUCT("city" AS key, "Chicago" AS value),
      STRUCT("language" AS key, "en" AS value),
      STRUCT("device" AS key, "all" AS value),
      STRUCT("operating_system" AS key, "all" AS value),
      STRUCT("campaign_type" AS key, "search" AS value),
      STRUCT("campaign_goal" AS key, "conversion" AS value),
      STRUCT("campaign_group" AS key, "non-brand" AS value),
      STRUCT("bidding_type" AS key, "tcpa" AS value),
      STRUCT("optimization_goal" AS key, "install" AS value)
    ]
  ),
  -- Multi-language campaign: position 7 is a run of locale codes (`ende` = en+de),
  -- which is a valid `language` value, not an `audience`. Parses to 15 fields.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        "gads_v1_firefox_test_eu_at_national-test_ende_desktop_all_search_conversion_nonbrand_cpc_install_adgap"
      )[6],
      marketing.parse_campaign_name(
        "gads_v1_firefox_test_eu_at_national-test_ende_desktop_all_search_conversion_nonbrand_cpc_install_adgap"
      )[7],
      marketing.parse_campaign_name(
        "gads_v1_firefox_test_eu_at_national-test_ende_desktop_all_search_conversion_nonbrand_cpc_install_adgap"
      )[8]
    ],
    [
      STRUCT("city" AS key, "national-test" AS value),
      STRUCT("language" AS key, "ende" AS value),
      STRUCT("device" AS key, "desktop" AS value)
    ]
  ),
  -- Test - Reddit Ads - version 1. 17 segments: full layout, po last.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'reddit_v1_firefox_challengeTheDefault_na_us_all_en_desktop_all_search_conversion_brand_cpc_install_id123_po#123456789'
      )[0],
      marketing.parse_campaign_name(
        'reddit_v1_firefox_challengeTheDefault_na_us_all_en_desktop_all_search_conversion_brand_cpc_install_id123_po#123456789'
      )[16]
    ],
    [STRUCT('ad_network' AS key, 'reddit' AS value), STRUCT('po' AS key, 'po#123456789' AS value)]
  ),
  -- 16 segments ending in a po code: po recovered, not labeled ad_gap_id.
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'reddit_v1_firefox_challengeTheDefault_na_us_all_en_desktop_all_search_conversion_brand_cpc_install_po#123456789'
      )[14],
      marketing.parse_campaign_name(
        'reddit_v1_firefox_challengeTheDefault_na_us_all_en_desktop_all_search_conversion_brand_cpc_install_po#123456789'
      )[15]
    ],
    [
      STRUCT('optimization_goal' AS key, 'install' AS value),
      STRUCT('po' AS key, 'po#123456789' AS value)
    ]
  ),
  -- 13 segments: leading fields map, no po.
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'reddit_v1_firefox_challengeTheDefault_na_us_all_en_desktop_all_search_conversion_brand'
      )
    ),
    13
  ),
  -- Fewer than 13 segments -> NULL.
  mozfun.assert.null(marketing.parse_campaign_name('reddit_v1_123')),
  -- Test - Reddit Ads - version 2 (shares the Google Ads v2/3 format).
  mozfun.assert.map_equals(
    [
      marketing.parse_campaign_name(
        'reddit_v2_firefox_challengeTheDefault_eu_de_all_ypt_de_desktop_all_social_traffic_brand_cpc_ctr_adgapid_id1_id2_po#123456789'
      )[0],
      marketing.parse_campaign_name(
        'reddit_v2_firefox_challengeTheDefault_eu_de_all_ypt_de_desktop_all_social_traffic_brand_cpc_ctr_adgapid_id1_id2_po#123456789'
      )[17]
    ],
    [STRUCT('ad_network' AS key, 'reddit' AS value), STRUCT('po' AS key, 'po#123456789' AS value)]
  )

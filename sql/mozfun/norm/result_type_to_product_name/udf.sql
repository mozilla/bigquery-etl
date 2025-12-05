CREATE OR REPLACE FUNCTION norm.result_type_to_product_name(res STRING)
RETURNS STRING AS (
  CASE
    WHEN res IN ('autofill_origin', 'autofill_url')
      THEN 'autofill'
    WHEN res IN ('addon')
      THEN 'xchannels_add_on'
    WHEN res IN ('rs_amo', 'rust_amo')
      THEN 'suggest_add_on'
    WHEN res IN ('search_suggest', 'search_history', 'search_suggest_rich')
      THEN res -- previously 'default_partner_search_suggestion'
    WHEN res IN ('search_engine')
      THEN 'search_engine'
    WHEN res IN ('rust_yelp', 'ml_yelp', 'merino_yelp')
      THEN 'yelp_suggestion'
    WHEN res IN ('rust_fakespot_amazon', 'rust_fakespot_bestbuy', 'rust_fakespot_walmart')
      THEN 'fakespot_suggest'
    WHEN res IN ('trending_search', 'trending_search_rich')
      THEN 'trending_suggestion'
    WHEN res IN ('history')
      THEN 'history'
    WHEN res IN ('bookmark', 'keyword', 'restrict_keyword_bookmarks')
      THEN 'bookmark'
    WHEN res IN ('tab')
      THEN 'open_tab'
    WHEN res IN (
        'merino_adm_sponsored',
        'rs_adm_sponsored',
        'suggest_sponsor',
        'rust_adm_sponsored'
      )
      THEN 'admarketplace_sponsored'
    WHEN res IN ('merino_top_picks')
      THEN 'navigational'
    WHEN res IN (
        'rs_adm_nonsponsored',
        'merino_adm_nonsponsored',
        'suggest_non_sponsor',
        'rust_adm_nonsponsored'
      )
      THEN 'wikipedia_enhanced'
    WHEN res IN ('dynamic_wikipedia', 'merino_wikipedia')
      THEN 'wikipedia_dynamic'
    WHEN res IN ('weather', 'merino_weather')
      THEN 'weather'
    WHEN res IN (
        'action',
        'intervention_clear',
        'intervention_refresh',
        'intervention_unknown',
        'intervention_update'
      )
      THEN 'quick_action'
    WHEN res IN ('rs_pocket', 'rust_pocket')
      THEN 'pocket_collection'
    WHEN res IN ('rs_mdn', 'rust_mdn')
      THEN 'mdn'
    WHEN res IN ('rust_vpn')
      THEN 'vpn'
    WHEN res IN ('remote_tab')
      THEN 'tabs'
    WHEN res IN ('calc')
      THEN 'calculator'
    WHEN res IN ('tip_onboard', 'tip_redirect', 'tip_persist')
      THEN 'tip'
    WHEN res IN ('autofill_about')
      THEN 'about_autofill'
    WHEN res IN ('autofill_adaptive')
      THEN 'adaptive_autofill'
    WHEN res IN ('unit')
      THEN 'unit_converter'
    WHEN res IN ('fxsuggest_data_sharing_opt_in')
      THEN 'online_suggest_opt_in'
    WHEN res IN ('merino_polygon')
      THEN 'merino_market_opt_in'
    WHEN res IN ('tab_serp')
      THEN 'open_tab_to_serp'
    WHEN res IN ('merino_market')
      THEN 'market'
    WHEN res IN ('merino_sports')
      THEN 'sports'
    WHEN res IN ('merino_flights')
      THEN 'flights'
    WHEN res IN ('rust_important_dates')
      THEN 'important_dates'
    WHEN res IN (
        'top_site',
        'recent_search',
        'history_serp',
        'history_semantic',
        'history_semantic_serp',
        'url',
        'tab_to_search',
        'tab_semantic',
        'tab_semantic_serp',
        'clipboard',
        'restrict_keyword_history',
        'restrict_keyword_tabs',
        'restrict_keyword_actions'
      )
      THEN res
    WHEN res LIKE('rust_%_opt_in')
      THEN SUBSTR(res, 6)
    ELSE 'other'
  END
);

-- Tests
SELECT
  assert.equals("other", norm.result_type_to_product_name("not a valid type"))

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
      THEN 'default_partner_search_suggestion'
    WHEN res IN ('search_engine')
      THEN 'search_engine'
    WHEN res IN ('rust_yelp', 'ml_yelp')
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
    WHEN res IN ('weather')
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
    WHEN res IN ('history_serp')
      THEN 'history_serp'
    WHEN res IN ('history_semantic')
      THEN 'history_semantic'
    WHEN res IN ('history_semantic_serp')
      THEN 'history_semantic_serp'
    WHEN res IN ('recent_search')
      THEN 'recent_search'
    WHEN res IN ('rust_vpn')
      THEN 'vpn'
    WHEN res IN ('merino_market')
      THEN 'merino_market'
    ELSE 'other'
  END
);

-- Tests
SELECT
  assert.equals("other", norm.result_type_to_product_name("not a valid type"))

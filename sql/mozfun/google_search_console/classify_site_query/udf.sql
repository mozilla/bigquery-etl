CREATE OR REPLACE FUNCTION google_search_console.classify_site_query(
  site_domain_name STRING,
  query STRING,
  search_type STRING
)
RETURNS STRING AS (
  CASE
    -- Discover and Google News search impressions never have `query` values.
    WHEN search_type IN ('Discover', 'Google News')
      THEN NULL
    WHEN query IS NULL
      THEN 'Anonymized'
    WHEN site_domain_name = 'www.mozilla.org'
      THEN IF(
          REGEXP_CONTAINS(
            query,
            ARRAY_TO_STRING(
              [
                r'\bff\b',
                r'\bm.z',
                r'f.ref.x',
                r'fier',
                r'fire',
                r'firf',
                r'focus',
                r'fokku',
                r'fox',
                r'il+a\b',
                r'nightly',
                r'quantum',
                r'μοζ+ιλ+α',  -- moz+il+a (Greek)
                r'μοτζιλα',  -- motzila (Greek)
                r'лиса',  -- fox (Cyrillic)
                r'мази',  -- mazi (Cyrillic)
                r'мазі',  -- mazi (Cyrillic)
                r'моз',  -- moz (Cyrillic)
                r'муз',  -- muz (Cyrillic)
                r'фаер',  -- fire (Cyrillic)
                r'фаир',  -- fair (Cyrillic)
                r'файер',  -- fire (Cyrillic)
                r'файр',  -- fire (Cyrillic)
                r'фире',  -- fire (Cyrillic)
                r'фокс',  -- fox (Cyrillic)
                r'фох',  -- fox (Cyrillic)
                r'כןרקכםס',  -- yes, rakhems (Hebrew)
                r'מוזילה',  -- mozilla (Hebrew)
                r'פיירפוקס',  -- firefox (Hebrew)
                r'فاکس',  -- fax (Arabic)
                r'فاير',  -- fire (Arabic)
                r'فایر',  -- fire (Arabic)
                r'فكس',  -- fx (Arabic)
                r'فوكس',  -- fox (Arabic)
                r'فير',  -- fir (Arabic)
                r'موزلا',  -- mozilla (Arabic)
                r'موزيلا',  -- mozilla (Arabic)
                r'موزیلا',  -- mozilla (Arabic)
                r'फायरफक्स',  -- firefox (Indic)
                r'फायरफॉक्स',  -- firefox (Indic)
                r'फ़ायरफ़ॉक्स',  -- firefox (Indic)
                r'मोजिला',  -- mozilla (Indic)
                r'मोज़िला',  -- mozilla (Indic)
                r'ফায়ারফক্স',  -- firefox (Indic)
                r'মজিলা',  -- mozilla (Indic)
                r'মোজিলা',  -- mozilla (Indic)
                r'ฟายฟอก',  -- bleach (Thai)
                r'ฟายฟ๊อก',  -- firefox (Thai)
                r'ไฟ ฟอก',  -- fire bleach (Thai)
                r'ไฟฟ็อก',  -- fire fox (Thai)
                r'ไฟฟ๊อก',  -- fire fox (Thai)
                r'ไฟฟอก',  -- purifying light (Thai)
                r'ไฟร์ฟอกซ์',  -- firefox (Thai)
                r'ไฟล์ฟอก',  -- bleaching file (Thai)
                r'ไฟลฟอก',  -- fire bleach (Thai)
                r'모질라',  -- mozilla (Korean)
                r'파이어',  -- fire (Korean)
                r'폭스',  -- fox (Korean)
                r'화이어',  -- fire (Korean)
                r'ふぁいあ',  -- faia (Japanese)
                r'ファイア',  -- fire (Japanese)
                r'ファイや',  -- faiya (Japanese)
                r'ファイヤ',  -- fire (Japanese)
                r'ふぁいやー',  -- fire (Japanese)
                r'ふぃれふぉ',  -- firefox (Japanese)
                r'ふぉっくす',  -- fox (Japanese)
                r'フォックス',  -- fox (Japanese)
                r'モジラ',  -- mozilla (Japanese)
                r'火孤',  -- firefox (Chinese)
                r'火狐',  -- firefox (Chinese)
                r'狐狸'  -- fox (Chinese)
              ],
              '|'
            )
          ),
          'Brand',
          'Non-Brand'
        )
    ELSE 'Unknown'
  END
);

SELECT
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'mozilla', 'Discover'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'mozilla', 'Google News'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', NULL, 'Discover'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', NULL, 'Google News'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', NULL, 'Web'),
    'Anonymized'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'mozilla', 'Web'),
    'Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'firefox', 'Web'),
    'Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'browser', 'Web'),
    'Non-Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('addons.mozilla.org', 'mozilla', 'Web'),
    'Unknown'
  ),

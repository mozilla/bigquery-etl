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
    WHEN REGEXP_CONTAINS(
        query,
        ARRAY_TO_STRING(
          [
            r'.i.e.ox',
            r'\besr\b',
            r'\bf..ef[ioa]+',
            r'\bf[aie]+re?\s?[fbv]',
            r'\bf[ier]+\s?[fv][oei]?[xkc]',
            r'\bff\b',
            r'\bfi[aeiobcfkrvx]+x',
            r'\bfirf',
            r'f.r.f.x',
            r'faiya-fokkusu',  -- fire fox (Japanese)
            r'fox',
            r'huohu',  -- fire fox (Chinese)
            r'nightly',
            r'quantum',
            r'лиса',  -- fox (Cyrillic)
            r'фаер',  -- fire (Cyrillic)
            r'фаир',  -- `fair` (Cyrillic)
            r'файер',  -- fire (Cyrillic)
            r'файр',  -- fire (Cyrillic)
            r'фире',  -- fire (Cyrillic)
            r'фокс',  -- fox (Cyrillic)
            r'фох',  -- `fox` (Cyrillic)
            r'כןרקכםס',  -- yes you have been (Hebrew)
            r'פיירפוקס',  -- firefox (Hebrew)
            r'فاجا بوكس',  -- `faja buks` (Arabic)
            r'فایر فاکس',  -- fire fox (Arabic)
            r'فایرفاکس',  -- firefox (Arabic)
            r'فرفاكس',  -- `firfaks` (Arabic)
            r'فري فاكس',  -- `fri faks` (Arabic)
            r'فكس',  -- `fiks` (Arabic)
            r'فوكس',  -- fox (Arabic)
            r'فياير',  -- `fayayar` (Arabic)
            r'فير',  -- `fir` (Arabic)
            r'फायर फॉक्स',  -- fire fox (Indic)
            r'फायरफक्स',  -- firefox (Indic)
            r'फायरफॉक्स',  -- firefox (Indic)
            r'फ़ायरफ़ॉक्स',  -- firefox (Indic)
            r'फ़ायर्फ़ॉक्स',  -- firefox (Indic)
            r'फिरेफोक्स',  -- firefox (Indic)
            r'फ्री फॉक्स',  -- free fox (Indic)
            r'ফায়ার বক্স',  -- fire box (Indic)
            r'ফায়ারফক্স',  -- firefox (Indic)
            r'ฟายฟอก',  -- `fay fxk` (Thai)
            r'ฟายฟ๊อก',  -- firefox (Thai)
            r'ไฟ ฟอก',  -- `fi fxk` (Thai)
            r'ไฟฟอก',  -- `fi fxk` (Thai)
            r'ไฟฟ็อก',  -- firefox (Thai)
            r'ไฟฟ๊อก',  -- firefox (Thai)
            r'ไฟร์ฟอกซ์',  -- firefox (Thai)
            r'ไฟลฟอก',  -- `fil fxk` (Thai)
            r'ไฟล์ฟอก',  -- `fil fxk` (Thai)
            r'หมาไฟ',  -- fire dog (Thai)
            r'파이어',  -- fire (Korean)
            r'파폭',  -- `papog` (Korean)
            r'폭스',  -- fox (Korean)
            r'ふぁいあ',  -- `faia` (Japanese)
            r'ファイア',  -- fire (Japanese)
            r'ふあいあーふぉっくす',  -- `faia fokkusu` (Japanese)
            r'ふあいあふぉっくす',  -- `faiafokkusu` (Japanese)
            r'ファイフォ',  -- `faifo` (Japanese)
            r'ふぁいや',  -- fire (Japanese)
            r'ふあいや',  -- `faiya` (Japanese)
            r'ファイや',  -- `faiya` (Japanese)
            r'ファイヤ',  -- fire (Japanese)
            r'フアイヤーフオツクス',  -- firefox (Japanese)
            r'ふぁやざ',  -- `fayaza` (Japanese)
            r'ふぃれふぉ',  -- firefox (Japanese)
            r'ふぉｘ',  -- fox (Japanese)
            r'フォッ',  -- `fot` (Japanese)
            r'火孤',  -- `huogu` (Chinese)
            r'火狐'  -- firefox (Chinese)
          ],
          '|'
        )
      )
      THEN 'Firefox Brand'
    WHEN REGEXP_CONTAINS(
        query,
        ARRAY_TO_STRING(
          [
            r'\bp[eiocjklrv]{3,}t',
            r'getpo',
            r'pocke',
            r'покет'  -- pocket (Cyrillic)
          ],
          '|'
        )
      )
      THEN 'Pocket Brand'
    WHEN REGEXP_CONTAINS(
        query,
        ARRAY_TO_STRING(
          [
            r'\bmpl\b',
            r'm o z i l l a',
            r'm.zil',
            r'm\w*zilla',
            r'mizolla',
            r'mo[dnr]?zil+a',
            r'mo[jsx]il+a',
            r'moz://a',
            r'moz:lla',
            r'moz+\w*l',
            r'moz+ira',
            r'mozıl',
            r'mzoilla',
            r'μοζ+ιλ+α',  -- moz+il+a (Greek)
            r'μονζ+ιλ+α',  -- monz+il+a (Greek)
            r'μοτζ+ιλ+α',  -- motz+il+a (Greek)
            r'м.з+ил',  -- m.z+il (Cyrilic)
            r'м.з+іл',  -- m.z+il (Cyrilic)
            r'მოზილა',  -- mozilla (Georgian)
            r'מוזילה',  -- mozilla (Hebrew)
            r'موزلا',  -- mozilla (Arabic)
            r'موزيل',  -- `muzil` (Arabic)
            r'موزیلا',  -- mozilla (Arabic)
            r'मोजिला',  -- mozilla (Indic)
            r'मोज़िला',  -- mozilla (Indic)
            r'মজিলা',  -- `mojila` (Indic)
            r'মোজিলা',  -- mozilla (Indic)
            r'모질라',  -- mozilla (Korean)
            r'モジラ'  -- mozilla (Japanese)
          ],
          '|'
        )
      )
      THEN 'Mozilla Brand'
    ELSE 'Non-Brand'
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
    'Mozilla Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'firefox', 'Web'),
    'Firefox Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'mozilla firefox', 'Web'),
    'Firefox Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'browser', 'Web'),
    'Non-Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('addons.mozilla.org', 'firefox', 'Web'),
    'Firefox Brand'
  ),
  assert.equals(
    google_search_console.classify_site_query('www.mozilla.org', 'pocket', 'Web'),
    'Pocket Brand'
  ),

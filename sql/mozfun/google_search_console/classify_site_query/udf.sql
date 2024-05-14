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
            r'^f..ef[ioa]+',
            r'^f[aie]+r\s?[fbv]',
            r'^f[ier]+\s?[fv][oei]?[xkc]',
            r'f.r.f.x',
            r'faiya-fokkusu',
            r'fi\w+x',
            r'firf',
            r'huohu',
            r'nightly',
            r'quantum',
            r'כןרקכםס',
            r'פיירפוקס',
            r'فاجا بوكس',
            r'فارفكس',
            r'فافير',
            r'فرفاكس',
            r'فرفكس',
            r'فري فاكس',
            r'فري فكس',
            r'فريفكس',
            r'فكس',
            r'فور فكس',
            r'فورفكس',
            r'فياير',
            r'فير بكس',
            r'فير بوكس',
            r'فير ف',
            r'فير فاكس',
            r'فير فايز',
            r'فير فكس',
            r'فير فوك',
            r'فير فيكس',
            r'فير موكس',
            r'فير',
            r'فيربكس',
            r'فيربوكس',
            r'فيرر',
            r'فيرفاكس',
            r'فيرفكس',
            r'فيرفو',
            r'فيرفيكس',
            r'فيركس',
            r'فيركوس',
            r'فيرو فكس',
            r'فيرو فيكس',
            r'فيروفكس',
            r'فيروفيكس',
            r'فيروكس',
            r'فيريكس',
            r'फायर फॉक्स',
            r'फायरफक्स',
            r'फायरफॉक्स',
            r'फ़ायरफ़ॉक्स',
            r'फ़ायर्फ़ॉक्स',
            r'फिरेफोक्स',
            r'फ्री फॉक्स',
            r'ফায়ার বক্স',
            r'ফায়ার বক্স',
            r'ফায়ারফক্স',
            r'ফায়ারফক্স',
            r'ดฟายฟอก',
            r'ฟายฟอก',
            r'ฟายฟ๊อก',
            r'ไฟ ฟอก',
            r'ไฟฟอก',
            r'ไฟฟ็อก',
            r'ไฟฟ๊อก',
            r'ไฟฟ๊อก',
            r'ไฟร์ฟอกซ์',
            r'ไฟลฟอก',
            r'ไฟล์ฟอก',
            r'หมาไฟ',
            r'파워폭스',
            r'파이어 포스',
            r'파이어 폭스',
            r'파이어',
            r'파이어박스',
            r'파이어복스',
            r'파이어팍스',
            r'파이어포그',
            r'파이어포긋',
            r'파이어포스',
            r'파이어폭',
            r'파이어폭수',
            r'파이어폭스',
            r'파폭',
            r'ファイーフォックス',
            r'ファイア フォックス',
            r'ふぁいあ',
            r'ファイア',
            r'ファイアー フォックス',
            r'ファイアー',
            r'ふぁいあーふぉ',
            r'ファイアーフォクス',
            r'ファイアーフォッ',
            r'ふぁいあーふぉっく',
            r'ふぁいあーふぉっくす',
            r'ふあいあーふぉっくす',
            r'ファイアーフォックス',
            r'ふぁいあふぉ',
            r'ファイアフォクス',
            r'ファイアフォッ',
            r'ふぁいあふぉっくす',
            r'ふあいあふぉっくす',
            r'ファイアフォックス',
            r'ファイアボックス',
            r'ファイフォ',
            r'ファイフォックス',
            r'ふぁいや',
            r'ふあいや',
            r'ファイや',
            r'ファイヤ',
            r'ファイヤー フォックス',
            r'ふぁいやー',
            r'ファイヤー',
            r'ファイヤー^フォックス',
            r'ファイヤーファックス',
            r'ふぁいやーふぉ',
            r'ファイヤーフォークス',
            r'ファイヤーふぉくす',
            r'ファイヤーフォクス',
            r'ファイヤーフォッ',
            r'ふぁいやーふぉっく',
            r'ファイヤーフォック',
            r'ふぁいやーふぉっくす',
            r'ふぁいやーフォックス',
            r'ふあいやーふおつくす',
            r'ファイヤーふぉっくす',
            r'ファイヤーフォックス',
            r'ファイヤーフォツクス',
            r'フアイヤーフオツクス',
            r'ファイヤーボッ',
            r'ふぁいやーほっくす',
            r'ファイヤーほっくす',
            r'ファイヤーホックス',
            r'ファイヤーボックス',
            r'ふぁいやふぉっくす',
            r'ファイやフォックス',
            r'ファイヤフォックス',
            r'ファイルフォックス',
            r'ふぁやざ',
            r'フィヤーフォックス',
            r'ふぃれふぉ',
            r'ふぃれふぉx',
            r'ふぃれふぉｘ',
            r'ふぉｘ',
            r'フォッ',
            r'フォックス',
            r'フォックスファイヤー',
            r'フファイヤーフォックス',
            r'フリーフォックス',
            r'火孤',
            r'火狐'
          ],
          '|'
        )
      )
      THEN 'Firefox Brand'
    WHEN REGEXP_CONTAINS(
        query,
        ARRAY_TO_STRING(
          [
            r'm o z i l l a',
            r'm.zil',
            r'm\w*zilla',
            r'mizolla',
            r'moz://a',
            r'moz:lla',
            r'moz*\w*l+',
            r'moz+ira',
            r'mozıl',
            r'mzoilla',
            r'μοζ+ιλ+α',
            r'μονζ+ιλ+α',
            r'μοτζιλα',
            r'м.з+ил',
            r'м.з+іл',
            r'მოზილა',
            r'מוזילה',
            r'موزلا',
            r'موزيل',
            r'موزيلا',
            r'موزیلا',
            r'मोजिला',
            r'मोज़िला',
            r'মজিলা',
            r'মোজিলা',
            r'모질라',
            r'モジラ'
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

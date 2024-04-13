CREATE OR REPLACE FUNCTION google_search_console.classify_query(query STRING, search_type STRING)
RETURNS STRING AS (
  CASE
    WHEN query IS NULL
      -- Discover and Google News search impressions never have `query` values.
      THEN IF(search_type IN ('Discover', 'Google News'), NULL, 'Anonymized')
    -- Brand keywords source: https://docs.google.com/document/d/1cjUQrPxvUG_A-hFTNUg-3MvRvssieVhk8tkJ8HBrtw4/edit
    -- In the regular expression each keyword is escaped like `\Q...\E` to treat it as literal text.
    WHEN REGEXP_CONTAINS(
        query,
        CONCAT(
          r'\Q',
          ARRAY_TO_STRING(
            [
              'f5ref6',
              'fair box',
              'fairbox',
              'faiya-fokkusu',  -- fire fox (Japanese)
              'feier',
              'ff browser',
              'ff download',
              'ff',
              'fier',
              'fiferox',
              'fir',
              'ｆｉｒｅｆｏｘ',
              'forefpx',
              'fox',
              'frefx',
              'frox',
              'furefix',
              'huohu',  -- firefox (Chinese)
              'illa',
              'ireox',
              'm o z i l l a . o r g',
              'marzil',
              'mazola',
              'mizolla',
              'modgila',
              'modjila',
              'modsila',
              'modzil',
              'mogil',
              'moill',
              'moizlla',
              'mojala',
              'mojila',
              'mojira',
              'mojula',
              'moliza',
              'molliza',
              'monjila',
              'morgila',
              'morjila',
              'morzil',
              'mosil',
              'moxil',
              'moyila',
              'moyyila',
              'moz',
              'nightly',
              'pocket',
              'quantum',
              'rise 25',
              'thunderbird',
              'vox',
              'zila',
              'μοζζιλα',  -- mozilla (Greek)
              'μοζζιλλα',  -- mozilla (Greek)
              'μοζιλα',  -- mozilla (Greek)
              'μοζιλλα',  -- mozilla (Greek)
              'μονζιλα',  -- monzilla (Greek)
              'ашкуащч',  -- ashkuashtch (Cyrillic)
              'лиса',  -- fox (Cyrillic)
              'мазила',  -- mazila (Cyrillic)
              'мазилла',  -- mazilla (Cyrillic)
              'мазилу',  -- mazilu (Cyrillic)
              'мазіла',  -- mazila (Cyrillic)
              'мазілу',  -- mazilu (Cyrillic)
              'моззила',  -- mozilla (Cyrillic)
              'мозила',  -- mozila (Cyrillic)
              'мозилла',  -- mozilla (Cyrillic)
              'мозилу',  -- mozilu (Cyrillic)
              'мозилы',  -- mozily (Cyrillic)
              'мозіла',  -- mozilla (Cyrillic)
              'мозілла',  -- mozilla (Cyrillic)
              'мозілу',  -- mozilu (Cyrillic)
              'музила',  -- muzila (Cyrillic)
              'фокс',  -- fox (Cyrillic)
              'фох',  -- foh (Cyrillic)
              'фоь',  -- fo (Cyrillic)
              'ьфяшддф',  -- fyashddf (Cyrillic)
              'ьфяшдф',  -- fyashdf (Cyrillic)
              'ьщяшддф',  -- shsyashddf (Cyrillic)
              'ьщяшддфющкп',  -- shtyashddfyshkp (Cyrillic)
              'ьщяшдф',  -- yshchyashdf (Cyrillic)
              'כןרקכםס',  -- yes, rakhems (Hebrew)
              'מוזילה',  -- mozilla (Hebrew)
              'פיירפוקס',  -- firefox (Hebrew)
              'فاكس',  -- fax (Arabic)
              'فاير',  -- fire (Arabic)
              'فایرفاکس',  -- firefox (Arabic)
              'فكس',  -- fx (Arabic)
              'فوكس',  -- fox (Arabic)
              'فير',  -- fir (Arabic)
              'موزلا',  -- mozilla (Arabic)
              'موزيل',  -- moselle (Arabic)
              'موزیلا',  -- mozilla (Arabic)
              'फायरफॉक्स',  -- firefox (Indic)
              'मोजिला फायरफक्स',  -- mozilla firefox (Indic)
              'मोज़िला फ़ायरफ़ॉक्स',  -- mozilla firefox (Indic)
              'मोज़िला',  -- mozilla (Indic)
              'মজিলা ফায়ারফক্স',  -- mozilla firefox (Indic)
              'মোজিলা',  -- mozilla (Indic)
              'ดฟายฟอก',  -- firefoak (Thai)
              'ฟายฟอก',  -- bleach (Thai)
              'ฟายฟ๊อก',  -- firefox (Thai)
              'ไฟ ฟอก',  -- fire bleach (Thai)
              'ไฟฟ็อก',  -- fire fox (Thai)
              'ไฟฟ๊อก',  -- fire fox (Thai)
              'ไฟฟอก',  -- purifying light (Thai)
              'ไฟร์ฟอกซ์',  -- firefox (Thai)
              'ไฟล์ฟอก',  -- bleaching file (Thai)
              'ไฟลฟอก',  -- fire bleach (Thai)
              'หมาไฟ',  -- fire dog (Thai)
              '인터넷브라우저',  -- internet browser (Korean)
              '파이어',  -- fire (Korean)
              '파폭',  -- explosion (Korean)
              '폭스',  -- fox (Korean)
              'ファイアーフォックス',  -- firefox (Japanese)
              'ふぁいあーふぉっくす',  -- fireworks (Japanese)
              'ファイアフォックス',  -- firefox (Japanese)
              'ふぁいあふぉっくす',  -- fireworks (Japanese)
              'ファイヤー フォックス',  -- fire fox (Japanese)
              'ふぁいやーふぉっくす',  -- faiya fox (Japanese)
              'ファイヤーフォックス',  -- firefox (Japanese)
              'ファイヤーホックス',  -- fire hox (Japanese)
              'ファイヤフォックス',  -- firefox (Japanese)
              'ファイやフォックス',  -- phi and fox (Japanese)
              'ふぃれふぉx',  -- firefox (Japanese)
              'ふぃれふぉｘ',  -- firefox (Japanese)
              'モジラ',  -- mozilla (Japanese)
              '火孤',  -- firefox (Chinese)
              '火狐',  -- firefox (Chinese)
              '狐狸'  -- fox (Chinese)
            ],
            r'\E|\Q'
          ),
          r'\E'
        )
      )
      THEN 'Brand'
    ELSE 'Non-Brand'
  END
);

SELECT
  assert.equals(google_search_console.classify_query(NULL, 'Discover'), CAST(NULL AS STRING)),
  assert.equals(google_search_console.classify_query(NULL, 'Google News'), CAST(NULL AS STRING)),
  assert.equals(google_search_console.classify_query(NULL, 'Web'), 'Anonymized'),
  assert.equals(google_search_console.classify_query('mozilla', 'Web'), 'Brand'),
  assert.equals(google_search_console.classify_query('firefox', 'Web'), 'Brand'),
  assert.equals(google_search_console.classify_query('browser', 'Web'), 'Non-Brand'),

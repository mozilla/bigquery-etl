CREATE OR REPLACE FUNCTION google_search_console.classify_query(query STRING)
RETURNS STRING AS (
  CASE
    WHEN query IS NULL
      THEN 'Anonymized'
    -- Brand keywords source: https://docs.google.com/document/d/1cjUQrPxvUG_A-hFTNUg-3MvRvssieVhk8tkJ8HBrtw4/edit
    WHEN REGEXP_CONTAINS(
        query,
        ARRAY_TO_STRING(
          [
            r'f5ref6',
            r'fair box',
            r'fairbox',
            r'faiya-fokkusu',  -- fire fox (Japanese)
            r'feier',
            r'ff browser',
            r'ff download',
            r'ff',
            r'fier',
            r'fiferox',
            r'fir',
            r'ｆｉｒｅｆｏｘ',
            r'forefpx',
            r'fox',
            r'frefx',
            r'frox',
            r'furefix',
            r'huohu',  -- firefox (Chinese)
            r'illa',
            r'ireox',
            r'm o z i l l a \. o r g',
            r'marzil',
            r'mazola',
            r'mizolla',
            r'modgila',
            r'modjila',
            r'modsila',
            r'modzil',
            r'mogil',
            r'moill',
            r'moizlla',
            r'mojala',
            r'mojila',
            r'mojira',
            r'mojula',
            r'moliza',
            r'molliza',
            r'monjila',
            r'morgila',
            r'morjila',
            r'morzil',
            r'mosil',
            r'moxil',
            r'moyila',
            r'moyyila',
            r'moz',
            r'nightly',
            r'pocket',
            r'quantum',
            r'rise 25',
            r'thunderbird',
            r'vox',
            r'zila',
            r'μοζζιλα',  -- mozilla (Greek)
            r'μοζζιλλα',  -- mozilla (Greek)
            r'μοζιλα',  -- mozilla (Greek)
            r'μοζιλλα',  -- mozilla (Greek)
            r'μονζιλα',  -- monzilla (Greek)
            r'ашкуащч',  -- ashkuashtch (Cyrillic)
            r'лиса',  -- fox (Cyrillic)
            r'мазила',  -- mazila (Cyrillic)
            r'мазилла',  -- mazilla (Cyrillic)
            r'мазилу',  -- mazilu (Cyrillic)
            r'мазіла',  -- mazila (Cyrillic)
            r'мазілу',  -- mazilu (Cyrillic)
            r'моззила',  -- mozilla (Cyrillic)
            r'мозила',  -- mozila (Cyrillic)
            r'мозилла',  -- mozilla (Cyrillic)
            r'мозилу',  -- mozilu (Cyrillic)
            r'мозилы',  -- mozily (Cyrillic)
            r'мозіла',  -- mozilla (Cyrillic)
            r'мозілла',  -- mozilla (Cyrillic)
            r'мозілу',  -- mozilu (Cyrillic)
            r'музила',  -- muzila (Cyrillic)
            r'фокс',  -- fox (Cyrillic)
            r'фох',  -- foh (Cyrillic)
            r'фоь',  -- fo (Cyrillic)
            r'ьфяшддф',  -- fyashddf (Cyrillic)
            r'ьфяшдф',  -- fyashdf (Cyrillic)
            r'ьщяшддф',  -- shsyashddf (Cyrillic)
            r'ьщяшддфющкп',  -- shtyashddfyshkp (Cyrillic)
            r'ьщяшдф',  -- yshchyashdf (Cyrillic)
            r'כןרקכםס',  -- yes, rakhems (Hebrew)
            r'מוזילה',  -- mozilla (Hebrew)
            r'פיירפוקס',  -- firefox (Hebrew)
            r'فاكس',  -- fax (Arabic)
            r'فاير',  -- fire (Arabic)
            r'فایرفاکس',  -- firefox (Arabic)
            r'فكس',  -- fx (Arabic)
            r'فوكس',  -- fox (Arabic)
            r'فير',  -- fir (Arabic)
            r'موزلا',  -- mozilla (Arabic)
            r'موزيل',  -- moselle (Arabic)
            r'موزیلا',  -- mozilla (Arabic)
            r'फायरफक्स',  -- firefox (Indic)
            r'फायरफॉक्स',  -- firefox (Indic)
            r'फ़ायरफ़ॉक्स',  -- firefox (Indic)
            r'मोजिला',  -- mozilla (Indic)
            r'मोज़िला',  -- mozilla (Indic)
            r'ফায়ারফক্স',  -- firefox (Indic)
            r'মজিলা',  -- mozilla (Indic)
            r'মোজিলা',  -- mozilla (Indic)
            r'ดฟายฟอก',  -- firefoak (Thai)
            r'ฟายฟอก',  -- bleach (Thai)
            r'ฟายฟ๊อก',  -- firefox (Thai)
            r'ไฟ ฟอก',  -- fire bleach (Thai)
            r'ไฟฟ็อก',  -- fire fox (Thai)
            r'ไฟฟ๊อก',  -- fire fox (Thai)
            r'ไฟฟอก',  -- purifying light (Thai)
            r'ไฟร์ฟอกซ์',  -- firefox (Thai)
            r'ไฟล์ฟอก',  -- bleaching file (Thai)
            r'ไฟลฟอก',  -- fire bleach (Thai)
            r'หมาไฟ',  -- fire dog (Thai)
            r'인터넷브라우저',  -- internet browser (Korean)
            r'파이어',  -- fire (Korean)
            r'파폭',  -- explosion (Korean)
            r'폭스',  -- fox (Korean)
            r'ファイアーフォックス',  -- firefox (Japanese)
            r'ふぁいあーふぉっくす',  -- fireworks (Japanese)
            r'ファイアフォックス',  -- firefox (Japanese)
            r'ふぁいあふぉっくす',  -- fireworks (Japanese)
            r'ファイヤー フォックス',  -- fire fox (Japanese)
            r'ふぁいやーふぉっくす',  -- faiya fox (Japanese)
            r'ファイヤーフォックス',  -- firefox (Japanese)
            r'ファイヤーホックス',  -- fire hox (Japanese)
            r'ファイヤフォックス',  -- firefox (Japanese)
            r'ファイやフォックス',  -- phi and fox (Japanese)
            r'ふぃれふぉx',  -- firefox (Japanese)
            r'ふぃれふぉｘ',  -- firefox (Japanese)
            r'モジラ',  -- mozilla (Japanese)
            r'火孤',  -- firefox (Chinese)
            r'火狐',  -- firefox (Chinese)
            r'狐狸'  -- fox (Chinese)
          ],
          '|'
        )
      )
      THEN 'Brand'
    ELSE 'Non-Brand'
  END
);

SELECT
  assert.equals(google_search_console.classify_query(NULL), 'Anonymized'),
  assert.equals(google_search_console.classify_query('mozilla'), 'Brand'),
  assert.equals(google_search_console.classify_query('firefox'), 'Brand'),
  assert.equals(google_search_console.classify_query('browser'), 'Non-Brand'),

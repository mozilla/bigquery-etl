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
              'faiya-fokkusu',
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
              'huohu',  -- firefox
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
              'pocket extension',
              'pocket',
              'quantum browser',
              'quantum',
              'rise 25',
              'thunderbird',
              'vox',
              'zila',
              'μοζζιλα',  -- mozilla
              'μοζζιλλα',  -- mozilla
              'μοζιλα',  -- mozilla
              'μοζιλλα',  -- mozilla
              'μονζιλα',  -- monzilla
              'ашкуащч',  -- ashkuashtch
              'лиса',  -- fox
              'лиса',  -- Fox
              'мазила',
              'мазилла',  -- mazilla
              'мазилу',
              'мазіла',
              'мазілу',
              'моззила',  -- mozilla
              'мозила',
              'мозилла',  -- mozilla
              'мозилла',  -- mozilla
              'мозилу',
              'мозилы',
              'мозіла',  -- mozilla
              'мозілла',  -- mozilla
              'мозілу',  -- mozilu
              'музила',
              'фаерфокс',  -- firefox
              'фаєрфокс',  -- firefox
              'файрфокс',  -- firefox
              'фокс',  -- fox
              'фокс',  -- Fox
              'фох',  -- foh
              'фоь',  -- fo
              'ьфяшддф',  -- Fyashddf
              'ьфяшдф',  -- fyashdf
              'ьщяшддф',  -- Shsyashddf
              'ьщяшддфющкп',  -- Shtyashddfyshkp
              'ьщяшдф',  -- yshchyashdf
              'כןרקכםס',  -- Yes, Rakhems
              'מוזילה',  -- Mozilla
              'פיירפוקס',  -- Firefox
              'فاكس',  -- fax
              'فاير فوكس',  -- Firefox
              'فاير',  -- Fire
              'فايرفو',  -- Firevo
              'فایرفاکس',  -- Firefox
              'فكس',  -- FX
              'فوكس',  -- Fox
              'فوكس',  -- Fox
              'فير فكس',  -- FireFX
              'فير فوكس',  -- Firefox
              'فير فوكس',  -- Firefox browser
              'فير',  -- Ver
              'فيرفكس',  -- Firefox
              'فيرفوكس',  -- Firefox
              'فيرفيكس',  -- Firefox
              'فيروفكس',  -- Firefox
              'فيروكس',  -- ferox
              'موزلا',  -- Mozilla
              'موزيل',  -- Moselle
              'موزيلا',  -- Mozilla
              'موزیلا',  -- Mozilla
              'موزیلافاير فوكس',  -- Mozilla Firefox
              'फायरफॉक्स',  -- Firefox
              'मोजिला फायरफक्स',  -- mozilla firefox
              'मोज़िला फायरफॉक्स',  -- Mozilla Firefox
              'मोज़िला फ़ायरफ़ॉक्स',  -- Mozilla Firefox
              'मोज़िला',  -- Mozilla
              'মজিলা ফায়ারফক্স',  -- Mozilla Firefox
              'মোজিলা',  -- Mozilla
              'ดฟายฟอก',  -- FireFoak
              'ฟายฟอก',  -- bleach
              'ฟายฟ๊อก',  -- Firefox
              'ไฟ ฟอก',  -- fire bleach
              'ไฟฟ็อก',  -- fire fox
              'ไฟฟ๊อก',  -- fire fox
              'ไฟฟ๊อก',  -- Firefox
              'ไฟฟอก',  -- purifying light
              'ไฟร์ฟอกซ์',  -- Firefox
              'ไฟล์ฟอก',  -- bleaching file
              'ไฟลฟอก',  -- Fire bleach
              'หมาไฟ',  -- fire dog
              '인터넷브라우저',  -- internet browser
              '파이어',  -- fire
              '파폭',  -- explosion
              '폭스',  -- fox
              'ファイアーフォックス ダウンロード',  -- firefox download
              'ファイアーフォックス',  -- firefox
              'ふぁいあーふぉっくす',  -- Fireworks
              'ファイアフォックス',  -- firefox
              'ふぁいあふぉっくす',  -- Fireworks
              'ファイヤー フォックス',  -- fire fox
              'ふぁいやーふぉっくす',  -- Faiya Fox
              'ファイヤーフォックス',  -- firefox
              'ファイヤーホックス',  -- fire hox
              'ファイヤフォックス',  -- firefox
              'ファイやフォックス',  -- Phi and Fox
              'ふぃれふぉx',  -- firefox
              'ふぃれふぉｘ',  -- Firefox
              'モジラ',  -- Mozilla
              '火孤',  -- Huo Gu
              '火狐',  -- Firefox
              '狐狸'  -- fox
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

WITH a11y_consumers_instantiators AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel,
    client_id,
    SPLIT(payload.processes.parent.scalars.a11y_instantiators, '|')[OFFSET(0)] AS instantiator,
    hpac.key AS consumer
  FROM
    telemetry.main
  LEFT JOIN -- Do not exclude rows that have empty or NULL hpac
    UNNEST(mozfun.hist.extract(payload.histograms.a11y_consumers).values) AS hpac
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND (
      (mozfun.hist.extract(payload.histograms.a11y_consumers).values IS NOT NULL AND hpac.value = 1)
      OR SPLIT(payload.processes.parent.scalars.a11y_instantiators, '|')[OFFSET(0)] IN (
        'Magnify.exe',
        'Narrator.exe',
        'jfw.exe',
        'Zt.exe',
        "nvda.exe",
        "osk.exe",
        "sapisvr.exe",
        'TabTip.exe',
        "VoiceOver"
      )
    )
)
SELECT
  submission_date,
  normalized_channel,
  COUNT(
    DISTINCT
    CASE
    WHEN
      consumer = 0
      OR instantiator = 'nvda.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS nvda,
  COUNT(
    DISTINCT
    CASE
    WHEN
      consumer = 1
      OR instantiator = 'jfw.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS jaws,
  COUNT(DISTINCT CASE WHEN consumer = 2 THEN client_id ELSE NULL END) AS oldjaws,
  COUNT(
    DISTINCT
    CASE
    WHEN
      consumer = 3
      OR UPPER(instantiator) LIKE UPPER('%WindowEyes%')
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS we,
  COUNT(
    DISTINCT
    CASE
    WHEN
      consumer = 4
      OR instantiator = 'Snova.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS dolphin,
  COUNT(DISTINCT CASE WHEN consumer = 5 THEN client_id ELSE NULL END) AS serotek,
  COUNT(DISTINCT CASE WHEN consumer = 6 THEN client_id ELSE NULL END) AS cobra,
  COUNT(
    DISTINCT
    CASE
    WHEN
      consumer = 7
      OR instantiator = 'Zt.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS zoomtext,
  COUNT(DISTINCT CASE WHEN consumer = 8 THEN client_id ELSE NULL END) AS kazaguru,
  COUNT(DISTINCT CASE WHEN consumer = 9 THEN client_id ELSE NULL END) AS youdao,
  COUNT(DISTINCT CASE WHEN consumer = 10 THEN client_id ELSE NULL END) AS unknown,
  COUNT(DISTINCT CASE WHEN consumer = 11 THEN client_id ELSE NULL END) AS uiautomation,
  COUNT(DISTINCT CASE WHEN consumer = 12 THEN client_id ELSE NULL END) AS visperoshared,
  COUNT(DISTINCT CASE WHEN instantiator = 'Magnify.exe' THEN client_id ELSE NULL END) AS magnifier,
  COUNT(DISTINCT CASE WHEN instantiator = 'Narrator.exe' THEN client_id ELSE NULL END) AS narrator,
  COUNT(
    DISTINCT
    CASE
    WHEN
      instantiator = 'osk.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS on_screen_keyboard,
  COUNT(
    DISTINCT
    CASE
    WHEN
      instantiator = 'sapisvr.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS speech_recognition,
  COUNT(
    DISTINCT
    CASE
    WHEN
      instantiator = 'TabTip.exe'
    THEN
      client_id
    ELSE
      NULL
    END
  ) AS touch_keyboard_and_handwriting_panel,
  COUNT(DISTINCT CASE WHEN instantiator = 'VoiceOver' THEN client_id ELSE NULL END) AS voice_over,
  COUNT(DISTINCT CASE WHEN consumer NOT BETWEEN 0 AND 12 THEN client_id ELSE NULL END) AS other
FROM
  a11y_consumers_instantiators
GROUP BY
  submission_date,
  normalized_channel

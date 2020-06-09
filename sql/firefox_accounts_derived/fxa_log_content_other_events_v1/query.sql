SELECT
  jsonPayload.event,
  jsonPayload.flow_time,
  jsonPayload.locale,
  jsonPayload.useragent,
  jsonPayload.country,
  jsonPayload.entrypoint,
  jsonPayload.flow_id,
  jsonPayload.region,
  jsonPayload.service,
  jsonPayload.utm_campaign,
  jsonPayload.utm_content,
  jsonPayload.utm_medium,
  jsonPayload.utm_source,
  -- timestamp, -- This is a static value in 1970 for all records
  receiveTimestamp
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_content_19700101`
WHERE
  NOT (
    REGEXP_CONTAINS(jsonPayload.event, r"^flow\.([\w-]+)\.submit$")
    OR REGEXP_CONTAINS(
      jsonPayload.event,
      r"^experiment\.(?:control|designF|designG)\.passwordStrength\.([\w]+)$"
    )
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.signin-totp-code\.submit$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^screen\.signin-totp-code$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.signin-totp-code\.engage$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^screen\.settings\.two-step-authentication$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.([\w-]+)\.engage$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.[\w-]+\.forgot-password$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.[\w-]+\.have-account$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.((?:install|signin)_from)\.\w+$")
    OR REGEXP_CONTAINS(
      jsonPayload.event,
      r"^flow\.connect-another-device\.link\.(app-store)\.([\w-]+)$"
    )
    OR REGEXP_CONTAINS(jsonPayload.event, r"^screen\.(?:oauth\.)?([\w-]+)$")
    OR REGEXP_CONTAINS(
      jsonPayload.event,
      r"^settings\.communication-preferences\.(optIn|optOut)\.success$"
    )
    OR REGEXP_CONTAINS(jsonPayload.event, r"^settings\.clients\.disconnect\.submit\.([a-z]+)$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^([\w-]+).verification.clicked$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.signin-totp-code\.success$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.(support)\.success$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.(support)\.view$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.(support)\.fail$")
    OR REGEXP_CONTAINS(jsonPayload.event, r"^flow\.([\w-]+)\.view$")
  )
  AND jsonPayload.event NOT IN (
    'flow.reset-password.submit', --regex
    'flow.choose-what-to-sync.back',
    'flow.choose-what-to-sync.engage', --regex
    'flow.choose-what-do-sync.submit', --regex
    'flow.choose-what-to-sync.cwts_do_not_sync',
    'flow.would-you-like-to-sync.cwts_do_not_sync',
    'flow.update-firefox.engage', --regex
    'flow.update-firefox.view', --regex
    'screen.choose-what-to-sync',
    'settings.change-password.success',
    'settings.signout.success',
    'cached.signin.success',
    'screen.confirm-signup-code',
    'flow.confirm-signup-code.engage', --regex
    'flow.confirm-signup-code.submit', --regex
    'screen.connect-another-device',
    'flow.rp.engage' --regex
  )
  AND NOT REGEXP_CONTAINS(jsonPayload.event, r"^flow\.performance.[\w-]+")
  AND DATE(receiveTimestamp) = @submission_timestamp

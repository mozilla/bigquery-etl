WITH base AS (
  SELECT
    receiveTimestamp AS `timestamp`,
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
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_content_19700101`
)
SELECT
  *
FROM
  base
WHERE
  -- We exclude event types that are Amplitude events;
  -- these are already captured in the fxa_content_events_v1 table.
  NOT (
    REGEXP_CONTAINS(event, r"^flow\.([\w-]+)\.submit$")
    OR REGEXP_CONTAINS(
      event,
      r"^experiment\.(?:control|designF|designG)\.passwordStrength\.([\w]+)$"
    )
    OR REGEXP_CONTAINS(event, r"^flow\.signin-totp-code\.submit$")
    OR REGEXP_CONTAINS(event, r"^screen\.signin-totp-code$")
    OR REGEXP_CONTAINS(event, r"^flow\.signin-totp-code\.engage$")
    OR REGEXP_CONTAINS(event, r"^screen\.settings\.two-step-authentication$")
    OR REGEXP_CONTAINS(event, r"^flow\.([\w-]+)\.engage$")
    OR REGEXP_CONTAINS(event, r"^flow\.[\w-]+\.forgot-password$")
    OR REGEXP_CONTAINS(event, r"^flow\.[\w-]+\.have-account$")
    OR REGEXP_CONTAINS(event, r"^flow\.((?:install|signin)_from)\.\w+$")
    OR REGEXP_CONTAINS(event, r"^flow\.connect-another-device\.link\.(app-store)\.([\w-]+)$")
    OR REGEXP_CONTAINS(event, r"^screen\.(?:oauth\.)?([\w-]+)$")
    OR REGEXP_CONTAINS(event, r"^settings\.communication-preferences\.(optIn|optOut)\.success$")
    OR REGEXP_CONTAINS(event, r"^settings\.clients\.disconnect\.submit\.([a-z]+)$")
    OR REGEXP_CONTAINS(event, r"^([\w-]+).verification.clicked$")
    OR REGEXP_CONTAINS(event, r"^flow\.signin-totp-code\.success$")
    OR REGEXP_CONTAINS(event, r"^flow\.(support)\.success$")
    OR REGEXP_CONTAINS(event, r"^flow\.(support)\.view$")
    OR REGEXP_CONTAINS(event, r"^flow\.(support)\.fail$")
    OR REGEXP_CONTAINS(event, r"^flow\.([\w-]+)\.view$")
    OR event IN (
      'flow.reset-password.submit', -- also caught by regex above
      'flow.choose-what-to-sync.back',
      'flow.choose-what-to-sync.engage', -- also caught by regex above
      'flow.choose-what-do-sync.submit', -- also caught by regex above
      'flow.choose-what-to-sync.cwts_do_not_sync',
      'flow.would-you-like-to-sync.cwts_do_not_sync',
      'flow.update-firefox.engage', -- also caught by regex above
      'flow.update-firefox.view', -- also caught by regex above
      'screen.choose-what-to-sync',
      'settings.change-password.success',
      'settings.signout.success',
      'cached.signin.success',
      'screen.confirm-signup-code',
      'flow.confirm-signup-code.engage', -- also caught by regex above
      'flow.confirm-signup-code.submit', -- also caught by regex above
      'screen.connect-another-device',
      'flow.rp.engage' -- also caught by regex above
    )
  )
  AND DATE(`timestamp`) = @submission_date

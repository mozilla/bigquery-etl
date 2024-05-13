CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_frontend.registration_funnels_by_service`
AS
SELECT
  o.name AS service_name,
  r.*
FROM
  `moz-fx-data-shared-prod.accounts_frontend_derived.registration_funnels_by_service_v1` AS r
JOIN
  `moz-fx-data-shared-prod.accounts_db.fxa_oauth_clients` AS o
  ON r.service = o.id
WHERE
  o.id NOT IN (
    '00efbcb5b2dbfa0e',  -- Mozilla.social invitation flow
    '0d1a8469632d0f61',  -- Hubs Reticulum
    '1e5a77cf09e5b4b5',  -- AMO Stage
    '2963d96620413b9f',  -- 123done-heroku-untrusted
    '2a54021236022573',  -- Cinder Report Form
    '3c32bf6654542211',  -- 123done-heroku
    '3c49430b43dfba77',  -- Android Components Reference Browser
    '565585c1745a144d',  -- Firefox Private Network
    '59cceb6f8c32317c',  -- Mozilla subscriptions management
    '6c6c2958595c38de',  -- Thunderbird Appointment
    '6cd7216e25bc0fc0',  -- Add-ons Internal Stage
    '7377719276ad44ee',  -- Pocket mobile
    '798de7affc08df8b',  -- MDN Plus App
    '7ad9917f6c55fb77',  -- Firefox Reality
    '7f368c6886429f19',  -- Notes
    '85da77264642d6a1',  -- Firefox for Fire TV
    '98adfa37698f255b',  -- Firefox Lockwise
    'a3dbd8c5a6fd93e2',  -- Notes
    'b8a3bdc3790a61f6',  -- Mozilla Support Stage
    'b966b5f51190da9e',  -- Add-ons Internal Production
    'c40f32fd2938f0b6',  -- Mozilla email preferences
    'c87d36d7fd9a1bcf',  -- Pontoon Staging
    'e7ce535d93522896',  -- Firefox Lockwise
    'ea3ca969f8c6bb0d'   -- Firefox Accounts
  )

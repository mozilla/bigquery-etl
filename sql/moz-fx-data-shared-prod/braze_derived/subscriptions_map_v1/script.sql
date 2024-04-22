CREATE TABLE IF NOT EXISTS
  braze_derived.subscriptions_map_v1(
    braze_subscription_name STRING,
    description STRING,
    mozilla_subscription_id STRING,
    firefox_subscription_id STRING,
    basket_slug STRING
  );

INSERT INTO
  braze_derived.subscriptions_map_v1(
    braze_subscription_name,
    description,
    mozilla_subscription_id,
    firefox_subscription_id,
    basket_slug
  )
VALUES
  (
    'antiharassment-waitlist',
    'Anti-Harassment tool Waitlist',
    'ebe059f4-ab45-467e-9dfc-25094ca85a51',
    '506f4171-7511-4818-ad44-d0b404eb78e7',
    'antiharassment-waitlist'
  ),
  (
    'app-dev',
    'Developer Newsletter',
    '3ade707a-e9de-4577-ab02-2ababbc669b5',
    'e3f5edd9-7009-4309-8224-b87c6d8d198b',
    'app-dev'
  ),
  (
    'didthis-waitlist',
    'DidThis Waitlist',
    '13d0a1e5-54eb-43bb-891d-f83576415fda',
    'ce88833c-415a-4d60-abc5-ee400b4106be',
    'didthis-waitlist'
  ),
  (
    'firefox-accounts-journey',
    'Welcome emails',
    '21ae0bbb-56ab-4c3d-8add-005a23a9bc9a',
    '9615c825-d9d7-4e9e-a6fe-e5557917ccf0',
    'firefox-accounts-journey'
  ),
  (
    'graceland-waitlist',
    'Graceland Waitlist',
    '0dd864a8-7afb-4e01-874c-5bd89c62f249',
    'fd553bbf-2a0c-48bc-bd55-97f5f06e2fe7',
    'graceland-waitlist'
  ),
  (
    'guardian-vpn-waitlist',
    'Mozilla VPN Waitlist',
    '755c3084-9124-44b9-bed0-3411ad7a02da',
    '1456e35f-cabe-4d8f-9ab4-eae8c46847c4',
    'guardian-vpn-waitlist'
  ),
  (
    'knowledge-is-power',
    'Mozilla Knowledge is Power',
    'f5486fa3-4e95-4fb7-9a14-e3fcd71e9c6c',
    'f1261ad9-081a-4e06-ac30-31575c2dd41c',
    'knowledge-is-power'
  ),
  (
    'mdnplus',
    'MDN',
    'd63436eb-2bf6-492f-a5c6-dc63c86f1711',
    '5d939d48-7653-4aa0-9f65-6496396f4e21',
    'mdnplus'
  ),
  (
    'monitor-waitlist',
    'Monitor Waitlist',
    '7ca5d9bb-758a-4a40-843b-8887b5c8ffaf',
    'e8d42269-c172-4811-8f99-fd55e5ac7e7b',
    'monitor-waitlist'
  ),
  (
    'mozilla-ai-challenge',
    'Responsible AI Challenge',
    'f0ee135a-eac2-4319-aede-f68d60c5e6d5',
    '9d08e23a-431c-4393-93f0-736d6be81e18',
    'mozilla-ai-challenge'
  ),
  (
    'mozilla-and-you',
    'Firefox News',
    'b9ea49d0-ddd0-4db1-a390-7268e0498a24',
    'e1de4636-7c12-4769-a746-978daae231d0',
    'mozilla-and-you'
  ),
  (
    'mozilla-innovation',
    'Innovation Newsletter',
    '39fcaf11-2926-4366-b756-2c02e65cbce7',
    '040fcb8e-d805-44c9-8355-634a7fa37ba3',
    'mozilla-innovation'
  ),
  (
    'mozilla-social-waitlist',
    'Mozilla Social Waitlist',
    'd64e060f-2955-4926-9e36-d54b5d784975',
    'f1dfd97b-592c-4e7f-ad75-43c9c043d200',
    'mozilla-social-waitlist'
  ),
  (
    'relay-phone-masking-waitlist',
    'Relay Phone Masking Waitlist',
    '4011707b-1a55-4177-9f5d-2a6d86225242',
    '6bb0e270-0f0a-4603-9b4c-a643c2e1cbaa',
    'relay-phone-masking-waitlist'
  ),
  (
    'relay-vpn-bundle-waitlist',
    'Relay VPN Bundle Waitlist',
    'b0bffabc-a735-4c70-b48f-c788c42cc3f9',
    '21c2d72d-5f48-405a-94e5-ac9a910939bb',
    'relay-vpn-bundle-waitlist'
  ),
  (
    'relay-waitlist',
    'Firefox Relay Waitlist',
    '9f68d648-ac73-49bc-a94e-1c22191c49a9',
    '07b8198e-f891-4f74-b1a2-c44d11f07bec',
    'relay-waitlist'
  ),
  (
    'security-privacy-news',
    'Security and Privacy News from Mozilla',
    '3c2d58e5-dfaa-43ef-8e37-ae86a5db360d',
    '4b4adbf3-f116-4403-94c9-9c241247bce6',
    'security-privacy-news'
  ),
  (
    'take-action-for-the-internet',
    'Take Action for the Internet',
    'cd16fdf7-88bb-4ff3-a8d9-cc5102790f49',
    '3396d77f-3996-4364-90df-f80813947b4c',
    'take-action-for-the-internet'
  ),
  (
    'test-pilot',
    'New Product Testing',
    '7a6cc4c5-441e-4c3b-95a6-e9de7edc78fd',
    'a021bee3-6d33-46c5-b7c1-d6e64aa07bd6',
    'test-pilot'
  );

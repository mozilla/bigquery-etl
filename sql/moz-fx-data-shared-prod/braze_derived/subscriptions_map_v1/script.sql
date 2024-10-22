CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1`(
    braze_subscription_name STRING,
    description STRING,
    mozilla_subscription_id STRING,
    firefox_subscription_id STRING,
    mozilla_dev_subscription_id STRING,
    basket_slug STRING
  );

INSERT INTO
  `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1`(
    braze_subscription_name,
    description,
    mozilla_subscription_id,
    firefox_subscription_id,
    mozilla_dev_subscription_id,
    basket_slug
  )
VALUES
  (
    'about-addons',
    'Firefox addons developer newsletter',
    'b9f6b780-8fc3-4eab-b033-10d4f5f7a370',
    '09b3ebc5-02d8-4621-932c-2119ca493956',
    '56ad1633-dcc7-4841-aa4d-64026e4e17cb',
    'about-addons'
  ),
  (
    'about-mozilla',
    'Mozilla Community Newsletter',
    'e23dfa20-6dd9-4a2f-a9cb-7bd2f8db5925',
    '2a94147c-466f-4cbe-8362-3305886d2dea',
    '0d4f7b75-fb25-4db0-a83a-98cb3558e185',
    'about-mozilla'
  ),
  (
    'antiharassment-waitlist',
    'Anti-Harassment tool Waitlist',
    'ebe059f4-ab45-467e-9dfc-25094ca85a51',
    '506f4171-7511-4818-ad44-d0b404eb78e7',
    'a1e5b62d-eb64-4281-b8c6-0a7157905eb6',
    'antiharassment-waitlist'
  ),
  (
    'app-dev',
    'Developer Newsletter',
    '3ade707a-e9de-4577-ab02-2ababbc669b5',
    'e3f5edd9-7009-4309-8224-b87c6d8d198b',
    '23069735-6496-4a19-8240-b910f94660b0',
    'app-dev'
  ),
  (
    'didthis-waitlist',
    'DidThis Waitlist',
    '13d0a1e5-54eb-43bb-891d-f83576415fda',
    'ce88833c-415a-4d60-abc5-ee400b4106be',
    '8d349590-84e1-44ef-bef8-c743f2429139',
    'didthis-waitlist'
  ),
  (
    'firefox-accounts-journey',
    'Welcome emails',
    '21ae0bbb-56ab-4c3d-8add-005a23a9bc9a',
    '9615c825-d9d7-4e9e-a6fe-e5557917ccf0',
    'e095ad1d-ce90-4d4e-981e-9afc2da22a21',
    'firefox-accounts-journey'
  ),
  (
    'formulaic-app',
    'Formulaic newsletter',
    '3349e1ab-acb8-4396-8fff-ee9d5cf96678',
    '9ceceace-0184-4fb6-a2d8-0c1943026bb7',
    '91f8d0bf-5d01-48b5-bcc4-e12373e3f89b',
    'formulaic-app'
  ),
  (
    'graceland-waitlist',
    'Graceland Waitlist',
    '0dd864a8-7afb-4e01-874c-5bd89c62f249',
    'fd553bbf-2a0c-48bc-bd55-97f5f06e2fe7',
    '2bd5aae2-a0c3-4d7c-9afa-5e8877549de2',
    'graceland-waitlist'
  ),
  (
    'guardian-vpn-waitlist',
    'Mozilla VPN Waitlist',
    '755c3084-9124-44b9-bed0-3411ad7a02da',
    '1456e35f-cabe-4d8f-9ab4-eae8c46847c4',
    '2a87f6bc-0028-4f2f-8d91-af253ad3cf14',
    'guardian-vpn-waitlist'
  ),
  (
    'knowledge-is-power',
    'Mozilla Knowledge is Power',
    'f5486fa3-4e95-4fb7-9a14-e3fcd71e9c6c',
    'f1261ad9-081a-4e06-ac30-31575c2dd41c',
    'eb2208b3-5e7d-4d72-8e43-803a43373fb8',
    'knowledge-is-power'
  ),
  (
    'mdnplus',
    'MDN',
    'd63436eb-2bf6-492f-a5c6-dc63c86f1711',
    '5d939d48-7653-4aa0-9f65-6496396f4e21',
    '55dce620-720b-42e3-9d35-ab43de4e13c0',
    'mdnplus'
  ),
  (
    'monitor-waitlist',
    'Monitor Waitlist',
    '7ca5d9bb-758a-4a40-843b-8887b5c8ffaf',
    'e8d42269-c172-4811-8f99-fd55e5ac7e7b',
    '791113d3-82e9-4e45-b4e0-5e7d3f815765',
    'monitor-waitlist'
  ),
  (
    'mozilla-ai-challenge',
    'Responsible AI Challenge',
    'f0ee135a-eac2-4319-aede-f68d60c5e6d5',
    '9d08e23a-431c-4393-93f0-736d6be81e18',
    'e49e6683-6e77-479d-a2ed-c9753989864c',
    'mozilla-ai-challenge'
  ),
  (
    'mozilla-and-you',
    'Firefox News',
    'b9ea49d0-ddd0-4db1-a390-7268e0498a24',
    'e1de4636-7c12-4769-a746-978daae231d0',
    '96f6577e-6efd-4744-9d96-dbe67b1f48c5',
    'mozilla-and-you'
  ),
  (
    'mozilla-builder',
    'Mozilla Builder newsletter',
    '7e8d71f9-2e26-4308-ac0f-05a591cbae5a',
    '1825ad87-9b63-45e4-b0ae-92c2e38feace',
    '74435f4a-e25e-41bb-973a-a769179e05a0',
    'mozilla-builder'
  ),
  (
    'mozilla-builders-application-2024',
    'Mozilla Builder Application',
    'd48a2578-4963-4ac9-9d4e-3005106a3606',
    'd985ba67-a7ad-47a3-8358-04d223a16079',
    '6fdbce2c-0110-4bd5-94ce-ff6f4e2ca905',
    'mozilla-builders-application-2024'
  ),
  (
    'mozilla-innovation',
    'Innovation Newsletter',
    '39fcaf11-2926-4366-b756-2c02e65cbce7',
    '040fcb8e-d805-44c9-8355-634a7fa37ba3',
    'cfcd1d98-21f1-4485-8594-ebf68451ec2d',
    'mozilla-innovation'
  ),
  (
    'mozilla-social-waitlist',
    'Mozilla Social Waitlist',
    'd64e060f-2955-4926-9e36-d54b5d784975',
    'f1dfd97b-592c-4e7f-ad75-43c9c043d200',
    'a0fc09f3-a0cd-4321-8325-6235cec6d162',
    'mozilla-social-waitlist'
  ),
  (
    'nothing-personal-college-interest',
    'Firefox University interest',
    '7edc5999-8893-4453-b4f3-70ab7c479dd2',
    '9b2bfa6c-0725-4a2d-8b21-a9f05f2d0bba',
    'c0681ddd-1ae8-49df-bffd-6f3e6dd0aa92',
    'nothing-personal-college-interest'
  ),
  (
    'relay-phone-masking-waitlist',
    'Relay Phone Masking Waitlist',
    '4011707b-1a55-4177-9f5d-2a6d86225242',
    '6bb0e270-0f0a-4603-9b4c-a643c2e1cbaa',
    'b9000bb3-7377-4908-9579-b0b7341aaad6',
    'relay-phone-masking-waitlist'
  ),
  (
    'relay-vpn-bundle-waitlist',
    'Relay VPN Bundle Waitlist',
    'b0bffabc-a735-4c70-b48f-c788c42cc3f9',
    '21c2d72d-5f48-405a-94e5-ac9a910939bb',
    '6eb4658e-f030-4b08-a763-0930ac1112eb',
    'relay-vpn-bundle-waitlist'
  ),
  (
    'relay-waitlist',
    'Firefox Relay Waitlist',
    '9f68d648-ac73-49bc-a94e-1c22191c49a9',
    '07b8198e-f891-4f74-b1a2-c44d11f07bec',
    '07feceb4-1db5-4ada-a559-5615063fbeca',
    'relay-waitlist'
  ),
  (
    'security-privacy-news',
    'Security and Privacy News from Mozilla',
    '3c2d58e5-dfaa-43ef-8e37-ae86a5db360d',
    '4b4adbf3-f116-4403-94c9-9c241247bce6',
    '288e2dbb-7f48-4b77-b238-8d8e1bd01810',
    'security-privacy-news'
  ),
  (
    'take-action-for-the-internet',
    'Take Action for the Internet',
    'cd16fdf7-88bb-4ff3-a8d9-cc5102790f49',
    '3396d77f-3996-4364-90df-f80813947b4c',
    '8627ca41-5358-4107-9d50-677ed31d57ff',
    'take-action-for-the-internet'
  ),
  (
    'test-pilot',
    'New Product Testing',
    '7a6cc4c5-441e-4c3b-95a6-e9de7edc78fd',
    'a021bee3-6d33-46c5-b7c1-d6e64aa07bd6',
    '1da52ebe-442e-41b5-bab2-dc293f559c66',
    'test-pilot'
  );

CREATE OR REPLACE FUNCTION norm.partner_org_clients(distribution_id STRING)
RETURNS STRING AS (
  CASE
    WHEN distribution_id IN ('1und1', 'gmx', 'gmxcouk', 'gmxes', 'gmxfr', 'mail.com', 'webde')
      THEN 'United Internet'
    WHEN distribution_id IN (
        'yandex-drp',
        'yandex-portals',
        'yandex-ru',
        'yandex-ru-mz',
        'yandex-tr',
        'yandex-ua',
        'yandex',
        'yandex-uk',
        'yandex-tr-tamindir',
        'mailru-001',
        'orku-001'
      )
      THEN 'Yandex'
    WHEN distribution_id IN ('seznam')
      THEN 'Seznam'
    WHEN distribution_id IN (
        'softonic-004',
        'softonic-003',
        'softonic-002',
        'softonic-005',
        'softonic-009',
        'softonic-006',
        'softonic-008'
      )
      THEN 'Softonic'
    WHEN distribution_id IN (
        'yahoo',
        'yahoode',
        'yahoofr',
        'yahootw',
        'yahoohk',
        'yahoogb',
        'yahooca',
        'yahooid'
      )
      THEN 'Yahoo'
    WHEN distribution_id IN ('aol', 'aoluk', 'aolde')
      THEN 'AOL'
    WHEN distribution_id IN ('mozilla-chipde-001')
      THEN 'Chipde'
    WHEN distribution_id IN ('canonical', 'canonical-001', 'canonical-002')
      THEN 'Linux - Canonical'
    WHEN distribution_id IN ('mint-001', 'mint')
      THEN 'Linux - Mint'
    WHEN distribution_id IN (
        'fedora',
        'redhat',
        'archlinux',
        'altlinux',
        'alpinelinux',
        'almalinux',
        'artixlinux',
        'MX-Linux',
        'rocky-linux',
        'gentoo',
        'openSUSE',
        'suse'
      )
      THEN 'Linux - Misc.'
    WHEN distribution_id IN ('mozilla-deb', 'Debian', 'debian')
      THEN 'Linux - Deb (store)'
    WHEN distribution_id IN ('mozilla-flatpak')
      THEN 'Linux - Flatpak (store)'
    WHEN distribution_id IN ('mozilla-MSIX')
      THEN 'Microsoft (store)'
    WHEN distribution_id IN ('euballot')
      THEN 'Microsoft (EU ballot)'
    WHEN distribution_id IN ('toshiba-001')
      THEN 'Toshiba'
    WHEN distribution_id IN ('acer', 'acer-001', 'acer-002', 'acer-g-003', 'acer-003', 'acer-004')
      THEN 'Acer'
    WHEN distribution_id IN ('wildtangent-001')
      THEN 'Wild Tangent'
    WHEN distribution_id IN (
        'sweetlabs-b-oem2',
        'sweetlabs-oem2',
        'sweetlabs-b-r-oem2',
        'sweetlabs-b-oem1',
        'sweetlabs-b-oem3',
        'sweetlabs-b-r-oem1',
        'sweetlabs-oem1'
      )
      THEN 'Sweetlabs'
    WHEN distribution_id IN ('playanext-wt-001', 'playanext-wt-us-001')
      THEN 'Playanext'
    WHEN distribution_id IN (
        'isltd-g-001',
        'isltd-g-aura-001',
        'isltd-y-aura-001',
        'isltd-y-001',
        'isltd-001',
        'isltd-002',
        'isltd-003'
      )
      THEN 'Ironsource'
    WHEN distribution_id IN ('MozillaOnline')
      THEN 'Mozilla - China'
    WHEN distribution_id IN (
        'mozilla-win-eol-esr115',
        'mozilla-mac-eol-esr1',
        'mozilla-mac-eol-esr115'
      )
      THEN 'Mozilla - ESR migration'
    WHEN distribution_id IN ('mozilla-EMEfree', 'EMEfree')
      THEN 'Mozilla - EME free'
    WHEN distribution_id IN (
        'mozilla101',
        'mozilla102',
        'mozilla139',
        'mozilla138',
        'mozilla86',
        'mozilla116',
        'mozilla63',
        'mozilla88',
        'mozilla118',
        'mozilla134',
        'mozilla105',
        'mozilla94',
        'mozilla117',
        'mozilla14',
        'mozilla93',
        'mozilla15',
        'mozilla114',
        'mozilla104',
        'mozilla103',
        'mozilla111',
        'mozilla81',
        'mozilla50',
        'mozilla76',
        'mozilla113',
        'mozilla90',
        'mozilla80',
        'mozilla87',
        'mozilla97',
        'mozilla100',
        'mozilla77',
        'mozilla75',
        'mozilla78',
        'mozilla110',
        'mozilla52',
        'mozilla79',
        'mozilla106',
        'mozilla85',
        'mozilla53',
        'mozilla115',
        'mozilla112',
        'mozilla98',
        'mozilla99',
        'mozilla28',
        'mozilla12',
        'mozilla84',
        'mozilla11',
        'mozilla26',
        'mozilla68',
        'mozilla83',
        'mozilla91',
        'mozilla94-default',
        'mozilla121',
        'mozilla41',
        'mozilla92',
        'mozilla82',
        'mozilla96',
        'mozilla132',
        'mozilla67',
        'mozilla61',
        'mozilla13',
        'mozilla51',
        'mozilla19',
        'mozilla66',
        'mozilla131',
        'mozilla89',
        'mozilla104-utility-existing',
        'mozilla35',
        'mozilla130',
        'mozilla32',
        'mozilla120',
        'mozilla34',
        'mozilla38',
        'mozilla119',
        'mozilla36',
        'mozilla43',
        'mozilla21',
        'mozilla45',
        'mozilla95',
        'mozilla-cliqz-001',
        'mozilla60',
        'mozilla122',
        'mozilla25',
        'mozilla135',
        'mozilla102-no-thanks',
        'mozilla22',
        'mozilla-cliqz-008',
        'mozilla86-utility-existing',
        'mozilla-cliqz-005',
        'mozilla18',
        'mozilla40',
        'mozilla-cliqz-006'
      )
      THEN 'Mozilla - Funnelcakes'
    WHEN distribution_id IS NULL
      THEN 'non-distribution'
    ELSE 'Uncategorized'
  END
);

SELECT
  mozfun.assert.equals(mozfun.norm.partner_org_clients('mozilla18'), 'Mozilla - Funnelcakes'),
  mozfun.assert.equals(mozfun.norm.partner_org_clients('mozilla22'), 'Mozilla - Funnelcakes'),
  mozfun.assert.equals(mozfun.norm.partner_org_clients('webde'), 'United Internet'),
  mozfun.assert.equals(mozfun.norm.partner_org_clients(NULL), 'non-distribution'),
  mozfun.assert.equals(mozfun.norm.partner_org_clients('abcdefghijkl'), 'Uncategorized'),
  mozfun.assert.equals(mozfun.norm.partner_org_clients('isltd-001'), 'Ironsource'),
  mozfun.assert.equals(
    mozfun.norm.partner_org_clients('mozilla-win-eol-esr115'),
    'Mozilla - ESR migration'
  ),
  mozfun.assert.equals(
    mozfun.norm.partner_org_clients('mozilla-mac-eol-esr1'),
    'Mozilla - ESR migration'
  ),
  mozfun.assert.equals(
    mozfun.norm.partner_org_clients('mozilla-mac-eol-esr115'),
    'Mozilla - ESR migration'
  );

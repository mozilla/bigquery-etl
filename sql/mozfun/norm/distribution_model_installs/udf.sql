CREATE OR REPLACE FUNCTION norm.distribution_model_installs(distribution_id STRING)
RETURNS STRING AS (
  CASE
    WHEN distribution_id IN ('1und1', 'gmx', 'gmxcouk', 'gmxes', 'gmxfr', 'mail.com', 'webde')
      THEN 'partner website'
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
      THEN 'partner website'
    WHEN distribution_id IN ('seznam')
      THEN 'partner website'
    WHEN distribution_id IN (
        'softonic-004',
        'softonic-003',
        'softonic-002',
        'softonic-005',
        'softonic-009',
        'softonic-006',
        'softonic-008'
      )
      THEN 'partner website'
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
      THEN 'partner website'
    WHEN distribution_id IN ('aol', 'aoluk', 'aolde')
      THEN 'partner website'
    WHEN distribution_id IN ('mozilla-chipde-001')
      THEN 'partner website'
    WHEN distribution_id IN ('canonical', 'canonical-001', 'canonical-002')
      THEN 'OS pre-installed'
    WHEN distribution_id IN ('mint-001', 'mint')
      THEN 'OS pre-installed'
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
      THEN 'OS pre-installed'
    WHEN distribution_id IN ('mozilla-deb', 'Debian', 'debian')
      THEN 'secondary store'
    WHEN distribution_id IN ('mozilla-flatpak')
      THEN 'secondary store'
    WHEN distribution_id IN ('mozilla-MSIX')
      THEN 'secondary store'
    WHEN distribution_id IN ('euballot')
      THEN 'secondary store'
    WHEN distribution_id IN ('toshiba-001')
      THEN 'OEM pre-installed'
    WHEN distribution_id IN ('acer', 'acer-001', 'acer-002')
      THEN 'OEM pre-installed'
    WHEN distribution_id IN ('acer-g-003', 'acer-003', 'acer-004')
      THEN 'OEM onboarding'
    WHEN distribution_id IN ('wildtangent-001')
      THEN 'OEM onboarding'
    WHEN distribution_id IN (
        'sweetlabs-b-oem2',
        'sweetlabs-oem2',
        'sweetlabs-b-r-oem2',
        'sweetlabs-b-oem1',
        'sweetlabs-b-oem3',
        'sweetlabs-b-r-oem1',
        'sweetlabs-oem1'
      )
      THEN 'OEM onboarding'
    WHEN distribution_id IN ('playanext-wt-001', 'playanext-wt-us-001')
      THEN 'OEM onboarding'
    WHEN distribution_id IN (
        'isltd-g-001',
        'isltd-g-aura-001',
        'isltd-y-aura-001',
        'isltd-y-001',
        'isltd-001',
        'isltd-002',
        'isltd-003'
      )
      THEN 'OEM onboarding'
    WHEN distribution_id IN ('MozillaOnline')
      THEN 'partner website'
    WHEN distribution_id IN (
        'mozilla-win-eol-esr115',
        'mozilla-mac-eol-esr1',
        'mozilla-mac-eol-esr115'
      )
      THEN 'mozilla internal accounting'
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
      THEN 'mozilla internal accounting'
    WHEN (distribution_id IS NULL OR distribution_id != '0')
      THEN 'non-distribution'
    ELSE 'Uncategorized'
  END
);

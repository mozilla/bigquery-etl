with

telemetry_probes_with_expiration as (
  select
    channel,
    probe,
    type,
    release_version,
    last_version,
    expiry_version,
    case
      when expiry_version = "never" then last_version + 1
      else if( last_version + 1 < cast(expiry_version as int64), last_version + 1, cast(expiry_version as int64))
    end as expired_version_helper,
    first_added_date as release_date
  from `telemetry_dev_cycle_external.telemetry_probes_stats_v1` as probes
),

final as (
  select
    probes.channel,
    probes.probe,
    probes.type,
    probes.expiry_version,
    probes.release_version,
    --coalesce(release_dates.version as expired_version,
    release_dates.version as expired_version,
    --probes.expired_version_helper,
    --probes.last_version,
    probes.release_date,
    case
      when probes.expired_version_helper < 100 then coalesce(release_dates.publish_date, release_date)
      else release_dates.publish_date
    end as expired_date
  from telemetry_probes_with_expiration as probes
  left join `telemetry_dev_cycle_derived.firefox_major_release_dates_v1` as release_dates
  on probes.channel = release_dates.channel and probes.expired_version_helper = release_dates.version
)

select * from final



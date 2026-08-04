-- Transform raw Socorro crash JSON (loaded into a temp table with natural
-- arrays) into the socorro_crash_v2 shape: inject the crash_date partition
-- column and rewrap arrays as RECORD<list: ARRAY<RECORD<element>>> to match the
-- encoding the old Spark parquet writer produced.
--
-- {source_table} and {crash_date} are filled in by query.py.
--
-- BigQuery matches STRUCT/RECORD fields by position, not name, so the output
-- must list every column in the exact order schema.yaml (and the destination
-- table) declares them. That rules out `* EXCEPT (...)` followed by appending
-- the rewrapped fields, which would move them to the end and make BigQuery see
-- reordered columns as schema changes. Columns are therefore listed explicitly
-- in schema.yaml order; only the seven wrapped arrays and crash_date differ
-- from a plain passthrough.
SELECT
  DATE("{crash_date}") AS crash_date,
  abort_message,
  accessibility,
  adapter_device_id,
  adapter_driver_version,
  adapter_subsys_id,
  adapter_vendor_id,
  STRUCT(
    ARRAY(SELECT AS STRUCT element FROM UNNEST(additional_minidumps) AS element) AS list
  ) AS additional_minidumps,
  STRUCT(ARRAY(SELECT AS STRUCT element FROM UNNEST(addons) AS element) AS list) AS addons,
  addons_checked,
  address,
  android_board,
  android_brand,
  android_cpu_abi,
  android_cpu_abi2,
  android_device,
  android_hardware,
  android_manufacturer,
  android_model,
  android_version,
  app_init_dlls,
  app_notes,
  available_physical_memory,
  available_virtual_memory,
  bios_manufacturer,
  build_id,
  classifications,
  contains_memory_report,
  cpu_arch,
  cpu_info,
  cpu_microcode_version,
  crash_id,
  `date`,
  dom_ipc_enabled,
  e10s_cohort,
  flash_version,
  gmp_plugin,
  graphics_critical_error,
  graphics_startup_test,
  hang_type,
  -- Loaded as STRING (see LOOSE_INT_FIELDS in query.py); values that do not fit
  -- in INT64 become NULL rather than failing the load.
  SAFE_CAST(install_age AS INT64) AS install_age,
  ipc_channel_error,
  ipc_fatal_error_msg,
  ipc_fatal_error_protocol,
  ipc_message_name,
  ipc_system_error,
  is_garbage_collecting,
  java_stack_trace,
  jit_category,
  IF(
    json_dump IS NULL,
    NULL,
    (
      SELECT AS STRUCT
        json_dump.crash_info,
        IF(
          json_dump.crashing_thread IS NULL,
          NULL,
          (
            SELECT AS STRUCT
              STRUCT(
                ARRAY(
                  SELECT AS STRUCT
                    element
                  FROM
                    UNNEST(json_dump.crashing_thread.frames) AS element
                ) AS list
              ) AS frames,
              json_dump.crashing_thread.threads_index,
              json_dump.crashing_thread.total_frames
          )
        ) AS crashing_thread,
        json_dump.largest_free_vm_block,
        json_dump.main_module,
        STRUCT(
          ARRAY(SELECT AS STRUCT element FROM UNNEST(json_dump.modules) AS element) AS list
        ) AS modules,
        json_dump.pid,
        json_dump.status,
        json_dump.system_info,
        json_dump.thread_count,
        STRUCT(
          ARRAY(
            SELECT AS STRUCT
              STRUCT(
                thread.frame_count,
                STRUCT(
                  ARRAY(SELECT AS STRUCT element FROM UNNEST(thread.frames) AS element) AS list
                ) AS frames
              ) AS element
            FROM
              UNNEST(json_dump.threads) AS thread
          ) AS list
        ) AS threads,
        json_dump.tiny_block_size,
        json_dump.write_combine_size
    )
  ) AS json_dump,
  SAFE_CAST(last_crash AS INT64) AS last_crash,
  memory_measures,
  IF(
    memory_report IS NULL,
    NULL,
    (
      SELECT AS STRUCT
        memory_report.hasMozMallocUsableSize,
        STRUCT(
          ARRAY(SELECT AS STRUCT element FROM UNNEST(memory_report.reports) AS element) AS list
        ) AS reports,
        memory_report.version
    )
  ) AS memory_report,
  moz_crash_reason,
  oom_allocation_size,
  platform,
  platform_pretty_version,
  platform_version,
  plugin_filename,
  plugin_name,
  plugin_version,
  process_type,
  processor_notes,
  product,
  productid,
  proto_signature,
  reason,
  release_channel,
  safe_mode,
  shutdown_progress,
  signature,
  startup_crash,
  submitted_from_infobar,
  theme,
  topmost_filenames,
  total_physical_memory,
  total_virtual_memory,
  SAFE_CAST(uptime AS INT64) AS uptime,
  user_comments,
  useragent_locale,
  uuid,
  version,
  winsock_lsp,
  minidump_sha256_hash,
  dom_fission_enabled
FROM
  `{source_table}`

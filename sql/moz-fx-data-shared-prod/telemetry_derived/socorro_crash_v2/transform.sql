-- Transform raw Socorro crash JSON (loaded into a temp table with natural
-- arrays) into the socorro_crash_v2 shape: inject the crash_date partition
-- column and rewrap arrays as RECORD<list: ARRAY<RECORD<element>>> to match the
-- encoding the old Spark parquet writer produced.
--
-- {source_table} and {crash_date} are filled in by query.py.
--
-- Every scalar column is carried through by name via the * EXCEPT below; only
-- the seven wrapped arrays and crash_date are rewritten explicitly.
SELECT
  DATE("{crash_date}") AS crash_date,
  * EXCEPT (additional_minidumps, addons, json_dump, memory_report),
  STRUCT(
    ARRAY(SELECT AS STRUCT element FROM UNNEST(additional_minidumps) AS element) AS list
  ) AS additional_minidumps,
  STRUCT(ARRAY(SELECT AS STRUCT element FROM UNNEST(addons) AS element) AS list) AS addons,
  (
    SELECT AS STRUCT
      json_dump.* EXCEPT (crashing_thread, modules, threads),
      STRUCT(
        json_dump.crashing_thread.* EXCEPT (frames),
        STRUCT(
          ARRAY(
            SELECT AS STRUCT
              element
            FROM
              UNNEST(json_dump.crashing_thread.frames) AS element
          ) AS list
        ) AS frames
      ) AS crashing_thread,
      STRUCT(
        ARRAY(SELECT AS STRUCT element FROM UNNEST(json_dump.modules) AS element) AS list
      ) AS modules,
      STRUCT(
        ARRAY(
          SELECT AS STRUCT
            thread.* EXCEPT (frames),
            STRUCT(
              ARRAY(SELECT AS STRUCT element FROM UNNEST(thread.frames) AS element) AS list
            ) AS frames
          FROM
            UNNEST(json_dump.threads) AS thread
        ) AS list
      ) AS threads
  ) AS json_dump,
  (
    SELECT AS STRUCT
      memory_report.* EXCEPT (reports),
      STRUCT(
        ARRAY(SELECT AS STRUCT element FROM UNNEST(memory_report.reports) AS element) AS list
      ) AS reports
  ) AS memory_report
FROM
  `{source_table}`

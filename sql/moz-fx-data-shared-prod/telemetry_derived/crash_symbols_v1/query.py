"""Upload symbols used in crash pings."""
from datetime import datetime
from functools import partial
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path

import click
import requests
import symbolic
import yaml
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError
from urllib3.util import Retry

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
OUTPUT_SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields


def _bytes_split_generator(item, sep):
    # https://github.com/mozilla-services/eliot/blob/be74cd6c0ef09dd85a71c8b1b22c3297b6b9f9bf/eliot/libsymbolic.py#L18-L36
    index = 0
    len_item = len(item)
    while index <= len_item:
        next_index = item.find(sep, index)
        if next_index == -1:
            break
        yield item[index:next_index]
        index = next_index + len(sep)


def _get_module_filename(sym_file, debug_filename):
    # https://github.com/mozilla-services/eliot/blob/be74cd6c0ef09dd85a71c8b1b22c3297b6b9f9bf/eliot/libsymbolic.py#L39-L68
    # Iterate through the first few lines of the file until we hit FILE in which
    # case there's no INFO for some reason or we hit the first INFO.
    for line in _bytes_split_generator(sym_file, b"\n"):
        if line.startswith(b"INFO"):
            parts = line.split(b" ")
            if len(parts) == 4:
                return parts[-1].decode("utf-8").strip()
            else:
                break
        elif line.startswith((b"FILE", b"PUBLIC", b"FUNC")):
            break
    return debug_filename


def _get_symbols(session, source_url, submission_date, row):
    """Get symbols needed from a single symbol file.

    This may be rearchitected to use eliot by sending per-module jobs.
    """
    debug_file = row["debug_file"]
    debug_id = row["debug_id"]
    module_offsets = row["module_offsets"]

    symbols = []
    if debug_file.endswith(".pdb"):
        sym_filename = debug_file[:-4] + ".sym"
    else:
        sym_filename = debug_file + ".sym"

    request_timestamp = datetime.utcnow()
    try:
        resp = session.get(
            f"{source_url}/{debug_file}/{debug_id}/{sym_filename}",
            allow_redirects=True,
        )
        if resp.status_code not in (400, 404):
            resp.raise_for_status()
    except (ConnectionError, HTTPError) as e:
        print("ERROR: could not get symbols: " f"{debug_file} {debug_id} {e}")
        return e, []
    if resp.status_code == 404:
        print("WARNING: symbols not found: " f"{debug_file} {debug_id}")
        return None, []
    if resp.status_code == 400:
        print(
            f"400 Invalid Request {resp.request.url}: "
            f"{resp.request.body.decode('utf-8')!r}"
        )
        resp.raise_for_status()

    sym_file = resp.content

    norm_debug_id = symbolic.normalize_debug_id(debug_id)
    sym_archive = symbolic.Archive.from_bytes(sym_file)
    symcache = sym_archive.get_object(debug_id=norm_debug_id).make_symcache()
    module_filename = _get_module_filename(sym_file, debug_file)

    # https://github.com/mozilla-services/eliot/blob/be74cd6c0ef09dd85a71c8b1b22c3297b6b9f9bf/eliot/symbolicate_resource.py#L505-L553
    for module_offset in module_offsets:
        frame_info = {
            "submission_date": submission_date,
            "request_timestamp": request_timestamp.isoformat(),
            "debug_file": debug_file,
            "debug_id": debug_id,
            "module_offset": hex(module_offset),
            "module": module_filename,
        }

        if module_offset < 0:
            continue  # ignore invalid offset

        sourceloc_list = symcache.lookup(module_offset)
        if not sourceloc_list:
            continue  # no symbols for this offset

        # sourceloc_list can have multiple entries: It starts with the innermost
        # inline stack frame, and then advances to its caller, and then its
        # caller, and so on, until it gets to the outer function.
        # We process the outer function first, and then add inline stack frames
        # afterwards. The outer function is the last item in sourceloc_list.
        sourceloc = sourceloc_list[-1]

        frame_info["function"] = sourceloc.symbol
        frame_info["function_offset"] = hex(module_offset - sourceloc.sym_addr)
        if sourceloc.full_path:
            frame_info["file"] = sourceloc.full_path

            # Only add a "line" if it's non-zero and not None, and if there's a
            # file--otherwise the line doesn't mean anything
            if sourceloc.line:
                frame_info["line"] = sourceloc.line

        if len(sourceloc_list) > 1:
            # We have inline information. Add an "inlines" property with a list
            # of { function, file, line } entries.
            inlines = []
            for inline_sourceloc in sourceloc_list[:-1]:
                inline_data = {
                    "function": inline_sourceloc.symbol,
                }

                if inline_sourceloc.full_path:
                    inline_data["file"] = inline_sourceloc.full_path

                    if inline_sourceloc.line:
                        inline_data["line"] = inline_sourceloc.line

                inlines.append(inline_data)

            frame_info["inlines"] = inlines
        symbols.append(frame_info)
    return None, symbols


@click.command()
@click.option(
    "--collect-only-missing/--collect-all",
    default=True,
    help="Collect only missing symbols or collect all symbols for --submission-date",
)
@click.option(
    "--parallelism",
    default=40,
    help="Number of threads to use when downloading symbols."
    " Default assumes at least 100GiB of ram available.",
)
@click.option(
    "--submission-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
)
@click.option(
    "--source-table",
    default="moz-fx-data-shared-prod.telemetry_derived.crash_frames_v1",
)
@click.option(
    "--source-url",
    default="https://symbols.mozilla.org",
)
@click.option(
    "--destination-table",
    default="moz-fx-data-shared-prod.telemetry_derived.crash_symbols_v1",
)
def main(
    collect_only_missing,
    parallelism,
    submission_date,
    source_table,
    source_url,
    destination_table,
):
    bq = bigquery.Client()
    query_job = bq.query(
        f"""
        SELECT
          debug_file,
          debug_id,
          ARRAY_AGG(DISTINCT module_offset ORDER BY module_offset) AS module_offsets,
        FROM
          `{source_table}`
        WHERE
          DATE(submission_timestamp) = @submission_date
          AND debug_file != ""
          AND debug_id != ""
        GROUP BY
          debug_file,
          debug_id
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "submission_date", "DATE", f"{submission_date:%F}"
                ),
            ],
        ),
    )
    rows = [{**row} for row in query_job.result()]

    if collect_only_missing:
        # don't collect symbols already present in the destination table
        try:
            bq.get_table(destination_table)
        except NotFound:
            pass  # collect all symbols
        else:
            existing_symbols_job = bq.query(
                f"""
                SELECT
                  debug_id,
                  debug_file,
                  ARRAY_AGG(DISTINCT module_offset) AS module_offsets,
                FROM
                  `{destination_table}`
                WHERE
                  submission_date = @submission_date
                GROUP BY
                  debug_id,
                  debug_file
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "submission_date", "DATE", f"{submission_date:%F}"
                        ),
                    ],
                ),
            )
            existing_symbols = {
                (row["debug_file"], row["debug_id"]): set(row["module_offsets"])
                for row in existing_symbols_job.result()
            }
            for row in rows:
                existing_module_offsets = existing_symbols.get(
                    (row["debug_file"], row["debug_id"])
                )
                if existing_module_offsets is not None:
                    row["module_offsets"] = [
                        o
                        for o in row["module_offsets"]
                        if o not in existing_module_offsets
                    ]
            rows = [row for row in rows if row["module_offsets"]]  # drop empty rows

    retry_strategy = Retry(
        total=4,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    _get_symbols_with_args = partial(_get_symbols, session, source_url, submission_date)

    symbols = []
    symbol_errors = []
    with Pool(parallelism) as pool:
        for error, _symbols in pool.imap(_get_symbols_with_args, rows, chunksize=1):
            if error is None:
                symbols.extend(symbols)
            else:
                symbol_errors.append(error)

    load_job = bq.load_table_from_json(
        json_rows=symbols,
        destination=f"{destination_table}${submission_date:%Y%m%d}",
        job_config=bigquery.LoadJobConfig(
            ignore_unknown_values=False,
            schema=OUTPUT_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    try:
        load_job.result()
    except BadRequest as e:
        print(e.errors)
        print(load_job.errors)
        raise

    if symbol_errors:
        # errors collecting symbols are deferred to here so that retries
        # only need to collect symbols that previously failed
        raise Exception(
            "failed to collect some symbols, "
            "run again in append mode to retry only missing symbols"
        )


if __name__ == "__main__":
    main()

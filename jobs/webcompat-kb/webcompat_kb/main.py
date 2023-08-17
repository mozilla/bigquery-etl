import click
import logging
import requests
import re

from google.cloud import bigquery

BUGZILLA_API = "https://bugzilla.mozilla.org/rest"

OTHER_BROWSER = ["bugs.chromium.org", "bugs.webkit.org", "crbug.com"]
STANDARDS_ISSUES = ["github.com/w3c", "github.com/whatwg", "github.com/wicg"]
STANDARDS_POSITIONS = ["standards-positions"]
INTERVENTIONS = ["github.com/mozilla-extensions/webcompat-addon"]
FIELD_MAP = {
    "blocker": 1,
    "critical": 1,
    "major": 2,
    "normal": 3,
    "minor": 4,
    "trivial": 4,
    "enhancement": 4,
    "n/a": None,
    "--": None,
}


def get_urls_from_story(user_story_str):
    pattern = r"url:(\S*)"
    return re.findall(pattern, user_story_str)


RELATION_CONFIG = {
    "core_bugs": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "core_bug", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        "source": "depends_on",
        "store_id": "core",
    },
    "breakage_reports": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "breakage_bug", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        "source": "blocks",
        "store_id": "breakage",
    },
    "interventions": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "code_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": INTERVENTIONS,
    },
    "other_browser_issues": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "issue_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": OTHER_BROWSER,
    },
    "standards_issues": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "issue_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": STANDARDS_ISSUES,
    },
    "standards_positions": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "discussion_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": STANDARDS_POSITIONS,
    },
    "url_patterns": {
        "fields": [
            {"name": "bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "url_pattern", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "cf_user_story",
        "custom_func": get_urls_from_story,
    },
}

LINK_FIELDS = ["other_browser_issues", "standards_issues", "standards_positions"]


def fetch_bugs(params=None):
    if params is None:
        params = {}

    fields = [
        "id",
        "summary",
        "status",
        "resolution",
        "product",
        "component",
        "see_also",
        "depends_on",
        "blocks",
        "priority",
        "severity",
        "cf_user_story",
    ]

    url = f"{BUGZILLA_API}/bug"
    params["include_fields"] = ",".join(fields)

    response = requests.get(url, params=params)
    response.raise_for_status()
    result = response.json()

    return result["bugs"]


def process_individual_bug(bug, relation_config, processed_bugs, bug_ids):
    bug_id = bug["id"]

    if bug_id not in processed_bugs:
        processed_bugs[bug_id] = {rel: [] for rel in relation_config.keys()}

    for rel, config in relation_config.items():
        if "custom_func" in config:
            source_data = config["custom_func"](bug[config["source"]])
        else:
            source_data = bug[config["source"]]

        for data in source_data:
            if "condition" in config and not any(
                c in data for c in config["condition"]
            ):
                continue

            processed_bugs[bug_id][rel].append(data)

            if config.get("store_id"):
                if config["store_id"] not in bug_ids:
                    bug_ids[config["store_id"]] = []

                bug_ids[config["store_id"]].append(data)

    return processed_bugs, bug_ids


def process_fields(bug_list, relation_config):
    processed_bugs = {}
    bug_ids = {}

    for bug in bug_list:
        processed_bugs, bug_ids = process_individual_bug(
            bug, relation_config, processed_bugs, bug_ids
        )

    return processed_bugs, bug_ids


def add_links(kb_processed, dep_processed):
    result = {**kb_processed}

    for key in result:
        for bug_id in result[key]["core_bugs"]:
            for sub_key in LINK_FIELDS:
                if sub_key in result[key] and sub_key in dep_processed.get(bug_id, {}):
                    result[key][sub_key].extend(
                        x
                        for x in dep_processed[bug_id][sub_key]
                        if x not in result[key][sub_key]
                    )

    return result


def build_relations(bugs, relation_config):
    relations = {key: [] for key in relation_config.keys()}

    for bug_id, data in bugs.items():
        for field_key, items in data.items():
            fields = relation_config[field_key]["fields"]

            for row in items:
                relation_row = {fields[0]["name"]: bug_id, fields[1]["name"]: row}
                relations[field_key].append(relation_row)

    return relations


def merge_relations(main_dict, additional_dict):
    for key, value in additional_dict.items():
        if key in main_dict:
            main_dict[key].extend(value)
        else:
            main_dict[key] = value
    return main_dict


def split_bugs(dep_bugs, bug_ids):
    core_bugs, breakage_bugs = [], []

    for bug in dep_bugs:
        if bug["id"] in bug_ids.get("core", []):
            core_bugs.append(bug)
        elif bug["id"] in bug_ids.get("breakage", []):
            breakage_bugs.append(bug)

    return core_bugs, breakage_bugs


def extract_int_from_field(field):
    if field:
        if field.lower() in FIELD_MAP:
            return FIELD_MAP[field.lower()]

        match = re.search(r"\d+", field)
        if match:
            return int(match.group())

    return None


def update_relations(relations, client, bq_dataset_id):
    for key, value in relations.items():
        if value:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=[
                    bigquery.SchemaField(item["name"], item["type"], mode=item["mode"])
                    for item in RELATION_CONFIG[key]["fields"]
                ],
                write_disposition="WRITE_TRUNCATE",
            )

            relation_table = f"{bq_dataset_id}.{key}"
            job = client.load_table_from_json(
                value, relation_table, job_config=job_config
            )

            logging.info(f"Writing to `{relation_table}` table")

            try:
                job.result()
            except Exception as e:
                print(f"ERROR: {e}")
                if job.errors:
                    for error in job.errors:
                        logging.error(error)

            table = client.get_table(relation_table)
            logging.info(f"Loaded {table.num_rows} rows into {table}")


def update_bugs(bugs, client, bq_dataset_id):
    res = []
    for bug in bugs:
        bq_bug = {
            "number": bug["id"],
            "title": bug["summary"],
            "status": bug["status"],
            "resolution": bug["resolution"],
            "product": bug["product"],
            "component": bug["component"],
            "severity": extract_int_from_field(bug["severity"]),
            "priority": extract_int_from_field(bug["priority"]),
        }
        res.append(bq_bug)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("resolution", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("component", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("severity", "INTEGER"),
            bigquery.SchemaField("priority", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    bugs_table = f"{bq_dataset_id}.bugzilla_bugs"

    job = client.load_table_from_json(
        res,
        bugs_table,
        job_config=job_config,
    )

    logging.info("Writing to `bugzilla_bugs` table")

    try:
        job.result()
    except Exception as e:
        print(f"ERROR: {e}")
        if job.errors:
            for error in job.errors:
                logging.error(error)

    table = client.get_table(bugs_table)
    logging.info(f"Loaded {table.num_rows} rows into {table}")


@click.command()
@click.option("--bq_project_id", help="BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="BigQuery dataset id", required=True)
def main(bq_project_id, bq_dataset_id):
    client = bigquery.Client(project=bq_project_id)

    logging.info("Fetching KB bugs from bugzilla")
    bug_filters = {
        "product": "Web Compatibility",
        "component": "Knowledge Base",
    }
    kb_bugs = fetch_bugs(bug_filters)

    # Process KB bugs fields and get their dependant core/breakage bugs ids.
    kb_data, kb_ids = process_fields(kb_bugs, RELATION_CONFIG)

    dep_ids = set(item for sublist in kb_ids.values() for item in sublist)

    if not dep_ids:
        return

    logging.info("Fetching core bugs and breakage reports from bugzilla")
    dep_bugs = fetch_bugs({"id": ",".join(map(str, dep_ids))})
    kb_bugs.extend(dep_bugs)

    # Separate bugs into core and breakage.
    core_bugs, breakage_bugs = split_bugs(dep_bugs, kb_ids)

    # Process core bugs and update KB data with missing links from core bugs.
    if core_bugs:
        core_config = {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
        core_data, _ = process_fields(core_bugs, core_config)
        kb_data = add_links(kb_data, core_data)

    # Build relations for BQ.
    rels = build_relations(kb_data, RELATION_CONFIG)

    # Process breakage bugs for url patterns and merge them to the existing relations.
    if breakage_bugs:
        break_config = {"url_patterns": RELATION_CONFIG["url_patterns"]}
        break_data, _ = process_fields(breakage_bugs, break_config)
        add = build_relations(break_data, break_config)
        rels = merge_relations(rels, add)

    update_bugs(kb_bugs, client, bq_dataset_id)
    update_relations(rels, client, bq_dataset_id)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()

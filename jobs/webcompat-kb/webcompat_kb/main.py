import click
import logging
import requests
import re
import time

from google.cloud import bigquery
from datetime import datetime, timedelta, timezone

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
CORE_AS_KB_KEYWORD = "webcompat:platform-bug"

FILTER_CONFIG = {
    "wc": {
        "product": "Web Compatibility",
        "component": ["Knowledge Base", "Site Reports"],
        "f1": "OP",
        "f2": "bug_status",
        "o2": "changedafter",
        "v2": "2020-01-01",
        "j1": "OR",
        "f3": "resolution",
        "o3": "isempty",
        "f4": "CP",
    },
    "interventions": {
        "product": "Web Compatibility",
        "component": "Interventions",
    },
    "other": {
        "v1": "Web Compatibility",
        "f1": "product",
        "o1": "notequals",
        "keywords": "webcompat:",
        "keywords_type": "regexp",
    },
    "parity": {
        "f1": "OP",
        "f2": "bug_status",
        "o2": "changedafter",
        "v2": "2020-01-01",
        "j1": "OR",
        "f3": "resolution",
        "o3": "isempty",
        "f4": "CP",
        "f5": "keywords",
        "o5": "regexp",
        "v5": "parity-",
    },
}

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
}

LINK_FIELDS = ["other_browser_issues", "standards_issues", "standards_positions"]


def extract_int_from_field(field):
    if field:
        if field.lower() in FIELD_MAP:
            return FIELD_MAP[field.lower()]

        match = re.search(r"\d+", field)
        if match:
            return int(match.group())

    return None


def parse_string_to_json(input_string):
    if not input_string:
        return ""

    lines = input_string.splitlines()

    result_dict = {}

    for line in lines:
        if line:
            key_value = line.split(":", 1)
            if len(key_value) == 2:
                key, value = key_value
                if key in result_dict:
                    if isinstance(result_dict[key], list):
                        result_dict[key].append(value)
                    else:
                        result_dict[key] = [result_dict[key], value]
                else:
                    result_dict[key] = value
    if not result_dict:
        return ""

    return result_dict


def parse_datetime_str(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class BugzillaToBigQuery:
    def __init__(self, bq_project_id, bq_dataset_id, bugzilla_api_key):
        self.client = bigquery.Client(project=bq_project_id)
        self.bq_dataset_id = bq_dataset_id
        self.bugzilla_api_key = bugzilla_api_key
        self.bugs_fetch_completed = True
        self.history_fetch_completed = True
        self.start_time = None

    def fetch_bugs(self, params=None):
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
            "creation_time",
            "assigned_to",
            "keywords",
            "url",
            "cf_user_story",
            "cf_last_resolved",
            "last_change_time",
            "whiteboard",
            "creator",
        ]

        headers = {}
        if self.bugzilla_api_key:
            headers = {"X-Bugzilla-API-Key": self.bugzilla_api_key}

        url = f"{BUGZILLA_API}/bug"
        params["include_fields"] = ",".join(fields)

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            return result["bugs"]
        except Exception as e:
            logging.error(f"Error: {e}")
            self.bugs_fetch_completed = False
            return []

    def filter_core_as_kb_bugs(self, other, kb_bugs_ids, site_reports_ids):
        core_as_kb_bugs = []
        ckb_depends_on_ids = set()

        for bug in other:
            if CORE_AS_KB_KEYWORD in bug["keywords"]:
                # Check if the core bug already has a kb entry and skip if so
                if any(blocked_id in kb_bugs_ids for blocked_id in bug["blocks"]):
                    continue

                # Only store a breakage bug as it's the relation we care about
                bug["blocks"] = [
                    blocked_id
                    for blocked_id in bug["blocks"]
                    if blocked_id in site_reports_ids
                ]

                ckb_depends_on_ids.update(bug["depends_on"])
                core_as_kb_bugs.append(bug)

        return core_as_kb_bugs, ckb_depends_on_ids

    def fetch_all_bugs(self):
        fetched_bugs = {}

        for category, filter_config in FILTER_CONFIG.items():
            logging.info(f"Fetching {category.replace('_', ' ').title()} bugs")
            fetched_bugs[category] = self.fetch_bugs(filter_config)

        kb_bugs = []
        kb_depends_on_ids = set()
        site_reports_ids = set()
        kb_bugs_ids = set()

        for bug in fetched_bugs["wc"]:
            if bug["component"] == "Knowledge Base":
                kb_bugs.append(bug)
                kb_depends_on_ids.update(bug["depends_on"])
                kb_bugs_ids.add(bug["id"])

            elif bug["component"] == "Site Reports":
                site_reports_ids.add(bug["id"])

        core_as_kb_bugs, ckb_depends_on_ids = self.filter_core_as_kb_bugs(
            fetched_bugs["other"], kb_bugs_ids, site_reports_ids
        )

        kb_depends_on_ids.update(ckb_depends_on_ids)
        merged_kb_bugs = kb_bugs + core_as_kb_bugs

        logging.info("Fetching blocking bugs for KB bugs")

        core_bugs = self.fetch_bugs({"id": ",".join(map(str, kb_depends_on_ids))})

        all_bugs = (
            fetched_bugs["wc"]
            + fetched_bugs["interventions"]
            + fetched_bugs["other"]
            + fetched_bugs["parity"]
            + core_bugs
        )

        return all_bugs, merged_kb_bugs, core_bugs

    def split_bugs(self, dep_bugs, bug_ids):
        core_bugs, breakage_bugs = [], []

        for bug in dep_bugs:
            if bug["id"] in bug_ids.get("core", []):
                core_bugs.append(bug)
            elif bug["id"] in bug_ids.get("breakage", []):
                breakage_bugs.append(bug)

        return core_bugs, breakage_bugs

    def process_individual_bug(self, bug, relation_config, processed_bugs, bug_ids):
        bug_id = bug["id"]

        if bug_id not in processed_bugs:
            processed_bugs[bug_id] = {rel: [] for rel in relation_config.keys()}

        for rel, config in relation_config.items():
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

    def process_fields(self, bug_list, relation_config):
        processed_bugs = {}
        bug_ids = {}

        for bug in bug_list:
            processed_bugs, bug_ids = self.process_individual_bug(
                bug, relation_config, processed_bugs, bug_ids
            )

        return processed_bugs, bug_ids

    def add_links(self, kb_processed, dep_processed):
        result = {**kb_processed}

        for key in result:
            for bug_id in result[key]["core_bugs"]:
                for sub_key in LINK_FIELDS:
                    if sub_key in result[key] and sub_key in dep_processed.get(
                        bug_id, {}
                    ):
                        result[key][sub_key].extend(
                            x
                            for x in dep_processed[bug_id][sub_key]
                            if x not in result[key][sub_key]
                        )

        return result

    def build_relations(self, bugs, relation_config):
        relations = {key: [] for key in relation_config.keys()}

        for bug_id, data in bugs.items():
            for field_key, items in data.items():
                fields = relation_config[field_key]["fields"]

                for row in items:
                    relation_row = {fields[0]["name"]: bug_id, fields[1]["name"]: row}
                    relations[field_key].append(relation_row)

        return relations

    def convert_bug_data(self, bug):
        resolved = None
        if bug["status"] in ["RESOLVED", "VERIFIED"] and bug["cf_last_resolved"]:
            resolved = bug["cf_last_resolved"]

        user_story = parse_string_to_json(bug["cf_user_story"])

        assigned_to = (
            bug["assigned_to"] if bug["assigned_to"] != "nobody@mozilla.org" else None
        )

        return {
            "number": bug["id"],
            "title": bug["summary"],
            "status": bug["status"],
            "resolution": bug["resolution"],
            "product": bug["product"],
            "component": bug["component"],
            "severity": extract_int_from_field(bug["severity"]),
            "priority": extract_int_from_field(bug["priority"]),
            "creation_time": bug["creation_time"],
            "assigned_to": assigned_to,
            "keywords": bug["keywords"],
            "url": bug["url"],
            "user_story": user_story,
            "resolved_time": resolved,
            "whiteboard": bug["whiteboard"],
        }

    def update_bugs(self, bugs):
        res = [self.convert_bug_data(bug) for bug in bugs]

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
                bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("assigned_to", "STRING"),
                bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("user_story", "JSON"),
                bigquery.SchemaField("resolved_time", "TIMESTAMP"),
                bigquery.SchemaField("whiteboard", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        bugs_table = f"{self.bq_dataset_id}.bugzilla_bugs"

        job = self.client.load_table_from_json(
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

        table = self.client.get_table(bugs_table)
        logging.info(f"Loaded {table.num_rows} rows into {table}")

    def update_kb_ids(self, ids):
        res = [{"number": kb_id} for kb_id in ids]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        kb_bugs_table = f"{self.bq_dataset_id}.kb_bugs"

        job = self.client.load_table_from_json(
            res,
            kb_bugs_table,
            job_config=job_config,
        )

        logging.info("Writing to `kb_bugs` table")

        try:
            job.result()
        except Exception as e:
            print(f"ERROR: {e}")
            if job.errors:
                for error in job.errors:
                    logging.error(error)

        table = self.client.get_table(kb_bugs_table)
        logging.info(f"Loaded {table.num_rows} rows into {table}")

    def update_relations(self, relations):
        for key, value in relations.items():
            if value:
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=[
                        bigquery.SchemaField(
                            item["name"], item["type"], mode=item["mode"]
                        )
                        for item in RELATION_CONFIG[key]["fields"]
                    ],
                    write_disposition="WRITE_TRUNCATE",
                )

                relation_table = f"{self.bq_dataset_id}.{key}"
                job = self.client.load_table_from_json(
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

                table = self.client.get_table(relation_table)
                logging.info(f"Loaded {table.num_rows} rows into {table}")

    def get_last_import_datetime(self):
        query = f"""
                SELECT MAX(run_at) AS last_run_at
                FROM `{self.bq_dataset_id}.import_runs`
                WHERE is_history_fetch_completed = TRUE
            """
        res = self.client.query(query).result()
        row = list(res)[0]
        return row["last_run_at"]

    def fetch_history(self, bug_id, last_import_time=None):
        if not bug_id:
            raise ValueError("No bug id provided")

        params = {}

        if last_import_time:
            params["new_since"] = last_import_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        headers = {}

        if self.bugzilla_api_key:
            headers = {"X-Bugzilla-API-Key": self.bugzilla_api_key}

        url = f"{BUGZILLA_API}/bug/{bug_id}/history"

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()
            return result["bugs"][0]
        except Exception as e:
            logging.error(f"Error: {e}")
            self.history_fetch_completed = False
            return []

    def fetch_bugs_history(self, ids, last_import_time=None):
        history = []

        for bug_id in ids:
            try:
                logging.info(f"Fetching history from bugzilla for {bug_id}")
                bug_history = self.fetch_history(bug_id, last_import_time)
                history.append(bug_history)
                time.sleep(2)

            except Exception as e:
                logging.error(f"Failed to fetch history for bug {bug_id}: {e}")

        return history

    def update_history(self, records):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("who", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("change_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "changes",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("field_name", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("added", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("removed", "STRING", mode="REQUIRED"),
                    ],
                ),
            ],
            write_disposition="WRITE_APPEND",
        )

        history_table = f"{self.bq_dataset_id}.bugs_history"

        job = self.client.load_table_from_json(
            records,
            history_table,
            job_config=job_config,
        )

        logging.info("Writing to `bugs_history` table")

        try:
            job.result()
        except Exception as e:
            print(f"ERROR: {e}")
            if job.errors:
                for error in job.errors:
                    logging.error(error)

        table = self.client.get_table(history_table)
        logging.info(f"Loaded {len(records)} rows into {table}")

        return records

    def get_existing_history_records_by_ids(self, bug_ids):
        numbers_list = list(bug_ids)
        formatted_numbers = ", ".join(map(str, numbers_list))

        query = f"""
                    SELECT *
                    FROM `{self.bq_dataset_id}.bugs_history`
                    WHERE number IN ({formatted_numbers})
                """
        result = self.client.query(query).result()
        return result

    def extract_flattened_history(self, records, is_existing=False):
        history_set = set()
        for record in records:
            changes = record["changes"]
            change_time = record["change_time"]

            # Convert BQ timestamp to string to match bugzilla format
            if is_existing:
                change_time = change_time.strftime("%Y-%m-%dT%H:%M:%SZ")

            for change in changes:
                history_row = (
                    record["number"],
                    record["who"],
                    change_time,
                    change["field_name"],
                    change["added"],
                    change["removed"],
                )
                history_set.add(history_row)

        return history_set

    def unflatten_history(self, diff):
        changes = {}
        for item in diff:
            number, who, change_time, field_name, added, removed = item
            key = (number, who, change_time)

            if key not in changes:
                changes[key] = {
                    "number": number,
                    "who": who,
                    "change_time": change_time,
                    "changes": [],
                }

            changes[key]["changes"].append(
                {"field_name": field_name, "added": added, "removed": removed}
            )

        return list(changes.values())

    def filter_only_unsaved_changes(self, history_updates, bug_ids):
        existing_records = self.get_existing_history_records_by_ids(bug_ids)

        if not existing_records:
            return history_updates

        existing_history = self.extract_flattened_history(existing_records, True)
        new_history = self.extract_flattened_history(history_updates)

        diff = new_history - existing_history

        return self.unflatten_history(diff)

    def extract_relevant_fields(self, updated_history):
        result = []
        bug_ids = set()

        for bug_history in updated_history:
            filtered_changes = []

            for record in bug_history["history"]:
                relevant_changes = [
                    change
                    for change in record.get("changes", [])
                    if change.get("field_name") in ["keywords", "status"]
                ]

                if relevant_changes:
                    filtered_record = {
                        "number": bug_history["id"],
                        "who": record["who"],
                        "change_time": record["when"],
                        "changes": relevant_changes,
                    }
                    filtered_changes.append(filtered_record)
                    bug_ids.add(bug_history["id"])

            if filtered_changes:
                result.extend(filtered_changes)

        return result, bug_ids

    def filter_relevant_history(self, updated_history):
        only_unsaved_changes = []
        result, bug_ids = self.extract_relevant_fields(updated_history)

        if result:
            only_unsaved_changes = self.filter_only_unsaved_changes(result, bug_ids)

        return only_unsaved_changes

    def get_bugs_updated_since_last_import(self, all_bugs, last_import_time):
        return [
            bug["id"]
            for bug in all_bugs
            if parse_datetime_str(bug["last_change_time"]) > last_import_time
        ]

    def get_imported_ids(self):
        query = f"""
                SELECT number
                FROM `{self.bq_dataset_id}.bugzilla_bugs`
            """
        res = self.client.query(query).result()
        rows = list(res)

        imported_ids = {bug["number"] for bug in rows}

        return imported_ids

    def create_keyword_map(self, history):
        keyword_history = {}

        for record in history:
            bug_id = record["number"]
            timestamp = parse_datetime_str(record["change_time"])

            for change in record["changes"]:
                if "keywords" in change["field_name"]:
                    if bug_id not in keyword_history:
                        keyword_history[bug_id] = {"added": {}, "removed": {}}

                    keyword_records = keyword_history[bug_id]

                    for action in ["added", "removed"]:
                        keywords = change[action]
                        if keywords:
                            for keyword in keywords.split(", "):
                                if keyword not in keyword_records[action]:
                                    keyword_records[action][keyword] = []

                                keyword_records[action][keyword].append(timestamp)

        return keyword_history

    def is_removed_earliest(self, added_times, removed_times):
        events = [(at, "added") for at in added_times] + [
            (rt, "removed") for rt in removed_times
        ]
        events.sort()

        if not events:
            return False

        return events[0][1] == "removed"

    def get_missing_keywords(self, bug_id, current_keywords, keyword_history):
        missing_keywords = []

        # Check if keyword exists, but is not in "added" history
        for keyword in current_keywords:
            if bug_id not in keyword_history or keyword not in keyword_history[
                bug_id
            ].get("added", {}):
                if keyword not in missing_keywords:
                    missing_keywords.append(keyword)

        # Check for keywords that have "removed" record as the earliest
        # event in the sorted timeline
        if bug_id in keyword_history:
            for keyword, removed_times in (
                keyword_history[bug_id].get("removed", {}).items()
            ):
                added_times = keyword_history[bug_id].get("added", {}).get(keyword, [])

                removed_earliest = self.is_removed_earliest(added_times, removed_times)

                if removed_earliest and keyword not in missing_keywords:
                    missing_keywords.append(keyword)

        return missing_keywords

    def build_missing_history(self, bugs_without_history):
        result = []
        for bug, missing_keywords in bugs_without_history:
            record = {
                "number": bug["id"],
                "who": bug["creator"],
                "change_time": bug["creation_time"],
                "changes": [
                    {
                        "added": ", ".join(missing_keywords),
                        "field_name": "keywords",
                        "removed": "",
                    }
                ],
            }
            result.append(record)
        return result

    def create_synthetic_history(self, bugs, history):
        keyword_history = self.create_keyword_map(history)

        bugs_without_history = []

        for bug in bugs:
            bug_id = bug["id"]
            current_keywords = bug["keywords"]

            missing_keywords = self.get_missing_keywords(
                bug_id, current_keywords, keyword_history
            )

            if missing_keywords:
                bugs_without_history.append((bug, missing_keywords))

        return self.build_missing_history(bugs_without_history)

    def fetch_history_for_new_bugs(self, all_bugs):
        only_unsaved_changes = []

        existing_ids = self.get_imported_ids()
        all_ids = {bug["id"] for bug in all_bugs}
        new_ids = all_ids - existing_ids

        logging.info(f"Fetching new bugs history: {list(new_ids)}")

        new_bugs = [bug for bug in all_bugs if bug["id"] in new_ids]

        history, _ = self.extract_relevant_fields(self.fetch_bugs_history(new_ids))

        synthetic_history = self.create_synthetic_history(new_bugs, history)

        new_bugs_history = history + synthetic_history

        if new_bugs_history:
            only_unsaved_changes = self.filter_only_unsaved_changes(
                new_bugs_history, new_ids
            )

        return only_unsaved_changes, new_ids

    def fetch_history_updates(self, all_existing_bugs):
        last_import_time = self.get_last_import_datetime()

        if last_import_time:
            updated_bug_ids = self.get_bugs_updated_since_last_import(
                all_existing_bugs, last_import_time
            )

            logging.info(
                f"Fetching bugs updated after last import: {updated_bug_ids} at {last_import_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"  # noqa
            )

            if updated_bug_ids:
                bugs_history = self.fetch_bugs_history(
                    updated_bug_ids, last_import_time
                )

                return bugs_history

        return []

    def fetch_update_history(self, all_bugs):
        filtered_new_history, new_ids = self.fetch_history_for_new_bugs(all_bugs)

        existing_bugs = [bug for bug in all_bugs if bug["id"] not in new_ids]

        existing_bugs_history = self.fetch_history_updates(existing_bugs)

        if (
            filtered_new_history or existing_bugs_history
        ) and self.history_fetch_completed:
            filtered_existing = self.filter_relevant_history(existing_bugs_history)
            filtered_records = filtered_existing + filtered_new_history
            return self.update_history(filtered_records)

        return []

    def record_import_run(
        self, history_fetch_completed, count, history_count, last_change_time
    ):
        elapsed_time = time.monotonic() - self.start_time
        elapsed_time_delta = timedelta(seconds=elapsed_time)
        run_at = last_change_time - elapsed_time_delta
        formatted_time = run_at.strftime("%Y-%m-%dT%H:%M:%SZ")

        rows_to_insert = [
            {
                "run_at": formatted_time,
                "bugs_imported": count,
                "bugs_history_updated": history_count,
                "is_history_fetch_completed": history_fetch_completed,
            },
        ]
        bugbug_runs_table = f"{self.bq_dataset_id}.import_runs"
        errors = self.client.insert_rows_json(bugbug_runs_table, rows_to_insert)
        if errors:
            logging.error(errors)
        else:
            logging.info("Last import run recorded")

    def run(self):
        self.start_time = time.monotonic()

        all_bugs, kb_bugs, core_bugs = self.fetch_all_bugs()

        if not self.bugs_fetch_completed:
            logging.info("Fetching bugs from Bugzilla was not completed, aborting")
            return

        # Process KB bugs fields and get their dependant core/breakage bugs ids.
        kb_data, kb_dep_ids = self.process_fields(kb_bugs, RELATION_CONFIG)

        dep_ids = {item for sublist in kb_dep_ids.values() for item in sublist}

        # Check for missing bugs
        all_ids = {item["id"] for item in all_bugs}
        missing_ids = dep_ids - all_ids

        if missing_ids:
            logging.info(
                "Fetching missing core bugs and breakage reports from Bugzilla"
            )
            missing_bugs = self.fetch_bugs({"id": ",".join(map(str, missing_ids))})

            # Separate core bugs for updating relations.
            core_missing, _ = self.split_bugs(missing_bugs, kb_dep_ids)

            core_bugs.extend(core_missing)
            all_bugs.extend(missing_bugs)

        # Process core bugs and update KB data with missing links from core bugs.
        if core_bugs:
            core_config = {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
            core_data, _ = self.process_fields(core_bugs, core_config)
            kb_data = self.add_links(kb_data, core_data)

        # Build relations for BQ tables.
        rels = self.build_relations(kb_data, RELATION_CONFIG)

        kb_ids = list(kb_data.keys())

        all_bugs_unique = list({item["id"]: item for item in all_bugs}.values())

        history_changes = self.fetch_update_history(all_bugs_unique)

        self.update_bugs(all_bugs_unique)
        self.update_kb_ids(kb_ids)
        self.update_relations(rels)

        last_change_time_max = parse_datetime_str(
            max(all_bugs_unique, key=lambda x: x["last_change_time"])[
                "last_change_time"
            ]
        )

        self.record_import_run(
            self.history_fetch_completed,
            len(all_bugs_unique),
            len(history_changes),
            last_change_time_max,
        )


@click.command()
@click.option("--bq_project_id", help="BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="BigQuery dataset id", required=True)
@click.option(
    "--bugzilla_api_key", help="Bugzilla API key", required=False, default=None
)
def main(bq_project_id, bq_dataset_id, bugzilla_api_key):
    logging.getLogger().setLevel(logging.INFO)

    bz_bq = BugzillaToBigQuery(bq_project_id, bq_dataset_id, bugzilla_api_key)
    bz_bq.run()


if __name__ == "__main__":
    main()

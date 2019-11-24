# Adapted from https://gist.github.com/mreid-moz/33837c4c511b4fb7923f67a91232beb7
# Creates Amplitude-compatible event views, for more information see https://bugzilla.mozilla.org/show_bug.cgi?id=1574490
# Usage:
# CONFIGS_PATH = ~/mozilla/github/telemetry-streaming/configs
# for f (devtools_prerelease_schemas.json devtools_release_schemas.json fennec_ios_events_schemas.json fire_tv_events_schemas.json focus_android_events_schemas.json); do
#   python make_events_view.py $CONFIGS_PATH/$f
# done
### Use the "simple" path for rocket.
# python make_events_view.py $CONFIGS_PATH/rocket_android_events_schemas.json simplify

from collections import defaultdict
import json
import re
import sys

def esc(s):
	s = re.sub(r'[\']', '\\\'', s)
	s = re.sub(r'[\n]', '\\\\n', s)
	return s

def snake_case(n):
	return re.sub(r'-', '_', n)

# The following values will look up the corresponding top-level field of the event:
# timestamp, category, method, object, value
#
# Events can also optionally have an extra object that's a map of key-value pairs.
# To look up a key from extra named foo, use: extra.foo
#
# Finally, you can inject a literal value that's not looked up from the event.
# To inject the literal string "foo" as the value, use: literal.foo
def get_source_field_value(source_field):
	if source_field.startswith("literal."):
		return "'{}'".format(esc(source_field[8:]))
	elif source_field.startswith("extra."):
		return "`moz-fx-data-derived-datasets.udf.get_key`(event_map_values, '{}')".format(source_field[6:])
	return "event_{}".format(source_field)

input_filename = sys.argv[1]
with open(input_filename) as fin:
	schema = json.load(fin)

print("-- Source: {}".format(schema['source']))

simplify = False
if len(sys.argv) > 2 and sys.argv[2] == 'simplify':
	print("-- Attempting to simplify extra_props calculation: '{}'".format(sys.argv[2]))
	simplify = True

dryrun = False
if len(sys.argv) > 3 and sys.argv[3] == 'dryrun':
	print("-- Writing dry-run query: '{}'".format(sys.argv[3]))
	dryrun = True

# Establish a "base_events" CTE to reshape
# the various events sources to look the same.
base_events = {}

base_events["telemetry.events"] = """
SELECT
  *,
  timestamp AS submission_timestamp,
  event_string_value AS event_value,
  session_start_time AS created
FROM
  `moz-fx-data-shared-prod.telemetry.events`
"""

base_events["telemetry.mobile_event"] = """
SELECT
  *,
  DATE(submission_timestamp) AS submission_date,
  event.f0_ AS timestamp,
  event.f0_ AS event_timestamp,
  event.f1_ AS event_category,
  event.f2_ AS event_method,
  event.f3_ AS event_object,
  event.f4_ AS event_value,
  event.f5_ AS event_map_values,
  metadata.uri.app_version,
  osversion AS os_version,
  metadata.geo.country
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_event`
  CROSS JOIN UNNEST(events) AS event
"""

# mobile_event and focus_event have the same structure.
base_events["telemetry.focus_event"] = base_events["telemetry.mobile_event"].replace("mobile_event", "focus_event")

# This is the main query that will power the resulting view
query_template = """{}
WITH base_events AS (
{}
), all_events AS (
SELECT
    submission_date,
    submission_timestamp,
    client_id AS device_id,
    `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS session_id_offset,
    CONCAT(event_category, '.', event_method) AS event_type,
    {}
    event_timestamp AS timestamp,
    app_version,
    os AS platform,
    os AS os_name,
    os_version,
    country,
    (SELECT
      ARRAY_AGG(CONCAT('"',
        CAST(key AS STRING), '":"',
        CAST(value AS STRING), '"'))
     FROM
       UNNEST(event_map_values)) AS event_props_1,
    event_map_values,
    event_object,
    event_value,
    event_method,
    event_category,
    created
FROM
    base_events
{}
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", SPLIT(event_name, " - ")[OFFSET(1)], "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method),
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
      {}
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  {} AS user_props
FROM
  all_events_with_insert_ids
)

SELECT
  * EXCEPT (event_props_1, event_props_2, user_props),
  CONCAT('{{', ARRAY_TO_STRING((
   SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(ARRAY_CONCAT(event_props_1, event_props_2)) AS e
  ), ","), '}}') AS event_properties,
  CONCAT('{{', ARRAY_TO_STRING(user_props, ","), '}}') AS user_properties
FROM extra_props
{}"""

filter_snippet = ''
if dryrun:
	filter_snippet = """
WHERE submission_date = '2019-10-01' LIMIT 10
"""

# strip off the "_schemas.json", use the rest as the name of the view
view_name = '{}_v1'.format(input_filename[0:-13].split("/")[-1])
print("-- Dest: {}".format(view_name))

prefix = ''
if dryrun:
	prefix = '--'
create_snippet = """{}CREATE OR REPLACE VIEW
{}    `moz-fx-data-shared-prod.telemetry.{}` AS""".format(prefix, prefix, view_name)

source_table = 'telemetry.events'

criteria = [] # Use these to build up the WHERE clause

filters = schema.get("filters", {})
docTypes = filters.get("docType", [])
if "main" in docTypes or "event" in docTypes:
	# look in the events table
	criteria.append("doc_type IN ('{}')".format("', '".join(docTypes)))
elif len(docTypes) > 1:
	print("-- TODO: Why are there so many doctypes: {}".format(', '.join(docTypes)))
else:
	source_table = 'telemetry.{}'.format(docTypes[0])

source_table = snake_case(source_table)
print("-- Source table: {}".format(source_table))

# Limit to specific experiment ids
if "experimentId" in filters:
	experiment_pieces = [ "`moz-fx-data-derived-datasets.udf.get_key`(experiments, '{}') IS NOT NULL".format(e) for e in filters["experimentId"]]
	criteria.append("({})".format(" OR ".join(experiment_pieces)))

# Build WHERE clause from criteria
where = ''
if len(criteria) > 0:
	where = 'WHERE {}'.format(" AND ".join(criteria))

event_name_mappings = []
amplitude_fields = defaultdict(list)
user_fields = defaultdict(list)
# figure out event naming mappings. Full name is `group - name`
for group in schema.get("eventGroups", []):
	groupName = group.get("eventGroupName")
	for event in group.get("events", []):
		eventName = event.get("name")
		matchers = []
		props = event.get("schema", {}).get("properties", {})
		for prop in props.keys():
			if prop == "timestamp":
				continue
			enum_val = props.get(prop).get("enum")
			# Null can be one of the allowed values, handle it separately.
			enum_val_without_nulls = [ev for ev in enum_val if ev is not None]
			enum_null = ''
			if None in enum_val:
				enum_null = 'OR event_{} IS NULL'.format(prop)
			matchers.append("(event_{} IN ('{}') {})".format(prop, "', '".join(enum_val_without_nulls), enum_null))
		all_matchers = " AND ".join(matchers)
		full_event_name = "{} - {}".format(groupName, eventName)
		event_name_mappings.append("WHEN {} THEN '{}'".format(all_matchers, esc(full_event_name)))

		# Collect amplitude and user properties
		props = event.get("amplitudeProperties", {})
		for f, v in props.items():
			amplitude_fields[f].append([esc(full_event_name), get_source_field_value(v)])

		props = event.get("userProperties", {})
		for f, v in props.items():
			user_fields[f].append("WHEN event_name = '{}' THEN {}".format(esc(full_event_name), get_source_field_value(v)))

event_name = ''
if len(event_name_mappings) > 0:
	event_name = "CASE\n        {}\n    END AS event_name,".format(" \n        ".join(event_name_mappings))


# Figure out the amplitudeProperties mappings
# https://github.com/mozilla/telemetry-streaming/tree/master/docs/amplitude#event
# Note: It is unclear to me whether "event_props_1" and "event_props_2" are expected
#       to contain the same things, so there is a maybe-extraneous DISTINCT step to
#       combine them. Maybe we don't even need event_props_1.
amplitude_properties = ''

# Collect the set of fields -> case when name = foo then val1 when name = bar then val2 end
if amplitude_fields:
	#amplitude_properties = 'UNION ALL\n          '
	amplitude_props_components = []
	for f, criteria in amplitude_fields.items():
		distinct_criteria = set([c[1] for c in criteria])
		if simplify and len(distinct_criteria) == 1:
			# Optimization: criteria are all the same for this field. Simplify it.
			# This avoids the "query is too complex" error for rocket_android_events.
			amplitude_props_components.append("SELECT '{}' AS key, {} AS value".format(esc(f), criteria[0][1]))
		elif simplify:
			# The criteria differ by event name, leave it alone.
			print("-- Tried to simplify, but found {} distinct criteria".format(len(distinct_criteria)))
		else:
			formatted_criteria = [ "WHEN event_name = '{}' THEN {}".format(c[0], c[1]) for c in criteria ]
			amplitude_props_components.append("SELECT '{}' AS key, CASE\n          {}\n          END AS value".format(esc(f), "\n          ".join(formatted_criteria)))
	amplitude_properties += "\n      UNION ALL ".join(amplitude_props_components)

user_properties = 'ARRAY<STRING>[]'
if user_fields:
	user_properties = ''
	user_props_components = []
	for f, criteria in user_fields.items():
		user_props_components.append("SELECT '{}' AS key, CASE\n          {}\n          END AS value".format(esc(f), "\n          ".join(criteria)))
	user_properties += "\n      UNION ALL ".join(user_props_components)
	user_properties = """(SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
  {}
))""".format(user_properties)

# Final query
final_query = query_template.format(
	create_snippet,
	base_events[source_table],
	event_name,
	where,
	amplitude_properties,
	user_properties,
	filter_snippet)

out_file = view_name.split("/")[-1] + "/view.sql"

print("-- writing view to {}".format(out_file))

with open(out_file, "w") as f:
	f.write(final_query)

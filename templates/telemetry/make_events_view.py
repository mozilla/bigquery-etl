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
  session_start_time AS created,
  NULL AS city
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
  metadata.geo.country,
  metadata.geo.city,
  metadata.uri.app_name
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_event`
  CROSS JOIN UNNEST(events) AS event
"""

# mobile_event and focus_event have the same structure.
base_events["telemetry.focus_event"] = base_events["telemetry.mobile_event"].replace("mobile_event", "focus_event")

settings_columns = {}
settings_columns["telemetry.mobile_event"] = """settings"""
settings_columns["telemetry.focus_event"] = settings_columns["telemetry.mobile_event"]
settings_columns["telemetry.events"] = "NULL AS settings" # TODO: some environment fields are needed here, see: https://github.com/mozilla/telemetry-streaming/blob/b85cdffc72a6f9ab224f9eececc38dfa09d98b8c/src/main/scala/com/mozilla/telemetry/pings/Ping.scala#L434-L447

if input_filename.split('/')[-1] == "fire_tv_events_schemas.json":
	event_name_expr = "SUBSTR(event_name, 23)" # fire_tv event names contain whitespaces
else:
	event_name_expr = """SPLIT(event_name, " - ")[OFFSET(1)]"""

# This is the main query that will power the resulting view
query_template = """{}
WITH base_events AS (
{}
), all_events AS (
SELECT
    submission_date,
    submission_timestamp,
    client_id AS device_id,
    (created + COALESCE(SAFE_CAST(`moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS INT64), 0)) AS session_id,
    {}
    event_timestamp AS timestamp,
    (event_timestamp + created) AS time,
    app_version,
    os AS platform,
    os AS os_name,
    os_version,
    country,
    city,
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
    created,
    {}
FROM
    base_events
{}
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", {}, "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id,
  event_name AS event_type
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
  * EXCEPT (event_props_1, event_props_2, user_props, settings),
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
appNames = filters.get("appName", [])
if len(appNames)>0:
	criteria.append("app_name IN ('{}')".format("', '".join(appNames)))
osNames = filters.get("os", [])
if len(osNames)>0:
	criteria.append("os IN ('{}')".format("', '".join(osNames)))

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

user_properties_ping_overrides = {}
user_properties_ping_overrides['fennec_ios_events_schemas.json'] = [ # setting_key, alias, as_bool; see https://github.com/mozilla/telemetry-streaming/blob/b85cdffc72a6f9ab224f9eececc38dfa09d98b8c/src/main/scala/com/mozilla/telemetry/pings/MobileEvent.scala#L41-L59
	('defaultSearchEngine', 'pref_default_search_engine', False),
	('prefKeyAutomaticSliderValue', 'pref_automatic_slider_value', False),
	('prefKeyAutomaticSwitchOnOff', 'pref_automatic_switch_on_off', False),
	('prefKeyThemeName', 'pref_theme_name', False),
	('profile.ASBookmarkHighlightsVisible', 'pref_activity_stream_bookmark_highlights_visible', True),
	('profile.ASPocketStoriesVisible', 'pref_activity_stream_pocket_stories_visible', True),
	('profile.ASRecentHighlightsVisible', 'pref_activity_stream_recent_highlights_visible', True),
	('profile.blockPopups', 'pref_block_popups', True),
	('profile.prefkey.trackingprotection.enabled', 'pref_tracking_protection_enabled', False),
	('profile.prefkey.trackingprotection.normalbrowsing', 'pref_tracking_protection_normal_browsing', False),
	('profile.prefkey.trackingprotection.privatebrowsing', 'pref_tracking_protection_private_browsing', False),
	('profile.prefkey.trackingprotection.strength', 'pref_tracking_protection_strength', False),
	('profile.saveLogins', 'pref_save_logins', True),
	('profile.settings.closePrivateTabs', 'pref_settings_close_private_tabs', True),
	('profile.show-translation', 'pref_show_translation', False),
	('profile.showClipboardBar', 'pref_show_clipboard_bar', True),
	('windowHeight', 'pref_window_height', False),
	('windowWidth', 'pref_window_width', False),
]
user_properties_ping_overrides['focus_android_events_schemas.json'] = [ # setting_key, alias, as_bool; see https://github.com/mozilla/telemetry-streaming/blob/b85cdffc72a6f9ab224f9eececc38dfa09d98b8c/src/main/scala/com/mozilla/telemetry/pings/FocusEvent.scala#L34-L47
	('pref_privacy_block_ads', 'pref_privacy_block_ads', True),
	('pref_locale', 'pref_locale', False),
	('pref_privacy_block_social', 'pref_privacy_block_social', True),
	('pref_secure', 'pref_secure', True),
	('pref_privacy_block_analytics', 'pref_privacy_block_analytics', True),
	('pref_search_engine', 'pref_search_engine', False),
	('pref_privacy_block_other', 'pref_privacy_block_other', True),
	('pref_default_browser', 'pref_default_browser', True),
	('pref_performance_block_webfonts', 'pref_performance_block_webfonts', True),
	('pref_performance_block_images', 'pref_performance_block_images', True),
	('pref_autocomplete_installed', 'pref_autocomplete_installed', True),
	('pref_autocomplete_custom', 'pref_autocomplete_custom', True),
	('pref_key_tips', 'pref_key_tips', True)
]
user_properties_ping_overrides['fire_tv_events_schemas.json'] = [ # setting_key, alias, as_bool; see https://github.com/mozilla/telemetry-streaming/blob/b85cdffc72a6f9ab224f9eececc38dfa09d98b8c/src/main/scala/com/mozilla/telemetry/pings/FireTvEventPing.scala#L40-L45
	('tracking_protection_enabled', 'tracking_protection_enabled', True),
	('total_home_tile_count', 'total_home_tile_count', False),
	('custom_home_tile_count', 'custom_home_tile_count', False),
	('remote_control_name', 'remote_control_name', False),
	('app_id', 'app_id', False)
]
user_properties_ping_overrides['rocket_android_events_schemas.json'] = [ # setting_key, alias, as_bool; see https://github.com/mozilla/telemetry-streaming/blob/b85cdffc72a6f9ab224f9eececc38dfa09d98b8c/src/main/scala/com/mozilla/telemetry/pings/RocketEvent.scala#L34-L41
	('pref_search_engine', 'pref_search_engine', False),
    ('pref_privacy_turbo_mode', 'pref_privacy_turbo_mode', True),
    ('pref_performance_block_images', 'pref_performance_block_images', True),
    ('pref_default_browser', 'pref_default_browser', True),
    ('pref_save_downloads_to', 'pref_save_downloads_to', False),
    ('pref_webview_version', 'pref_webview_version', False),
    ('install_referrer', 'install_referrer', False),
    ('experiment_name', 'experiment_name', False),
    ('experiment_bucket', 'experiment_bucket', False),
    ('pref_locale', 'pref_locale', False),
    ('pref_key_s_tracker_token', 'pref_key_s_tracker_token', False)
]

user_properties_ping_override = user_properties_ping_overrides.get(input_filename.split('/')[-1])
if (user_properties_ping_override):
	user_properties_override_string = """(SELECT ARRAY_AGG(\n    CASE\n"""
	for setting_key, alias, as_bool in user_properties_ping_override:
		if as_bool:
			value_string = """'":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING)"""
		else:
			value_string = """'":"', CAST(value AS STRING), '"'"""
		user_properties_override_string += f"""        WHEN key='{setting_key}' THEN CONCAT('"', '{alias}', {value_string})\n"""
	user_properties_override_string += """    END\n    IGNORE NULLS)\n  FROM\n    UNNEST(SETTINGS)\n  )"""

	user_properties = """ARRAY_CONCAT({},\n    {})""".format(user_properties, user_properties_override_string)

# Final query
final_query = query_template.format(
	create_snippet,
	base_events[source_table],
	event_name,
	settings_columns[source_table],
	where,
	event_name_expr,
	amplitude_properties,
	user_properties,
	filter_snippet)

out_file = view_name.split("/")[-1] + "/view.sql"

print("-- writing view to {}".format(out_file))

with open(out_file, "w") as f:
	f.write(final_query)

CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.rocket_android_events_v1` AS
WITH base_events AS (

SELECT
  *,
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
  `moz-fx-data-shared-prod.telemetry.focus_event`
  CROSS JOIN UNNEST(events) AS event

), all_events AS (
SELECT
    submission_timestamp,
    client_id AS device_id,
    (created + COALESCE(SAFE_CAST(`moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'session_id') AS INT64), 0)) AS session_id,
    CASE
        WHEN (event_category IN ('action') ) AND (event_method IN ('launch') ) AND (event_object IN ('app') ) AND (event_value IN ('launcher') ) THEN 'Rocket -  App is launched by Launcher' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('launch') ) AND (event_object IN ('app') ) AND (event_value IN ('shortcut') ) THEN 'Rocket -  App is launched by Shortcut' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('launch') ) AND (event_object IN ('app') ) AND (event_value IN ('private_mode') ) THEN 'Rocket -  App is launched from Private Shortcut' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('private_shortcut') ) AND event_value IS NULL THEN 'Rocket -  Show private shortcut prompt' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('private_shortcut') ) AND (event_value IN ('positive', 'negative', 'dismiss') ) THEN 'Rocket -  Click private shortcut prompt' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('setting') ) AND (event_value IN ('pref_private_shortcut') ) THEN 'Rocket -  Users clicked on a Setting' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('private_mode') ) AND (event_value IN ('exit') ) THEN 'Rocket -  Exit private mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('pin_shortcut') ) AND (event_object IN ('private_shortcut') ) AND event_value IS NULL THEN 'Rocket -  Private shortcut created' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('kill') ) AND (event_object IN ('app') ) AND event_value IS NULL THEN 'Rocket -  Kill app' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('launch') ) AND (event_object IN ('app') ) AND (event_value IN ('external_app') ) THEN 'Rocket -  App is launched by external app' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('setting') ) AND (event_value IN ('learn_more') ) THEN 'Rocket -  Users clicked on the Learn More link in Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('pref_locale') ) THEN 'Rocket -  Users change Locale in Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('foreground') ) AND (event_object IN ('app') ) AND event_value IS NULL THEN 'Rocket -  Session starts' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('background') ) AND (event_object IN ('app') ) AND event_value IS NULL THEN 'Rocket -  Session ends' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('long_press') ) AND (event_object IN ('browser') ) AND event_value IS NULL THEN 'Rocket -  Long Press ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('cancel') ) AND (event_object IN ('browser_contextmenu') ) AND event_value IS NULL THEN 'Rocket -  Cancel ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('link') ) THEN 'Rocket -  Share link via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('image') ) THEN 'Rocket -  Share image via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('save') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('image') ) THEN 'Rocket -  Save image via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('copy') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('link') ) THEN 'Rocket -  Copy link via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('copy') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('image') ) THEN 'Rocket -  Copy image via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('link') ) THEN 'Rocket -  Add link via ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('permission') ) AND (event_object IN ('browser') ) AND (event_value IN ('geolocation') ) THEN 'Rocket -  Permission-Geolocation' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('permission') ) AND (event_object IN ('browser') ) AND (event_value IN ('file') ) THEN 'Rocket -  Permission-File' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('permission') ) AND (event_object IN ('browser') ) AND (event_value IN ('audio', 'video', 'eme', 'midi') ) THEN 'Rocket -  Permission-Media' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('fullscreen') ) AND (event_object IN ('browser') ) AND (event_value IN ('enter') ) THEN 'Rocket -  Enter full screen' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('fullscreen') ) AND (event_object IN ('browser') ) AND (event_value IN ('exit') ) THEN 'Rocket -  Exit full screen' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('menu') ) AND (event_value IN ('home') ) THEN 'Rocket -  Show Menu from Home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tab_tray') ) AND (event_value IN ('home') ) THEN 'Rocket -  Show TabTray from Home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tab_tray') ) AND (event_value IN ('toolbar') ) THEN 'Rocket -  Show TabTray from Toolbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('menu') ) AND (event_value IN ('toolbar') ) THEN 'Rocket -  Show Menu from Toolbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('download') ) THEN 'Rocket -  Click Menu - Downloads' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('history') ) THEN 'Rocket -  Click Menu - History' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('capture') ) THEN 'Rocket -  Click Menu - MyShots' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('panel') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Click Panel - Bookmarks' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('panel') ) AND (event_value IN ('download') ) THEN 'Rocket -  Click Panel - Downloads' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('panel') ) AND (event_value IN ('history') ) THEN 'Rocket -  Click Panel - History' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('panel') ) AND (event_value IN ('capture') ) THEN 'Rocket -  Click Panel - MyShots' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('menu') ) AND (event_value IN ('turbo') ) THEN 'Rocket -  Click Menu - TurboMode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('menu') ) AND (event_value IN ('night_mode') ) THEN 'Rocket -  Click Menu - Night Mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('menu') ) AND (event_value IN ('block_image') ) THEN 'Rocket -  Click Menu - Block Images' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('clear_cache') ) THEN 'Rocket -  Click Menu - Clear cache' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('settings') ) THEN 'Rocket -  Click Menu - Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('exit') ) THEN 'Rocket -  Click Menu - Exit' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Click Menu - Bookmarks' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('theme') ) THEN 'Rocket -  Click Menu - Theme' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('add_topsite') ) THEN 'Rocket -  Click Menu - Add Topsite' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('menu') ) AND (event_value IN ('vertical') ) THEN 'Rocket -  Click Menu - Vertical Toggle' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('forward') ) THEN 'Rocket -  Click Toolbar - Forward' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('reload') ) THEN 'Rocket -  Click Toolbar - Reload' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('link') ) THEN 'Rocket -  Click Toolbar - Share Link' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Click Toolbar - Add bookmark' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('pin_shortcut') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('link') ) THEN 'Rocket -  Click Menu - Pin shortcut' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('pin_shortcut') ) AND (event_object IN ('menu') ) AND (event_value IN ('link') ) THEN 'Rocket -  Click Toolbar - Pin shortcut' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('capture') ) THEN 'Rocket -  Click Toolbar - Take Screenshot' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('home') ) AND (event_value IN ('link') ) THEN 'Rocket -  Click Top Site' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('home') ) AND (event_value IN ('link') ) THEN 'Rocket -  Remove Top Site' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('home') ) AND (event_value IN ('link') ) THEN 'Rocket -  Add Topsite' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('snackbar') ) AND (event_value IN ('add_topsite') ) THEN 'Rocket -  Click Add Topsite Snackbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('panel') ) AND (event_value IN ('add_topsite') ) THEN 'Rocket -  Select to Add Topsite' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('theme') ) THEN 'Rocket -  Show Theme Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('theme') ) THEN 'Rocket -  Click Theme Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('go_set_default') ) THEN 'Rocket -  Show Go-Set-Default Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('go_set_default') ) THEN 'Rocket -  Click Go-Set-Default Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('tab') ) AND (event_value IN ('home') ) THEN 'Rocket -  Search in Home and add a tab' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('link') ) THEN 'Rocket -  Enter an url in SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('select_query') ) AND (event_object IN ('search_bar') ) AND event_value IS NULL THEN 'Rocket -  Use SearchSuggestion SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_query') ) AND (event_object IN ('search_bar') ) AND event_value IS NULL THEN 'Rocket -  Search with text in SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('private_mode') ) AND (event_value IN ('enter', 'exit') ) THEN 'Rocket -  Toggle Private Mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('long_press') ) AND (event_object IN ('search_suggestion') ) AND event_value IS NULL THEN 'Rocket -  Long click on Search Suggestion' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('clear') ) AND (event_object IN ('search_bar') ) AND event_value IS NULL THEN 'Rocket -  Clear SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('cancel') ) AND (event_object IN ('search_bar') ) AND event_value IS NULL THEN 'Rocket -  Dismiss SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('search_box') ) THEN 'Rocket -  Show SearchBar from Home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('mini_urlbar') ) THEN 'Rocket -  Show SearchBar by clicking MINI_URLBAR' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('search_btn') ) THEN 'Rocket -  Show SearchBar by clicking SEARCH_BUTTON' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('tab') ) AND (event_value IN ('toolbar') ) THEN 'Rocket -  Add Tab from Toolbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('tab') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Add Tab from TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('private_mode') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Enter Private Mode from TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('tab') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Switch Tab From TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('tab') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Remove Tab From TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('swipe') ) AND (event_object IN ('tab') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Swipe Tab From TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('close_all') ) AND (event_value IN ('tab_tray') ) THEN 'Rocket -  Close all From TabTray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('panel') ) AND (event_value IN ('file') ) THEN 'Rocket -  Remove Download File' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('delete') ) AND (event_object IN ('panel') ) AND (event_value IN ('file') ) THEN 'Rocket -  Delete Download File' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('panel') ) AND (event_value IN ('file') ) THEN 'Rocket -  Open Download File via snackbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('menu') ) AND (event_value IN ('download') ) THEN 'Rocket -  Show File ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('panel') ) AND (event_value IN ('link') ) THEN 'Rocket -  History Open Link' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('panel') ) AND (event_value IN ('link') ) THEN 'Rocket -  History Remove Link' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('panel') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Bookmark Remove Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('edit') ) AND (event_object IN ('panel') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Bookmark Edit Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('panel') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Bookmark Open Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('menu') ) AND (event_value IN ('history') ) THEN 'Rocket -  Show History ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('menu') ) AND (event_value IN ('bookmark') ) THEN 'Rocket -  Show Bookmark ContextMenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('clear') ) AND (event_object IN ('panel') ) AND (event_value IN ('history') ) THEN 'Rocket -  Clear History' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('panel') ) AND (event_value IN ('capture') ) THEN 'Rocket -  Open Capture Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('capture') ) AND (event_value IN ('link') ) THEN 'Rocket -  Open Capture Link' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('edit') ) AND (event_object IN ('capture') ) AND (event_value IN ('image') ) THEN 'Rocket -  Edit Capture Image' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('capture') ) AND (event_value IN ('image') ) THEN 'Rocket -  Share Capture Image' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('capture') ) AND (event_value IN ('info') ) THEN 'Rocket -  Show Capture Info' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('delete') ) AND (event_object IN ('capture') ) AND (event_value IN ('image') ) THEN 'Rocket -  Delete Capture Image' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('feedback') ) AND (event_value IN ('dismiss', 'positive', 'negative') OR event_value IS NULL) THEN 'Rocket -  click Rate App' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('feedback') ) AND event_value IS NULL THEN 'Rocket -  Show Rate App' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('default_browser') ) AND event_value IS NULL THEN 'Rocket -  Default Browser Notification shown' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('get') ) AND (event_object IN ('firstrun_push') ) AND event_value IS NULL THEN 'Rocket -  Receive Firstrun Push config' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('firstrun_push') ) AND event_value IS NULL THEN 'Rocket -  Firstrun Push notification shown' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('default_browser') ) AND event_value IS NULL THEN 'Rocket -  Default Browser Notification Clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('promote_share') ) AND (event_value IN ('dismiss', 'share') ) THEN 'Rocket -  Promote Share Dialog Clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('promote_share') ) AND event_value IS NULL THEN 'Rocket -  Promote Share Dialog shown' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('themetoy') ) AND event_value IS NULL THEN 'Rocket -  Change Theme To' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('reset') ) AND (event_object IN ('themetoy') ) AND event_value IS NULL THEN 'Rocket -  Reset Theme To' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('clear') ) AND (event_object IN ('private_mode') ) AND event_value IS NULL THEN 'Rocket -  Erase Private Mode Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('home') ) AND event_value IS NULL THEN 'Rocket -  Home Impression' 
        WHEN (event_category IN ('Downloads') ) AND (event_method IN ('long_press') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('download') ) THEN 'Rocket -  Long Press Toolbar Download Indicator' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('find_in_page') ) AND (event_value IN ('next') ) THEN 'Rocket -  Click FindInPage Next' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('find_in_page') ) AND (event_value IN ('previous') ) THEN 'Rocket -  Click FindInPage Previous' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('find_in_page') ) THEN 'Rocket -  Click Menu FindInPage' 
        WHEN (event_category IN ('search') ) AND (event_method IN ('click') ) AND (event_object IN ('quicksearch') ) AND event_value IS NULL THEN 'Rocket -  Click Quick Search' 
        WHEN (event_category IN ('enter landscape mode') ) AND (event_method IN ('change') ) AND (event_object IN ('landscape_mode') ) AND (event_value IN ('enter') ) THEN 'Rocket -  Enter Landscape Mode' 
        WHEN (event_category IN ('enter landscape mode') ) AND (event_method IN ('change') ) AND (event_object IN ('landscape_mode') ) AND (event_value IN ('exit') ) THEN 'Rocket -  Exit Landscape Mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('update_msg') ) AND event_value IS NULL THEN 'Rocket -  Show in-app update intro dialog' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('update') ) AND event_value IS NULL THEN 'Rocket -  Show google play\'s in-app update dialog' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('update_msg') ) AND (event_value IN ('positive', 'negative') ) THEN 'Rocket -  Click in-app update intro dialog' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('update') ) AND (event_value IN ('positive', 'negative') ) THEN 'Rocket -  Click google play\'s in-app update dialog' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('update') ) AND (event_value IN ('downloaded') ) THEN 'Rocket -  Show in-app update install prompt' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('update') ) AND (event_value IN ('apply') ) THEN 'Rocket -  Click in-app update install prompt' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('firstrun') ) THEN 'Rocket -  Show Firstrun Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('firstrun') ) THEN 'Rocket -  Click Firstrun Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('logoman') ) AND event_value IS NULL THEN 'Rocket -  Show Logoman' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('logoman') ) AND event_value IS NULL THEN 'Rocket -  Click Logoman' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('swipe') ) AND (event_object IN ('logoman') ) AND event_value IS NULL THEN 'Rocket -  Swipe Logoman' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('get') ) AND (event_object IN ('notification') ) AND event_value IS NULL THEN 'Rocket -  Get Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('notification') ) AND event_value IS NULL THEN 'Rocket -  Show Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('swipe') ) AND (event_object IN ('notification') ) AND (event_value IN ('dismiss') ) THEN 'Rocket -  Dismiss Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('swipe') ) AND (event_object IN ('firstrun_push') ) AND (event_value IN ('dismiss') ) THEN 'Rocket -  Dismiss D1 Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('notification') ) AND event_value IS NULL THEN 'Rocket -  Open Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('firstrun_push') ) AND event_value IS NULL THEN 'Rocket -  Open D1 Notification' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('in_app_message') ) THEN 'Rocket -  Show In-App Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('in_app_message') ) THEN 'Rocket -  Click In-App Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('content_hub') ) AND event_value IS NULL THEN 'Rocket -  Click Content Hub' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('reload') ) AND (event_object IN ('content_home') ) AND event_value IS NULL THEN 'Rocket -  Reload Content Home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('content_home') ) THEN 'Rocket -  Show Content Home Search Bar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('select_query') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('content_home') ) THEN 'Rocket -  Select Query Content Home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('category') ) AND event_value IS NULL THEN 'Rocket -  Open Category' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('detail_page') ) AND (event_value IN ('more') ) THEN 'Rocket -  Open Detail Page More' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start') ) AND (event_object IN ('content_tab') ) AND event_value IS NULL THEN 'Rocket -  Start Content Tab' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('content_tab') ) AND event_value IS NULL THEN 'Rocket -  End Content Tab' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('impression') ) AND (event_object IN ('category') ) AND event_value IS NULL THEN 'Rocket -  Category Impression' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start') ) AND (event_object IN ('process') ) AND (event_value IN ('vertical') ) THEN 'Rocket -  Start Vertical Process' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('process') ) AND (event_value IN ('vertical') ) THEN 'Rocket -  End Vertical Process' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('vertical') ) THEN 'Rocket -  Show Vertical Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start') ) AND (event_object IN ('process') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Start Tab Swipe Process' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('process') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  End Tab Swipe Process' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start') ) AND (event_object IN ('tab_swipe') ) AND event_value IS NULL THEN 'Rocket -  Start Tab Swipe' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('tab_swipe') ) AND event_value IS NULL THEN 'Rocket -  End Tab Swipe' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('share') ) THEN 'Rocket -  Click Toolbar - Share' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('OPEN_IN_BROWSER') ) THEN 'Rocket -  Click Toolbar - Open in browser' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('reload') ) THEN 'Rocket -  Click Toolbar - Reload' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('back') ) THEN 'Rocket -  Click Toolbar - Back' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('toolbar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Click Toolbar - Tab Swipe' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('drawer') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Show Tab Swipe Drawer' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('drawer') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Click Tab Swipe Drawer' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('add') ) AND (event_object IN ('tab') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Add Tab Swipe Tab' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Change Tab Swipe Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Show SearchBar from Tab Swipe' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show_keyboard') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Show Keyboard from Tab Swipe SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start_typing') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Start Typing from Tab Swipe SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_query') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Search with text in SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('select_query') ) AND (event_object IN ('search_bar') ) AND (event_value IN ('tab_swipe') ) THEN 'Rocket -  Use SearchSuggestion in Tab Swipe SearchBar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('detail_page') ) THEN 'Rocket -  Change Travel Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('personalization') ) THEN 'Rocket -  Change Personalization in Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('language') ) THEN 'Rocket -  Change Language in Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('setting') ) AND (event_value IN ('lifestyle') ) THEN 'Rocket -  Click News Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('category') ) THEN 'Rocket -  Change Category in Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('lifestyle') ) THEN 'Rocket -  Change News Settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('pin') ) AND (event_object IN ('home') ) AND (event_value IN ('link') ) THEN 'Rocket -  Pin Topsite' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('challenge_page') ) AND (event_value IN ('join') ) THEN 'Rocket -  Click Challenge Page Join' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('task') ) THEN 'Rocket -  Show Task Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('task') ) AND event_value IS NULL THEN 'Rocket -  End Task' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('challenge_complete') ) THEN 'Rocket -  Show Challenge Complete Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('challenge_complete') ) THEN 'Rocket -  Click Challenge Complete Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('sign_in') ) AND (event_object IN ('account') ) AND event_value IS NULL THEN 'Rocket -  Account Sign In' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('redeem_page') ) AND event_value IS NULL THEN 'Rocket -  Show Redeem Page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('copy') ) AND (event_object IN ('redeem_page') ) AND (event_value IN ('code') ) THEN 'Rocket -  Copy Code on Redeem Page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('redeem_page') ) AND (event_value IN ('use') ) THEN 'Rocket -  Click Redeem on Redeem Page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('profile') ) AND (event_value IN ('reward') ) THEN 'Rocket -  Click Reward Profile' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('content_home') ) AND (event_value IN ('item') ) THEN 'Rocket -  Click Content Home Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('challenge_page') ) AND (event_value IN ('login') ) THEN 'Rocket -  Click Chellenge Page Login' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('long_press') ) AND (event_object IN ('content_home') ) AND (event_value IN ('item') ) THEN 'Rocket -  Long Press Content Home Item' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('content_home') ) AND (event_value IN ('contextmenu') ) THEN 'Rocket -  Click Content Home Contextmenu' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('launch') ) AND (event_object IN ('app') ) AND (event_value IN ('game_shortcut') ) THEN 'Rocket -  App is launched by Game Shortcut' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('update') ) THEN 'Rocket -  Show Update Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('update') ) THEN 'Rocket -  Click Update Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('game_shortcut') ) THEN 'Rocket -  Show Game Shortcut Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('travel_search_result') ) THEN 'Rocket -  Show Travel Search Result Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('travel_search_result') ) THEN 'Rocket -  Click Travel Search Result Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('setdefault_travel_search') ) THEN 'Rocket -  Show Set-default Travel Search Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('setdefault_travel_search') ) THEN 'Rocket -  Click Set-default Travel Search Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('toast') ) AND (event_value IN ('exit_warning') ) THEN 'Rocket -  Show Exit Warning Toast' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('set_default_by_settings') ) THEN 'Rocket -  Show Set-Default by Settings Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('message') ) AND (event_value IN ('set_default_by_link') ) THEN 'Rocket -  Show Set-Default by Link Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('set_default_by_settings') ) THEN 'Rocket -  Click Set-Default by Settings Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('message') ) AND (event_value IN ('set_default_by_link') ) THEN 'Rocket -  Click Set-Default by Link Message' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('toast') ) AND (event_value IN ('set_default_success') ) THEN 'Rocket -  Show Set-Default Success Toast' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('snackbar') ) AND (event_value IN ('set_default_try_again') ) THEN 'Rocket -  Show Set-Default Try-again Snackbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('snackbar') ) AND (event_value IN ('set_default_try_again') ) THEN 'Rocket -  Click Set-Default Try-again Snackbar' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('start') ) AND (event_object IN ('download') ) AND (event_value IN ('file') ) THEN 'Rocket -  Start Download File' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('end') ) AND (event_object IN ('download') ) AND (event_value IN ('file') ) THEN 'Rocket -  End Download File' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('firstrun') ) AND (event_value IN ('turbo') ) THEN 'Rocket -  Turn on Turbo Mode in First Run' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('firstrun') ) AND (event_value IN ('finish') ) THEN 'Rocket -  Finish First Run' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('whatsnew') ) THEN 'Rocket -  Show Whatsnew Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('onboarding') ) AND (event_value IN ('whatsnew') ) THEN 'Rocket -  Click Whatsnew Onboarding' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('firstrun') ) THEN 'Rocket -  Show Firstrun Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('whatsnew') ) THEN 'Rocket -  Show Whatsnew Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('firstrun') ) THEN 'Rocket -  Click Firstrun Contextual Hint' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('contextual_hint') ) AND (event_value IN ('whatsnew') ) THEN 'Rocket -  Click Whatsnew Contextual Hint' 

    END AS event_name,
    event_timestamp AS timestamp,
    (event_timestamp + created) AS time,
    app_version,
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
    settings
FROM
    base_events
WHERE app_name IN ('Zerda', 'OTHER') AND os IN ('Android')
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", SPLIT(event_name, " - ")[OFFSET(1)], "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id,
  event_name AS event_type
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method, event_name),
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
      SELECT 'to' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to') AS value
      UNION ALL SELECT 'on' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'on') AS value
      UNION ALL SELECT 'from' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'from') AS value
      UNION ALL SELECT 'mode' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'mode') AS value
      UNION ALL SELECT 'type' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'type') AS value
      UNION ALL SELECT 'source' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'source') AS value
      UNION ALL SELECT 'default' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'default') AS value
      UNION ALL SELECT 'position' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'position') AS value
      UNION ALL SELECT 'version' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'version') AS value
      UNION ALL SELECT 'category' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'category') AS value
      UNION ALL SELECT 'category_versio' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'category_versio') AS value
      UNION ALL SELECT 'snackbar' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'snackbar') AS value
      UNION ALL SELECT 'success' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'success') AS value
      UNION ALL SELECT 'delay' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'delay') AS value
      UNION ALL SELECT 'message' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'message') AS value
      UNION ALL SELECT 'engine' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'engine') AS value
      UNION ALL SELECT 'duration' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'duration') AS value
      UNION ALL SELECT 'from_build' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'from_build') AS value
      UNION ALL SELECT 'to_build' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to_build') AS value
      UNION ALL SELECT 'action' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'action') AS value
      UNION ALL SELECT 'finish' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'finish') AS value
      UNION ALL SELECT 'page' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'page') AS value
      UNION ALL SELECT 'message_id' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'message_id') AS value
      UNION ALL SELECT 'link' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'link') AS value
      UNION ALL SELECT 'background' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'background') AS value
      UNION ALL SELECT 'primary' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'primary') AS value
      UNION ALL SELECT 'vertical' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'vertical') AS value
      UNION ALL SELECT 'component_id' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'component_id') AS value
      UNION ALL SELECT 'feed' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'feed') AS value
      UNION ALL SELECT 'subcategory_id' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'subcategory_id') AS value
      UNION ALL SELECT 'version_id' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'version_id') AS value
      UNION ALL SELECT 'app_link' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'app_link') AS value
      UNION ALL SELECT 'session_time' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'session_time') AS value
      UNION ALL SELECT 'show_keyboard' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'show_keyboard') AS value
      UNION ALL SELECT 'url_counts' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'url_counts') AS value
      UNION ALL SELECT 'impression' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'impression') AS value
      UNION ALL SELECT 'loadtime' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'loadtime') AS value
      UNION ALL SELECT 'audience_name' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'audience_name') AS value
      UNION ALL SELECT 'finished' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'finished') AS value
      UNION ALL SELECT 'task' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'task') AS value
      UNION ALL SELECT 'item_name' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'item_name') AS value
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  ARRAY_CONCAT(ARRAY<STRING>[],
    (SELECT ARRAY_AGG(
    CASE
        WHEN key='pref_search_engine' THEN CONCAT('"', 'pref_search_engine', '":"', CAST(value AS STRING), '"')
        WHEN key='pref_privacy_turbo_mode' THEN CONCAT('"', 'pref_privacy_turbo_mode', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
        WHEN key='pref_performance_block_images' THEN CONCAT('"', 'pref_performance_block_images', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
        WHEN key='pref_default_browser' THEN CONCAT('"', 'pref_default_browser', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
        WHEN key='pref_save_downloads_to' THEN CONCAT('"', 'pref_save_downloads_to', '":"', CAST(value AS STRING), '"')
        WHEN key='pref_webview_version' THEN CONCAT('"', 'pref_webview_version', '":"', CAST(value AS STRING), '"')
        WHEN key='install_referrer' THEN CONCAT('"', 'install_referrer', '":"', CAST(value AS STRING), '"')
        WHEN key='experiment_name' THEN CONCAT('"', 'experiment_name', '":"', CAST(value AS STRING), '"')
        WHEN key='experiment_bucket' THEN CONCAT('"', 'experiment_bucket', '":"', CAST(value AS STRING), '"')
        WHEN key='pref_locale' THEN CONCAT('"', 'pref_locale', '":"', CAST(value AS STRING), '"')
        WHEN key='pref_key_s_tracker_token' THEN CONCAT('"', 'pref_key_s_tracker_token', '":"', CAST(value AS STRING), '"')
    END
    IGNORE NULLS)
  FROM
    UNNEST(SETTINGS)
  )) AS user_props
FROM
  all_events_with_insert_ids
)

SELECT
  * EXCEPT (event_props_1, event_props_2, user_props, settings),
  CONCAT('{', ARRAY_TO_STRING((
   SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(ARRAY_CONCAT(IFNULL(event_props_1, []), IFNULL(event_props_2, []))) AS e
  ), ","), '}') AS event_properties,
  CONCAT('{', ARRAY_TO_STRING(user_props, ","), '}') AS user_properties
FROM extra_props

-- udf.newtab_is_noise_widget_event identifies widget events that were mistakenly
-- recorded as genuine user actions and must not contribute to any interaction or
-- engagement count.
--
-- What's covered:
--
-- DENG-11596 (crossword): the widget emitted a `widgets_user_event` on every keystroke
-- and cursor move, plus echoes of context menu actions.
--
-- DENG-11798 (privacy): impression events were emitted as `widgets_user_event` with
-- `user_action` of `trackers_blocked_impression` or `message_impression`, so impressions
-- were counted as interactions. Widget impressions remain counted separately via the
-- `widgets_impression` event, which this function does not affect.
--
-- Values are enumerated as a denylist so that newly introduced action values default to
-- being counted.
--
-- The result is wrapped in COALESCE so that an event whose `user_action` or
-- `action_value` key is absent returns FALSE (treated as a genuine event) rather than
-- NULL, which would silently drop the row from a `WHERE NOT ...` filter.
CREATE OR REPLACE FUNCTION udf.newtab_is_noise_widget_event(
  event_category STRING,
  event_name STRING,
  event_details ARRAY<STRUCT<key STRING, value STRING>>
)
RETURNS BOOLEAN AS (
  COALESCE(
    event_category = 'newtab'
    AND event_name = 'widgets_user_event'
    AND (
      -- DENG-11596: crossword keystroke/cursor noise and context menu echoes
      (
        mozfun.map.get_key(event_details, 'widget_name') = 'crossword'
        AND mozfun.map.get_key(event_details, 'user_action') = 'interaction'
        AND mozfun.map.get_key(event_details, 'action_value') IN (
          -- keystroke and cursor noise
          'input_letter',
          'cell_click',
          'backspace',
          'next_clue_button',
          'previous_clue_button',
          'review_clue_selected',
          -- context menu echoes
          'all_clues_opened',
          'all_clues_closed',
          'reveal_grid_requested',
          'reveal_grid_completed'
        )
      )
      -- DENG-11798: privacy widget impressions recorded as user actions
      OR (
        mozfun.map.get_key(event_details, 'widget_name') = 'privacy'
        AND mozfun.map.get_key(event_details, 'user_action') IN (
          'trackers_blocked_impression',
          'message_impression'
        )
      )
    ),
    FALSE
  )
);

-- Tests
SELECT
  -- excluded: keystroke and cursor noise
  mozfun.assert.true(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'crossword' AS value),
        ('user_action', 'interaction'),
        ('action_value', 'input_letter')
      ]
    )
  ),
  -- excluded: context menu echo
  mozfun.assert.true(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'crossword' AS value),
        ('user_action', 'interaction'),
        ('action_value', 'reveal_grid_completed')
      ]
    )
  ),
  -- retained: `interaction` with an action value that is not on the denylist
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'crossword' AS value),
        ('user_action', 'interaction'),
        ('action_value', 'play_started')
      ]
    )
  ),
  -- retained: crossword `interaction` with no action_value key at all. Must be FALSE
  -- rather than NULL, otherwise `WHERE NOT ...` would silently drop the event.
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'crossword' AS value), ('user_action', 'interaction')]
    )
  ),
  -- retained: no user_action key, but a denylisted action_value. We cannot confirm this
  -- is an `interaction` event, so it defaults to being counted.
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'crossword' AS value), ('action_value', 'input_letter')]
    )
  ),
  -- retained: no extras at all
  mozfun.assert.false(udf.newtab_is_noise_widget_event('newtab', 'widgets_user_event', [])),
  -- retained: genuine crossword action
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'crossword' AS value),
        ('user_action', 'puzzle_completed'),
        ('action_value', '0')
      ]
    )
  ),
  -- retained: the same denylisted action value on a different widget
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'lists' AS value),
        ('user_action', 'interaction'),
        ('action_value', 'input_letter')
      ]
    )
  ),
  -- retained: impressions are never noise
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_impression',
      [STRUCT('widget_name' AS key, 'crossword' AS value)]
    )
  ),
  -- DENG-11798 excluded: privacy widget impressions recorded as user actions
  mozfun.assert.true(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'privacy' AS value),
        ('user_action', 'trackers_blocked_impression')
      ]
    )
  ),
  mozfun.assert.true(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'privacy' AS value), ('user_action', 'message_impression')]
    )
  ),
  -- retained: genuine privacy widget actions
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'privacy' AS value), ('user_action', 'tracking_message_click')]
    )
  ),
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'privacy' AS value), ('user_action', 'message_cta_click')]
    )
  ),
  -- retained: privacy widget event with no user_action key. Must be FALSE rather than
  -- NULL, otherwise `WHERE NOT ...` would silently drop the event.
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'privacy' AS value)]
    )
  ),
  -- retained: the same impression action values on a different widget are unaffected
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_user_event',
      [STRUCT('widget_name' AS key, 'crossword' AS value), ('user_action', 'message_impression')]
    )
  ),
  -- retained: privacy widget impressions arriving as the `widgets_impression` event are
  -- never noise - impression counts are preserved
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'newtab',
      'widgets_impression',
      [STRUCT('widget_name' AS key, 'privacy' AS value)]
    )
  ),
  -- retained: a different event category
  mozfun.assert.false(
    udf.newtab_is_noise_widget_event(
      'pocket',
      'widgets_user_event',
      [
        STRUCT('widget_name' AS key, 'crossword' AS value),
        ('user_action', 'interaction'),
        ('action_value', 'input_letter')
      ]
    )
  ),
  TRUE

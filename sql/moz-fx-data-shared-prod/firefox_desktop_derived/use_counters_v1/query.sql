-- Query for telemetry_derived.firefox_use_counters_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH firefox_desktop_use_counts_by_day_version_and_country_stg AS (
  SELECT
    DATE(a.submission_timestamp) AS submission_date,
    mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS version_major,
    metadata.geo.country AS geo_country,
    normalized_app_name AS platform,
    SUM(
      metrics.counter.use_counter_content_documents_destroyed
    ) AS use_counter_content_documents_destroyed,
    SUM(
      metrics.counter.use_counter_top_level_content_documents_destroyed
    ) AS use_counter_top_level_content_documents_destroyed,
    SUM(
      metrics.counter.use_counter_service_workers_destroyed
    ) AS use_counter_service_workers_destroyed,
    SUM(
      metrics.counter.use_counter_shared_workers_destroyed
    ) AS use_counter_shared_workers_destroyed,
    SUM(
      metrics.counter.use_counter_dedicated_workers_destroyed
    ) AS use_counter_dedicated_workers_destroyed,
    SUM(
      metrics.counter.use_counter_css_doc_alignment_baseline
    ) AS use_counter_css_doc_alignment_baseline,
    SUM(
      metrics.counter.use_counter_css_doc_background_repeat_x
    ) AS use_counter_css_doc_background_repeat_x,
    SUM(
      metrics.counter.use_counter_css_doc_background_repeat_y
    ) AS use_counter_css_doc_background_repeat_y,
    SUM(metrics.counter.use_counter_css_doc_baseline_shift) AS use_counter_css_doc_baseline_shift,
    SUM(
      metrics.counter.use_counter_css_doc_buffered_rendering
    ) AS use_counter_css_doc_buffered_rendering,
    SUM(metrics.counter.use_counter_css_doc_color_rendering) AS use_counter_css_doc_color_rendering,
    SUM(
      metrics.counter.use_counter_css_doc_css_accent_color
    ) AS use_counter_css_doc_css_accent_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_align_content
    ) AS use_counter_css_doc_css_align_content,
    SUM(metrics.counter.use_counter_css_doc_css_align_items) AS use_counter_css_doc_css_align_items,
    SUM(metrics.counter.use_counter_css_doc_css_align_self) AS use_counter_css_doc_css_align_self,
    SUM(
      metrics.counter.use_counter_css_doc_css_align_tracks
    ) AS use_counter_css_doc_css_align_tracks,
    SUM(metrics.counter.use_counter_css_doc_css_all) AS use_counter_css_doc_css_all,
    SUM(metrics.counter.use_counter_css_doc_css_animation) AS use_counter_css_doc_css_animation,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_composition
    ) AS use_counter_css_doc_css_animation_composition,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_delay
    ) AS use_counter_css_doc_css_animation_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_direction
    ) AS use_counter_css_doc_css_animation_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_duration
    ) AS use_counter_css_doc_css_animation_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_fill_mode
    ) AS use_counter_css_doc_css_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_iteration_count
    ) AS use_counter_css_doc_css_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_name
    ) AS use_counter_css_doc_css_animation_name,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_play_state
    ) AS use_counter_css_doc_css_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_timeline
    ) AS use_counter_css_doc_css_animation_timeline,
    SUM(
      metrics.counter.use_counter_css_doc_css_animation_timing_function
    ) AS use_counter_css_doc_css_animation_timing_function,
    SUM(metrics.counter.use_counter_css_doc_css_appearance) AS use_counter_css_doc_css_appearance,
    SUM(
      metrics.counter.use_counter_css_doc_css_aspect_ratio
    ) AS use_counter_css_doc_css_aspect_ratio,
    SUM(
      metrics.counter.use_counter_css_doc_css_backdrop_filter
    ) AS use_counter_css_doc_css_backdrop_filter,
    SUM(
      metrics.counter.use_counter_css_doc_css_backface_visibility
    ) AS use_counter_css_doc_css_backface_visibility,
    SUM(metrics.counter.use_counter_css_doc_css_background) AS use_counter_css_doc_css_background,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_attachment
    ) AS use_counter_css_doc_css_background_attachment,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_blend_mode
    ) AS use_counter_css_doc_css_background_blend_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_clip
    ) AS use_counter_css_doc_css_background_clip,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_color
    ) AS use_counter_css_doc_css_background_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_image
    ) AS use_counter_css_doc_css_background_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_origin
    ) AS use_counter_css_doc_css_background_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_position
    ) AS use_counter_css_doc_css_background_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_position_x
    ) AS use_counter_css_doc_css_background_position_x,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_position_y
    ) AS use_counter_css_doc_css_background_position_y,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_repeat
    ) AS use_counter_css_doc_css_background_repeat,
    SUM(
      metrics.counter.use_counter_css_doc_css_background_size
    ) AS use_counter_css_doc_css_background_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_baseline_source
    ) AS use_counter_css_doc_css_baseline_source,
    SUM(metrics.counter.use_counter_css_doc_css_block_size) AS use_counter_css_doc_css_block_size,
    SUM(metrics.counter.use_counter_css_doc_css_border) AS use_counter_css_doc_css_border,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block
    ) AS use_counter_css_doc_css_border_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_color
    ) AS use_counter_css_doc_css_border_block_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_end
    ) AS use_counter_css_doc_css_border_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_end_color
    ) AS use_counter_css_doc_css_border_block_end_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_end_style
    ) AS use_counter_css_doc_css_border_block_end_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_end_width
    ) AS use_counter_css_doc_css_border_block_end_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_start
    ) AS use_counter_css_doc_css_border_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_start_color
    ) AS use_counter_css_doc_css_border_block_start_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_start_style
    ) AS use_counter_css_doc_css_border_block_start_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_start_width
    ) AS use_counter_css_doc_css_border_block_start_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_style
    ) AS use_counter_css_doc_css_border_block_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_block_width
    ) AS use_counter_css_doc_css_border_block_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom
    ) AS use_counter_css_doc_css_border_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom_color
    ) AS use_counter_css_doc_css_border_bottom_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom_left_radius
    ) AS use_counter_css_doc_css_border_bottom_left_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom_right_radius
    ) AS use_counter_css_doc_css_border_bottom_right_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom_style
    ) AS use_counter_css_doc_css_border_bottom_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_bottom_width
    ) AS use_counter_css_doc_css_border_bottom_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_collapse
    ) AS use_counter_css_doc_css_border_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_color
    ) AS use_counter_css_doc_css_border_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_end_end_radius
    ) AS use_counter_css_doc_css_border_end_end_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_end_start_radius
    ) AS use_counter_css_doc_css_border_end_start_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image
    ) AS use_counter_css_doc_css_border_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image_outset
    ) AS use_counter_css_doc_css_border_image_outset,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image_repeat
    ) AS use_counter_css_doc_css_border_image_repeat,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image_slice
    ) AS use_counter_css_doc_css_border_image_slice,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image_source
    ) AS use_counter_css_doc_css_border_image_source,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_image_width
    ) AS use_counter_css_doc_css_border_image_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline
    ) AS use_counter_css_doc_css_border_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_color
    ) AS use_counter_css_doc_css_border_inline_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_end
    ) AS use_counter_css_doc_css_border_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_end_color
    ) AS use_counter_css_doc_css_border_inline_end_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_end_style
    ) AS use_counter_css_doc_css_border_inline_end_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_end_width
    ) AS use_counter_css_doc_css_border_inline_end_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_start
    ) AS use_counter_css_doc_css_border_inline_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_start_color
    ) AS use_counter_css_doc_css_border_inline_start_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_start_style
    ) AS use_counter_css_doc_css_border_inline_start_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_start_width
    ) AS use_counter_css_doc_css_border_inline_start_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_style
    ) AS use_counter_css_doc_css_border_inline_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_inline_width
    ) AS use_counter_css_doc_css_border_inline_width,
    SUM(metrics.counter.use_counter_css_doc_css_border_left) AS use_counter_css_doc_css_border_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_left_color
    ) AS use_counter_css_doc_css_border_left_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_left_style
    ) AS use_counter_css_doc_css_border_left_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_left_width
    ) AS use_counter_css_doc_css_border_left_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_radius
    ) AS use_counter_css_doc_css_border_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_right
    ) AS use_counter_css_doc_css_border_right,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_right_color
    ) AS use_counter_css_doc_css_border_right_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_right_style
    ) AS use_counter_css_doc_css_border_right_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_right_width
    ) AS use_counter_css_doc_css_border_right_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_spacing
    ) AS use_counter_css_doc_css_border_spacing,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_start_end_radius
    ) AS use_counter_css_doc_css_border_start_end_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_start_start_radius
    ) AS use_counter_css_doc_css_border_start_start_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_style
    ) AS use_counter_css_doc_css_border_style,
    SUM(metrics.counter.use_counter_css_doc_css_border_top) AS use_counter_css_doc_css_border_top,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_top_color
    ) AS use_counter_css_doc_css_border_top_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_top_left_radius
    ) AS use_counter_css_doc_css_border_top_left_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_top_right_radius
    ) AS use_counter_css_doc_css_border_top_right_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_top_style
    ) AS use_counter_css_doc_css_border_top_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_top_width
    ) AS use_counter_css_doc_css_border_top_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_border_width
    ) AS use_counter_css_doc_css_border_width,
    SUM(metrics.counter.use_counter_css_doc_css_bottom) AS use_counter_css_doc_css_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_box_decoration_break
    ) AS use_counter_css_doc_css_box_decoration_break,
    SUM(metrics.counter.use_counter_css_doc_css_box_shadow) AS use_counter_css_doc_css_box_shadow,
    SUM(metrics.counter.use_counter_css_doc_css_box_sizing) AS use_counter_css_doc_css_box_sizing,
    SUM(metrics.counter.use_counter_css_doc_css_break_after) AS use_counter_css_doc_css_break_after,
    SUM(
      metrics.counter.use_counter_css_doc_css_break_before
    ) AS use_counter_css_doc_css_break_before,
    SUM(
      metrics.counter.use_counter_css_doc_css_break_inside
    ) AS use_counter_css_doc_css_break_inside,
    SUM(
      metrics.counter.use_counter_css_doc_css_caption_side
    ) AS use_counter_css_doc_css_caption_side,
    SUM(metrics.counter.use_counter_css_doc_css_caret_color) AS use_counter_css_doc_css_caret_color,
    SUM(metrics.counter.use_counter_css_doc_css_clear) AS use_counter_css_doc_css_clear,
    SUM(metrics.counter.use_counter_css_doc_css_clip) AS use_counter_css_doc_css_clip,
    SUM(metrics.counter.use_counter_css_doc_css_clip_path) AS use_counter_css_doc_css_clip_path,
    SUM(metrics.counter.use_counter_css_doc_css_clip_rule) AS use_counter_css_doc_css_clip_rule,
    SUM(metrics.counter.use_counter_css_doc_css_color) AS use_counter_css_doc_css_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_color_adjust
    ) AS use_counter_css_doc_css_color_adjust,
    SUM(
      metrics.counter.use_counter_css_doc_css_color_interpolation
    ) AS use_counter_css_doc_css_color_interpolation,
    SUM(
      metrics.counter.use_counter_css_doc_css_color_interpolation_filters
    ) AS use_counter_css_doc_css_color_interpolation_filters,
    SUM(
      metrics.counter.use_counter_css_doc_css_color_scheme
    ) AS use_counter_css_doc_css_color_scheme,
    SUM(
      metrics.counter.use_counter_css_doc_css_column_count
    ) AS use_counter_css_doc_css_column_count,
    SUM(metrics.counter.use_counter_css_doc_css_column_fill) AS use_counter_css_doc_css_column_fill,
    SUM(metrics.counter.use_counter_css_doc_css_column_gap) AS use_counter_css_doc_css_column_gap,
    SUM(metrics.counter.use_counter_css_doc_css_column_rule) AS use_counter_css_doc_css_column_rule,
    SUM(
      metrics.counter.use_counter_css_doc_css_column_rule_color
    ) AS use_counter_css_doc_css_column_rule_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_column_rule_style
    ) AS use_counter_css_doc_css_column_rule_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_column_rule_width
    ) AS use_counter_css_doc_css_column_rule_width,
    SUM(metrics.counter.use_counter_css_doc_css_column_span) AS use_counter_css_doc_css_column_span,
    SUM(
      metrics.counter.use_counter_css_doc_css_column_width
    ) AS use_counter_css_doc_css_column_width,
    SUM(metrics.counter.use_counter_css_doc_css_columns) AS use_counter_css_doc_css_columns,
    SUM(metrics.counter.use_counter_css_doc_css_contain) AS use_counter_css_doc_css_contain,
    SUM(
      metrics.counter.use_counter_css_doc_css_contain_intrinsic_block_size
    ) AS use_counter_css_doc_css_contain_intrinsic_block_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_contain_intrinsic_height
    ) AS use_counter_css_doc_css_contain_intrinsic_height,
    SUM(
      metrics.counter.use_counter_css_doc_css_contain_intrinsic_inline_size
    ) AS use_counter_css_doc_css_contain_intrinsic_inline_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_contain_intrinsic_size
    ) AS use_counter_css_doc_css_contain_intrinsic_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_contain_intrinsic_width
    ) AS use_counter_css_doc_css_contain_intrinsic_width,
    SUM(metrics.counter.use_counter_css_doc_css_container) AS use_counter_css_doc_css_container,
    SUM(
      metrics.counter.use_counter_css_doc_css_container_name
    ) AS use_counter_css_doc_css_container_name,
    SUM(
      metrics.counter.use_counter_css_doc_css_container_type
    ) AS use_counter_css_doc_css_container_type,
    SUM(metrics.counter.use_counter_css_doc_css_content) AS use_counter_css_doc_css_content,
    SUM(
      metrics.counter.use_counter_css_doc_css_content_visibility
    ) AS use_counter_css_doc_css_content_visibility,
    SUM(
      metrics.counter.use_counter_css_doc_css_counter_increment
    ) AS use_counter_css_doc_css_counter_increment,
    SUM(
      metrics.counter.use_counter_css_doc_css_counter_reset
    ) AS use_counter_css_doc_css_counter_reset,
    SUM(metrics.counter.use_counter_css_doc_css_counter_set) AS use_counter_css_doc_css_counter_set,
    SUM(metrics.counter.use_counter_css_doc_css_cursor) AS use_counter_css_doc_css_cursor,
    SUM(metrics.counter.use_counter_css_doc_css_cx) AS use_counter_css_doc_css_cx,
    SUM(metrics.counter.use_counter_css_doc_css_cy) AS use_counter_css_doc_css_cy,
    SUM(metrics.counter.use_counter_css_doc_css_d) AS use_counter_css_doc_css_d,
    SUM(metrics.counter.use_counter_css_doc_css_direction) AS use_counter_css_doc_css_direction,
    SUM(metrics.counter.use_counter_css_doc_css_display) AS use_counter_css_doc_css_display,
    SUM(
      metrics.counter.use_counter_css_doc_css_dominant_baseline
    ) AS use_counter_css_doc_css_dominant_baseline,
    SUM(metrics.counter.use_counter_css_doc_css_empty_cells) AS use_counter_css_doc_css_empty_cells,
    SUM(metrics.counter.use_counter_css_doc_css_fill) AS use_counter_css_doc_css_fill,
    SUM(
      metrics.counter.use_counter_css_doc_css_fill_opacity
    ) AS use_counter_css_doc_css_fill_opacity,
    SUM(metrics.counter.use_counter_css_doc_css_fill_rule) AS use_counter_css_doc_css_fill_rule,
    SUM(metrics.counter.use_counter_css_doc_css_filter) AS use_counter_css_doc_css_filter,
    SUM(metrics.counter.use_counter_css_doc_css_flex) AS use_counter_css_doc_css_flex,
    SUM(metrics.counter.use_counter_css_doc_css_flex_basis) AS use_counter_css_doc_css_flex_basis,
    SUM(
      metrics.counter.use_counter_css_doc_css_flex_direction
    ) AS use_counter_css_doc_css_flex_direction,
    SUM(metrics.counter.use_counter_css_doc_css_flex_flow) AS use_counter_css_doc_css_flex_flow,
    SUM(metrics.counter.use_counter_css_doc_css_flex_grow) AS use_counter_css_doc_css_flex_grow,
    SUM(metrics.counter.use_counter_css_doc_css_flex_shrink) AS use_counter_css_doc_css_flex_shrink,
    SUM(metrics.counter.use_counter_css_doc_css_flex_wrap) AS use_counter_css_doc_css_flex_wrap,
    SUM(metrics.counter.use_counter_css_doc_css_float) AS use_counter_css_doc_css_float,
    SUM(metrics.counter.use_counter_css_doc_css_flood_color) AS use_counter_css_doc_css_flood_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_flood_opacity
    ) AS use_counter_css_doc_css_flood_opacity,
    SUM(metrics.counter.use_counter_css_doc_css_font) AS use_counter_css_doc_css_font,
    SUM(metrics.counter.use_counter_css_doc_css_font_family) AS use_counter_css_doc_css_font_family,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_feature_settings
    ) AS use_counter_css_doc_css_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_kerning
    ) AS use_counter_css_doc_css_font_kerning,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_language_override
    ) AS use_counter_css_doc_css_font_language_override,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_optical_sizing
    ) AS use_counter_css_doc_css_font_optical_sizing,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_palette
    ) AS use_counter_css_doc_css_font_palette,
    SUM(metrics.counter.use_counter_css_doc_css_font_size) AS use_counter_css_doc_css_font_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_size_adjust
    ) AS use_counter_css_doc_css_font_size_adjust,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_stretch
    ) AS use_counter_css_doc_css_font_stretch,
    SUM(metrics.counter.use_counter_css_doc_css_font_style) AS use_counter_css_doc_css_font_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_synthesis
    ) AS use_counter_css_doc_css_font_synthesis,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_synthesis_position
    ) AS use_counter_css_doc_css_font_synthesis_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_synthesis_small_caps
    ) AS use_counter_css_doc_css_font_synthesis_small_caps,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_synthesis_style
    ) AS use_counter_css_doc_css_font_synthesis_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_synthesis_weight
    ) AS use_counter_css_doc_css_font_synthesis_weight,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant
    ) AS use_counter_css_doc_css_font_variant,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_alternates
    ) AS use_counter_css_doc_css_font_variant_alternates,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_caps
    ) AS use_counter_css_doc_css_font_variant_caps,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_east_asian
    ) AS use_counter_css_doc_css_font_variant_east_asian,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_emoji
    ) AS use_counter_css_doc_css_font_variant_emoji,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_ligatures
    ) AS use_counter_css_doc_css_font_variant_ligatures,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_numeric
    ) AS use_counter_css_doc_css_font_variant_numeric,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variant_position
    ) AS use_counter_css_doc_css_font_variant_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_font_variation_settings
    ) AS use_counter_css_doc_css_font_variation_settings,
    SUM(metrics.counter.use_counter_css_doc_css_font_weight) AS use_counter_css_doc_css_font_weight,
    SUM(
      metrics.counter.use_counter_css_doc_css_forced_color_adjust
    ) AS use_counter_css_doc_css_forced_color_adjust,
    SUM(metrics.counter.use_counter_css_doc_css_gap) AS use_counter_css_doc_css_gap,
    SUM(metrics.counter.use_counter_css_doc_css_grid) AS use_counter_css_doc_css_grid,
    SUM(metrics.counter.use_counter_css_doc_css_grid_area) AS use_counter_css_doc_css_grid_area,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_auto_columns
    ) AS use_counter_css_doc_css_grid_auto_columns,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_auto_flow
    ) AS use_counter_css_doc_css_grid_auto_flow,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_auto_rows
    ) AS use_counter_css_doc_css_grid_auto_rows,
    SUM(metrics.counter.use_counter_css_doc_css_grid_column) AS use_counter_css_doc_css_grid_column,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_column_end
    ) AS use_counter_css_doc_css_grid_column_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_column_gap
    ) AS use_counter_css_doc_css_grid_column_gap,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_column_start
    ) AS use_counter_css_doc_css_grid_column_start,
    SUM(metrics.counter.use_counter_css_doc_css_grid_gap) AS use_counter_css_doc_css_grid_gap,
    SUM(metrics.counter.use_counter_css_doc_css_grid_row) AS use_counter_css_doc_css_grid_row,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_row_end
    ) AS use_counter_css_doc_css_grid_row_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_row_gap
    ) AS use_counter_css_doc_css_grid_row_gap,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_row_start
    ) AS use_counter_css_doc_css_grid_row_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_template
    ) AS use_counter_css_doc_css_grid_template,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_template_areas
    ) AS use_counter_css_doc_css_grid_template_areas,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_template_columns
    ) AS use_counter_css_doc_css_grid_template_columns,
    SUM(
      metrics.counter.use_counter_css_doc_css_grid_template_rows
    ) AS use_counter_css_doc_css_grid_template_rows,
    SUM(metrics.counter.use_counter_css_doc_css_height) AS use_counter_css_doc_css_height,
    SUM(
      metrics.counter.use_counter_css_doc_css_hyphenate_character
    ) AS use_counter_css_doc_css_hyphenate_character,
    SUM(metrics.counter.use_counter_css_doc_css_hyphens) AS use_counter_css_doc_css_hyphens,
    SUM(
      metrics.counter.use_counter_css_doc_css_image_orientation
    ) AS use_counter_css_doc_css_image_orientation,
    SUM(
      metrics.counter.use_counter_css_doc_css_image_rendering
    ) AS use_counter_css_doc_css_image_rendering,
    SUM(metrics.counter.use_counter_css_doc_css_ime_mode) AS use_counter_css_doc_css_ime_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_initial_letter
    ) AS use_counter_css_doc_css_initial_letter,
    SUM(metrics.counter.use_counter_css_doc_css_inline_size) AS use_counter_css_doc_css_inline_size,
    SUM(metrics.counter.use_counter_css_doc_css_inset) AS use_counter_css_doc_css_inset,
    SUM(metrics.counter.use_counter_css_doc_css_inset_block) AS use_counter_css_doc_css_inset_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_inset_block_end
    ) AS use_counter_css_doc_css_inset_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_inset_block_start
    ) AS use_counter_css_doc_css_inset_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_inset_inline
    ) AS use_counter_css_doc_css_inset_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_inset_inline_end
    ) AS use_counter_css_doc_css_inset_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_inset_inline_start
    ) AS use_counter_css_doc_css_inset_inline_start,
    SUM(metrics.counter.use_counter_css_doc_css_isolation) AS use_counter_css_doc_css_isolation,
    SUM(
      metrics.counter.use_counter_css_doc_css_justify_content
    ) AS use_counter_css_doc_css_justify_content,
    SUM(
      metrics.counter.use_counter_css_doc_css_justify_items
    ) AS use_counter_css_doc_css_justify_items,
    SUM(
      metrics.counter.use_counter_css_doc_css_justify_self
    ) AS use_counter_css_doc_css_justify_self,
    SUM(
      metrics.counter.use_counter_css_doc_css_justify_tracks
    ) AS use_counter_css_doc_css_justify_tracks,
    SUM(metrics.counter.use_counter_css_doc_css_left) AS use_counter_css_doc_css_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_letter_spacing
    ) AS use_counter_css_doc_css_letter_spacing,
    SUM(
      metrics.counter.use_counter_css_doc_css_lighting_color
    ) AS use_counter_css_doc_css_lighting_color,
    SUM(metrics.counter.use_counter_css_doc_css_line_break) AS use_counter_css_doc_css_line_break,
    SUM(metrics.counter.use_counter_css_doc_css_line_height) AS use_counter_css_doc_css_line_height,
    SUM(metrics.counter.use_counter_css_doc_css_list_style) AS use_counter_css_doc_css_list_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_list_style_image
    ) AS use_counter_css_doc_css_list_style_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_list_style_position
    ) AS use_counter_css_doc_css_list_style_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_list_style_type
    ) AS use_counter_css_doc_css_list_style_type,
    SUM(metrics.counter.use_counter_css_doc_css_margin) AS use_counter_css_doc_css_margin,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_block
    ) AS use_counter_css_doc_css_margin_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_block_end
    ) AS use_counter_css_doc_css_margin_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_block_start
    ) AS use_counter_css_doc_css_margin_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_bottom
    ) AS use_counter_css_doc_css_margin_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_inline
    ) AS use_counter_css_doc_css_margin_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_inline_end
    ) AS use_counter_css_doc_css_margin_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_inline_start
    ) AS use_counter_css_doc_css_margin_inline_start,
    SUM(metrics.counter.use_counter_css_doc_css_margin_left) AS use_counter_css_doc_css_margin_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_margin_right
    ) AS use_counter_css_doc_css_margin_right,
    SUM(metrics.counter.use_counter_css_doc_css_margin_top) AS use_counter_css_doc_css_margin_top,
    SUM(metrics.counter.use_counter_css_doc_css_marker) AS use_counter_css_doc_css_marker,
    SUM(metrics.counter.use_counter_css_doc_css_marker_end) AS use_counter_css_doc_css_marker_end,
    SUM(metrics.counter.use_counter_css_doc_css_marker_mid) AS use_counter_css_doc_css_marker_mid,
    SUM(
      metrics.counter.use_counter_css_doc_css_marker_start
    ) AS use_counter_css_doc_css_marker_start,
    SUM(metrics.counter.use_counter_css_doc_css_mask) AS use_counter_css_doc_css_mask,
    SUM(metrics.counter.use_counter_css_doc_css_mask_clip) AS use_counter_css_doc_css_mask_clip,
    SUM(
      metrics.counter.use_counter_css_doc_css_mask_composite
    ) AS use_counter_css_doc_css_mask_composite,
    SUM(metrics.counter.use_counter_css_doc_css_mask_image) AS use_counter_css_doc_css_mask_image,
    SUM(metrics.counter.use_counter_css_doc_css_mask_mode) AS use_counter_css_doc_css_mask_mode,
    SUM(metrics.counter.use_counter_css_doc_css_mask_origin) AS use_counter_css_doc_css_mask_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_mask_position
    ) AS use_counter_css_doc_css_mask_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_mask_position_x
    ) AS use_counter_css_doc_css_mask_position_x,
    SUM(
      metrics.counter.use_counter_css_doc_css_mask_position_y
    ) AS use_counter_css_doc_css_mask_position_y,
    SUM(metrics.counter.use_counter_css_doc_css_mask_repeat) AS use_counter_css_doc_css_mask_repeat,
    SUM(metrics.counter.use_counter_css_doc_css_mask_size) AS use_counter_css_doc_css_mask_size,
    SUM(metrics.counter.use_counter_css_doc_css_mask_type) AS use_counter_css_doc_css_mask_type,
    SUM(
      metrics.counter.use_counter_css_doc_css_masonry_auto_flow
    ) AS use_counter_css_doc_css_masonry_auto_flow,
    SUM(metrics.counter.use_counter_css_doc_css_math_depth) AS use_counter_css_doc_css_math_depth,
    SUM(metrics.counter.use_counter_css_doc_css_math_style) AS use_counter_css_doc_css_math_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_max_block_size
    ) AS use_counter_css_doc_css_max_block_size,
    SUM(metrics.counter.use_counter_css_doc_css_max_height) AS use_counter_css_doc_css_max_height,
    SUM(
      metrics.counter.use_counter_css_doc_css_max_inline_size
    ) AS use_counter_css_doc_css_max_inline_size,
    SUM(metrics.counter.use_counter_css_doc_css_max_width) AS use_counter_css_doc_css_max_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_min_block_size
    ) AS use_counter_css_doc_css_min_block_size,
    SUM(metrics.counter.use_counter_css_doc_css_min_height) AS use_counter_css_doc_css_min_height,
    SUM(
      metrics.counter.use_counter_css_doc_css_min_inline_size
    ) AS use_counter_css_doc_css_min_inline_size,
    SUM(metrics.counter.use_counter_css_doc_css_min_width) AS use_counter_css_doc_css_min_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_mix_blend_mode
    ) AS use_counter_css_doc_css_mix_blend_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation
    ) AS use_counter_css_doc_css_moz_animation,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_delay
    ) AS use_counter_css_doc_css_moz_animation_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_direction
    ) AS use_counter_css_doc_css_moz_animation_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_duration
    ) AS use_counter_css_doc_css_moz_animation_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_fill_mode
    ) AS use_counter_css_doc_css_moz_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_iteration_count
    ) AS use_counter_css_doc_css_moz_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_name
    ) AS use_counter_css_doc_css_moz_animation_name,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_play_state
    ) AS use_counter_css_doc_css_moz_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_animation_timing_function
    ) AS use_counter_css_doc_css_moz_animation_timing_function,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_appearance
    ) AS use_counter_css_doc_css_moz_appearance,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_backface_visibility
    ) AS use_counter_css_doc_css_moz_backface_visibility,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_end
    ) AS use_counter_css_doc_css_moz_border_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_end_color
    ) AS use_counter_css_doc_css_moz_border_end_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_end_style
    ) AS use_counter_css_doc_css_moz_border_end_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_end_width
    ) AS use_counter_css_doc_css_moz_border_end_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_image
    ) AS use_counter_css_doc_css_moz_border_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_start
    ) AS use_counter_css_doc_css_moz_border_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_start_color
    ) AS use_counter_css_doc_css_moz_border_start_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_start_style
    ) AS use_counter_css_doc_css_moz_border_start_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_border_start_width
    ) AS use_counter_css_doc_css_moz_border_start_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_align
    ) AS use_counter_css_doc_css_moz_box_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_collapse
    ) AS use_counter_css_doc_css_moz_box_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_direction
    ) AS use_counter_css_doc_css_moz_box_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_flex
    ) AS use_counter_css_doc_css_moz_box_flex,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_ordinal_group
    ) AS use_counter_css_doc_css_moz_box_ordinal_group,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_orient
    ) AS use_counter_css_doc_css_moz_box_orient,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_pack
    ) AS use_counter_css_doc_css_moz_box_pack,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_box_sizing
    ) AS use_counter_css_doc_css_moz_box_sizing,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_context_properties
    ) AS use_counter_css_doc_css_moz_context_properties,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_control_character_visibility
    ) AS use_counter_css_doc_css_moz_control_character_visibility,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_default_appearance
    ) AS use_counter_css_doc_css_moz_default_appearance,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_float_edge
    ) AS use_counter_css_doc_css_moz_float_edge,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_font_feature_settings
    ) AS use_counter_css_doc_css_moz_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_font_language_override
    ) AS use_counter_css_doc_css_moz_font_language_override,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_force_broken_image_icon
    ) AS use_counter_css_doc_css_moz_force_broken_image_icon,
    SUM(metrics.counter.use_counter_css_doc_css_moz_hyphens) AS use_counter_css_doc_css_moz_hyphens,
    SUM(metrics.counter.use_counter_css_doc_css_moz_inert) AS use_counter_css_doc_css_moz_inert,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_margin_end
    ) AS use_counter_css_doc_css_moz_margin_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_margin_start
    ) AS use_counter_css_doc_css_moz_margin_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_math_variant
    ) AS use_counter_css_doc_css_moz_math_variant,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_min_font_size_ratio
    ) AS use_counter_css_doc_css_moz_min_font_size_ratio,
    SUM(metrics.counter.use_counter_css_doc_css_moz_orient) AS use_counter_css_doc_css_moz_orient,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_osx_font_smoothing
    ) AS use_counter_css_doc_css_moz_osx_font_smoothing,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_padding_end
    ) AS use_counter_css_doc_css_moz_padding_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_padding_start
    ) AS use_counter_css_doc_css_moz_padding_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_perspective
    ) AS use_counter_css_doc_css_moz_perspective,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_perspective_origin
    ) AS use_counter_css_doc_css_moz_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_subtree_hidden_only_visually
    ) AS use_counter_css_doc_css_moz_subtree_hidden_only_visually,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_tab_size
    ) AS use_counter_css_doc_css_moz_tab_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_text_size_adjust
    ) AS use_counter_css_doc_css_moz_text_size_adjust,
    SUM(metrics.counter.use_counter_css_doc_css_moz_theme) AS use_counter_css_doc_css_moz_theme,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_top_layer
    ) AS use_counter_css_doc_css_moz_top_layer,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transform
    ) AS use_counter_css_doc_css_moz_transform,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transform_origin
    ) AS use_counter_css_doc_css_moz_transform_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transform_style
    ) AS use_counter_css_doc_css_moz_transform_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transition
    ) AS use_counter_css_doc_css_moz_transition,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transition_delay
    ) AS use_counter_css_doc_css_moz_transition_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transition_duration
    ) AS use_counter_css_doc_css_moz_transition_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transition_property
    ) AS use_counter_css_doc_css_moz_transition_property,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_transition_timing_function
    ) AS use_counter_css_doc_css_moz_transition_timing_function,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_user_focus
    ) AS use_counter_css_doc_css_moz_user_focus,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_user_input
    ) AS use_counter_css_doc_css_moz_user_input,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_user_modify
    ) AS use_counter_css_doc_css_moz_user_modify,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_user_select
    ) AS use_counter_css_doc_css_moz_user_select,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_dragging
    ) AS use_counter_css_doc_css_moz_window_dragging,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_input_region_margin
    ) AS use_counter_css_doc_css_moz_window_input_region_margin,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_opacity
    ) AS use_counter_css_doc_css_moz_window_opacity,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_shadow
    ) AS use_counter_css_doc_css_moz_window_shadow,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_transform
    ) AS use_counter_css_doc_css_moz_window_transform,
    SUM(
      metrics.counter.use_counter_css_doc_css_moz_window_transform_origin
    ) AS use_counter_css_doc_css_moz_window_transform_origin,
    SUM(metrics.counter.use_counter_css_doc_css_object_fit) AS use_counter_css_doc_css_object_fit,
    SUM(
      metrics.counter.use_counter_css_doc_css_object_position
    ) AS use_counter_css_doc_css_object_position,
    SUM(metrics.counter.use_counter_css_doc_css_offset) AS use_counter_css_doc_css_offset,
    SUM(
      metrics.counter.use_counter_css_doc_css_offset_anchor
    ) AS use_counter_css_doc_css_offset_anchor,
    SUM(
      metrics.counter.use_counter_css_doc_css_offset_distance
    ) AS use_counter_css_doc_css_offset_distance,
    SUM(metrics.counter.use_counter_css_doc_css_offset_path) AS use_counter_css_doc_css_offset_path,
    SUM(
      metrics.counter.use_counter_css_doc_css_offset_position
    ) AS use_counter_css_doc_css_offset_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_offset_rotate
    ) AS use_counter_css_doc_css_offset_rotate,
    SUM(metrics.counter.use_counter_css_doc_css_opacity) AS use_counter_css_doc_css_opacity,
    SUM(metrics.counter.use_counter_css_doc_css_order) AS use_counter_css_doc_css_order,
    SUM(metrics.counter.use_counter_css_doc_css_outline) AS use_counter_css_doc_css_outline,
    SUM(
      metrics.counter.use_counter_css_doc_css_outline_color
    ) AS use_counter_css_doc_css_outline_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_outline_offset
    ) AS use_counter_css_doc_css_outline_offset,
    SUM(
      metrics.counter.use_counter_css_doc_css_outline_style
    ) AS use_counter_css_doc_css_outline_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_outline_width
    ) AS use_counter_css_doc_css_outline_width,
    SUM(metrics.counter.use_counter_css_doc_css_overflow) AS use_counter_css_doc_css_overflow,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_anchor
    ) AS use_counter_css_doc_css_overflow_anchor,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_block
    ) AS use_counter_css_doc_css_overflow_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_clip_box
    ) AS use_counter_css_doc_css_overflow_clip_box,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_clip_box_block
    ) AS use_counter_css_doc_css_overflow_clip_box_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_clip_box_inline
    ) AS use_counter_css_doc_css_overflow_clip_box_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_clip_margin
    ) AS use_counter_css_doc_css_overflow_clip_margin,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_inline
    ) AS use_counter_css_doc_css_overflow_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_overflow_wrap
    ) AS use_counter_css_doc_css_overflow_wrap,
    SUM(metrics.counter.use_counter_css_doc_css_overflow_x) AS use_counter_css_doc_css_overflow_x,
    SUM(metrics.counter.use_counter_css_doc_css_overflow_y) AS use_counter_css_doc_css_overflow_y,
    SUM(
      metrics.counter.use_counter_css_doc_css_overscroll_behavior
    ) AS use_counter_css_doc_css_overscroll_behavior,
    SUM(
      metrics.counter.use_counter_css_doc_css_overscroll_behavior_block
    ) AS use_counter_css_doc_css_overscroll_behavior_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_overscroll_behavior_inline
    ) AS use_counter_css_doc_css_overscroll_behavior_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_overscroll_behavior_x
    ) AS use_counter_css_doc_css_overscroll_behavior_x,
    SUM(
      metrics.counter.use_counter_css_doc_css_overscroll_behavior_y
    ) AS use_counter_css_doc_css_overscroll_behavior_y,
    SUM(metrics.counter.use_counter_css_doc_css_padding) AS use_counter_css_doc_css_padding,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_block
    ) AS use_counter_css_doc_css_padding_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_block_end
    ) AS use_counter_css_doc_css_padding_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_block_start
    ) AS use_counter_css_doc_css_padding_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_bottom
    ) AS use_counter_css_doc_css_padding_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_inline
    ) AS use_counter_css_doc_css_padding_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_inline_end
    ) AS use_counter_css_doc_css_padding_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_inline_start
    ) AS use_counter_css_doc_css_padding_inline_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_left
    ) AS use_counter_css_doc_css_padding_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_padding_right
    ) AS use_counter_css_doc_css_padding_right,
    SUM(metrics.counter.use_counter_css_doc_css_padding_top) AS use_counter_css_doc_css_padding_top,
    SUM(metrics.counter.use_counter_css_doc_css_page) AS use_counter_css_doc_css_page,
    SUM(
      metrics.counter.use_counter_css_doc_css_page_break_after
    ) AS use_counter_css_doc_css_page_break_after,
    SUM(
      metrics.counter.use_counter_css_doc_css_page_break_before
    ) AS use_counter_css_doc_css_page_break_before,
    SUM(
      metrics.counter.use_counter_css_doc_css_page_break_inside
    ) AS use_counter_css_doc_css_page_break_inside,
    SUM(
      metrics.counter.use_counter_css_doc_css_page_orientation
    ) AS use_counter_css_doc_css_page_orientation,
    SUM(metrics.counter.use_counter_css_doc_css_paint_order) AS use_counter_css_doc_css_paint_order,
    SUM(metrics.counter.use_counter_css_doc_css_perspective) AS use_counter_css_doc_css_perspective,
    SUM(
      metrics.counter.use_counter_css_doc_css_perspective_origin
    ) AS use_counter_css_doc_css_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_place_content
    ) AS use_counter_css_doc_css_place_content,
    SUM(metrics.counter.use_counter_css_doc_css_place_items) AS use_counter_css_doc_css_place_items,
    SUM(metrics.counter.use_counter_css_doc_css_place_self) AS use_counter_css_doc_css_place_self,
    SUM(
      metrics.counter.use_counter_css_doc_css_pointer_events
    ) AS use_counter_css_doc_css_pointer_events,
    SUM(metrics.counter.use_counter_css_doc_css_position) AS use_counter_css_doc_css_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_print_color_adjust
    ) AS use_counter_css_doc_css_print_color_adjust,
    SUM(metrics.counter.use_counter_css_doc_css_quotes) AS use_counter_css_doc_css_quotes,
    SUM(metrics.counter.use_counter_css_doc_css_r) AS use_counter_css_doc_css_r,
    SUM(metrics.counter.use_counter_css_doc_css_resize) AS use_counter_css_doc_css_resize,
    SUM(metrics.counter.use_counter_css_doc_css_right) AS use_counter_css_doc_css_right,
    SUM(metrics.counter.use_counter_css_doc_css_rotate) AS use_counter_css_doc_css_rotate,
    SUM(metrics.counter.use_counter_css_doc_css_row_gap) AS use_counter_css_doc_css_row_gap,
    SUM(metrics.counter.use_counter_css_doc_css_ruby_align) AS use_counter_css_doc_css_ruby_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_ruby_position
    ) AS use_counter_css_doc_css_ruby_position,
    SUM(metrics.counter.use_counter_css_doc_css_rx) AS use_counter_css_doc_css_rx,
    SUM(metrics.counter.use_counter_css_doc_css_ry) AS use_counter_css_doc_css_ry,
    SUM(metrics.counter.use_counter_css_doc_css_scale) AS use_counter_css_doc_css_scale,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_behavior
    ) AS use_counter_css_doc_css_scroll_behavior,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin
    ) AS use_counter_css_doc_css_scroll_margin,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_block
    ) AS use_counter_css_doc_css_scroll_margin_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_block_end
    ) AS use_counter_css_doc_css_scroll_margin_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_block_start
    ) AS use_counter_css_doc_css_scroll_margin_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_bottom
    ) AS use_counter_css_doc_css_scroll_margin_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_inline
    ) AS use_counter_css_doc_css_scroll_margin_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_inline_end
    ) AS use_counter_css_doc_css_scroll_margin_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_inline_start
    ) AS use_counter_css_doc_css_scroll_margin_inline_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_left
    ) AS use_counter_css_doc_css_scroll_margin_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_right
    ) AS use_counter_css_doc_css_scroll_margin_right,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_margin_top
    ) AS use_counter_css_doc_css_scroll_margin_top,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding
    ) AS use_counter_css_doc_css_scroll_padding,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_block
    ) AS use_counter_css_doc_css_scroll_padding_block,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_block_end
    ) AS use_counter_css_doc_css_scroll_padding_block_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_block_start
    ) AS use_counter_css_doc_css_scroll_padding_block_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_bottom
    ) AS use_counter_css_doc_css_scroll_padding_bottom,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_inline
    ) AS use_counter_css_doc_css_scroll_padding_inline,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_inline_end
    ) AS use_counter_css_doc_css_scroll_padding_inline_end,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_inline_start
    ) AS use_counter_css_doc_css_scroll_padding_inline_start,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_left
    ) AS use_counter_css_doc_css_scroll_padding_left,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_right
    ) AS use_counter_css_doc_css_scroll_padding_right,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_padding_top
    ) AS use_counter_css_doc_css_scroll_padding_top,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_snap_align
    ) AS use_counter_css_doc_css_scroll_snap_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_snap_stop
    ) AS use_counter_css_doc_css_scroll_snap_stop,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_snap_type
    ) AS use_counter_css_doc_css_scroll_snap_type,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_timeline
    ) AS use_counter_css_doc_css_scroll_timeline,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_timeline_axis
    ) AS use_counter_css_doc_css_scroll_timeline_axis,
    SUM(
      metrics.counter.use_counter_css_doc_css_scroll_timeline_name
    ) AS use_counter_css_doc_css_scroll_timeline_name,
    SUM(
      metrics.counter.use_counter_css_doc_css_scrollbar_color
    ) AS use_counter_css_doc_css_scrollbar_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_scrollbar_gutter
    ) AS use_counter_css_doc_css_scrollbar_gutter,
    SUM(
      metrics.counter.use_counter_css_doc_css_scrollbar_width
    ) AS use_counter_css_doc_css_scrollbar_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_shape_image_threshold
    ) AS use_counter_css_doc_css_shape_image_threshold,
    SUM(
      metrics.counter.use_counter_css_doc_css_shape_margin
    ) AS use_counter_css_doc_css_shape_margin,
    SUM(
      metrics.counter.use_counter_css_doc_css_shape_outside
    ) AS use_counter_css_doc_css_shape_outside,
    SUM(
      metrics.counter.use_counter_css_doc_css_shape_rendering
    ) AS use_counter_css_doc_css_shape_rendering,
    SUM(metrics.counter.use_counter_css_doc_css_size) AS use_counter_css_doc_css_size,
    SUM(metrics.counter.use_counter_css_doc_css_stop_color) AS use_counter_css_doc_css_stop_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_stop_opacity
    ) AS use_counter_css_doc_css_stop_opacity,
    SUM(metrics.counter.use_counter_css_doc_css_stroke) AS use_counter_css_doc_css_stroke,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_dasharray
    ) AS use_counter_css_doc_css_stroke_dasharray,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_dashoffset
    ) AS use_counter_css_doc_css_stroke_dashoffset,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_linecap
    ) AS use_counter_css_doc_css_stroke_linecap,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_linejoin
    ) AS use_counter_css_doc_css_stroke_linejoin,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_miterlimit
    ) AS use_counter_css_doc_css_stroke_miterlimit,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_opacity
    ) AS use_counter_css_doc_css_stroke_opacity,
    SUM(
      metrics.counter.use_counter_css_doc_css_stroke_width
    ) AS use_counter_css_doc_css_stroke_width,
    SUM(metrics.counter.use_counter_css_doc_css_tab_size) AS use_counter_css_doc_css_tab_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_table_layout
    ) AS use_counter_css_doc_css_table_layout,
    SUM(metrics.counter.use_counter_css_doc_css_text_align) AS use_counter_css_doc_css_text_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_align_last
    ) AS use_counter_css_doc_css_text_align_last,
    SUM(metrics.counter.use_counter_css_doc_css_text_anchor) AS use_counter_css_doc_css_text_anchor,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_combine_upright
    ) AS use_counter_css_doc_css_text_combine_upright,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration
    ) AS use_counter_css_doc_css_text_decoration,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration_color
    ) AS use_counter_css_doc_css_text_decoration_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration_line
    ) AS use_counter_css_doc_css_text_decoration_line,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration_skip_ink
    ) AS use_counter_css_doc_css_text_decoration_skip_ink,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration_style
    ) AS use_counter_css_doc_css_text_decoration_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_decoration_thickness
    ) AS use_counter_css_doc_css_text_decoration_thickness,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_emphasis
    ) AS use_counter_css_doc_css_text_emphasis,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_emphasis_color
    ) AS use_counter_css_doc_css_text_emphasis_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_emphasis_position
    ) AS use_counter_css_doc_css_text_emphasis_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_emphasis_style
    ) AS use_counter_css_doc_css_text_emphasis_style,
    SUM(metrics.counter.use_counter_css_doc_css_text_indent) AS use_counter_css_doc_css_text_indent,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_justify
    ) AS use_counter_css_doc_css_text_justify,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_orientation
    ) AS use_counter_css_doc_css_text_orientation,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_overflow
    ) AS use_counter_css_doc_css_text_overflow,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_rendering
    ) AS use_counter_css_doc_css_text_rendering,
    SUM(metrics.counter.use_counter_css_doc_css_text_shadow) AS use_counter_css_doc_css_text_shadow,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_transform
    ) AS use_counter_css_doc_css_text_transform,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_underline_offset
    ) AS use_counter_css_doc_css_text_underline_offset,
    SUM(
      metrics.counter.use_counter_css_doc_css_text_underline_position
    ) AS use_counter_css_doc_css_text_underline_position,
    SUM(metrics.counter.use_counter_css_doc_css_text_wrap) AS use_counter_css_doc_css_text_wrap,
    SUM(metrics.counter.use_counter_css_doc_css_top) AS use_counter_css_doc_css_top,
    SUM(
      metrics.counter.use_counter_css_doc_css_touch_action
    ) AS use_counter_css_doc_css_touch_action,
    SUM(metrics.counter.use_counter_css_doc_css_transform) AS use_counter_css_doc_css_transform,
    SUM(
      metrics.counter.use_counter_css_doc_css_transform_box
    ) AS use_counter_css_doc_css_transform_box,
    SUM(
      metrics.counter.use_counter_css_doc_css_transform_origin
    ) AS use_counter_css_doc_css_transform_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_transform_style
    ) AS use_counter_css_doc_css_transform_style,
    SUM(metrics.counter.use_counter_css_doc_css_transition) AS use_counter_css_doc_css_transition,
    SUM(
      metrics.counter.use_counter_css_doc_css_transition_delay
    ) AS use_counter_css_doc_css_transition_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_transition_duration
    ) AS use_counter_css_doc_css_transition_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_transition_property
    ) AS use_counter_css_doc_css_transition_property,
    SUM(
      metrics.counter.use_counter_css_doc_css_transition_timing_function
    ) AS use_counter_css_doc_css_transition_timing_function,
    SUM(metrics.counter.use_counter_css_doc_css_translate) AS use_counter_css_doc_css_translate,
    SUM(
      metrics.counter.use_counter_css_doc_css_unicode_bidi
    ) AS use_counter_css_doc_css_unicode_bidi,
    SUM(metrics.counter.use_counter_css_doc_css_user_select) AS use_counter_css_doc_css_user_select,
    SUM(
      metrics.counter.use_counter_css_doc_css_vector_effect
    ) AS use_counter_css_doc_css_vector_effect,
    SUM(
      metrics.counter.use_counter_css_doc_css_vertical_align
    ) AS use_counter_css_doc_css_vertical_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_view_timeline
    ) AS use_counter_css_doc_css_view_timeline,
    SUM(
      metrics.counter.use_counter_css_doc_css_view_timeline_axis
    ) AS use_counter_css_doc_css_view_timeline_axis,
    SUM(
      metrics.counter.use_counter_css_doc_css_view_timeline_inset
    ) AS use_counter_css_doc_css_view_timeline_inset,
    SUM(
      metrics.counter.use_counter_css_doc_css_view_timeline_name
    ) AS use_counter_css_doc_css_view_timeline_name,
    SUM(metrics.counter.use_counter_css_doc_css_visibility) AS use_counter_css_doc_css_visibility,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_align_content
    ) AS use_counter_css_doc_css_webkit_align_content,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_align_items
    ) AS use_counter_css_doc_css_webkit_align_items,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_align_self
    ) AS use_counter_css_doc_css_webkit_align_self,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation
    ) AS use_counter_css_doc_css_webkit_animation,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_delay
    ) AS use_counter_css_doc_css_webkit_animation_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_direction
    ) AS use_counter_css_doc_css_webkit_animation_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_duration
    ) AS use_counter_css_doc_css_webkit_animation_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_fill_mode
    ) AS use_counter_css_doc_css_webkit_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_iteration_count
    ) AS use_counter_css_doc_css_webkit_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_name
    ) AS use_counter_css_doc_css_webkit_animation_name,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_play_state
    ) AS use_counter_css_doc_css_webkit_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_animation_timing_function
    ) AS use_counter_css_doc_css_webkit_animation_timing_function,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_appearance
    ) AS use_counter_css_doc_css_webkit_appearance,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_backface_visibility
    ) AS use_counter_css_doc_css_webkit_backface_visibility,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_background_clip
    ) AS use_counter_css_doc_css_webkit_background_clip,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_background_origin
    ) AS use_counter_css_doc_css_webkit_background_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_background_size
    ) AS use_counter_css_doc_css_webkit_background_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_bottom_left_radius
    ) AS use_counter_css_doc_css_webkit_border_bottom_left_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_bottom_right_radius
    ) AS use_counter_css_doc_css_webkit_border_bottom_right_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_image
    ) AS use_counter_css_doc_css_webkit_border_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_radius
    ) AS use_counter_css_doc_css_webkit_border_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_top_left_radius
    ) AS use_counter_css_doc_css_webkit_border_top_left_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_border_top_right_radius
    ) AS use_counter_css_doc_css_webkit_border_top_right_radius,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_align
    ) AS use_counter_css_doc_css_webkit_box_align,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_direction
    ) AS use_counter_css_doc_css_webkit_box_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_flex
    ) AS use_counter_css_doc_css_webkit_box_flex,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_ordinal_group
    ) AS use_counter_css_doc_css_webkit_box_ordinal_group,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_orient
    ) AS use_counter_css_doc_css_webkit_box_orient,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_pack
    ) AS use_counter_css_doc_css_webkit_box_pack,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_shadow
    ) AS use_counter_css_doc_css_webkit_box_shadow,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_box_sizing
    ) AS use_counter_css_doc_css_webkit_box_sizing,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_clip_path
    ) AS use_counter_css_doc_css_webkit_clip_path,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_filter
    ) AS use_counter_css_doc_css_webkit_filter,
    SUM(metrics.counter.use_counter_css_doc_css_webkit_flex) AS use_counter_css_doc_css_webkit_flex,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_basis
    ) AS use_counter_css_doc_css_webkit_flex_basis,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_direction
    ) AS use_counter_css_doc_css_webkit_flex_direction,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_flow
    ) AS use_counter_css_doc_css_webkit_flex_flow,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_grow
    ) AS use_counter_css_doc_css_webkit_flex_grow,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_shrink
    ) AS use_counter_css_doc_css_webkit_flex_shrink,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_flex_wrap
    ) AS use_counter_css_doc_css_webkit_flex_wrap,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_justify_content
    ) AS use_counter_css_doc_css_webkit_justify_content,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_line_clamp
    ) AS use_counter_css_doc_css_webkit_line_clamp,
    SUM(metrics.counter.use_counter_css_doc_css_webkit_mask) AS use_counter_css_doc_css_webkit_mask,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_clip
    ) AS use_counter_css_doc_css_webkit_mask_clip,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_composite
    ) AS use_counter_css_doc_css_webkit_mask_composite,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_image
    ) AS use_counter_css_doc_css_webkit_mask_image,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_origin
    ) AS use_counter_css_doc_css_webkit_mask_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_position
    ) AS use_counter_css_doc_css_webkit_mask_position,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_position_x
    ) AS use_counter_css_doc_css_webkit_mask_position_x,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_position_y
    ) AS use_counter_css_doc_css_webkit_mask_position_y,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_repeat
    ) AS use_counter_css_doc_css_webkit_mask_repeat,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_mask_size
    ) AS use_counter_css_doc_css_webkit_mask_size,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_order
    ) AS use_counter_css_doc_css_webkit_order,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_perspective
    ) AS use_counter_css_doc_css_webkit_perspective,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_perspective_origin
    ) AS use_counter_css_doc_css_webkit_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_fill_color
    ) AS use_counter_css_doc_css_webkit_text_fill_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_security
    ) AS use_counter_css_doc_css_webkit_text_security,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_size_adjust
    ) AS use_counter_css_doc_css_webkit_text_size_adjust,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_stroke
    ) AS use_counter_css_doc_css_webkit_text_stroke,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_stroke_color
    ) AS use_counter_css_doc_css_webkit_text_stroke_color,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_text_stroke_width
    ) AS use_counter_css_doc_css_webkit_text_stroke_width,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transform
    ) AS use_counter_css_doc_css_webkit_transform,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transform_origin
    ) AS use_counter_css_doc_css_webkit_transform_origin,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transform_style
    ) AS use_counter_css_doc_css_webkit_transform_style,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transition
    ) AS use_counter_css_doc_css_webkit_transition,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transition_delay
    ) AS use_counter_css_doc_css_webkit_transition_delay,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transition_duration
    ) AS use_counter_css_doc_css_webkit_transition_duration,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transition_property
    ) AS use_counter_css_doc_css_webkit_transition_property,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_transition_timing_function
    ) AS use_counter_css_doc_css_webkit_transition_timing_function,
    SUM(
      metrics.counter.use_counter_css_doc_css_webkit_user_select
    ) AS use_counter_css_doc_css_webkit_user_select,
    SUM(metrics.counter.use_counter_css_doc_css_white_space) AS use_counter_css_doc_css_white_space,
    SUM(metrics.counter.use_counter_css_doc_css_width) AS use_counter_css_doc_css_width,
    SUM(metrics.counter.use_counter_css_doc_css_will_change) AS use_counter_css_doc_css_will_change,
    SUM(metrics.counter.use_counter_css_doc_css_word_break) AS use_counter_css_doc_css_word_break,
    SUM(
      metrics.counter.use_counter_css_doc_css_word_spacing
    ) AS use_counter_css_doc_css_word_spacing,
    SUM(metrics.counter.use_counter_css_doc_css_word_wrap) AS use_counter_css_doc_css_word_wrap,
    SUM(
      metrics.counter.use_counter_css_doc_css_writing_mode
    ) AS use_counter_css_doc_css_writing_mode,
    SUM(metrics.counter.use_counter_css_doc_css_x) AS use_counter_css_doc_css_x,
    SUM(metrics.counter.use_counter_css_doc_css_x_lang) AS use_counter_css_doc_css_x_lang,
    SUM(metrics.counter.use_counter_css_doc_css_x_span) AS use_counter_css_doc_css_x_span,
    SUM(
      metrics.counter.use_counter_css_doc_css_x_text_scale
    ) AS use_counter_css_doc_css_x_text_scale,
    SUM(metrics.counter.use_counter_css_doc_css_y) AS use_counter_css_doc_css_y,
    SUM(metrics.counter.use_counter_css_doc_css_z_index) AS use_counter_css_doc_css_z_index,
    SUM(metrics.counter.use_counter_css_doc_css_zoom) AS use_counter_css_doc_css_zoom,
    SUM(metrics.counter.use_counter_css_doc_max_zoom) AS use_counter_css_doc_max_zoom,
    SUM(metrics.counter.use_counter_css_doc_min_zoom) AS use_counter_css_doc_min_zoom,
    SUM(metrics.counter.use_counter_css_doc_orientation) AS use_counter_css_doc_orientation,
    SUM(metrics.counter.use_counter_css_doc_orphans) AS use_counter_css_doc_orphans,
    SUM(metrics.counter.use_counter_css_doc_speak) AS use_counter_css_doc_speak,
    SUM(
      metrics.counter.use_counter_css_doc_text_size_adjust
    ) AS use_counter_css_doc_text_size_adjust,
    SUM(metrics.counter.use_counter_css_doc_user_zoom) AS use_counter_css_doc_user_zoom,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_app_region
    ) AS use_counter_css_doc_webkit_app_region,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_after
    ) AS use_counter_css_doc_webkit_border_after,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_after_color
    ) AS use_counter_css_doc_webkit_border_after_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_after_style
    ) AS use_counter_css_doc_webkit_border_after_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_after_width
    ) AS use_counter_css_doc_webkit_border_after_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_before
    ) AS use_counter_css_doc_webkit_border_before,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_before_color
    ) AS use_counter_css_doc_webkit_border_before_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_before_style
    ) AS use_counter_css_doc_webkit_border_before_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_before_width
    ) AS use_counter_css_doc_webkit_border_before_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_end
    ) AS use_counter_css_doc_webkit_border_end,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_end_color
    ) AS use_counter_css_doc_webkit_border_end_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_end_style
    ) AS use_counter_css_doc_webkit_border_end_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_end_width
    ) AS use_counter_css_doc_webkit_border_end_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_horizontal_spacing
    ) AS use_counter_css_doc_webkit_border_horizontal_spacing,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_start
    ) AS use_counter_css_doc_webkit_border_start,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_start_color
    ) AS use_counter_css_doc_webkit_border_start_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_start_style
    ) AS use_counter_css_doc_webkit_border_start_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_start_width
    ) AS use_counter_css_doc_webkit_border_start_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_border_vertical_spacing
    ) AS use_counter_css_doc_webkit_border_vertical_spacing,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_box_decoration_break
    ) AS use_counter_css_doc_webkit_box_decoration_break,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_box_reflect
    ) AS use_counter_css_doc_webkit_box_reflect,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_break_after
    ) AS use_counter_css_doc_webkit_column_break_after,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_break_before
    ) AS use_counter_css_doc_webkit_column_break_before,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_break_inside
    ) AS use_counter_css_doc_webkit_column_break_inside,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_count
    ) AS use_counter_css_doc_webkit_column_count,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_gap
    ) AS use_counter_css_doc_webkit_column_gap,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_rule
    ) AS use_counter_css_doc_webkit_column_rule,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_rule_color
    ) AS use_counter_css_doc_webkit_column_rule_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_rule_style
    ) AS use_counter_css_doc_webkit_column_rule_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_rule_width
    ) AS use_counter_css_doc_webkit_column_rule_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_span
    ) AS use_counter_css_doc_webkit_column_span,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_column_width
    ) AS use_counter_css_doc_webkit_column_width,
    SUM(metrics.counter.use_counter_css_doc_webkit_columns) AS use_counter_css_doc_webkit_columns,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_font_feature_settings
    ) AS use_counter_css_doc_webkit_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_font_size_delta
    ) AS use_counter_css_doc_webkit_font_size_delta,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_font_smoothing
    ) AS use_counter_css_doc_webkit_font_smoothing,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_highlight
    ) AS use_counter_css_doc_webkit_highlight,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_hyphenate_character
    ) AS use_counter_css_doc_webkit_hyphenate_character,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_line_break
    ) AS use_counter_css_doc_webkit_line_break,
    SUM(metrics.counter.use_counter_css_doc_webkit_locale) AS use_counter_css_doc_webkit_locale,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_logical_height
    ) AS use_counter_css_doc_webkit_logical_height,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_logical_width
    ) AS use_counter_css_doc_webkit_logical_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_after
    ) AS use_counter_css_doc_webkit_margin_after,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_after_collapse
    ) AS use_counter_css_doc_webkit_margin_after_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_before
    ) AS use_counter_css_doc_webkit_margin_before,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_before_collapse
    ) AS use_counter_css_doc_webkit_margin_before_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_bottom_collapse
    ) AS use_counter_css_doc_webkit_margin_bottom_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_collapse
    ) AS use_counter_css_doc_webkit_margin_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_end
    ) AS use_counter_css_doc_webkit_margin_end,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_start
    ) AS use_counter_css_doc_webkit_margin_start,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_margin_top_collapse
    ) AS use_counter_css_doc_webkit_margin_top_collapse,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image
    ) AS use_counter_css_doc_webkit_mask_box_image,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image_outset
    ) AS use_counter_css_doc_webkit_mask_box_image_outset,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image_repeat
    ) AS use_counter_css_doc_webkit_mask_box_image_repeat,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image_slice
    ) AS use_counter_css_doc_webkit_mask_box_image_slice,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image_source
    ) AS use_counter_css_doc_webkit_mask_box_image_source,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_box_image_width
    ) AS use_counter_css_doc_webkit_mask_box_image_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_repeat_x
    ) AS use_counter_css_doc_webkit_mask_repeat_x,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_mask_repeat_y
    ) AS use_counter_css_doc_webkit_mask_repeat_y,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_max_logical_height
    ) AS use_counter_css_doc_webkit_max_logical_height,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_max_logical_width
    ) AS use_counter_css_doc_webkit_max_logical_width,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_min_logical_height
    ) AS use_counter_css_doc_webkit_min_logical_height,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_min_logical_width
    ) AS use_counter_css_doc_webkit_min_logical_width,
    SUM(metrics.counter.use_counter_css_doc_webkit_opacity) AS use_counter_css_doc_webkit_opacity,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_padding_after
    ) AS use_counter_css_doc_webkit_padding_after,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_padding_before
    ) AS use_counter_css_doc_webkit_padding_before,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_padding_end
    ) AS use_counter_css_doc_webkit_padding_end,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_padding_start
    ) AS use_counter_css_doc_webkit_padding_start,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_perspective_origin_x
    ) AS use_counter_css_doc_webkit_perspective_origin_x,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_perspective_origin_y
    ) AS use_counter_css_doc_webkit_perspective_origin_y,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_print_color_adjust
    ) AS use_counter_css_doc_webkit_print_color_adjust,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_rtl_ordering
    ) AS use_counter_css_doc_webkit_rtl_ordering,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_ruby_position
    ) AS use_counter_css_doc_webkit_ruby_position,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_shape_image_threshold
    ) AS use_counter_css_doc_webkit_shape_image_threshold,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_shape_margin
    ) AS use_counter_css_doc_webkit_shape_margin,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_shape_outside
    ) AS use_counter_css_doc_webkit_shape_outside,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_tap_highlight_color
    ) AS use_counter_css_doc_webkit_tap_highlight_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_combine
    ) AS use_counter_css_doc_webkit_text_combine,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_decorations_in_effect
    ) AS use_counter_css_doc_webkit_text_decorations_in_effect,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_emphasis
    ) AS use_counter_css_doc_webkit_text_emphasis,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_emphasis_color
    ) AS use_counter_css_doc_webkit_text_emphasis_color,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_emphasis_position
    ) AS use_counter_css_doc_webkit_text_emphasis_position,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_emphasis_style
    ) AS use_counter_css_doc_webkit_text_emphasis_style,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_text_orientation
    ) AS use_counter_css_doc_webkit_text_orientation,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_transform_origin_x
    ) AS use_counter_css_doc_webkit_transform_origin_x,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_transform_origin_y
    ) AS use_counter_css_doc_webkit_transform_origin_y,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_transform_origin_z
    ) AS use_counter_css_doc_webkit_transform_origin_z,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_user_drag
    ) AS use_counter_css_doc_webkit_user_drag,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_user_modify
    ) AS use_counter_css_doc_webkit_user_modify,
    SUM(
      metrics.counter.use_counter_css_doc_webkit_writing_mode
    ) AS use_counter_css_doc_webkit_writing_mode,
    SUM(metrics.counter.use_counter_css_doc_widows) AS use_counter_css_doc_widows,
    SUM(
      metrics.counter.use_counter_css_page_alignment_baseline
    ) AS use_counter_css_page_alignment_baseline,
    SUM(
      metrics.counter.use_counter_css_page_background_repeat_x
    ) AS use_counter_css_page_background_repeat_x,
    SUM(
      metrics.counter.use_counter_css_page_background_repeat_y
    ) AS use_counter_css_page_background_repeat_y,
    SUM(metrics.counter.use_counter_css_page_baseline_shift) AS use_counter_css_page_baseline_shift,
    SUM(
      metrics.counter.use_counter_css_page_buffered_rendering
    ) AS use_counter_css_page_buffered_rendering,
    SUM(
      metrics.counter.use_counter_css_page_color_rendering
    ) AS use_counter_css_page_color_rendering,
    SUM(
      metrics.counter.use_counter_css_page_css_accent_color
    ) AS use_counter_css_page_css_accent_color,
    SUM(
      metrics.counter.use_counter_css_page_css_align_content
    ) AS use_counter_css_page_css_align_content,
    SUM(
      metrics.counter.use_counter_css_page_css_align_items
    ) AS use_counter_css_page_css_align_items,
    SUM(metrics.counter.use_counter_css_page_css_align_self) AS use_counter_css_page_css_align_self,
    SUM(
      metrics.counter.use_counter_css_page_css_align_tracks
    ) AS use_counter_css_page_css_align_tracks,
    SUM(metrics.counter.use_counter_css_page_css_all) AS use_counter_css_page_css_all,
    SUM(metrics.counter.use_counter_css_page_css_animation) AS use_counter_css_page_css_animation,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_composition
    ) AS use_counter_css_page_css_animation_composition,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_delay
    ) AS use_counter_css_page_css_animation_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_direction
    ) AS use_counter_css_page_css_animation_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_duration
    ) AS use_counter_css_page_css_animation_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_fill_mode
    ) AS use_counter_css_page_css_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_iteration_count
    ) AS use_counter_css_page_css_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_name
    ) AS use_counter_css_page_css_animation_name,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_play_state
    ) AS use_counter_css_page_css_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_timeline
    ) AS use_counter_css_page_css_animation_timeline,
    SUM(
      metrics.counter.use_counter_css_page_css_animation_timing_function
    ) AS use_counter_css_page_css_animation_timing_function,
    SUM(metrics.counter.use_counter_css_page_css_appearance) AS use_counter_css_page_css_appearance,
    SUM(
      metrics.counter.use_counter_css_page_css_aspect_ratio
    ) AS use_counter_css_page_css_aspect_ratio,
    SUM(
      metrics.counter.use_counter_css_page_css_backdrop_filter
    ) AS use_counter_css_page_css_backdrop_filter,
    SUM(
      metrics.counter.use_counter_css_page_css_backface_visibility
    ) AS use_counter_css_page_css_backface_visibility,
    SUM(metrics.counter.use_counter_css_page_css_background) AS use_counter_css_page_css_background,
    SUM(
      metrics.counter.use_counter_css_page_css_background_attachment
    ) AS use_counter_css_page_css_background_attachment,
    SUM(
      metrics.counter.use_counter_css_page_css_background_blend_mode
    ) AS use_counter_css_page_css_background_blend_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_background_clip
    ) AS use_counter_css_page_css_background_clip,
    SUM(
      metrics.counter.use_counter_css_page_css_background_color
    ) AS use_counter_css_page_css_background_color,
    SUM(
      metrics.counter.use_counter_css_page_css_background_image
    ) AS use_counter_css_page_css_background_image,
    SUM(
      metrics.counter.use_counter_css_page_css_background_origin
    ) AS use_counter_css_page_css_background_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_background_position
    ) AS use_counter_css_page_css_background_position,
    SUM(
      metrics.counter.use_counter_css_page_css_background_position_x
    ) AS use_counter_css_page_css_background_position_x,
    SUM(
      metrics.counter.use_counter_css_page_css_background_position_y
    ) AS use_counter_css_page_css_background_position_y,
    SUM(
      metrics.counter.use_counter_css_page_css_background_repeat
    ) AS use_counter_css_page_css_background_repeat,
    SUM(
      metrics.counter.use_counter_css_page_css_background_size
    ) AS use_counter_css_page_css_background_size,
    SUM(
      metrics.counter.use_counter_css_page_css_baseline_source
    ) AS use_counter_css_page_css_baseline_source,
    SUM(metrics.counter.use_counter_css_page_css_block_size) AS use_counter_css_page_css_block_size,
    SUM(metrics.counter.use_counter_css_page_css_border) AS use_counter_css_page_css_border,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block
    ) AS use_counter_css_page_css_border_block,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_color
    ) AS use_counter_css_page_css_border_block_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_end
    ) AS use_counter_css_page_css_border_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_end_color
    ) AS use_counter_css_page_css_border_block_end_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_end_style
    ) AS use_counter_css_page_css_border_block_end_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_end_width
    ) AS use_counter_css_page_css_border_block_end_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_start
    ) AS use_counter_css_page_css_border_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_start_color
    ) AS use_counter_css_page_css_border_block_start_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_start_style
    ) AS use_counter_css_page_css_border_block_start_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_start_width
    ) AS use_counter_css_page_css_border_block_start_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_style
    ) AS use_counter_css_page_css_border_block_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_block_width
    ) AS use_counter_css_page_css_border_block_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom
    ) AS use_counter_css_page_css_border_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom_color
    ) AS use_counter_css_page_css_border_bottom_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom_left_radius
    ) AS use_counter_css_page_css_border_bottom_left_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom_right_radius
    ) AS use_counter_css_page_css_border_bottom_right_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom_style
    ) AS use_counter_css_page_css_border_bottom_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_bottom_width
    ) AS use_counter_css_page_css_border_bottom_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_collapse
    ) AS use_counter_css_page_css_border_collapse,
    SUM(
      metrics.counter.use_counter_css_page_css_border_color
    ) AS use_counter_css_page_css_border_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_end_end_radius
    ) AS use_counter_css_page_css_border_end_end_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_end_start_radius
    ) AS use_counter_css_page_css_border_end_start_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image
    ) AS use_counter_css_page_css_border_image,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image_outset
    ) AS use_counter_css_page_css_border_image_outset,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image_repeat
    ) AS use_counter_css_page_css_border_image_repeat,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image_slice
    ) AS use_counter_css_page_css_border_image_slice,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image_source
    ) AS use_counter_css_page_css_border_image_source,
    SUM(
      metrics.counter.use_counter_css_page_css_border_image_width
    ) AS use_counter_css_page_css_border_image_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline
    ) AS use_counter_css_page_css_border_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_color
    ) AS use_counter_css_page_css_border_inline_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_end
    ) AS use_counter_css_page_css_border_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_end_color
    ) AS use_counter_css_page_css_border_inline_end_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_end_style
    ) AS use_counter_css_page_css_border_inline_end_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_end_width
    ) AS use_counter_css_page_css_border_inline_end_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_start
    ) AS use_counter_css_page_css_border_inline_start,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_start_color
    ) AS use_counter_css_page_css_border_inline_start_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_start_style
    ) AS use_counter_css_page_css_border_inline_start_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_start_width
    ) AS use_counter_css_page_css_border_inline_start_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_style
    ) AS use_counter_css_page_css_border_inline_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_inline_width
    ) AS use_counter_css_page_css_border_inline_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_left
    ) AS use_counter_css_page_css_border_left,
    SUM(
      metrics.counter.use_counter_css_page_css_border_left_color
    ) AS use_counter_css_page_css_border_left_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_left_style
    ) AS use_counter_css_page_css_border_left_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_left_width
    ) AS use_counter_css_page_css_border_left_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_radius
    ) AS use_counter_css_page_css_border_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_right
    ) AS use_counter_css_page_css_border_right,
    SUM(
      metrics.counter.use_counter_css_page_css_border_right_color
    ) AS use_counter_css_page_css_border_right_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_right_style
    ) AS use_counter_css_page_css_border_right_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_right_width
    ) AS use_counter_css_page_css_border_right_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_spacing
    ) AS use_counter_css_page_css_border_spacing,
    SUM(
      metrics.counter.use_counter_css_page_css_border_start_end_radius
    ) AS use_counter_css_page_css_border_start_end_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_start_start_radius
    ) AS use_counter_css_page_css_border_start_start_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_style
    ) AS use_counter_css_page_css_border_style,
    SUM(metrics.counter.use_counter_css_page_css_border_top) AS use_counter_css_page_css_border_top,
    SUM(
      metrics.counter.use_counter_css_page_css_border_top_color
    ) AS use_counter_css_page_css_border_top_color,
    SUM(
      metrics.counter.use_counter_css_page_css_border_top_left_radius
    ) AS use_counter_css_page_css_border_top_left_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_top_right_radius
    ) AS use_counter_css_page_css_border_top_right_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_border_top_style
    ) AS use_counter_css_page_css_border_top_style,
    SUM(
      metrics.counter.use_counter_css_page_css_border_top_width
    ) AS use_counter_css_page_css_border_top_width,
    SUM(
      metrics.counter.use_counter_css_page_css_border_width
    ) AS use_counter_css_page_css_border_width,
    SUM(metrics.counter.use_counter_css_page_css_bottom) AS use_counter_css_page_css_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_box_decoration_break
    ) AS use_counter_css_page_css_box_decoration_break,
    SUM(metrics.counter.use_counter_css_page_css_box_shadow) AS use_counter_css_page_css_box_shadow,
    SUM(metrics.counter.use_counter_css_page_css_box_sizing) AS use_counter_css_page_css_box_sizing,
    SUM(
      metrics.counter.use_counter_css_page_css_break_after
    ) AS use_counter_css_page_css_break_after,
    SUM(
      metrics.counter.use_counter_css_page_css_break_before
    ) AS use_counter_css_page_css_break_before,
    SUM(
      metrics.counter.use_counter_css_page_css_break_inside
    ) AS use_counter_css_page_css_break_inside,
    SUM(
      metrics.counter.use_counter_css_page_css_caption_side
    ) AS use_counter_css_page_css_caption_side,
    SUM(
      metrics.counter.use_counter_css_page_css_caret_color
    ) AS use_counter_css_page_css_caret_color,
    SUM(metrics.counter.use_counter_css_page_css_clear) AS use_counter_css_page_css_clear,
    SUM(metrics.counter.use_counter_css_page_css_clip) AS use_counter_css_page_css_clip,
    SUM(metrics.counter.use_counter_css_page_css_clip_path) AS use_counter_css_page_css_clip_path,
    SUM(metrics.counter.use_counter_css_page_css_clip_rule) AS use_counter_css_page_css_clip_rule,
    SUM(metrics.counter.use_counter_css_page_css_color) AS use_counter_css_page_css_color,
    SUM(
      metrics.counter.use_counter_css_page_css_color_adjust
    ) AS use_counter_css_page_css_color_adjust,
    SUM(
      metrics.counter.use_counter_css_page_css_color_interpolation
    ) AS use_counter_css_page_css_color_interpolation,
    SUM(
      metrics.counter.use_counter_css_page_css_color_interpolation_filters
    ) AS use_counter_css_page_css_color_interpolation_filters,
    SUM(
      metrics.counter.use_counter_css_page_css_color_scheme
    ) AS use_counter_css_page_css_color_scheme,
    SUM(
      metrics.counter.use_counter_css_page_css_column_count
    ) AS use_counter_css_page_css_column_count,
    SUM(
      metrics.counter.use_counter_css_page_css_column_fill
    ) AS use_counter_css_page_css_column_fill,
    SUM(metrics.counter.use_counter_css_page_css_column_gap) AS use_counter_css_page_css_column_gap,
    SUM(
      metrics.counter.use_counter_css_page_css_column_rule
    ) AS use_counter_css_page_css_column_rule,
    SUM(
      metrics.counter.use_counter_css_page_css_column_rule_color
    ) AS use_counter_css_page_css_column_rule_color,
    SUM(
      metrics.counter.use_counter_css_page_css_column_rule_style
    ) AS use_counter_css_page_css_column_rule_style,
    SUM(
      metrics.counter.use_counter_css_page_css_column_rule_width
    ) AS use_counter_css_page_css_column_rule_width,
    SUM(
      metrics.counter.use_counter_css_page_css_column_span
    ) AS use_counter_css_page_css_column_span,
    SUM(
      metrics.counter.use_counter_css_page_css_column_width
    ) AS use_counter_css_page_css_column_width,
    SUM(metrics.counter.use_counter_css_page_css_columns) AS use_counter_css_page_css_columns,
    SUM(metrics.counter.use_counter_css_page_css_contain) AS use_counter_css_page_css_contain,
    SUM(
      metrics.counter.use_counter_css_page_css_contain_intrinsic_block_size
    ) AS use_counter_css_page_css_contain_intrinsic_block_size,
    SUM(
      metrics.counter.use_counter_css_page_css_contain_intrinsic_height
    ) AS use_counter_css_page_css_contain_intrinsic_height,
    SUM(
      metrics.counter.use_counter_css_page_css_contain_intrinsic_inline_size
    ) AS use_counter_css_page_css_contain_intrinsic_inline_size,
    SUM(
      metrics.counter.use_counter_css_page_css_contain_intrinsic_size
    ) AS use_counter_css_page_css_contain_intrinsic_size,
    SUM(
      metrics.counter.use_counter_css_page_css_contain_intrinsic_width
    ) AS use_counter_css_page_css_contain_intrinsic_width,
    SUM(metrics.counter.use_counter_css_page_css_container) AS use_counter_css_page_css_container,
    SUM(
      metrics.counter.use_counter_css_page_css_container_name
    ) AS use_counter_css_page_css_container_name,
    SUM(
      metrics.counter.use_counter_css_page_css_container_type
    ) AS use_counter_css_page_css_container_type,
    SUM(metrics.counter.use_counter_css_page_css_content) AS use_counter_css_page_css_content,
    SUM(
      metrics.counter.use_counter_css_page_css_content_visibility
    ) AS use_counter_css_page_css_content_visibility,
    SUM(
      metrics.counter.use_counter_css_page_css_counter_increment
    ) AS use_counter_css_page_css_counter_increment,
    SUM(
      metrics.counter.use_counter_css_page_css_counter_reset
    ) AS use_counter_css_page_css_counter_reset,
    SUM(
      metrics.counter.use_counter_css_page_css_counter_set
    ) AS use_counter_css_page_css_counter_set,
    SUM(metrics.counter.use_counter_css_page_css_cursor) AS use_counter_css_page_css_cursor,
    SUM(metrics.counter.use_counter_css_page_css_cx) AS use_counter_css_page_css_cx,
    SUM(metrics.counter.use_counter_css_page_css_cy) AS use_counter_css_page_css_cy,
    SUM(metrics.counter.use_counter_css_page_css_d) AS use_counter_css_page_css_d,
    SUM(metrics.counter.use_counter_css_page_css_direction) AS use_counter_css_page_css_direction,
    SUM(metrics.counter.use_counter_css_page_css_display) AS use_counter_css_page_css_display,
    SUM(
      metrics.counter.use_counter_css_page_css_dominant_baseline
    ) AS use_counter_css_page_css_dominant_baseline,
    SUM(
      metrics.counter.use_counter_css_page_css_empty_cells
    ) AS use_counter_css_page_css_empty_cells,
    SUM(metrics.counter.use_counter_css_page_css_fill) AS use_counter_css_page_css_fill,
    SUM(
      metrics.counter.use_counter_css_page_css_fill_opacity
    ) AS use_counter_css_page_css_fill_opacity,
    SUM(metrics.counter.use_counter_css_page_css_fill_rule) AS use_counter_css_page_css_fill_rule,
    SUM(metrics.counter.use_counter_css_page_css_filter) AS use_counter_css_page_css_filter,
    SUM(metrics.counter.use_counter_css_page_css_flex) AS use_counter_css_page_css_flex,
    SUM(metrics.counter.use_counter_css_page_css_flex_basis) AS use_counter_css_page_css_flex_basis,
    SUM(
      metrics.counter.use_counter_css_page_css_flex_direction
    ) AS use_counter_css_page_css_flex_direction,
    SUM(metrics.counter.use_counter_css_page_css_flex_flow) AS use_counter_css_page_css_flex_flow,
    SUM(metrics.counter.use_counter_css_page_css_flex_grow) AS use_counter_css_page_css_flex_grow,
    SUM(
      metrics.counter.use_counter_css_page_css_flex_shrink
    ) AS use_counter_css_page_css_flex_shrink,
    SUM(metrics.counter.use_counter_css_page_css_flex_wrap) AS use_counter_css_page_css_flex_wrap,
    SUM(metrics.counter.use_counter_css_page_css_float) AS use_counter_css_page_css_float,
    SUM(
      metrics.counter.use_counter_css_page_css_flood_color
    ) AS use_counter_css_page_css_flood_color,
    SUM(
      metrics.counter.use_counter_css_page_css_flood_opacity
    ) AS use_counter_css_page_css_flood_opacity,
    SUM(metrics.counter.use_counter_css_page_css_font) AS use_counter_css_page_css_font,
    SUM(
      metrics.counter.use_counter_css_page_css_font_family
    ) AS use_counter_css_page_css_font_family,
    SUM(
      metrics.counter.use_counter_css_page_css_font_feature_settings
    ) AS use_counter_css_page_css_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_page_css_font_kerning
    ) AS use_counter_css_page_css_font_kerning,
    SUM(
      metrics.counter.use_counter_css_page_css_font_language_override
    ) AS use_counter_css_page_css_font_language_override,
    SUM(
      metrics.counter.use_counter_css_page_css_font_optical_sizing
    ) AS use_counter_css_page_css_font_optical_sizing,
    SUM(
      metrics.counter.use_counter_css_page_css_font_palette
    ) AS use_counter_css_page_css_font_palette,
    SUM(metrics.counter.use_counter_css_page_css_font_size) AS use_counter_css_page_css_font_size,
    SUM(
      metrics.counter.use_counter_css_page_css_font_size_adjust
    ) AS use_counter_css_page_css_font_size_adjust,
    SUM(
      metrics.counter.use_counter_css_page_css_font_stretch
    ) AS use_counter_css_page_css_font_stretch,
    SUM(metrics.counter.use_counter_css_page_css_font_style) AS use_counter_css_page_css_font_style,
    SUM(
      metrics.counter.use_counter_css_page_css_font_synthesis
    ) AS use_counter_css_page_css_font_synthesis,
    SUM(
      metrics.counter.use_counter_css_page_css_font_synthesis_position
    ) AS use_counter_css_page_css_font_synthesis_position,
    SUM(
      metrics.counter.use_counter_css_page_css_font_synthesis_small_caps
    ) AS use_counter_css_page_css_font_synthesis_small_caps,
    SUM(
      metrics.counter.use_counter_css_page_css_font_synthesis_style
    ) AS use_counter_css_page_css_font_synthesis_style,
    SUM(
      metrics.counter.use_counter_css_page_css_font_synthesis_weight
    ) AS use_counter_css_page_css_font_synthesis_weight,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant
    ) AS use_counter_css_page_css_font_variant,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_alternates
    ) AS use_counter_css_page_css_font_variant_alternates,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_caps
    ) AS use_counter_css_page_css_font_variant_caps,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_east_asian
    ) AS use_counter_css_page_css_font_variant_east_asian,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_emoji
    ) AS use_counter_css_page_css_font_variant_emoji,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_ligatures
    ) AS use_counter_css_page_css_font_variant_ligatures,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_numeric
    ) AS use_counter_css_page_css_font_variant_numeric,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variant_position
    ) AS use_counter_css_page_css_font_variant_position,
    SUM(
      metrics.counter.use_counter_css_page_css_font_variation_settings
    ) AS use_counter_css_page_css_font_variation_settings,
    SUM(
      metrics.counter.use_counter_css_page_css_font_weight
    ) AS use_counter_css_page_css_font_weight,
    SUM(
      metrics.counter.use_counter_css_page_css_forced_color_adjust
    ) AS use_counter_css_page_css_forced_color_adjust,
    SUM(metrics.counter.use_counter_css_page_css_gap) AS use_counter_css_page_css_gap,
    SUM(metrics.counter.use_counter_css_page_css_grid) AS use_counter_css_page_css_grid,
    SUM(metrics.counter.use_counter_css_page_css_grid_area) AS use_counter_css_page_css_grid_area,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_auto_columns
    ) AS use_counter_css_page_css_grid_auto_columns,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_auto_flow
    ) AS use_counter_css_page_css_grid_auto_flow,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_auto_rows
    ) AS use_counter_css_page_css_grid_auto_rows,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_column
    ) AS use_counter_css_page_css_grid_column,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_column_end
    ) AS use_counter_css_page_css_grid_column_end,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_column_gap
    ) AS use_counter_css_page_css_grid_column_gap,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_column_start
    ) AS use_counter_css_page_css_grid_column_start,
    SUM(metrics.counter.use_counter_css_page_css_grid_gap) AS use_counter_css_page_css_grid_gap,
    SUM(metrics.counter.use_counter_css_page_css_grid_row) AS use_counter_css_page_css_grid_row,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_row_end
    ) AS use_counter_css_page_css_grid_row_end,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_row_gap
    ) AS use_counter_css_page_css_grid_row_gap,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_row_start
    ) AS use_counter_css_page_css_grid_row_start,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_template
    ) AS use_counter_css_page_css_grid_template,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_template_areas
    ) AS use_counter_css_page_css_grid_template_areas,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_template_columns
    ) AS use_counter_css_page_css_grid_template_columns,
    SUM(
      metrics.counter.use_counter_css_page_css_grid_template_rows
    ) AS use_counter_css_page_css_grid_template_rows,
    SUM(metrics.counter.use_counter_css_page_css_height) AS use_counter_css_page_css_height,
    SUM(
      metrics.counter.use_counter_css_page_css_hyphenate_character
    ) AS use_counter_css_page_css_hyphenate_character,
    SUM(metrics.counter.use_counter_css_page_css_hyphens) AS use_counter_css_page_css_hyphens,
    SUM(
      metrics.counter.use_counter_css_page_css_image_orientation
    ) AS use_counter_css_page_css_image_orientation,
    SUM(
      metrics.counter.use_counter_css_page_css_image_rendering
    ) AS use_counter_css_page_css_image_rendering,
    SUM(metrics.counter.use_counter_css_page_css_ime_mode) AS use_counter_css_page_css_ime_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_initial_letter
    ) AS use_counter_css_page_css_initial_letter,
    SUM(
      metrics.counter.use_counter_css_page_css_inline_size
    ) AS use_counter_css_page_css_inline_size,
    SUM(metrics.counter.use_counter_css_page_css_inset) AS use_counter_css_page_css_inset,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_block
    ) AS use_counter_css_page_css_inset_block,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_block_end
    ) AS use_counter_css_page_css_inset_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_block_start
    ) AS use_counter_css_page_css_inset_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_inline
    ) AS use_counter_css_page_css_inset_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_inline_end
    ) AS use_counter_css_page_css_inset_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_inset_inline_start
    ) AS use_counter_css_page_css_inset_inline_start,
    SUM(metrics.counter.use_counter_css_page_css_isolation) AS use_counter_css_page_css_isolation,
    SUM(
      metrics.counter.use_counter_css_page_css_justify_content
    ) AS use_counter_css_page_css_justify_content,
    SUM(
      metrics.counter.use_counter_css_page_css_justify_items
    ) AS use_counter_css_page_css_justify_items,
    SUM(
      metrics.counter.use_counter_css_page_css_justify_self
    ) AS use_counter_css_page_css_justify_self,
    SUM(
      metrics.counter.use_counter_css_page_css_justify_tracks
    ) AS use_counter_css_page_css_justify_tracks,
    SUM(metrics.counter.use_counter_css_page_css_left) AS use_counter_css_page_css_left,
    SUM(
      metrics.counter.use_counter_css_page_css_letter_spacing
    ) AS use_counter_css_page_css_letter_spacing,
    SUM(
      metrics.counter.use_counter_css_page_css_lighting_color
    ) AS use_counter_css_page_css_lighting_color,
    SUM(metrics.counter.use_counter_css_page_css_line_break) AS use_counter_css_page_css_line_break,
    SUM(
      metrics.counter.use_counter_css_page_css_line_height
    ) AS use_counter_css_page_css_line_height,
    SUM(metrics.counter.use_counter_css_page_css_list_style) AS use_counter_css_page_css_list_style,
    SUM(
      metrics.counter.use_counter_css_page_css_list_style_image
    ) AS use_counter_css_page_css_list_style_image,
    SUM(
      metrics.counter.use_counter_css_page_css_list_style_position
    ) AS use_counter_css_page_css_list_style_position,
    SUM(
      metrics.counter.use_counter_css_page_css_list_style_type
    ) AS use_counter_css_page_css_list_style_type,
    SUM(metrics.counter.use_counter_css_page_css_margin) AS use_counter_css_page_css_margin,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_block
    ) AS use_counter_css_page_css_margin_block,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_block_end
    ) AS use_counter_css_page_css_margin_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_block_start
    ) AS use_counter_css_page_css_margin_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_bottom
    ) AS use_counter_css_page_css_margin_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_inline
    ) AS use_counter_css_page_css_margin_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_inline_end
    ) AS use_counter_css_page_css_margin_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_inline_start
    ) AS use_counter_css_page_css_margin_inline_start,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_left
    ) AS use_counter_css_page_css_margin_left,
    SUM(
      metrics.counter.use_counter_css_page_css_margin_right
    ) AS use_counter_css_page_css_margin_right,
    SUM(metrics.counter.use_counter_css_page_css_margin_top) AS use_counter_css_page_css_margin_top,
    SUM(metrics.counter.use_counter_css_page_css_marker) AS use_counter_css_page_css_marker,
    SUM(metrics.counter.use_counter_css_page_css_marker_end) AS use_counter_css_page_css_marker_end,
    SUM(metrics.counter.use_counter_css_page_css_marker_mid) AS use_counter_css_page_css_marker_mid,
    SUM(
      metrics.counter.use_counter_css_page_css_marker_start
    ) AS use_counter_css_page_css_marker_start,
    SUM(metrics.counter.use_counter_css_page_css_mask) AS use_counter_css_page_css_mask,
    SUM(metrics.counter.use_counter_css_page_css_mask_clip) AS use_counter_css_page_css_mask_clip,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_composite
    ) AS use_counter_css_page_css_mask_composite,
    SUM(metrics.counter.use_counter_css_page_css_mask_image) AS use_counter_css_page_css_mask_image,
    SUM(metrics.counter.use_counter_css_page_css_mask_mode) AS use_counter_css_page_css_mask_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_origin
    ) AS use_counter_css_page_css_mask_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_position
    ) AS use_counter_css_page_css_mask_position,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_position_x
    ) AS use_counter_css_page_css_mask_position_x,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_position_y
    ) AS use_counter_css_page_css_mask_position_y,
    SUM(
      metrics.counter.use_counter_css_page_css_mask_repeat
    ) AS use_counter_css_page_css_mask_repeat,
    SUM(metrics.counter.use_counter_css_page_css_mask_size) AS use_counter_css_page_css_mask_size,
    SUM(metrics.counter.use_counter_css_page_css_mask_type) AS use_counter_css_page_css_mask_type,
    SUM(
      metrics.counter.use_counter_css_page_css_masonry_auto_flow
    ) AS use_counter_css_page_css_masonry_auto_flow,
    SUM(metrics.counter.use_counter_css_page_css_math_depth) AS use_counter_css_page_css_math_depth,
    SUM(metrics.counter.use_counter_css_page_css_math_style) AS use_counter_css_page_css_math_style,
    SUM(
      metrics.counter.use_counter_css_page_css_max_block_size
    ) AS use_counter_css_page_css_max_block_size,
    SUM(metrics.counter.use_counter_css_page_css_max_height) AS use_counter_css_page_css_max_height,
    SUM(
      metrics.counter.use_counter_css_page_css_max_inline_size
    ) AS use_counter_css_page_css_max_inline_size,
    SUM(metrics.counter.use_counter_css_page_css_max_width) AS use_counter_css_page_css_max_width,
    SUM(
      metrics.counter.use_counter_css_page_css_min_block_size
    ) AS use_counter_css_page_css_min_block_size,
    SUM(metrics.counter.use_counter_css_page_css_min_height) AS use_counter_css_page_css_min_height,
    SUM(
      metrics.counter.use_counter_css_page_css_min_inline_size
    ) AS use_counter_css_page_css_min_inline_size,
    SUM(metrics.counter.use_counter_css_page_css_min_width) AS use_counter_css_page_css_min_width,
    SUM(
      metrics.counter.use_counter_css_page_css_mix_blend_mode
    ) AS use_counter_css_page_css_mix_blend_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation
    ) AS use_counter_css_page_css_moz_animation,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_delay
    ) AS use_counter_css_page_css_moz_animation_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_direction
    ) AS use_counter_css_page_css_moz_animation_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_duration
    ) AS use_counter_css_page_css_moz_animation_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_fill_mode
    ) AS use_counter_css_page_css_moz_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_iteration_count
    ) AS use_counter_css_page_css_moz_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_name
    ) AS use_counter_css_page_css_moz_animation_name,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_play_state
    ) AS use_counter_css_page_css_moz_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_animation_timing_function
    ) AS use_counter_css_page_css_moz_animation_timing_function,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_appearance
    ) AS use_counter_css_page_css_moz_appearance,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_backface_visibility
    ) AS use_counter_css_page_css_moz_backface_visibility,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_end
    ) AS use_counter_css_page_css_moz_border_end,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_end_color
    ) AS use_counter_css_page_css_moz_border_end_color,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_end_style
    ) AS use_counter_css_page_css_moz_border_end_style,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_end_width
    ) AS use_counter_css_page_css_moz_border_end_width,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_image
    ) AS use_counter_css_page_css_moz_border_image,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_start
    ) AS use_counter_css_page_css_moz_border_start,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_start_color
    ) AS use_counter_css_page_css_moz_border_start_color,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_start_style
    ) AS use_counter_css_page_css_moz_border_start_style,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_border_start_width
    ) AS use_counter_css_page_css_moz_border_start_width,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_align
    ) AS use_counter_css_page_css_moz_box_align,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_collapse
    ) AS use_counter_css_page_css_moz_box_collapse,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_direction
    ) AS use_counter_css_page_css_moz_box_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_flex
    ) AS use_counter_css_page_css_moz_box_flex,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_ordinal_group
    ) AS use_counter_css_page_css_moz_box_ordinal_group,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_orient
    ) AS use_counter_css_page_css_moz_box_orient,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_pack
    ) AS use_counter_css_page_css_moz_box_pack,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_box_sizing
    ) AS use_counter_css_page_css_moz_box_sizing,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_context_properties
    ) AS use_counter_css_page_css_moz_context_properties,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_control_character_visibility
    ) AS use_counter_css_page_css_moz_control_character_visibility,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_default_appearance
    ) AS use_counter_css_page_css_moz_default_appearance,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_float_edge
    ) AS use_counter_css_page_css_moz_float_edge,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_font_feature_settings
    ) AS use_counter_css_page_css_moz_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_font_language_override
    ) AS use_counter_css_page_css_moz_font_language_override,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_force_broken_image_icon
    ) AS use_counter_css_page_css_moz_force_broken_image_icon,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_hyphens
    ) AS use_counter_css_page_css_moz_hyphens,
    SUM(metrics.counter.use_counter_css_page_css_moz_inert) AS use_counter_css_page_css_moz_inert,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_margin_end
    ) AS use_counter_css_page_css_moz_margin_end,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_margin_start
    ) AS use_counter_css_page_css_moz_margin_start,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_math_variant
    ) AS use_counter_css_page_css_moz_math_variant,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_min_font_size_ratio
    ) AS use_counter_css_page_css_moz_min_font_size_ratio,
    SUM(metrics.counter.use_counter_css_page_css_moz_orient) AS use_counter_css_page_css_moz_orient,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_osx_font_smoothing
    ) AS use_counter_css_page_css_moz_osx_font_smoothing,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_padding_end
    ) AS use_counter_css_page_css_moz_padding_end,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_padding_start
    ) AS use_counter_css_page_css_moz_padding_start,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_perspective
    ) AS use_counter_css_page_css_moz_perspective,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_perspective_origin
    ) AS use_counter_css_page_css_moz_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_subtree_hidden_only_visually
    ) AS use_counter_css_page_css_moz_subtree_hidden_only_visually,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_tab_size
    ) AS use_counter_css_page_css_moz_tab_size,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_text_size_adjust
    ) AS use_counter_css_page_css_moz_text_size_adjust,
    SUM(metrics.counter.use_counter_css_page_css_moz_theme) AS use_counter_css_page_css_moz_theme,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_top_layer
    ) AS use_counter_css_page_css_moz_top_layer,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transform
    ) AS use_counter_css_page_css_moz_transform,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transform_origin
    ) AS use_counter_css_page_css_moz_transform_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transform_style
    ) AS use_counter_css_page_css_moz_transform_style,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transition
    ) AS use_counter_css_page_css_moz_transition,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transition_delay
    ) AS use_counter_css_page_css_moz_transition_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transition_duration
    ) AS use_counter_css_page_css_moz_transition_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transition_property
    ) AS use_counter_css_page_css_moz_transition_property,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_transition_timing_function
    ) AS use_counter_css_page_css_moz_transition_timing_function,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_user_focus
    ) AS use_counter_css_page_css_moz_user_focus,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_user_input
    ) AS use_counter_css_page_css_moz_user_input,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_user_modify
    ) AS use_counter_css_page_css_moz_user_modify,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_user_select
    ) AS use_counter_css_page_css_moz_user_select,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_dragging
    ) AS use_counter_css_page_css_moz_window_dragging,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_input_region_margin
    ) AS use_counter_css_page_css_moz_window_input_region_margin,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_opacity
    ) AS use_counter_css_page_css_moz_window_opacity,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_shadow
    ) AS use_counter_css_page_css_moz_window_shadow,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_transform
    ) AS use_counter_css_page_css_moz_window_transform,
    SUM(
      metrics.counter.use_counter_css_page_css_moz_window_transform_origin
    ) AS use_counter_css_page_css_moz_window_transform_origin,
    SUM(metrics.counter.use_counter_css_page_css_object_fit) AS use_counter_css_page_css_object_fit,
    SUM(
      metrics.counter.use_counter_css_page_css_object_position
    ) AS use_counter_css_page_css_object_position,
    SUM(metrics.counter.use_counter_css_page_css_offset) AS use_counter_css_page_css_offset,
    SUM(
      metrics.counter.use_counter_css_page_css_offset_anchor
    ) AS use_counter_css_page_css_offset_anchor,
    SUM(
      metrics.counter.use_counter_css_page_css_offset_distance
    ) AS use_counter_css_page_css_offset_distance,
    SUM(
      metrics.counter.use_counter_css_page_css_offset_path
    ) AS use_counter_css_page_css_offset_path,
    SUM(
      metrics.counter.use_counter_css_page_css_offset_position
    ) AS use_counter_css_page_css_offset_position,
    SUM(
      metrics.counter.use_counter_css_page_css_offset_rotate
    ) AS use_counter_css_page_css_offset_rotate,
    SUM(metrics.counter.use_counter_css_page_css_opacity) AS use_counter_css_page_css_opacity,
    SUM(metrics.counter.use_counter_css_page_css_order) AS use_counter_css_page_css_order,
    SUM(metrics.counter.use_counter_css_page_css_outline) AS use_counter_css_page_css_outline,
    SUM(
      metrics.counter.use_counter_css_page_css_outline_color
    ) AS use_counter_css_page_css_outline_color,
    SUM(
      metrics.counter.use_counter_css_page_css_outline_offset
    ) AS use_counter_css_page_css_outline_offset,
    SUM(
      metrics.counter.use_counter_css_page_css_outline_style
    ) AS use_counter_css_page_css_outline_style,
    SUM(
      metrics.counter.use_counter_css_page_css_outline_width
    ) AS use_counter_css_page_css_outline_width,
    SUM(metrics.counter.use_counter_css_page_css_overflow) AS use_counter_css_page_css_overflow,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_anchor
    ) AS use_counter_css_page_css_overflow_anchor,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_block
    ) AS use_counter_css_page_css_overflow_block,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_clip_box
    ) AS use_counter_css_page_css_overflow_clip_box,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_clip_box_block
    ) AS use_counter_css_page_css_overflow_clip_box_block,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_clip_box_inline
    ) AS use_counter_css_page_css_overflow_clip_box_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_clip_margin
    ) AS use_counter_css_page_css_overflow_clip_margin,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_inline
    ) AS use_counter_css_page_css_overflow_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_overflow_wrap
    ) AS use_counter_css_page_css_overflow_wrap,
    SUM(metrics.counter.use_counter_css_page_css_overflow_x) AS use_counter_css_page_css_overflow_x,
    SUM(metrics.counter.use_counter_css_page_css_overflow_y) AS use_counter_css_page_css_overflow_y,
    SUM(
      metrics.counter.use_counter_css_page_css_overscroll_behavior
    ) AS use_counter_css_page_css_overscroll_behavior,
    SUM(
      metrics.counter.use_counter_css_page_css_overscroll_behavior_block
    ) AS use_counter_css_page_css_overscroll_behavior_block,
    SUM(
      metrics.counter.use_counter_css_page_css_overscroll_behavior_inline
    ) AS use_counter_css_page_css_overscroll_behavior_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_overscroll_behavior_x
    ) AS use_counter_css_page_css_overscroll_behavior_x,
    SUM(
      metrics.counter.use_counter_css_page_css_overscroll_behavior_y
    ) AS use_counter_css_page_css_overscroll_behavior_y,
    SUM(metrics.counter.use_counter_css_page_css_padding) AS use_counter_css_page_css_padding,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_block
    ) AS use_counter_css_page_css_padding_block,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_block_end
    ) AS use_counter_css_page_css_padding_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_block_start
    ) AS use_counter_css_page_css_padding_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_bottom
    ) AS use_counter_css_page_css_padding_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_inline
    ) AS use_counter_css_page_css_padding_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_inline_end
    ) AS use_counter_css_page_css_padding_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_inline_start
    ) AS use_counter_css_page_css_padding_inline_start,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_left
    ) AS use_counter_css_page_css_padding_left,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_right
    ) AS use_counter_css_page_css_padding_right,
    SUM(
      metrics.counter.use_counter_css_page_css_padding_top
    ) AS use_counter_css_page_css_padding_top,
    SUM(metrics.counter.use_counter_css_page_css_page) AS use_counter_css_page_css_page,
    SUM(
      metrics.counter.use_counter_css_page_css_page_break_after
    ) AS use_counter_css_page_css_page_break_after,
    SUM(
      metrics.counter.use_counter_css_page_css_page_break_before
    ) AS use_counter_css_page_css_page_break_before,
    SUM(
      metrics.counter.use_counter_css_page_css_page_break_inside
    ) AS use_counter_css_page_css_page_break_inside,
    SUM(
      metrics.counter.use_counter_css_page_css_page_orientation
    ) AS use_counter_css_page_css_page_orientation,
    SUM(
      metrics.counter.use_counter_css_page_css_paint_order
    ) AS use_counter_css_page_css_paint_order,
    SUM(
      metrics.counter.use_counter_css_page_css_perspective
    ) AS use_counter_css_page_css_perspective,
    SUM(
      metrics.counter.use_counter_css_page_css_perspective_origin
    ) AS use_counter_css_page_css_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_place_content
    ) AS use_counter_css_page_css_place_content,
    SUM(
      metrics.counter.use_counter_css_page_css_place_items
    ) AS use_counter_css_page_css_place_items,
    SUM(metrics.counter.use_counter_css_page_css_place_self) AS use_counter_css_page_css_place_self,
    SUM(
      metrics.counter.use_counter_css_page_css_pointer_events
    ) AS use_counter_css_page_css_pointer_events,
    SUM(metrics.counter.use_counter_css_page_css_position) AS use_counter_css_page_css_position,
    SUM(
      metrics.counter.use_counter_css_page_css_print_color_adjust
    ) AS use_counter_css_page_css_print_color_adjust,
    SUM(metrics.counter.use_counter_css_page_css_quotes) AS use_counter_css_page_css_quotes,
    SUM(metrics.counter.use_counter_css_page_css_r) AS use_counter_css_page_css_r,
    SUM(metrics.counter.use_counter_css_page_css_resize) AS use_counter_css_page_css_resize,
    SUM(metrics.counter.use_counter_css_page_css_right) AS use_counter_css_page_css_right,
    SUM(metrics.counter.use_counter_css_page_css_rotate) AS use_counter_css_page_css_rotate,
    SUM(metrics.counter.use_counter_css_page_css_row_gap) AS use_counter_css_page_css_row_gap,
    SUM(metrics.counter.use_counter_css_page_css_ruby_align) AS use_counter_css_page_css_ruby_align,
    SUM(
      metrics.counter.use_counter_css_page_css_ruby_position
    ) AS use_counter_css_page_css_ruby_position,
    SUM(metrics.counter.use_counter_css_page_css_rx) AS use_counter_css_page_css_rx,
    SUM(metrics.counter.use_counter_css_page_css_ry) AS use_counter_css_page_css_ry,
    SUM(metrics.counter.use_counter_css_page_css_scale) AS use_counter_css_page_css_scale,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_behavior
    ) AS use_counter_css_page_css_scroll_behavior,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin
    ) AS use_counter_css_page_css_scroll_margin,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_block
    ) AS use_counter_css_page_css_scroll_margin_block,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_block_end
    ) AS use_counter_css_page_css_scroll_margin_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_block_start
    ) AS use_counter_css_page_css_scroll_margin_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_bottom
    ) AS use_counter_css_page_css_scroll_margin_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_inline
    ) AS use_counter_css_page_css_scroll_margin_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_inline_end
    ) AS use_counter_css_page_css_scroll_margin_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_inline_start
    ) AS use_counter_css_page_css_scroll_margin_inline_start,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_left
    ) AS use_counter_css_page_css_scroll_margin_left,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_right
    ) AS use_counter_css_page_css_scroll_margin_right,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_margin_top
    ) AS use_counter_css_page_css_scroll_margin_top,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding
    ) AS use_counter_css_page_css_scroll_padding,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_block
    ) AS use_counter_css_page_css_scroll_padding_block,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_block_end
    ) AS use_counter_css_page_css_scroll_padding_block_end,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_block_start
    ) AS use_counter_css_page_css_scroll_padding_block_start,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_bottom
    ) AS use_counter_css_page_css_scroll_padding_bottom,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_inline
    ) AS use_counter_css_page_css_scroll_padding_inline,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_inline_end
    ) AS use_counter_css_page_css_scroll_padding_inline_end,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_inline_start
    ) AS use_counter_css_page_css_scroll_padding_inline_start,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_left
    ) AS use_counter_css_page_css_scroll_padding_left,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_right
    ) AS use_counter_css_page_css_scroll_padding_right,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_padding_top
    ) AS use_counter_css_page_css_scroll_padding_top,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_snap_align
    ) AS use_counter_css_page_css_scroll_snap_align,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_snap_stop
    ) AS use_counter_css_page_css_scroll_snap_stop,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_snap_type
    ) AS use_counter_css_page_css_scroll_snap_type,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_timeline
    ) AS use_counter_css_page_css_scroll_timeline,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_timeline_axis
    ) AS use_counter_css_page_css_scroll_timeline_axis,
    SUM(
      metrics.counter.use_counter_css_page_css_scroll_timeline_name
    ) AS use_counter_css_page_css_scroll_timeline_name,
    SUM(
      metrics.counter.use_counter_css_page_css_scrollbar_color
    ) AS use_counter_css_page_css_scrollbar_color,
    SUM(
      metrics.counter.use_counter_css_page_css_scrollbar_gutter
    ) AS use_counter_css_page_css_scrollbar_gutter,
    SUM(
      metrics.counter.use_counter_css_page_css_scrollbar_width
    ) AS use_counter_css_page_css_scrollbar_width,
    SUM(
      metrics.counter.use_counter_css_page_css_shape_image_threshold
    ) AS use_counter_css_page_css_shape_image_threshold,
    SUM(
      metrics.counter.use_counter_css_page_css_shape_margin
    ) AS use_counter_css_page_css_shape_margin,
    SUM(
      metrics.counter.use_counter_css_page_css_shape_outside
    ) AS use_counter_css_page_css_shape_outside,
    SUM(
      metrics.counter.use_counter_css_page_css_shape_rendering
    ) AS use_counter_css_page_css_shape_rendering,
    SUM(metrics.counter.use_counter_css_page_css_size) AS use_counter_css_page_css_size,
    SUM(metrics.counter.use_counter_css_page_css_stop_color) AS use_counter_css_page_css_stop_color,
    SUM(
      metrics.counter.use_counter_css_page_css_stop_opacity
    ) AS use_counter_css_page_css_stop_opacity,
    SUM(metrics.counter.use_counter_css_page_css_stroke) AS use_counter_css_page_css_stroke,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_dasharray
    ) AS use_counter_css_page_css_stroke_dasharray,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_dashoffset
    ) AS use_counter_css_page_css_stroke_dashoffset,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_linecap
    ) AS use_counter_css_page_css_stroke_linecap,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_linejoin
    ) AS use_counter_css_page_css_stroke_linejoin,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_miterlimit
    ) AS use_counter_css_page_css_stroke_miterlimit,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_opacity
    ) AS use_counter_css_page_css_stroke_opacity,
    SUM(
      metrics.counter.use_counter_css_page_css_stroke_width
    ) AS use_counter_css_page_css_stroke_width,
    SUM(metrics.counter.use_counter_css_page_css_tab_size) AS use_counter_css_page_css_tab_size,
    SUM(
      metrics.counter.use_counter_css_page_css_table_layout
    ) AS use_counter_css_page_css_table_layout,
    SUM(metrics.counter.use_counter_css_page_css_text_align) AS use_counter_css_page_css_text_align,
    SUM(
      metrics.counter.use_counter_css_page_css_text_align_last
    ) AS use_counter_css_page_css_text_align_last,
    SUM(
      metrics.counter.use_counter_css_page_css_text_anchor
    ) AS use_counter_css_page_css_text_anchor,
    SUM(
      metrics.counter.use_counter_css_page_css_text_combine_upright
    ) AS use_counter_css_page_css_text_combine_upright,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration
    ) AS use_counter_css_page_css_text_decoration,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration_color
    ) AS use_counter_css_page_css_text_decoration_color,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration_line
    ) AS use_counter_css_page_css_text_decoration_line,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration_skip_ink
    ) AS use_counter_css_page_css_text_decoration_skip_ink,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration_style
    ) AS use_counter_css_page_css_text_decoration_style,
    SUM(
      metrics.counter.use_counter_css_page_css_text_decoration_thickness
    ) AS use_counter_css_page_css_text_decoration_thickness,
    SUM(
      metrics.counter.use_counter_css_page_css_text_emphasis
    ) AS use_counter_css_page_css_text_emphasis,
    SUM(
      metrics.counter.use_counter_css_page_css_text_emphasis_color
    ) AS use_counter_css_page_css_text_emphasis_color,
    SUM(
      metrics.counter.use_counter_css_page_css_text_emphasis_position
    ) AS use_counter_css_page_css_text_emphasis_position,
    SUM(
      metrics.counter.use_counter_css_page_css_text_emphasis_style
    ) AS use_counter_css_page_css_text_emphasis_style,
    SUM(
      metrics.counter.use_counter_css_page_css_text_indent
    ) AS use_counter_css_page_css_text_indent,
    SUM(
      metrics.counter.use_counter_css_page_css_text_justify
    ) AS use_counter_css_page_css_text_justify,
    SUM(
      metrics.counter.use_counter_css_page_css_text_orientation
    ) AS use_counter_css_page_css_text_orientation,
    SUM(
      metrics.counter.use_counter_css_page_css_text_overflow
    ) AS use_counter_css_page_css_text_overflow,
    SUM(
      metrics.counter.use_counter_css_page_css_text_rendering
    ) AS use_counter_css_page_css_text_rendering,
    SUM(
      metrics.counter.use_counter_css_page_css_text_shadow
    ) AS use_counter_css_page_css_text_shadow,
    SUM(
      metrics.counter.use_counter_css_page_css_text_transform
    ) AS use_counter_css_page_css_text_transform,
    SUM(
      metrics.counter.use_counter_css_page_css_text_underline_offset
    ) AS use_counter_css_page_css_text_underline_offset,
    SUM(
      metrics.counter.use_counter_css_page_css_text_underline_position
    ) AS use_counter_css_page_css_text_underline_position,
    SUM(metrics.counter.use_counter_css_page_css_text_wrap) AS use_counter_css_page_css_text_wrap,
    SUM(metrics.counter.use_counter_css_page_css_top) AS use_counter_css_page_css_top,
    SUM(
      metrics.counter.use_counter_css_page_css_touch_action
    ) AS use_counter_css_page_css_touch_action,
    SUM(metrics.counter.use_counter_css_page_css_transform) AS use_counter_css_page_css_transform,
    SUM(
      metrics.counter.use_counter_css_page_css_transform_box
    ) AS use_counter_css_page_css_transform_box,
    SUM(
      metrics.counter.use_counter_css_page_css_transform_origin
    ) AS use_counter_css_page_css_transform_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_transform_style
    ) AS use_counter_css_page_css_transform_style,
    SUM(metrics.counter.use_counter_css_page_css_transition) AS use_counter_css_page_css_transition,
    SUM(
      metrics.counter.use_counter_css_page_css_transition_delay
    ) AS use_counter_css_page_css_transition_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_transition_duration
    ) AS use_counter_css_page_css_transition_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_transition_property
    ) AS use_counter_css_page_css_transition_property,
    SUM(
      metrics.counter.use_counter_css_page_css_transition_timing_function
    ) AS use_counter_css_page_css_transition_timing_function,
    SUM(metrics.counter.use_counter_css_page_css_translate) AS use_counter_css_page_css_translate,
    SUM(
      metrics.counter.use_counter_css_page_css_unicode_bidi
    ) AS use_counter_css_page_css_unicode_bidi,
    SUM(
      metrics.counter.use_counter_css_page_css_user_select
    ) AS use_counter_css_page_css_user_select,
    SUM(
      metrics.counter.use_counter_css_page_css_vector_effect
    ) AS use_counter_css_page_css_vector_effect,
    SUM(
      metrics.counter.use_counter_css_page_css_vertical_align
    ) AS use_counter_css_page_css_vertical_align,
    SUM(
      metrics.counter.use_counter_css_page_css_view_timeline
    ) AS use_counter_css_page_css_view_timeline,
    SUM(
      metrics.counter.use_counter_css_page_css_view_timeline_axis
    ) AS use_counter_css_page_css_view_timeline_axis,
    SUM(
      metrics.counter.use_counter_css_page_css_view_timeline_inset
    ) AS use_counter_css_page_css_view_timeline_inset,
    SUM(
      metrics.counter.use_counter_css_page_css_view_timeline_name
    ) AS use_counter_css_page_css_view_timeline_name,
    SUM(metrics.counter.use_counter_css_page_css_visibility) AS use_counter_css_page_css_visibility,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_align_content
    ) AS use_counter_css_page_css_webkit_align_content,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_align_items
    ) AS use_counter_css_page_css_webkit_align_items,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_align_self
    ) AS use_counter_css_page_css_webkit_align_self,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation
    ) AS use_counter_css_page_css_webkit_animation,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_delay
    ) AS use_counter_css_page_css_webkit_animation_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_direction
    ) AS use_counter_css_page_css_webkit_animation_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_duration
    ) AS use_counter_css_page_css_webkit_animation_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_fill_mode
    ) AS use_counter_css_page_css_webkit_animation_fill_mode,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_iteration_count
    ) AS use_counter_css_page_css_webkit_animation_iteration_count,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_name
    ) AS use_counter_css_page_css_webkit_animation_name,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_play_state
    ) AS use_counter_css_page_css_webkit_animation_play_state,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_animation_timing_function
    ) AS use_counter_css_page_css_webkit_animation_timing_function,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_appearance
    ) AS use_counter_css_page_css_webkit_appearance,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_backface_visibility
    ) AS use_counter_css_page_css_webkit_backface_visibility,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_background_clip
    ) AS use_counter_css_page_css_webkit_background_clip,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_background_origin
    ) AS use_counter_css_page_css_webkit_background_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_background_size
    ) AS use_counter_css_page_css_webkit_background_size,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_bottom_left_radius
    ) AS use_counter_css_page_css_webkit_border_bottom_left_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_bottom_right_radius
    ) AS use_counter_css_page_css_webkit_border_bottom_right_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_image
    ) AS use_counter_css_page_css_webkit_border_image,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_radius
    ) AS use_counter_css_page_css_webkit_border_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_top_left_radius
    ) AS use_counter_css_page_css_webkit_border_top_left_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_border_top_right_radius
    ) AS use_counter_css_page_css_webkit_border_top_right_radius,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_align
    ) AS use_counter_css_page_css_webkit_box_align,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_direction
    ) AS use_counter_css_page_css_webkit_box_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_flex
    ) AS use_counter_css_page_css_webkit_box_flex,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_ordinal_group
    ) AS use_counter_css_page_css_webkit_box_ordinal_group,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_orient
    ) AS use_counter_css_page_css_webkit_box_orient,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_pack
    ) AS use_counter_css_page_css_webkit_box_pack,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_shadow
    ) AS use_counter_css_page_css_webkit_box_shadow,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_box_sizing
    ) AS use_counter_css_page_css_webkit_box_sizing,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_clip_path
    ) AS use_counter_css_page_css_webkit_clip_path,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_filter
    ) AS use_counter_css_page_css_webkit_filter,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex
    ) AS use_counter_css_page_css_webkit_flex,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_basis
    ) AS use_counter_css_page_css_webkit_flex_basis,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_direction
    ) AS use_counter_css_page_css_webkit_flex_direction,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_flow
    ) AS use_counter_css_page_css_webkit_flex_flow,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_grow
    ) AS use_counter_css_page_css_webkit_flex_grow,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_shrink
    ) AS use_counter_css_page_css_webkit_flex_shrink,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_flex_wrap
    ) AS use_counter_css_page_css_webkit_flex_wrap,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_justify_content
    ) AS use_counter_css_page_css_webkit_justify_content,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_line_clamp
    ) AS use_counter_css_page_css_webkit_line_clamp,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask
    ) AS use_counter_css_page_css_webkit_mask,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_clip
    ) AS use_counter_css_page_css_webkit_mask_clip,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_composite
    ) AS use_counter_css_page_css_webkit_mask_composite,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_image
    ) AS use_counter_css_page_css_webkit_mask_image,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_origin
    ) AS use_counter_css_page_css_webkit_mask_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_position
    ) AS use_counter_css_page_css_webkit_mask_position,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_position_x
    ) AS use_counter_css_page_css_webkit_mask_position_x,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_position_y
    ) AS use_counter_css_page_css_webkit_mask_position_y,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_repeat
    ) AS use_counter_css_page_css_webkit_mask_repeat,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_mask_size
    ) AS use_counter_css_page_css_webkit_mask_size,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_order
    ) AS use_counter_css_page_css_webkit_order,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_perspective
    ) AS use_counter_css_page_css_webkit_perspective,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_perspective_origin
    ) AS use_counter_css_page_css_webkit_perspective_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_fill_color
    ) AS use_counter_css_page_css_webkit_text_fill_color,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_security
    ) AS use_counter_css_page_css_webkit_text_security,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_size_adjust
    ) AS use_counter_css_page_css_webkit_text_size_adjust,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_stroke
    ) AS use_counter_css_page_css_webkit_text_stroke,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_stroke_color
    ) AS use_counter_css_page_css_webkit_text_stroke_color,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_text_stroke_width
    ) AS use_counter_css_page_css_webkit_text_stroke_width,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transform
    ) AS use_counter_css_page_css_webkit_transform,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transform_origin
    ) AS use_counter_css_page_css_webkit_transform_origin,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transform_style
    ) AS use_counter_css_page_css_webkit_transform_style,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transition
    ) AS use_counter_css_page_css_webkit_transition,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transition_delay
    ) AS use_counter_css_page_css_webkit_transition_delay,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transition_duration
    ) AS use_counter_css_page_css_webkit_transition_duration,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transition_property
    ) AS use_counter_css_page_css_webkit_transition_property,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_transition_timing_function
    ) AS use_counter_css_page_css_webkit_transition_timing_function,
    SUM(
      metrics.counter.use_counter_css_page_css_webkit_user_select
    ) AS use_counter_css_page_css_webkit_user_select,
    SUM(
      metrics.counter.use_counter_css_page_css_white_space
    ) AS use_counter_css_page_css_white_space,
    SUM(metrics.counter.use_counter_css_page_css_width) AS use_counter_css_page_css_width,
    SUM(
      metrics.counter.use_counter_css_page_css_will_change
    ) AS use_counter_css_page_css_will_change,
    SUM(metrics.counter.use_counter_css_page_css_word_break) AS use_counter_css_page_css_word_break,
    SUM(
      metrics.counter.use_counter_css_page_css_word_spacing
    ) AS use_counter_css_page_css_word_spacing,
    SUM(metrics.counter.use_counter_css_page_css_word_wrap) AS use_counter_css_page_css_word_wrap,
    SUM(
      metrics.counter.use_counter_css_page_css_writing_mode
    ) AS use_counter_css_page_css_writing_mode,
    SUM(metrics.counter.use_counter_css_page_css_x) AS use_counter_css_page_css_x,
    SUM(metrics.counter.use_counter_css_page_css_x_lang) AS use_counter_css_page_css_x_lang,
    SUM(metrics.counter.use_counter_css_page_css_x_span) AS use_counter_css_page_css_x_span,
    SUM(
      metrics.counter.use_counter_css_page_css_x_text_scale
    ) AS use_counter_css_page_css_x_text_scale,
    SUM(metrics.counter.use_counter_css_page_css_y) AS use_counter_css_page_css_y,
    SUM(metrics.counter.use_counter_css_page_css_z_index) AS use_counter_css_page_css_z_index,
    SUM(metrics.counter.use_counter_css_page_css_zoom) AS use_counter_css_page_css_zoom,
    SUM(metrics.counter.use_counter_css_page_max_zoom) AS use_counter_css_page_max_zoom,
    SUM(metrics.counter.use_counter_css_page_min_zoom) AS use_counter_css_page_min_zoom,
    SUM(metrics.counter.use_counter_css_page_orientation) AS use_counter_css_page_orientation,
    SUM(metrics.counter.use_counter_css_page_orphans) AS use_counter_css_page_orphans,
    SUM(metrics.counter.use_counter_css_page_speak) AS use_counter_css_page_speak,
    SUM(
      metrics.counter.use_counter_css_page_text_size_adjust
    ) AS use_counter_css_page_text_size_adjust,
    SUM(metrics.counter.use_counter_css_page_user_zoom) AS use_counter_css_page_user_zoom,
    SUM(
      metrics.counter.use_counter_css_page_webkit_app_region
    ) AS use_counter_css_page_webkit_app_region,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_after
    ) AS use_counter_css_page_webkit_border_after,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_after_color
    ) AS use_counter_css_page_webkit_border_after_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_after_style
    ) AS use_counter_css_page_webkit_border_after_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_after_width
    ) AS use_counter_css_page_webkit_border_after_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_before
    ) AS use_counter_css_page_webkit_border_before,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_before_color
    ) AS use_counter_css_page_webkit_border_before_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_before_style
    ) AS use_counter_css_page_webkit_border_before_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_before_width
    ) AS use_counter_css_page_webkit_border_before_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_end
    ) AS use_counter_css_page_webkit_border_end,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_end_color
    ) AS use_counter_css_page_webkit_border_end_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_end_style
    ) AS use_counter_css_page_webkit_border_end_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_end_width
    ) AS use_counter_css_page_webkit_border_end_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_horizontal_spacing
    ) AS use_counter_css_page_webkit_border_horizontal_spacing,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_start
    ) AS use_counter_css_page_webkit_border_start,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_start_color
    ) AS use_counter_css_page_webkit_border_start_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_start_style
    ) AS use_counter_css_page_webkit_border_start_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_start_width
    ) AS use_counter_css_page_webkit_border_start_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_border_vertical_spacing
    ) AS use_counter_css_page_webkit_border_vertical_spacing,
    SUM(
      metrics.counter.use_counter_css_page_webkit_box_decoration_break
    ) AS use_counter_css_page_webkit_box_decoration_break,
    SUM(
      metrics.counter.use_counter_css_page_webkit_box_reflect
    ) AS use_counter_css_page_webkit_box_reflect,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_break_after
    ) AS use_counter_css_page_webkit_column_break_after,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_break_before
    ) AS use_counter_css_page_webkit_column_break_before,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_break_inside
    ) AS use_counter_css_page_webkit_column_break_inside,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_count
    ) AS use_counter_css_page_webkit_column_count,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_gap
    ) AS use_counter_css_page_webkit_column_gap,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_rule
    ) AS use_counter_css_page_webkit_column_rule,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_rule_color
    ) AS use_counter_css_page_webkit_column_rule_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_rule_style
    ) AS use_counter_css_page_webkit_column_rule_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_rule_width
    ) AS use_counter_css_page_webkit_column_rule_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_span
    ) AS use_counter_css_page_webkit_column_span,
    SUM(
      metrics.counter.use_counter_css_page_webkit_column_width
    ) AS use_counter_css_page_webkit_column_width,
    SUM(metrics.counter.use_counter_css_page_webkit_columns) AS use_counter_css_page_webkit_columns,
    SUM(
      metrics.counter.use_counter_css_page_webkit_font_feature_settings
    ) AS use_counter_css_page_webkit_font_feature_settings,
    SUM(
      metrics.counter.use_counter_css_page_webkit_font_size_delta
    ) AS use_counter_css_page_webkit_font_size_delta,
    SUM(
      metrics.counter.use_counter_css_page_webkit_font_smoothing
    ) AS use_counter_css_page_webkit_font_smoothing,
    SUM(
      metrics.counter.use_counter_css_page_webkit_highlight
    ) AS use_counter_css_page_webkit_highlight,
    SUM(
      metrics.counter.use_counter_css_page_webkit_hyphenate_character
    ) AS use_counter_css_page_webkit_hyphenate_character,
    SUM(
      metrics.counter.use_counter_css_page_webkit_line_break
    ) AS use_counter_css_page_webkit_line_break,
    SUM(metrics.counter.use_counter_css_page_webkit_locale) AS use_counter_css_page_webkit_locale,
    SUM(
      metrics.counter.use_counter_css_page_webkit_logical_height
    ) AS use_counter_css_page_webkit_logical_height,
    SUM(
      metrics.counter.use_counter_css_page_webkit_logical_width
    ) AS use_counter_css_page_webkit_logical_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_after
    ) AS use_counter_css_page_webkit_margin_after,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_after_collapse
    ) AS use_counter_css_page_webkit_margin_after_collapse,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_before
    ) AS use_counter_css_page_webkit_margin_before,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_before_collapse
    ) AS use_counter_css_page_webkit_margin_before_collapse,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_bottom_collapse
    ) AS use_counter_css_page_webkit_margin_bottom_collapse,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_collapse
    ) AS use_counter_css_page_webkit_margin_collapse,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_end
    ) AS use_counter_css_page_webkit_margin_end,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_start
    ) AS use_counter_css_page_webkit_margin_start,
    SUM(
      metrics.counter.use_counter_css_page_webkit_margin_top_collapse
    ) AS use_counter_css_page_webkit_margin_top_collapse,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image
    ) AS use_counter_css_page_webkit_mask_box_image,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image_outset
    ) AS use_counter_css_page_webkit_mask_box_image_outset,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image_repeat
    ) AS use_counter_css_page_webkit_mask_box_image_repeat,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image_slice
    ) AS use_counter_css_page_webkit_mask_box_image_slice,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image_source
    ) AS use_counter_css_page_webkit_mask_box_image_source,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_box_image_width
    ) AS use_counter_css_page_webkit_mask_box_image_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_repeat_x
    ) AS use_counter_css_page_webkit_mask_repeat_x,
    SUM(
      metrics.counter.use_counter_css_page_webkit_mask_repeat_y
    ) AS use_counter_css_page_webkit_mask_repeat_y,
    SUM(
      metrics.counter.use_counter_css_page_webkit_max_logical_height
    ) AS use_counter_css_page_webkit_max_logical_height,
    SUM(
      metrics.counter.use_counter_css_page_webkit_max_logical_width
    ) AS use_counter_css_page_webkit_max_logical_width,
    SUM(
      metrics.counter.use_counter_css_page_webkit_min_logical_height
    ) AS use_counter_css_page_webkit_min_logical_height,
    SUM(
      metrics.counter.use_counter_css_page_webkit_min_logical_width
    ) AS use_counter_css_page_webkit_min_logical_width,
    SUM(metrics.counter.use_counter_css_page_webkit_opacity) AS use_counter_css_page_webkit_opacity,
    SUM(
      metrics.counter.use_counter_css_page_webkit_padding_after
    ) AS use_counter_css_page_webkit_padding_after,
    SUM(
      metrics.counter.use_counter_css_page_webkit_padding_before
    ) AS use_counter_css_page_webkit_padding_before,
    SUM(
      metrics.counter.use_counter_css_page_webkit_padding_end
    ) AS use_counter_css_page_webkit_padding_end,
    SUM(
      metrics.counter.use_counter_css_page_webkit_padding_start
    ) AS use_counter_css_page_webkit_padding_start,
    SUM(
      metrics.counter.use_counter_css_page_webkit_perspective_origin_x
    ) AS use_counter_css_page_webkit_perspective_origin_x,
    SUM(
      metrics.counter.use_counter_css_page_webkit_perspective_origin_y
    ) AS use_counter_css_page_webkit_perspective_origin_y,
    SUM(
      metrics.counter.use_counter_css_page_webkit_print_color_adjust
    ) AS use_counter_css_page_webkit_print_color_adjust,
    SUM(
      metrics.counter.use_counter_css_page_webkit_rtl_ordering
    ) AS use_counter_css_page_webkit_rtl_ordering,
    SUM(
      metrics.counter.use_counter_css_page_webkit_ruby_position
    ) AS use_counter_css_page_webkit_ruby_position,
    SUM(
      metrics.counter.use_counter_css_page_webkit_shape_image_threshold
    ) AS use_counter_css_page_webkit_shape_image_threshold,
    SUM(
      metrics.counter.use_counter_css_page_webkit_shape_margin
    ) AS use_counter_css_page_webkit_shape_margin,
    SUM(
      metrics.counter.use_counter_css_page_webkit_shape_outside
    ) AS use_counter_css_page_webkit_shape_outside,
    SUM(
      metrics.counter.use_counter_css_page_webkit_tap_highlight_color
    ) AS use_counter_css_page_webkit_tap_highlight_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_combine
    ) AS use_counter_css_page_webkit_text_combine,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_decorations_in_effect
    ) AS use_counter_css_page_webkit_text_decorations_in_effect,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_emphasis
    ) AS use_counter_css_page_webkit_text_emphasis,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_emphasis_color
    ) AS use_counter_css_page_webkit_text_emphasis_color,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_emphasis_position
    ) AS use_counter_css_page_webkit_text_emphasis_position,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_emphasis_style
    ) AS use_counter_css_page_webkit_text_emphasis_style,
    SUM(
      metrics.counter.use_counter_css_page_webkit_text_orientation
    ) AS use_counter_css_page_webkit_text_orientation,
    SUM(
      metrics.counter.use_counter_css_page_webkit_transform_origin_x
    ) AS use_counter_css_page_webkit_transform_origin_x,
    SUM(
      metrics.counter.use_counter_css_page_webkit_transform_origin_y
    ) AS use_counter_css_page_webkit_transform_origin_y,
    SUM(
      metrics.counter.use_counter_css_page_webkit_transform_origin_z
    ) AS use_counter_css_page_webkit_transform_origin_z,
    SUM(
      metrics.counter.use_counter_css_page_webkit_user_drag
    ) AS use_counter_css_page_webkit_user_drag,
    SUM(
      metrics.counter.use_counter_css_page_webkit_user_modify
    ) AS use_counter_css_page_webkit_user_modify,
    SUM(
      metrics.counter.use_counter_css_page_webkit_writing_mode
    ) AS use_counter_css_page_webkit_writing_mode,
    SUM(metrics.counter.use_counter_css_page_widows) AS use_counter_css_page_widows,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_ambient_light_event
    ) AS use_counter_deprecated_ops_doc_ambient_light_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_app_cache
    ) AS use_counter_deprecated_ops_doc_app_cache,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_components
    ) AS use_counter_deprecated_ops_doc_components,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_create_image_bitmap_canvas_rendering_context2_d
    ) AS use_counter_deprecated_ops_doc_create_image_bitmap_canvas_rendering_context2_d,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_deprecated_testing_attribute
    ) AS use_counter_deprecated_ops_doc_deprecated_testing_attribute,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_deprecated_testing_interface
    ) AS use_counter_deprecated_ops_doc_deprecated_testing_interface,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_deprecated_testing_method
    ) AS use_counter_deprecated_ops_doc_deprecated_testing_method,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_document_release_capture
    ) AS use_counter_deprecated_ops_doc_document_release_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_domquad_bounds_attr
    ) AS use_counter_deprecated_ops_doc_domquad_bounds_attr,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_draw_window_canvas_rendering_context2_d
    ) AS use_counter_deprecated_ops_doc_draw_window_canvas_rendering_context2_d,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_element_release_capture
    ) AS use_counter_deprecated_ops_doc_element_release_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_element_set_capture
    ) AS use_counter_deprecated_ops_doc_element_set_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_external_add_search_provider
    ) AS use_counter_deprecated_ops_doc_external_add_search_provider,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_form_submission_untrusted_event
    ) AS use_counter_deprecated_ops_doc_form_submission_untrusted_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_idbopen_dboptions_storage_type
    ) AS use_counter_deprecated_ops_doc_idbopen_dboptions_storage_type,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_image_bitmap_rendering_context_transfer_image_bitmap
    ) AS use_counter_deprecated_ops_doc_image_bitmap_rendering_context_transfer_image_bitmap,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_import_xulinto_content
    ) AS use_counter_deprecated_ops_doc_import_xulinto_content,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_init_mouse_event
    ) AS use_counter_deprecated_ops_doc_init_mouse_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_init_nsmouse_event
    ) AS use_counter_deprecated_ops_doc_init_nsmouse_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_install_trigger_deprecated
    ) AS use_counter_deprecated_ops_doc_install_trigger_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_install_trigger_install_deprecated
    ) AS use_counter_deprecated_ops_doc_install_trigger_install_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_is_external_ctap2_security_key_supported
    ) AS use_counter_deprecated_ops_doc_is_external_ctap2_security_key_supported,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_lenient_setter
    ) AS use_counter_deprecated_ops_doc_lenient_setter,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_lenient_this
    ) AS use_counter_deprecated_ops_doc_lenient_this,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_math_ml_deprecated_math_space_value2
    ) AS use_counter_deprecated_ops_doc_math_ml_deprecated_math_space_value2,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_math_ml_deprecated_math_variant
    ) AS use_counter_deprecated_ops_doc_math_ml_deprecated_math_variant,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_math_ml_deprecated_stixgeneral_operator_stretching
    ) AS use_counter_deprecated_ops_doc_math_ml_deprecated_stixgeneral_operator_stretching,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_motion_event
    ) AS use_counter_deprecated_ops_doc_motion_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_mouse_event_moz_pressure
    ) AS use_counter_deprecated_ops_doc_mouse_event_moz_pressure,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_moz_input_source
    ) AS use_counter_deprecated_ops_doc_moz_input_source,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_moz_request_full_screen_deprecated_prefix
    ) AS use_counter_deprecated_ops_doc_moz_request_full_screen_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_mozfullscreenchange_deprecated_prefix
    ) AS use_counter_deprecated_ops_doc_mozfullscreenchange_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_mozfullscreenerror_deprecated_prefix
    ) AS use_counter_deprecated_ops_doc_mozfullscreenerror_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_mutation_event
    ) AS use_counter_deprecated_ops_doc_mutation_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_navigator_get_user_media
    ) AS use_counter_deprecated_ops_doc_navigator_get_user_media,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_node_iterator_detach
    ) AS use_counter_deprecated_ops_doc_node_iterator_detach,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_offscreen_canvas_to_blob
    ) AS use_counter_deprecated_ops_doc_offscreen_canvas_to_blob,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_orientation_event
    ) AS use_counter_deprecated_ops_doc_orientation_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_proximity_event
    ) AS use_counter_deprecated_ops_doc_proximity_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_rtcpeer_connection_get_streams
    ) AS use_counter_deprecated_ops_doc_rtcpeer_connection_get_streams,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_size_to_content
    ) AS use_counter_deprecated_ops_doc_size_to_content,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_svgdeselect_all
    ) AS use_counter_deprecated_ops_doc_svgdeselect_all,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_svgfarthest_viewport_element
    ) AS use_counter_deprecated_ops_doc_svgfarthest_viewport_element,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_svgnearest_viewport_element
    ) AS use_counter_deprecated_ops_doc_svgnearest_viewport_element,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_sync_xmlhttp_request_deprecated
    ) AS use_counter_deprecated_ops_doc_sync_xmlhttp_request_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_use_of_capture_events
    ) AS use_counter_deprecated_ops_doc_use_of_capture_events,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_use_of_release_events
    ) AS use_counter_deprecated_ops_doc_use_of_release_events,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_webrtc_deprecated_prefix
    ) AS use_counter_deprecated_ops_doc_webrtc_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_window_cc_ontrollers
    ) AS use_counter_deprecated_ops_doc_window_cc_ontrollers,
    SUM(
      metrics.counter.use_counter_deprecated_ops_doc_window_content_untrusted
    ) AS use_counter_deprecated_ops_doc_window_content_untrusted,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_ambient_light_event
    ) AS use_counter_deprecated_ops_page_ambient_light_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_app_cache
    ) AS use_counter_deprecated_ops_page_app_cache,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_components
    ) AS use_counter_deprecated_ops_page_components,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_create_image_bitmap_canvas_rendering_context2_d
    ) AS use_counter_deprecated_ops_page_create_image_bitmap_canvas_rendering_context2_d,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_deprecated_testing_attribute
    ) AS use_counter_deprecated_ops_page_deprecated_testing_attribute,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_deprecated_testing_interface
    ) AS use_counter_deprecated_ops_page_deprecated_testing_interface,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_deprecated_testing_method
    ) AS use_counter_deprecated_ops_page_deprecated_testing_method,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_document_release_capture
    ) AS use_counter_deprecated_ops_page_document_release_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_domquad_bounds_attr
    ) AS use_counter_deprecated_ops_page_domquad_bounds_attr,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_draw_window_canvas_rendering_context2_d
    ) AS use_counter_deprecated_ops_page_draw_window_canvas_rendering_context2_d,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_element_release_capture
    ) AS use_counter_deprecated_ops_page_element_release_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_element_set_capture
    ) AS use_counter_deprecated_ops_page_element_set_capture,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_external_add_search_provider
    ) AS use_counter_deprecated_ops_page_external_add_search_provider,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_form_submission_untrusted_event
    ) AS use_counter_deprecated_ops_page_form_submission_untrusted_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_idbopen_dboptions_storage_type
    ) AS use_counter_deprecated_ops_page_idbopen_dboptions_storage_type,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_image_bitmap_rendering_context_transfer_image_bitmap
    ) AS use_counter_deprecated_ops_page_image_bitmap_rendering_context_transfer_image_bitmap,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_import_xulinto_content
    ) AS use_counter_deprecated_ops_page_import_xulinto_content,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_init_mouse_event
    ) AS use_counter_deprecated_ops_page_init_mouse_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_init_nsmouse_event
    ) AS use_counter_deprecated_ops_page_init_nsmouse_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_install_trigger_deprecated
    ) AS use_counter_deprecated_ops_page_install_trigger_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_install_trigger_install_deprecated
    ) AS use_counter_deprecated_ops_page_install_trigger_install_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_is_external_ctap2_security_key_supported
    ) AS use_counter_deprecated_ops_page_is_external_ctap2_security_key_supported,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_lenient_setter
    ) AS use_counter_deprecated_ops_page_lenient_setter,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_lenient_this
    ) AS use_counter_deprecated_ops_page_lenient_this,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_math_ml_deprecated_math_space_value2
    ) AS use_counter_deprecated_ops_page_math_ml_deprecated_math_space_value2,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_math_ml_deprecated_math_variant
    ) AS use_counter_deprecated_ops_page_math_ml_deprecated_math_variant,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_math_ml_deprecated_stixgeneral_operator_stretching
    ) AS use_counter_deprecated_ops_page_math_ml_deprecated_stixgeneral_operator_stretching,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_motion_event
    ) AS use_counter_deprecated_ops_page_motion_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_mouse_event_moz_pressure
    ) AS use_counter_deprecated_ops_page_mouse_event_moz_pressure,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_moz_input_source
    ) AS use_counter_deprecated_ops_page_moz_input_source,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_moz_request_full_screen_deprecated_prefix
    ) AS use_counter_deprecated_ops_page_moz_request_full_screen_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_mozfullscreenchange_deprecated_prefix
    ) AS use_counter_deprecated_ops_page_mozfullscreenchange_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_mozfullscreenerror_deprecated_prefix
    ) AS use_counter_deprecated_ops_page_mozfullscreenerror_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_mutation_event
    ) AS use_counter_deprecated_ops_page_mutation_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_navigator_get_user_media
    ) AS use_counter_deprecated_ops_page_navigator_get_user_media,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_node_iterator_detach
    ) AS use_counter_deprecated_ops_page_node_iterator_detach,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_offscreen_canvas_to_blob
    ) AS use_counter_deprecated_ops_page_offscreen_canvas_to_blob,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_orientation_event
    ) AS use_counter_deprecated_ops_page_orientation_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_proximity_event
    ) AS use_counter_deprecated_ops_page_proximity_event,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_rtcpeer_connection_get_streams
    ) AS use_counter_deprecated_ops_page_rtcpeer_connection_get_streams,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_size_to_content
    ) AS use_counter_deprecated_ops_page_size_to_content,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_svgdeselect_all
    ) AS use_counter_deprecated_ops_page_svgdeselect_all,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_svgfarthest_viewport_element
    ) AS use_counter_deprecated_ops_page_svgfarthest_viewport_element,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_svgnearest_viewport_element
    ) AS use_counter_deprecated_ops_page_svgnearest_viewport_element,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_sync_xmlhttp_request_deprecated
    ) AS use_counter_deprecated_ops_page_sync_xmlhttp_request_deprecated,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_use_of_capture_events
    ) AS use_counter_deprecated_ops_page_use_of_capture_events,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_use_of_release_events
    ) AS use_counter_deprecated_ops_page_use_of_release_events,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_webrtc_deprecated_prefix
    ) AS use_counter_deprecated_ops_page_webrtc_deprecated_prefix,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_window_cc_ontrollers
    ) AS use_counter_deprecated_ops_page_window_cc_ontrollers,
    SUM(
      metrics.counter.use_counter_deprecated_ops_page_window_content_untrusted
    ) AS use_counter_deprecated_ops_page_window_content_untrusted,
    SUM(metrics.counter.use_counter_doc_clipboard_read) AS use_counter_doc_clipboard_read,
    SUM(metrics.counter.use_counter_doc_clipboard_readtext) AS use_counter_doc_clipboard_readtext,
    SUM(metrics.counter.use_counter_doc_clipboard_write) AS use_counter_doc_clipboard_write,
    SUM(metrics.counter.use_counter_doc_console_assert) AS use_counter_doc_console_assert,
    SUM(metrics.counter.use_counter_doc_console_clear) AS use_counter_doc_console_clear,
    SUM(metrics.counter.use_counter_doc_console_count) AS use_counter_doc_console_count,
    SUM(metrics.counter.use_counter_doc_console_countreset) AS use_counter_doc_console_countreset,
    SUM(metrics.counter.use_counter_doc_console_debug) AS use_counter_doc_console_debug,
    SUM(metrics.counter.use_counter_doc_console_dir) AS use_counter_doc_console_dir,
    SUM(metrics.counter.use_counter_doc_console_dirxml) AS use_counter_doc_console_dirxml,
    SUM(metrics.counter.use_counter_doc_console_error) AS use_counter_doc_console_error,
    SUM(metrics.counter.use_counter_doc_console_exception) AS use_counter_doc_console_exception,
    SUM(metrics.counter.use_counter_doc_console_group) AS use_counter_doc_console_group,
    SUM(
      metrics.counter.use_counter_doc_console_groupcollapsed
    ) AS use_counter_doc_console_groupcollapsed,
    SUM(metrics.counter.use_counter_doc_console_groupend) AS use_counter_doc_console_groupend,
    SUM(metrics.counter.use_counter_doc_console_info) AS use_counter_doc_console_info,
    SUM(metrics.counter.use_counter_doc_console_log) AS use_counter_doc_console_log,
    SUM(metrics.counter.use_counter_doc_console_profile) AS use_counter_doc_console_profile,
    SUM(metrics.counter.use_counter_doc_console_profileend) AS use_counter_doc_console_profileend,
    SUM(metrics.counter.use_counter_doc_console_table) AS use_counter_doc_console_table,
    SUM(metrics.counter.use_counter_doc_console_time) AS use_counter_doc_console_time,
    SUM(metrics.counter.use_counter_doc_console_timeend) AS use_counter_doc_console_timeend,
    SUM(metrics.counter.use_counter_doc_console_timelog) AS use_counter_doc_console_timelog,
    SUM(metrics.counter.use_counter_doc_console_timestamp) AS use_counter_doc_console_timestamp,
    SUM(metrics.counter.use_counter_doc_console_trace) AS use_counter_doc_console_trace,
    SUM(metrics.counter.use_counter_doc_console_warn) AS use_counter_doc_console_warn,
    SUM(
      metrics.counter.use_counter_doc_customelementregistry_define
    ) AS use_counter_doc_customelementregistry_define,
    SUM(metrics.counter.use_counter_doc_customized_builtin) AS use_counter_doc_customized_builtin,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_addelement
    ) AS use_counter_doc_datatransfer_addelement,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozcleardataat
    ) AS use_counter_doc_datatransfer_mozcleardataat,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozcursor_getter
    ) AS use_counter_doc_datatransfer_mozcursor_getter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozcursor_setter
    ) AS use_counter_doc_datatransfer_mozcursor_setter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozgetdataat
    ) AS use_counter_doc_datatransfer_mozgetdataat,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozitemcount_getter
    ) AS use_counter_doc_datatransfer_mozitemcount_getter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozitemcount_setter
    ) AS use_counter_doc_datatransfer_mozitemcount_setter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozsetdataat
    ) AS use_counter_doc_datatransfer_mozsetdataat,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozsourcenode_getter
    ) AS use_counter_doc_datatransfer_mozsourcenode_getter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozsourcenode_setter
    ) AS use_counter_doc_datatransfer_mozsourcenode_setter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_moztypesat
    ) AS use_counter_doc_datatransfer_moztypesat,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozusercancelled_getter
    ) AS use_counter_doc_datatransfer_mozusercancelled_getter,
    SUM(
      metrics.counter.use_counter_doc_datatransfer_mozusercancelled_setter
    ) AS use_counter_doc_datatransfer_mozusercancelled_setter,
    SUM(
      metrics.counter.use_counter_doc_datatransferitem_webkitgetasentry
    ) AS use_counter_doc_datatransferitem_webkitgetasentry,
    SUM(
      metrics.counter.use_counter_doc_document_caretrangefrompoint
    ) AS use_counter_doc_document_caretrangefrompoint,
    SUM(
      metrics.counter.use_counter_doc_document_exec_command_content_read_only
    ) AS use_counter_doc_document_exec_command_content_read_only,
    SUM(
      metrics.counter.use_counter_doc_document_exitpictureinpicture
    ) AS use_counter_doc_document_exitpictureinpicture,
    SUM(
      metrics.counter.use_counter_doc_document_featurepolicy
    ) AS use_counter_doc_document_featurepolicy,
    SUM(
      metrics.counter.use_counter_doc_document_mozsetimageelement
    ) AS use_counter_doc_document_mozsetimageelement,
    SUM(
      metrics.counter.use_counter_doc_document_onbeforecopy
    ) AS use_counter_doc_document_onbeforecopy,
    SUM(
      metrics.counter.use_counter_doc_document_onbeforecut
    ) AS use_counter_doc_document_onbeforecut,
    SUM(
      metrics.counter.use_counter_doc_document_onbeforepaste
    ) AS use_counter_doc_document_onbeforepaste,
    SUM(metrics.counter.use_counter_doc_document_oncancel) AS use_counter_doc_document_oncancel,
    SUM(metrics.counter.use_counter_doc_document_onfreeze) AS use_counter_doc_document_onfreeze,
    SUM(
      metrics.counter.use_counter_doc_document_onmousewheel
    ) AS use_counter_doc_document_onmousewheel,
    SUM(metrics.counter.use_counter_doc_document_onresume) AS use_counter_doc_document_onresume,
    SUM(metrics.counter.use_counter_doc_document_onsearch) AS use_counter_doc_document_onsearch,
    SUM(
      metrics.counter.use_counter_doc_document_onwebkitfullscreenchange
    ) AS use_counter_doc_document_onwebkitfullscreenchange,
    SUM(
      metrics.counter.use_counter_doc_document_onwebkitfullscreenerror
    ) AS use_counter_doc_document_onwebkitfullscreenerror,
    SUM(metrics.counter.use_counter_doc_document_open) AS use_counter_doc_document_open,
    SUM(
      metrics.counter.use_counter_doc_document_pictureinpictureelement
    ) AS use_counter_doc_document_pictureinpictureelement,
    SUM(
      metrics.counter.use_counter_doc_document_pictureinpictureenabled
    ) AS use_counter_doc_document_pictureinpictureenabled,
    SUM(
      metrics.counter.use_counter_doc_document_query_command_state_or_value_content_read_only
    ) AS use_counter_doc_document_query_command_state_or_value_content_read_only,
    SUM(
      metrics.counter.use_counter_doc_document_query_command_state_or_value_insert_br_on_return
    ) AS use_counter_doc_document_query_command_state_or_value_insert_br_on_return,
    SUM(
      metrics.counter.use_counter_doc_document_query_command_supported_or_enabled_content_read_only
    ) AS use_counter_doc_document_query_command_supported_or_enabled_content_read_only,
    SUM(
      metrics.counter.use_counter_doc_document_query_command_supported_or_enabled_insert_br_on_return
    ) AS use_counter_doc_document_query_command_supported_or_enabled_insert_br_on_return,
    SUM(
      metrics.counter.use_counter_doc_document_registerelement
    ) AS use_counter_doc_document_registerelement,
    SUM(
      metrics.counter.use_counter_doc_document_wasdiscarded
    ) AS use_counter_doc_document_wasdiscarded,
    SUM(
      metrics.counter.use_counter_doc_document_webkitcancelfullscreen
    ) AS use_counter_doc_document_webkitcancelfullscreen,
    SUM(
      metrics.counter.use_counter_doc_document_webkitcurrentfullscreenelement
    ) AS use_counter_doc_document_webkitcurrentfullscreenelement,
    SUM(
      metrics.counter.use_counter_doc_document_webkitexitfullscreen
    ) AS use_counter_doc_document_webkitexitfullscreen,
    SUM(
      metrics.counter.use_counter_doc_document_webkitfullscreenelement
    ) AS use_counter_doc_document_webkitfullscreenelement,
    SUM(
      metrics.counter.use_counter_doc_document_webkitfullscreenenabled
    ) AS use_counter_doc_document_webkitfullscreenenabled,
    SUM(
      metrics.counter.use_counter_doc_document_webkithidden
    ) AS use_counter_doc_document_webkithidden,
    SUM(
      metrics.counter.use_counter_doc_document_webkitisfullscreen
    ) AS use_counter_doc_document_webkitisfullscreen,
    SUM(
      metrics.counter.use_counter_doc_document_webkitvisibilitystate
    ) AS use_counter_doc_document_webkitvisibilitystate,
    SUM(
      metrics.counter.use_counter_doc_document_xmlencoding
    ) AS use_counter_doc_document_xmlencoding,
    SUM(
      metrics.counter.use_counter_doc_document_xmlstandalone
    ) AS use_counter_doc_document_xmlstandalone,
    SUM(metrics.counter.use_counter_doc_document_xmlversion) AS use_counter_doc_document_xmlversion,
    SUM(
      metrics.counter.use_counter_doc_domparser_parsefromstring
    ) AS use_counter_doc_domparser_parsefromstring,
    SUM(
      metrics.counter.use_counter_doc_element_attachshadow
    ) AS use_counter_doc_element_attachshadow,
    SUM(
      metrics.counter.use_counter_doc_element_computedstylemap
    ) AS use_counter_doc_element_computedstylemap,
    SUM(
      metrics.counter.use_counter_doc_element_onmousewheel
    ) AS use_counter_doc_element_onmousewheel,
    SUM(
      metrics.counter.use_counter_doc_element_releasecapture
    ) AS use_counter_doc_element_releasecapture,
    SUM(
      metrics.counter.use_counter_doc_element_releasepointercapture
    ) AS use_counter_doc_element_releasepointercapture,
    SUM(
      metrics.counter.use_counter_doc_element_scrollintoviewifneeded
    ) AS use_counter_doc_element_scrollintoviewifneeded,
    SUM(metrics.counter.use_counter_doc_element_setcapture) AS use_counter_doc_element_setcapture,
    SUM(metrics.counter.use_counter_doc_element_sethtml) AS use_counter_doc_element_sethtml,
    SUM(
      metrics.counter.use_counter_doc_element_setpointercapture
    ) AS use_counter_doc_element_setpointercapture,
    SUM(
      metrics.counter.use_counter_doc_enumerate_devices_insec
    ) AS use_counter_doc_enumerate_devices_insec,
    SUM(
      metrics.counter.use_counter_doc_enumerate_devices_unfocused
    ) AS use_counter_doc_enumerate_devices_unfocused,
    SUM(metrics.counter.use_counter_doc_fe_blend) AS use_counter_doc_fe_blend,
    SUM(metrics.counter.use_counter_doc_fe_color_matrix) AS use_counter_doc_fe_color_matrix,
    SUM(
      metrics.counter.use_counter_doc_fe_component_transfer
    ) AS use_counter_doc_fe_component_transfer,
    SUM(metrics.counter.use_counter_doc_fe_composite) AS use_counter_doc_fe_composite,
    SUM(metrics.counter.use_counter_doc_fe_convolve_matrix) AS use_counter_doc_fe_convolve_matrix,
    SUM(metrics.counter.use_counter_doc_fe_diffuse_lighting) AS use_counter_doc_fe_diffuse_lighting,
    SUM(metrics.counter.use_counter_doc_fe_displacement_map) AS use_counter_doc_fe_displacement_map,
    SUM(metrics.counter.use_counter_doc_fe_flood) AS use_counter_doc_fe_flood,
    SUM(metrics.counter.use_counter_doc_fe_gaussian_blur) AS use_counter_doc_fe_gaussian_blur,
    SUM(metrics.counter.use_counter_doc_fe_image) AS use_counter_doc_fe_image,
    SUM(metrics.counter.use_counter_doc_fe_merge) AS use_counter_doc_fe_merge,
    SUM(metrics.counter.use_counter_doc_fe_morphology) AS use_counter_doc_fe_morphology,
    SUM(metrics.counter.use_counter_doc_fe_offset) AS use_counter_doc_fe_offset,
    SUM(
      metrics.counter.use_counter_doc_fe_specular_lighting
    ) AS use_counter_doc_fe_specular_lighting,
    SUM(metrics.counter.use_counter_doc_fe_tile) AS use_counter_doc_fe_tile,
    SUM(metrics.counter.use_counter_doc_fe_turbulence) AS use_counter_doc_fe_turbulence,
    SUM(
      metrics.counter.use_counter_doc_filtered_cross_origin_iframe
    ) AS use_counter_doc_filtered_cross_origin_iframe,
    SUM(
      metrics.counter.use_counter_doc_get_user_media_insec
    ) AS use_counter_doc_get_user_media_insec,
    SUM(
      metrics.counter.use_counter_doc_get_user_media_unfocused
    ) AS use_counter_doc_get_user_media_unfocused,
    SUM(
      metrics.counter.use_counter_doc_htmlbuttonelement_popovertargetaction
    ) AS use_counter_doc_htmlbuttonelement_popovertargetaction,
    SUM(
      metrics.counter.use_counter_doc_htmlbuttonelement_popovertargetelement
    ) AS use_counter_doc_htmlbuttonelement_popovertargetelement,
    SUM(
      metrics.counter.use_counter_doc_htmldocument_named_getter_hit
    ) AS use_counter_doc_htmldocument_named_getter_hit,
    SUM(
      metrics.counter.use_counter_doc_htmlelement_attributestylemap
    ) AS use_counter_doc_htmlelement_attributestylemap,
    SUM(
      metrics.counter.use_counter_doc_htmlelement_hidepopover
    ) AS use_counter_doc_htmlelement_hidepopover,
    SUM(metrics.counter.use_counter_doc_htmlelement_popover) AS use_counter_doc_htmlelement_popover,
    SUM(
      metrics.counter.use_counter_doc_htmlelement_showpopover
    ) AS use_counter_doc_htmlelement_showpopover,
    SUM(
      metrics.counter.use_counter_doc_htmlelement_togglepopover
    ) AS use_counter_doc_htmlelement_togglepopover,
    SUM(
      metrics.counter.use_counter_doc_htmliframeelement_loading
    ) AS use_counter_doc_htmliframeelement_loading,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_capture
    ) AS use_counter_doc_htmlinputelement_capture,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_incremental
    ) AS use_counter_doc_htmlinputelement_incremental,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_onsearch
    ) AS use_counter_doc_htmlinputelement_onsearch,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_popovertargetaction
    ) AS use_counter_doc_htmlinputelement_popovertargetaction,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_popovertargetelement
    ) AS use_counter_doc_htmlinputelement_popovertargetelement,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_webkitdirectory
    ) AS use_counter_doc_htmlinputelement_webkitdirectory,
    SUM(
      metrics.counter.use_counter_doc_htmlinputelement_webkitentries
    ) AS use_counter_doc_htmlinputelement_webkitentries,
    SUM(
      metrics.counter.use_counter_doc_htmlmediaelement_disableremoteplayback
    ) AS use_counter_doc_htmlmediaelement_disableremoteplayback,
    SUM(
      metrics.counter.use_counter_doc_htmlmediaelement_remote
    ) AS use_counter_doc_htmlmediaelement_remote,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_cancelvideoframecallback
    ) AS use_counter_doc_htmlvideoelement_cancelvideoframecallback,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_disablepictureinpicture
    ) AS use_counter_doc_htmlvideoelement_disablepictureinpicture,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_onenterpictureinpicture
    ) AS use_counter_doc_htmlvideoelement_onenterpictureinpicture,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_onleavepictureinpicture
    ) AS use_counter_doc_htmlvideoelement_onleavepictureinpicture,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_playsinline
    ) AS use_counter_doc_htmlvideoelement_playsinline,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_requestpictureinpicture
    ) AS use_counter_doc_htmlvideoelement_requestpictureinpicture,
    SUM(
      metrics.counter.use_counter_doc_htmlvideoelement_requestvideoframecallback
    ) AS use_counter_doc_htmlvideoelement_requestvideoframecallback,
    SUM(
      metrics.counter.use_counter_doc_imagedata_colorspace
    ) AS use_counter_doc_imagedata_colorspace,
    SUM(metrics.counter.use_counter_doc_js_asmjs) AS use_counter_doc_js_asmjs,
    SUM(metrics.counter.use_counter_doc_js_late_weekday) AS use_counter_doc_js_late_weekday,
    SUM(metrics.counter.use_counter_doc_js_wasm) AS use_counter_doc_js_wasm,
    SUM(
      metrics.counter.use_counter_doc_location_ancestororigins
    ) AS use_counter_doc_location_ancestororigins,
    SUM(
      metrics.counter.use_counter_doc_mediadevices_enumeratedevices
    ) AS use_counter_doc_mediadevices_enumeratedevices,
    SUM(
      metrics.counter.use_counter_doc_mediadevices_getdisplaymedia
    ) AS use_counter_doc_mediadevices_getdisplaymedia,
    SUM(
      metrics.counter.use_counter_doc_mediadevices_getusermedia
    ) AS use_counter_doc_mediadevices_getusermedia,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_audio_failure
    ) AS use_counter_doc_mixed_content_not_upgraded_audio_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_audio_success
    ) AS use_counter_doc_mixed_content_not_upgraded_audio_success,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_image_failure
    ) AS use_counter_doc_mixed_content_not_upgraded_image_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_image_success
    ) AS use_counter_doc_mixed_content_not_upgraded_image_success,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_video_failure
    ) AS use_counter_doc_mixed_content_not_upgraded_video_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_not_upgraded_video_success
    ) AS use_counter_doc_mixed_content_not_upgraded_video_success,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_audio_failure
    ) AS use_counter_doc_mixed_content_upgraded_audio_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_audio_success
    ) AS use_counter_doc_mixed_content_upgraded_audio_success,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_image_failure
    ) AS use_counter_doc_mixed_content_upgraded_image_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_image_success
    ) AS use_counter_doc_mixed_content_upgraded_image_success,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_video_failure
    ) AS use_counter_doc_mixed_content_upgraded_video_failure,
    SUM(
      metrics.counter.use_counter_doc_mixed_content_upgraded_video_success
    ) AS use_counter_doc_mixed_content_upgraded_video_success,
    SUM(
      metrics.counter.use_counter_doc_moz_get_user_media_insec
    ) AS use_counter_doc_moz_get_user_media_insec,
    SUM(metrics.counter.use_counter_doc_navigator_canshare) AS use_counter_doc_navigator_canshare,
    SUM(
      metrics.counter.use_counter_doc_navigator_clearappbadge
    ) AS use_counter_doc_navigator_clearappbadge,
    SUM(
      metrics.counter.use_counter_doc_navigator_mozgetusermedia
    ) AS use_counter_doc_navigator_mozgetusermedia,
    SUM(
      metrics.counter.use_counter_doc_navigator_setappbadge
    ) AS use_counter_doc_navigator_setappbadge,
    SUM(metrics.counter.use_counter_doc_navigator_share) AS use_counter_doc_navigator_share,
    SUM(
      metrics.counter.use_counter_doc_navigator_useractivation
    ) AS use_counter_doc_navigator_useractivation,
    SUM(metrics.counter.use_counter_doc_navigator_wakelock) AS use_counter_doc_navigator_wakelock,
    SUM(metrics.counter.use_counter_doc_onbounce) AS use_counter_doc_onbounce,
    SUM(metrics.counter.use_counter_doc_ondommousescroll) AS use_counter_doc_ondommousescroll,
    SUM(metrics.counter.use_counter_doc_onfinish) AS use_counter_doc_onfinish,
    SUM(
      metrics.counter.use_counter_doc_onmozmousepixelscroll
    ) AS use_counter_doc_onmozmousepixelscroll,
    SUM(metrics.counter.use_counter_doc_onoverflow) AS use_counter_doc_onoverflow,
    SUM(metrics.counter.use_counter_doc_onstart) AS use_counter_doc_onstart,
    SUM(metrics.counter.use_counter_doc_onunderflow) AS use_counter_doc_onunderflow,
    SUM(
      metrics.counter.use_counter_doc_percentage_stroke_width_in_svg
    ) AS use_counter_doc_percentage_stroke_width_in_svg,
    SUM(
      metrics.counter.use_counter_doc_percentage_stroke_width_in_svgtext
    ) AS use_counter_doc_percentage_stroke_width_in_svgtext,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_caches_delete
    ) AS use_counter_doc_private_browsing_caches_delete,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_caches_has
    ) AS use_counter_doc_private_browsing_caches_has,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_caches_keys
    ) AS use_counter_doc_private_browsing_caches_keys,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_caches_match
    ) AS use_counter_doc_private_browsing_caches_match,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_caches_open
    ) AS use_counter_doc_private_browsing_caches_open,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_idbfactory_delete_database
    ) AS use_counter_doc_private_browsing_idbfactory_delete_database,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_idbfactory_open
    ) AS use_counter_doc_private_browsing_idbfactory_open,
    SUM(
      metrics.counter.use_counter_doc_private_browsing_navigator_service_worker
    ) AS use_counter_doc_private_browsing_navigator_service_worker,
    SUM(
      metrics.counter.use_counter_doc_pushmanager_subscribe
    ) AS use_counter_doc_pushmanager_subscribe,
    SUM(
      metrics.counter.use_counter_doc_pushsubscription_unsubscribe
    ) AS use_counter_doc_pushsubscription_unsubscribe,
    SUM(
      metrics.counter.use_counter_doc_range_createcontextualfragment
    ) AS use_counter_doc_range_createcontextualfragment,
    SUM(
      metrics.counter.use_counter_doc_sanitizer_constructor
    ) AS use_counter_doc_sanitizer_constructor,
    SUM(metrics.counter.use_counter_doc_sanitizer_sanitize) AS use_counter_doc_sanitizer_sanitize,
    SUM(metrics.counter.use_counter_doc_scheduler_posttask) AS use_counter_doc_scheduler_posttask,
    SUM(
      metrics.counter.use_counter_doc_shadowroot_pictureinpictureelement
    ) AS use_counter_doc_shadowroot_pictureinpictureelement,
    SUM(
      metrics.counter.use_counter_doc_svgsvgelement_currentscale_getter
    ) AS use_counter_doc_svgsvgelement_currentscale_getter,
    SUM(
      metrics.counter.use_counter_doc_svgsvgelement_currentscale_setter
    ) AS use_counter_doc_svgsvgelement_currentscale_setter,
    SUM(
      metrics.counter.use_counter_doc_svgsvgelement_getelementbyid
    ) AS use_counter_doc_svgsvgelement_getelementbyid,
    SUM(
      metrics.counter.use_counter_doc_window_absoluteorientationsensor
    ) AS use_counter_doc_window_absoluteorientationsensor,
    SUM(
      metrics.counter.use_counter_doc_window_accelerometer
    ) AS use_counter_doc_window_accelerometer,
    SUM(
      metrics.counter.use_counter_doc_window_backgroundfetchmanager
    ) AS use_counter_doc_window_backgroundfetchmanager,
    SUM(
      metrics.counter.use_counter_doc_window_backgroundfetchrecord
    ) AS use_counter_doc_window_backgroundfetchrecord,
    SUM(
      metrics.counter.use_counter_doc_window_backgroundfetchregistration
    ) AS use_counter_doc_window_backgroundfetchregistration,
    SUM(
      metrics.counter.use_counter_doc_window_beforeinstallpromptevent
    ) AS use_counter_doc_window_beforeinstallpromptevent,
    SUM(metrics.counter.use_counter_doc_window_bluetooth) AS use_counter_doc_window_bluetooth,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothcharacteristicproperties
    ) AS use_counter_doc_window_bluetoothcharacteristicproperties,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothdevice
    ) AS use_counter_doc_window_bluetoothdevice,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothremotegattcharacteristic
    ) AS use_counter_doc_window_bluetoothremotegattcharacteristic,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothremotegattdescriptor
    ) AS use_counter_doc_window_bluetoothremotegattdescriptor,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothremotegattserver
    ) AS use_counter_doc_window_bluetoothremotegattserver,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothremotegattservice
    ) AS use_counter_doc_window_bluetoothremotegattservice,
    SUM(
      metrics.counter.use_counter_doc_window_bluetoothuuid
    ) AS use_counter_doc_window_bluetoothuuid,
    SUM(
      metrics.counter.use_counter_doc_window_canvascapturemediastreamtrack
    ) AS use_counter_doc_window_canvascapturemediastreamtrack,
    SUM(metrics.counter.use_counter_doc_window_chrome) AS use_counter_doc_window_chrome,
    SUM(
      metrics.counter.use_counter_doc_window_clipboarditem
    ) AS use_counter_doc_window_clipboarditem,
    SUM(
      metrics.counter.use_counter_doc_window_cssimagevalue
    ) AS use_counter_doc_window_cssimagevalue,
    SUM(
      metrics.counter.use_counter_doc_window_csskeywordvalue
    ) AS use_counter_doc_window_csskeywordvalue,
    SUM(metrics.counter.use_counter_doc_window_cssmathclamp) AS use_counter_doc_window_cssmathclamp,
    SUM(
      metrics.counter.use_counter_doc_window_cssmathinvert
    ) AS use_counter_doc_window_cssmathinvert,
    SUM(metrics.counter.use_counter_doc_window_cssmathmax) AS use_counter_doc_window_cssmathmax,
    SUM(metrics.counter.use_counter_doc_window_cssmathmin) AS use_counter_doc_window_cssmathmin,
    SUM(
      metrics.counter.use_counter_doc_window_cssmathnegate
    ) AS use_counter_doc_window_cssmathnegate,
    SUM(
      metrics.counter.use_counter_doc_window_cssmathproduct
    ) AS use_counter_doc_window_cssmathproduct,
    SUM(metrics.counter.use_counter_doc_window_cssmathsum) AS use_counter_doc_window_cssmathsum,
    SUM(metrics.counter.use_counter_doc_window_cssmathvalue) AS use_counter_doc_window_cssmathvalue,
    SUM(
      metrics.counter.use_counter_doc_window_cssmatrixcomponent
    ) AS use_counter_doc_window_cssmatrixcomponent,
    SUM(
      metrics.counter.use_counter_doc_window_cssnumericarray
    ) AS use_counter_doc_window_cssnumericarray,
    SUM(
      metrics.counter.use_counter_doc_window_cssnumericvalue
    ) AS use_counter_doc_window_cssnumericvalue,
    SUM(
      metrics.counter.use_counter_doc_window_cssperspective
    ) AS use_counter_doc_window_cssperspective,
    SUM(
      metrics.counter.use_counter_doc_window_csspositionvalue
    ) AS use_counter_doc_window_csspositionvalue,
    SUM(
      metrics.counter.use_counter_doc_window_csspropertyrule
    ) AS use_counter_doc_window_csspropertyrule,
    SUM(metrics.counter.use_counter_doc_window_cssrotate) AS use_counter_doc_window_cssrotate,
    SUM(metrics.counter.use_counter_doc_window_cssscale) AS use_counter_doc_window_cssscale,
    SUM(metrics.counter.use_counter_doc_window_cssskew) AS use_counter_doc_window_cssskew,
    SUM(metrics.counter.use_counter_doc_window_cssskewx) AS use_counter_doc_window_cssskewx,
    SUM(metrics.counter.use_counter_doc_window_cssskewy) AS use_counter_doc_window_cssskewy,
    SUM(
      metrics.counter.use_counter_doc_window_cssstylevalue
    ) AS use_counter_doc_window_cssstylevalue,
    SUM(
      metrics.counter.use_counter_doc_window_csstransformcomponent
    ) AS use_counter_doc_window_csstransformcomponent,
    SUM(
      metrics.counter.use_counter_doc_window_csstransformvalue
    ) AS use_counter_doc_window_csstransformvalue,
    SUM(metrics.counter.use_counter_doc_window_csstranslate) AS use_counter_doc_window_csstranslate,
    SUM(metrics.counter.use_counter_doc_window_cssunitvalue) AS use_counter_doc_window_cssunitvalue,
    SUM(
      metrics.counter.use_counter_doc_window_cssunparsedvalue
    ) AS use_counter_doc_window_cssunparsedvalue,
    SUM(
      metrics.counter.use_counter_doc_window_cssvariablereferencevalue
    ) AS use_counter_doc_window_cssvariablereferencevalue,
    SUM(
      metrics.counter.use_counter_doc_window_defaultstatus
    ) AS use_counter_doc_window_defaultstatus,
    SUM(
      metrics.counter.use_counter_doc_window_devicemotioneventacceleration
    ) AS use_counter_doc_window_devicemotioneventacceleration,
    SUM(
      metrics.counter.use_counter_doc_window_devicemotioneventrotationrate
    ) AS use_counter_doc_window_devicemotioneventrotationrate,
    SUM(metrics.counter.use_counter_doc_window_domerror) AS use_counter_doc_window_domerror,
    SUM(
      metrics.counter.use_counter_doc_window_encodedvideochunk
    ) AS use_counter_doc_window_encodedvideochunk,
    SUM(
      metrics.counter.use_counter_doc_window_enterpictureinpictureevent
    ) AS use_counter_doc_window_enterpictureinpictureevent,
    SUM(metrics.counter.use_counter_doc_window_external) AS use_counter_doc_window_external,
    SUM(
      metrics.counter.use_counter_doc_window_federatedcredential
    ) AS use_counter_doc_window_federatedcredential,
    SUM(metrics.counter.use_counter_doc_window_gyroscope) AS use_counter_doc_window_gyroscope,
    SUM(
      metrics.counter.use_counter_doc_window_htmlcontentelement
    ) AS use_counter_doc_window_htmlcontentelement,
    SUM(
      metrics.counter.use_counter_doc_window_htmlshadowelement
    ) AS use_counter_doc_window_htmlshadowelement,
    SUM(metrics.counter.use_counter_doc_window_imagecapture) AS use_counter_doc_window_imagecapture,
    SUM(
      metrics.counter.use_counter_doc_window_inputdevicecapabilities
    ) AS use_counter_doc_window_inputdevicecapabilities,
    SUM(
      metrics.counter.use_counter_doc_window_inputdeviceinfo
    ) AS use_counter_doc_window_inputdeviceinfo,
    SUM(metrics.counter.use_counter_doc_window_keyboard) AS use_counter_doc_window_keyboard,
    SUM(
      metrics.counter.use_counter_doc_window_keyboardlayoutmap
    ) AS use_counter_doc_window_keyboardlayoutmap,
    SUM(
      metrics.counter.use_counter_doc_window_linearaccelerationsensor
    ) AS use_counter_doc_window_linearaccelerationsensor,
    SUM(
      metrics.counter.use_counter_doc_window_mediasettingsrange
    ) AS use_counter_doc_window_mediasettingsrange,
    SUM(metrics.counter.use_counter_doc_window_midiaccess) AS use_counter_doc_window_midiaccess,
    SUM(
      metrics.counter.use_counter_doc_window_midiconnectionevent
    ) AS use_counter_doc_window_midiconnectionevent,
    SUM(metrics.counter.use_counter_doc_window_midiinput) AS use_counter_doc_window_midiinput,
    SUM(metrics.counter.use_counter_doc_window_midiinputmap) AS use_counter_doc_window_midiinputmap,
    SUM(
      metrics.counter.use_counter_doc_window_midimessageevent
    ) AS use_counter_doc_window_midimessageevent,
    SUM(metrics.counter.use_counter_doc_window_midioutput) AS use_counter_doc_window_midioutput,
    SUM(
      metrics.counter.use_counter_doc_window_midioutputmap
    ) AS use_counter_doc_window_midioutputmap,
    SUM(metrics.counter.use_counter_doc_window_midiport) AS use_counter_doc_window_midiport,
    SUM(
      metrics.counter.use_counter_doc_window_networkinformation
    ) AS use_counter_doc_window_networkinformation,
    SUM(
      metrics.counter.use_counter_doc_window_offscreenbuffering
    ) AS use_counter_doc_window_offscreenbuffering,
    SUM(
      metrics.counter.use_counter_doc_window_onbeforeinstallprompt
    ) AS use_counter_doc_window_onbeforeinstallprompt,
    SUM(metrics.counter.use_counter_doc_window_oncancel) AS use_counter_doc_window_oncancel,
    SUM(metrics.counter.use_counter_doc_window_onmousewheel) AS use_counter_doc_window_onmousewheel,
    SUM(
      metrics.counter.use_counter_doc_window_onorientationchange
    ) AS use_counter_doc_window_onorientationchange,
    SUM(metrics.counter.use_counter_doc_window_onsearch) AS use_counter_doc_window_onsearch,
    SUM(
      metrics.counter.use_counter_doc_window_onselectionchange
    ) AS use_counter_doc_window_onselectionchange,
    SUM(
      metrics.counter.use_counter_doc_window_open_empty_url
    ) AS use_counter_doc_window_open_empty_url,
    SUM(metrics.counter.use_counter_doc_window_opendatabase) AS use_counter_doc_window_opendatabase,
    SUM(metrics.counter.use_counter_doc_window_orientation) AS use_counter_doc_window_orientation,
    SUM(
      metrics.counter.use_counter_doc_window_orientationsensor
    ) AS use_counter_doc_window_orientationsensor,
    SUM(
      metrics.counter.use_counter_doc_window_overconstrainederror
    ) AS use_counter_doc_window_overconstrainederror,
    SUM(
      metrics.counter.use_counter_doc_window_passwordcredential
    ) AS use_counter_doc_window_passwordcredential,
    SUM(
      metrics.counter.use_counter_doc_window_paymentaddress
    ) AS use_counter_doc_window_paymentaddress,
    SUM(
      metrics.counter.use_counter_doc_window_paymentinstruments
    ) AS use_counter_doc_window_paymentinstruments,
    SUM(
      metrics.counter.use_counter_doc_window_paymentmanager
    ) AS use_counter_doc_window_paymentmanager,
    SUM(
      metrics.counter.use_counter_doc_window_paymentmethodchangeevent
    ) AS use_counter_doc_window_paymentmethodchangeevent,
    SUM(
      metrics.counter.use_counter_doc_window_paymentrequest
    ) AS use_counter_doc_window_paymentrequest,
    SUM(
      metrics.counter.use_counter_doc_window_paymentrequestupdateevent
    ) AS use_counter_doc_window_paymentrequestupdateevent,
    SUM(
      metrics.counter.use_counter_doc_window_paymentresponse
    ) AS use_counter_doc_window_paymentresponse,
    SUM(
      metrics.counter.use_counter_doc_window_performancelongtasktiming
    ) AS use_counter_doc_window_performancelongtasktiming,
    SUM(
      metrics.counter.use_counter_doc_window_photocapabilities
    ) AS use_counter_doc_window_photocapabilities,
    SUM(
      metrics.counter.use_counter_doc_window_pictureinpictureevent
    ) AS use_counter_doc_window_pictureinpictureevent,
    SUM(
      metrics.counter.use_counter_doc_window_pictureinpicturewindow
    ) AS use_counter_doc_window_pictureinpicturewindow,
    SUM(metrics.counter.use_counter_doc_window_presentation) AS use_counter_doc_window_presentation,
    SUM(
      metrics.counter.use_counter_doc_window_presentationavailability
    ) AS use_counter_doc_window_presentationavailability,
    SUM(
      metrics.counter.use_counter_doc_window_presentationconnection
    ) AS use_counter_doc_window_presentationconnection,
    SUM(
      metrics.counter.use_counter_doc_window_presentationconnectionavailableevent
    ) AS use_counter_doc_window_presentationconnectionavailableevent,
    SUM(
      metrics.counter.use_counter_doc_window_presentationconnectioncloseevent
    ) AS use_counter_doc_window_presentationconnectioncloseevent,
    SUM(
      metrics.counter.use_counter_doc_window_presentationconnectionlist
    ) AS use_counter_doc_window_presentationconnectionlist,
    SUM(
      metrics.counter.use_counter_doc_window_presentationreceiver
    ) AS use_counter_doc_window_presentationreceiver,
    SUM(
      metrics.counter.use_counter_doc_window_presentationrequest
    ) AS use_counter_doc_window_presentationrequest,
    SUM(
      metrics.counter.use_counter_doc_window_relativeorientationsensor
    ) AS use_counter_doc_window_relativeorientationsensor,
    SUM(
      metrics.counter.use_counter_doc_window_remoteplayback
    ) AS use_counter_doc_window_remoteplayback,
    SUM(metrics.counter.use_counter_doc_window_report) AS use_counter_doc_window_report,
    SUM(metrics.counter.use_counter_doc_window_reportbody) AS use_counter_doc_window_reportbody,
    SUM(
      metrics.counter.use_counter_doc_window_reportingobserver
    ) AS use_counter_doc_window_reportingobserver,
    SUM(metrics.counter.use_counter_doc_window_rtcerror) AS use_counter_doc_window_rtcerror,
    SUM(
      metrics.counter.use_counter_doc_window_rtcerrorevent
    ) AS use_counter_doc_window_rtcerrorevent,
    SUM(
      metrics.counter.use_counter_doc_window_rtcicetransport
    ) AS use_counter_doc_window_rtcicetransport,
    SUM(
      metrics.counter.use_counter_doc_window_rtcpeerconnectioniceerrorevent
    ) AS use_counter_doc_window_rtcpeerconnectioniceerrorevent,
    SUM(metrics.counter.use_counter_doc_window_sensor) AS use_counter_doc_window_sensor,
    SUM(
      metrics.counter.use_counter_doc_window_sensorerrorevent
    ) AS use_counter_doc_window_sensorerrorevent,
    SUM(
      metrics.counter.use_counter_doc_window_sidebar_getter
    ) AS use_counter_doc_window_sidebar_getter,
    SUM(
      metrics.counter.use_counter_doc_window_sidebar_setter
    ) AS use_counter_doc_window_sidebar_setter,
    SUM(
      metrics.counter.use_counter_doc_window_speechrecognitionalternative
    ) AS use_counter_doc_window_speechrecognitionalternative,
    SUM(
      metrics.counter.use_counter_doc_window_speechrecognitionresult
    ) AS use_counter_doc_window_speechrecognitionresult,
    SUM(
      metrics.counter.use_counter_doc_window_speechrecognitionresultlist
    ) AS use_counter_doc_window_speechrecognitionresultlist,
    SUM(metrics.counter.use_counter_doc_window_stylemedia) AS use_counter_doc_window_stylemedia,
    SUM(
      metrics.counter.use_counter_doc_window_stylepropertymap
    ) AS use_counter_doc_window_stylepropertymap,
    SUM(
      metrics.counter.use_counter_doc_window_stylepropertymapreadonly
    ) AS use_counter_doc_window_stylepropertymapreadonly,
    SUM(
      metrics.counter.use_counter_doc_window_svgdiscardelement
    ) AS use_counter_doc_window_svgdiscardelement,
    SUM(metrics.counter.use_counter_doc_window_syncmanager) AS use_counter_doc_window_syncmanager,
    SUM(
      metrics.counter.use_counter_doc_window_taskattributiontiming
    ) AS use_counter_doc_window_taskattributiontiming,
    SUM(metrics.counter.use_counter_doc_window_textevent) AS use_counter_doc_window_textevent,
    SUM(metrics.counter.use_counter_doc_window_touch) AS use_counter_doc_window_touch,
    SUM(metrics.counter.use_counter_doc_window_touchevent) AS use_counter_doc_window_touchevent,
    SUM(metrics.counter.use_counter_doc_window_touchlist) AS use_counter_doc_window_touchlist,
    SUM(metrics.counter.use_counter_doc_window_usb) AS use_counter_doc_window_usb,
    SUM(
      metrics.counter.use_counter_doc_window_usbalternateinterface
    ) AS use_counter_doc_window_usbalternateinterface,
    SUM(
      metrics.counter.use_counter_doc_window_usbconfiguration
    ) AS use_counter_doc_window_usbconfiguration,
    SUM(
      metrics.counter.use_counter_doc_window_usbconnectionevent
    ) AS use_counter_doc_window_usbconnectionevent,
    SUM(metrics.counter.use_counter_doc_window_usbdevice) AS use_counter_doc_window_usbdevice,
    SUM(metrics.counter.use_counter_doc_window_usbendpoint) AS use_counter_doc_window_usbendpoint,
    SUM(metrics.counter.use_counter_doc_window_usbinterface) AS use_counter_doc_window_usbinterface,
    SUM(
      metrics.counter.use_counter_doc_window_usbintransferresult
    ) AS use_counter_doc_window_usbintransferresult,
    SUM(
      metrics.counter.use_counter_doc_window_usbisochronousintransferpacket
    ) AS use_counter_doc_window_usbisochronousintransferpacket,
    SUM(
      metrics.counter.use_counter_doc_window_usbisochronousintransferresult
    ) AS use_counter_doc_window_usbisochronousintransferresult,
    SUM(
      metrics.counter.use_counter_doc_window_usbisochronousouttransferpacket
    ) AS use_counter_doc_window_usbisochronousouttransferpacket,
    SUM(
      metrics.counter.use_counter_doc_window_usbisochronousouttransferresult
    ) AS use_counter_doc_window_usbisochronousouttransferresult,
    SUM(
      metrics.counter.use_counter_doc_window_usbouttransferresult
    ) AS use_counter_doc_window_usbouttransferresult,
    SUM(
      metrics.counter.use_counter_doc_window_useractivation
    ) AS use_counter_doc_window_useractivation,
    SUM(
      metrics.counter.use_counter_doc_window_videocolorspace
    ) AS use_counter_doc_window_videocolorspace,
    SUM(metrics.counter.use_counter_doc_window_videodecoder) AS use_counter_doc_window_videodecoder,
    SUM(metrics.counter.use_counter_doc_window_videoencoder) AS use_counter_doc_window_videoencoder,
    SUM(metrics.counter.use_counter_doc_window_videoframe) AS use_counter_doc_window_videoframe,
    SUM(metrics.counter.use_counter_doc_window_wakelock) AS use_counter_doc_window_wakelock,
    SUM(
      metrics.counter.use_counter_doc_window_wakelocksentinel
    ) AS use_counter_doc_window_wakelocksentinel,
    SUM(
      metrics.counter.use_counter_doc_window_webkitcancelanimationframe
    ) AS use_counter_doc_window_webkitcancelanimationframe,
    SUM(
      metrics.counter.use_counter_doc_window_webkitmediastream
    ) AS use_counter_doc_window_webkitmediastream,
    SUM(
      metrics.counter.use_counter_doc_window_webkitmutationobserver
    ) AS use_counter_doc_window_webkitmutationobserver,
    SUM(
      metrics.counter.use_counter_doc_window_webkitrequestanimationframe
    ) AS use_counter_doc_window_webkitrequestanimationframe,
    SUM(
      metrics.counter.use_counter_doc_window_webkitrequestfilesystem
    ) AS use_counter_doc_window_webkitrequestfilesystem,
    SUM(
      metrics.counter.use_counter_doc_window_webkitresolvelocalfilesystemurl
    ) AS use_counter_doc_window_webkitresolvelocalfilesystemurl,
    SUM(
      metrics.counter.use_counter_doc_window_webkitrtcpeerconnection
    ) AS use_counter_doc_window_webkitrtcpeerconnection,
    SUM(
      metrics.counter.use_counter_doc_window_webkitspeechgrammar
    ) AS use_counter_doc_window_webkitspeechgrammar,
    SUM(
      metrics.counter.use_counter_doc_window_webkitspeechgrammarlist
    ) AS use_counter_doc_window_webkitspeechgrammarlist,
    SUM(
      metrics.counter.use_counter_doc_window_webkitspeechrecognition
    ) AS use_counter_doc_window_webkitspeechrecognition,
    SUM(
      metrics.counter.use_counter_doc_window_webkitspeechrecognitionerror
    ) AS use_counter_doc_window_webkitspeechrecognitionerror,
    SUM(
      metrics.counter.use_counter_doc_window_webkitspeechrecognitionevent
    ) AS use_counter_doc_window_webkitspeechrecognitionevent,
    SUM(
      metrics.counter.use_counter_doc_window_webkitstorageinfo
    ) AS use_counter_doc_window_webkitstorageinfo,
    SUM(
      metrics.counter.use_counter_doc_workernavigator_permissions
    ) AS use_counter_doc_workernavigator_permissions,
    SUM(metrics.counter.use_counter_doc_wr_filter_fallback) AS use_counter_doc_wr_filter_fallback,
    SUM(metrics.counter.use_counter_doc_xslstylesheet) AS use_counter_doc_xslstylesheet,
    SUM(
      metrics.counter.use_counter_doc_xsltprocessor_constructor
    ) AS use_counter_doc_xsltprocessor_constructor,
    SUM(
      metrics.counter.use_counter_doc_you_tube_flash_embed
    ) AS use_counter_doc_you_tube_flash_embed,
    SUM(metrics.counter.use_counter_page_clipboard_read) AS use_counter_page_clipboard_read,
    SUM(metrics.counter.use_counter_page_clipboard_readtext) AS use_counter_page_clipboard_readtext,
    SUM(metrics.counter.use_counter_page_clipboard_write) AS use_counter_page_clipboard_write,
    SUM(metrics.counter.use_counter_page_console_assert) AS use_counter_page_console_assert,
    SUM(metrics.counter.use_counter_page_console_clear) AS use_counter_page_console_clear,
    SUM(metrics.counter.use_counter_page_console_count) AS use_counter_page_console_count,
    SUM(metrics.counter.use_counter_page_console_countreset) AS use_counter_page_console_countreset,
    SUM(metrics.counter.use_counter_page_console_debug) AS use_counter_page_console_debug,
    SUM(metrics.counter.use_counter_page_console_dir) AS use_counter_page_console_dir,
    SUM(metrics.counter.use_counter_page_console_dirxml) AS use_counter_page_console_dirxml,
    SUM(metrics.counter.use_counter_page_console_error) AS use_counter_page_console_error,
    SUM(metrics.counter.use_counter_page_console_exception) AS use_counter_page_console_exception,
    SUM(metrics.counter.use_counter_page_console_group) AS use_counter_page_console_group,
    SUM(
      metrics.counter.use_counter_page_console_groupcollapsed
    ) AS use_counter_page_console_groupcollapsed,
    SUM(metrics.counter.use_counter_page_console_groupend) AS use_counter_page_console_groupend,
    SUM(metrics.counter.use_counter_page_console_info) AS use_counter_page_console_info,
    SUM(metrics.counter.use_counter_page_console_log) AS use_counter_page_console_log,
    SUM(metrics.counter.use_counter_page_console_profile) AS use_counter_page_console_profile,
    SUM(metrics.counter.use_counter_page_console_profileend) AS use_counter_page_console_profileend,
    SUM(metrics.counter.use_counter_page_console_table) AS use_counter_page_console_table,
    SUM(metrics.counter.use_counter_page_console_time) AS use_counter_page_console_time,
    SUM(metrics.counter.use_counter_page_console_timeend) AS use_counter_page_console_timeend,
    SUM(metrics.counter.use_counter_page_console_timelog) AS use_counter_page_console_timelog,
    SUM(metrics.counter.use_counter_page_console_timestamp) AS use_counter_page_console_timestamp,
    SUM(metrics.counter.use_counter_page_console_trace) AS use_counter_page_console_trace,
    SUM(metrics.counter.use_counter_page_console_warn) AS use_counter_page_console_warn,
    SUM(
      metrics.counter.use_counter_page_customelementregistry_define
    ) AS use_counter_page_customelementregistry_define,
    SUM(metrics.counter.use_counter_page_customized_builtin) AS use_counter_page_customized_builtin,
    SUM(
      metrics.counter.use_counter_page_datatransfer_addelement
    ) AS use_counter_page_datatransfer_addelement,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozcleardataat
    ) AS use_counter_page_datatransfer_mozcleardataat,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozcursor_getter
    ) AS use_counter_page_datatransfer_mozcursor_getter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozcursor_setter
    ) AS use_counter_page_datatransfer_mozcursor_setter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozgetdataat
    ) AS use_counter_page_datatransfer_mozgetdataat,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozitemcount_getter
    ) AS use_counter_page_datatransfer_mozitemcount_getter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozitemcount_setter
    ) AS use_counter_page_datatransfer_mozitemcount_setter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozsetdataat
    ) AS use_counter_page_datatransfer_mozsetdataat,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozsourcenode_getter
    ) AS use_counter_page_datatransfer_mozsourcenode_getter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozsourcenode_setter
    ) AS use_counter_page_datatransfer_mozsourcenode_setter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_moztypesat
    ) AS use_counter_page_datatransfer_moztypesat,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozusercancelled_getter
    ) AS use_counter_page_datatransfer_mozusercancelled_getter,
    SUM(
      metrics.counter.use_counter_page_datatransfer_mozusercancelled_setter
    ) AS use_counter_page_datatransfer_mozusercancelled_setter,
    SUM(
      metrics.counter.use_counter_page_datatransferitem_webkitgetasentry
    ) AS use_counter_page_datatransferitem_webkitgetasentry,
    SUM(
      metrics.counter.use_counter_page_document_caretrangefrompoint
    ) AS use_counter_page_document_caretrangefrompoint,
    SUM(
      metrics.counter.use_counter_page_document_exec_command_content_read_only
    ) AS use_counter_page_document_exec_command_content_read_only,
    SUM(
      metrics.counter.use_counter_page_document_exitpictureinpicture
    ) AS use_counter_page_document_exitpictureinpicture,
    SUM(
      metrics.counter.use_counter_page_document_featurepolicy
    ) AS use_counter_page_document_featurepolicy,
    SUM(
      metrics.counter.use_counter_page_document_mozsetimageelement
    ) AS use_counter_page_document_mozsetimageelement,
    SUM(
      metrics.counter.use_counter_page_document_onbeforecopy
    ) AS use_counter_page_document_onbeforecopy,
    SUM(
      metrics.counter.use_counter_page_document_onbeforecut
    ) AS use_counter_page_document_onbeforecut,
    SUM(
      metrics.counter.use_counter_page_document_onbeforepaste
    ) AS use_counter_page_document_onbeforepaste,
    SUM(metrics.counter.use_counter_page_document_oncancel) AS use_counter_page_document_oncancel,
    SUM(metrics.counter.use_counter_page_document_onfreeze) AS use_counter_page_document_onfreeze,
    SUM(
      metrics.counter.use_counter_page_document_onmousewheel
    ) AS use_counter_page_document_onmousewheel,
    SUM(metrics.counter.use_counter_page_document_onresume) AS use_counter_page_document_onresume,
    SUM(metrics.counter.use_counter_page_document_onsearch) AS use_counter_page_document_onsearch,
    SUM(
      metrics.counter.use_counter_page_document_onwebkitfullscreenchange
    ) AS use_counter_page_document_onwebkitfullscreenchange,
    SUM(
      metrics.counter.use_counter_page_document_onwebkitfullscreenerror
    ) AS use_counter_page_document_onwebkitfullscreenerror,
    SUM(metrics.counter.use_counter_page_document_open) AS use_counter_page_document_open,
    SUM(
      metrics.counter.use_counter_page_document_pictureinpictureelement
    ) AS use_counter_page_document_pictureinpictureelement,
    SUM(
      metrics.counter.use_counter_page_document_pictureinpictureenabled
    ) AS use_counter_page_document_pictureinpictureenabled,
    SUM(
      metrics.counter.use_counter_page_document_query_command_state_or_value_content_read_only
    ) AS use_counter_page_document_query_command_state_or_value_content_read_only,
    SUM(
      metrics.counter.use_counter_page_document_query_command_state_or_value_insert_br_on_return
    ) AS use_counter_page_document_query_command_state_or_value_insert_br_on_return,
    SUM(
      metrics.counter.use_counter_page_document_query_command_supported_or_enabled_content_read_only
    ) AS use_counter_page_document_query_command_supported_or_enabled_content_read_only,
    SUM(
      metrics.counter.use_counter_page_document_query_command_supported_or_enabled_insert_br_on_return
    ) AS use_counter_page_document_query_command_supported_or_enabled_insert_br_on_return,
    SUM(
      metrics.counter.use_counter_page_document_registerelement
    ) AS use_counter_page_document_registerelement,
    SUM(
      metrics.counter.use_counter_page_document_wasdiscarded
    ) AS use_counter_page_document_wasdiscarded,
    SUM(
      metrics.counter.use_counter_page_document_webkitcancelfullscreen
    ) AS use_counter_page_document_webkitcancelfullscreen,
    SUM(
      metrics.counter.use_counter_page_document_webkitcurrentfullscreenelement
    ) AS use_counter_page_document_webkitcurrentfullscreenelement,
    SUM(
      metrics.counter.use_counter_page_document_webkitexitfullscreen
    ) AS use_counter_page_document_webkitexitfullscreen,
    SUM(
      metrics.counter.use_counter_page_document_webkitfullscreenelement
    ) AS use_counter_page_document_webkitfullscreenelement,
    SUM(
      metrics.counter.use_counter_page_document_webkitfullscreenenabled
    ) AS use_counter_page_document_webkitfullscreenenabled,
    SUM(
      metrics.counter.use_counter_page_document_webkithidden
    ) AS use_counter_page_document_webkithidden,
    SUM(
      metrics.counter.use_counter_page_document_webkitisfullscreen
    ) AS use_counter_page_document_webkitisfullscreen,
    SUM(
      metrics.counter.use_counter_page_document_webkitvisibilitystate
    ) AS use_counter_page_document_webkitvisibilitystate,
    SUM(
      metrics.counter.use_counter_page_document_xmlencoding
    ) AS use_counter_page_document_xmlencoding,
    SUM(
      metrics.counter.use_counter_page_document_xmlstandalone
    ) AS use_counter_page_document_xmlstandalone,
    SUM(
      metrics.counter.use_counter_page_document_xmlversion
    ) AS use_counter_page_document_xmlversion,
    SUM(
      metrics.counter.use_counter_page_domparser_parsefromstring
    ) AS use_counter_page_domparser_parsefromstring,
    SUM(
      metrics.counter.use_counter_page_element_attachshadow
    ) AS use_counter_page_element_attachshadow,
    SUM(
      metrics.counter.use_counter_page_element_computedstylemap
    ) AS use_counter_page_element_computedstylemap,
    SUM(
      metrics.counter.use_counter_page_element_onmousewheel
    ) AS use_counter_page_element_onmousewheel,
    SUM(
      metrics.counter.use_counter_page_element_releasecapture
    ) AS use_counter_page_element_releasecapture,
    SUM(
      metrics.counter.use_counter_page_element_releasepointercapture
    ) AS use_counter_page_element_releasepointercapture,
    SUM(
      metrics.counter.use_counter_page_element_scrollintoviewifneeded
    ) AS use_counter_page_element_scrollintoviewifneeded,
    SUM(metrics.counter.use_counter_page_element_setcapture) AS use_counter_page_element_setcapture,
    SUM(metrics.counter.use_counter_page_element_sethtml) AS use_counter_page_element_sethtml,
    SUM(
      metrics.counter.use_counter_page_element_setpointercapture
    ) AS use_counter_page_element_setpointercapture,
    SUM(
      metrics.counter.use_counter_page_enumerate_devices_insec
    ) AS use_counter_page_enumerate_devices_insec,
    SUM(
      metrics.counter.use_counter_page_enumerate_devices_unfocused
    ) AS use_counter_page_enumerate_devices_unfocused,
    SUM(metrics.counter.use_counter_page_fe_blend) AS use_counter_page_fe_blend,
    SUM(metrics.counter.use_counter_page_fe_color_matrix) AS use_counter_page_fe_color_matrix,
    SUM(
      metrics.counter.use_counter_page_fe_component_transfer
    ) AS use_counter_page_fe_component_transfer,
    SUM(metrics.counter.use_counter_page_fe_composite) AS use_counter_page_fe_composite,
    SUM(metrics.counter.use_counter_page_fe_convolve_matrix) AS use_counter_page_fe_convolve_matrix,
    SUM(
      metrics.counter.use_counter_page_fe_diffuse_lighting
    ) AS use_counter_page_fe_diffuse_lighting,
    SUM(
      metrics.counter.use_counter_page_fe_displacement_map
    ) AS use_counter_page_fe_displacement_map,
    SUM(metrics.counter.use_counter_page_fe_flood) AS use_counter_page_fe_flood,
    SUM(metrics.counter.use_counter_page_fe_gaussian_blur) AS use_counter_page_fe_gaussian_blur,
    SUM(metrics.counter.use_counter_page_fe_image) AS use_counter_page_fe_image,
    SUM(metrics.counter.use_counter_page_fe_merge) AS use_counter_page_fe_merge,
    SUM(metrics.counter.use_counter_page_fe_morphology) AS use_counter_page_fe_morphology,
    SUM(metrics.counter.use_counter_page_fe_offset) AS use_counter_page_fe_offset,
    SUM(
      metrics.counter.use_counter_page_fe_specular_lighting
    ) AS use_counter_page_fe_specular_lighting,
    SUM(metrics.counter.use_counter_page_fe_tile) AS use_counter_page_fe_tile,
    SUM(metrics.counter.use_counter_page_fe_turbulence) AS use_counter_page_fe_turbulence,
    SUM(
      metrics.counter.use_counter_page_filtered_cross_origin_iframe
    ) AS use_counter_page_filtered_cross_origin_iframe,
    SUM(
      metrics.counter.use_counter_page_get_user_media_insec
    ) AS use_counter_page_get_user_media_insec,
    SUM(
      metrics.counter.use_counter_page_get_user_media_unfocused
    ) AS use_counter_page_get_user_media_unfocused,
    SUM(
      metrics.counter.use_counter_page_htmlbuttonelement_popovertargetaction
    ) AS use_counter_page_htmlbuttonelement_popovertargetaction,
    SUM(
      metrics.counter.use_counter_page_htmlbuttonelement_popovertargetelement
    ) AS use_counter_page_htmlbuttonelement_popovertargetelement,
    SUM(
      metrics.counter.use_counter_page_htmldocument_named_getter_hit
    ) AS use_counter_page_htmldocument_named_getter_hit,
    SUM(
      metrics.counter.use_counter_page_htmlelement_attributestylemap
    ) AS use_counter_page_htmlelement_attributestylemap,
    SUM(
      metrics.counter.use_counter_page_htmlelement_hidepopover
    ) AS use_counter_page_htmlelement_hidepopover,
    SUM(
      metrics.counter.use_counter_page_htmlelement_popover
    ) AS use_counter_page_htmlelement_popover,
    SUM(
      metrics.counter.use_counter_page_htmlelement_showpopover
    ) AS use_counter_page_htmlelement_showpopover,
    SUM(
      metrics.counter.use_counter_page_htmlelement_togglepopover
    ) AS use_counter_page_htmlelement_togglepopover,
    SUM(
      metrics.counter.use_counter_page_htmliframeelement_loading
    ) AS use_counter_page_htmliframeelement_loading,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_capture
    ) AS use_counter_page_htmlinputelement_capture,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_incremental
    ) AS use_counter_page_htmlinputelement_incremental,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_onsearch
    ) AS use_counter_page_htmlinputelement_onsearch,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_popovertargetaction
    ) AS use_counter_page_htmlinputelement_popovertargetaction,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_popovertargetelement
    ) AS use_counter_page_htmlinputelement_popovertargetelement,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_webkitdirectory
    ) AS use_counter_page_htmlinputelement_webkitdirectory,
    SUM(
      metrics.counter.use_counter_page_htmlinputelement_webkitentries
    ) AS use_counter_page_htmlinputelement_webkitentries,
    SUM(
      metrics.counter.use_counter_page_htmlmediaelement_disableremoteplayback
    ) AS use_counter_page_htmlmediaelement_disableremoteplayback,
    SUM(
      metrics.counter.use_counter_page_htmlmediaelement_remote
    ) AS use_counter_page_htmlmediaelement_remote,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_cancelvideoframecallback
    ) AS use_counter_page_htmlvideoelement_cancelvideoframecallback,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_disablepictureinpicture
    ) AS use_counter_page_htmlvideoelement_disablepictureinpicture,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_onenterpictureinpicture
    ) AS use_counter_page_htmlvideoelement_onenterpictureinpicture,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_onleavepictureinpicture
    ) AS use_counter_page_htmlvideoelement_onleavepictureinpicture,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_playsinline
    ) AS use_counter_page_htmlvideoelement_playsinline,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_requestpictureinpicture
    ) AS use_counter_page_htmlvideoelement_requestpictureinpicture,
    SUM(
      metrics.counter.use_counter_page_htmlvideoelement_requestvideoframecallback
    ) AS use_counter_page_htmlvideoelement_requestvideoframecallback,
    SUM(
      metrics.counter.use_counter_page_imagedata_colorspace
    ) AS use_counter_page_imagedata_colorspace,
    SUM(metrics.counter.use_counter_page_js_asmjs) AS use_counter_page_js_asmjs,
    SUM(metrics.counter.use_counter_page_js_late_weekday) AS use_counter_page_js_late_weekday,
    SUM(metrics.counter.use_counter_page_js_wasm) AS use_counter_page_js_wasm,
    SUM(
      metrics.counter.use_counter_page_location_ancestororigins
    ) AS use_counter_page_location_ancestororigins,
    SUM(
      metrics.counter.use_counter_page_mediadevices_enumeratedevices
    ) AS use_counter_page_mediadevices_enumeratedevices,
    SUM(
      metrics.counter.use_counter_page_mediadevices_getdisplaymedia
    ) AS use_counter_page_mediadevices_getdisplaymedia,
    SUM(
      metrics.counter.use_counter_page_mediadevices_getusermedia
    ) AS use_counter_page_mediadevices_getusermedia,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_audio_failure
    ) AS use_counter_page_mixed_content_not_upgraded_audio_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_audio_success
    ) AS use_counter_page_mixed_content_not_upgraded_audio_success,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_image_failure
    ) AS use_counter_page_mixed_content_not_upgraded_image_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_image_success
    ) AS use_counter_page_mixed_content_not_upgraded_image_success,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_video_failure
    ) AS use_counter_page_mixed_content_not_upgraded_video_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_not_upgraded_video_success
    ) AS use_counter_page_mixed_content_not_upgraded_video_success,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_audio_failure
    ) AS use_counter_page_mixed_content_upgraded_audio_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_audio_success
    ) AS use_counter_page_mixed_content_upgraded_audio_success,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_image_failure
    ) AS use_counter_page_mixed_content_upgraded_image_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_image_success
    ) AS use_counter_page_mixed_content_upgraded_image_success,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_video_failure
    ) AS use_counter_page_mixed_content_upgraded_video_failure,
    SUM(
      metrics.counter.use_counter_page_mixed_content_upgraded_video_success
    ) AS use_counter_page_mixed_content_upgraded_video_success,
    SUM(
      metrics.counter.use_counter_page_moz_get_user_media_insec
    ) AS use_counter_page_moz_get_user_media_insec,
    SUM(metrics.counter.use_counter_page_navigator_canshare) AS use_counter_page_navigator_canshare,
    SUM(
      metrics.counter.use_counter_page_navigator_clearappbadge
    ) AS use_counter_page_navigator_clearappbadge,
    SUM(
      metrics.counter.use_counter_page_navigator_mozgetusermedia
    ) AS use_counter_page_navigator_mozgetusermedia,
    SUM(
      metrics.counter.use_counter_page_navigator_setappbadge
    ) AS use_counter_page_navigator_setappbadge,
    SUM(metrics.counter.use_counter_page_navigator_share) AS use_counter_page_navigator_share,
    SUM(
      metrics.counter.use_counter_page_navigator_useractivation
    ) AS use_counter_page_navigator_useractivation,
    SUM(metrics.counter.use_counter_page_navigator_wakelock) AS use_counter_page_navigator_wakelock,
    SUM(metrics.counter.use_counter_page_onbounce) AS use_counter_page_onbounce,
    SUM(metrics.counter.use_counter_page_ondommousescroll) AS use_counter_page_ondommousescroll,
    SUM(metrics.counter.use_counter_page_onfinish) AS use_counter_page_onfinish,
    SUM(
      metrics.counter.use_counter_page_onmozmousepixelscroll
    ) AS use_counter_page_onmozmousepixelscroll,
    SUM(metrics.counter.use_counter_page_onoverflow) AS use_counter_page_onoverflow,
    SUM(metrics.counter.use_counter_page_onstart) AS use_counter_page_onstart,
    SUM(metrics.counter.use_counter_page_onunderflow) AS use_counter_page_onunderflow,
    SUM(
      metrics.counter.use_counter_page_percentage_stroke_width_in_svg
    ) AS use_counter_page_percentage_stroke_width_in_svg,
    SUM(
      metrics.counter.use_counter_page_percentage_stroke_width_in_svgtext
    ) AS use_counter_page_percentage_stroke_width_in_svgtext,
    SUM(
      metrics.counter.use_counter_page_private_browsing_caches_delete
    ) AS use_counter_page_private_browsing_caches_delete,
    SUM(
      metrics.counter.use_counter_page_private_browsing_caches_has
    ) AS use_counter_page_private_browsing_caches_has,
    SUM(
      metrics.counter.use_counter_page_private_browsing_caches_keys
    ) AS use_counter_page_private_browsing_caches_keys,
    SUM(
      metrics.counter.use_counter_page_private_browsing_caches_match
    ) AS use_counter_page_private_browsing_caches_match,
    SUM(
      metrics.counter.use_counter_page_private_browsing_caches_open
    ) AS use_counter_page_private_browsing_caches_open,
    SUM(
      metrics.counter.use_counter_page_private_browsing_idbfactory_delete_database
    ) AS use_counter_page_private_browsing_idbfactory_delete_database,
    SUM(
      metrics.counter.use_counter_page_private_browsing_idbfactory_open
    ) AS use_counter_page_private_browsing_idbfactory_open,
    SUM(
      metrics.counter.use_counter_page_private_browsing_navigator_service_worker
    ) AS use_counter_page_private_browsing_navigator_service_worker,
    SUM(
      metrics.counter.use_counter_page_pushmanager_subscribe
    ) AS use_counter_page_pushmanager_subscribe,
    SUM(
      metrics.counter.use_counter_page_pushsubscription_unsubscribe
    ) AS use_counter_page_pushsubscription_unsubscribe,
    SUM(
      metrics.counter.use_counter_page_range_createcontextualfragment
    ) AS use_counter_page_range_createcontextualfragment,
    SUM(
      metrics.counter.use_counter_page_sanitizer_constructor
    ) AS use_counter_page_sanitizer_constructor,
    SUM(metrics.counter.use_counter_page_sanitizer_sanitize) AS use_counter_page_sanitizer_sanitize,
    SUM(metrics.counter.use_counter_page_scheduler_posttask) AS use_counter_page_scheduler_posttask,
    SUM(
      metrics.counter.use_counter_page_shadowroot_pictureinpictureelement
    ) AS use_counter_page_shadowroot_pictureinpictureelement,
    SUM(
      metrics.counter.use_counter_page_svgsvgelement_currentscale_getter
    ) AS use_counter_page_svgsvgelement_currentscale_getter,
    SUM(
      metrics.counter.use_counter_page_svgsvgelement_currentscale_setter
    ) AS use_counter_page_svgsvgelement_currentscale_setter,
    SUM(
      metrics.counter.use_counter_page_svgsvgelement_getelementbyid
    ) AS use_counter_page_svgsvgelement_getelementbyid,
    SUM(
      metrics.counter.use_counter_page_window_absoluteorientationsensor
    ) AS use_counter_page_window_absoluteorientationsensor,
    SUM(
      metrics.counter.use_counter_page_window_accelerometer
    ) AS use_counter_page_window_accelerometer,
    SUM(
      metrics.counter.use_counter_page_window_backgroundfetchmanager
    ) AS use_counter_page_window_backgroundfetchmanager,
    SUM(
      metrics.counter.use_counter_page_window_backgroundfetchrecord
    ) AS use_counter_page_window_backgroundfetchrecord,
    SUM(
      metrics.counter.use_counter_page_window_backgroundfetchregistration
    ) AS use_counter_page_window_backgroundfetchregistration,
    SUM(
      metrics.counter.use_counter_page_window_beforeinstallpromptevent
    ) AS use_counter_page_window_beforeinstallpromptevent,
    SUM(metrics.counter.use_counter_page_window_bluetooth) AS use_counter_page_window_bluetooth,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothcharacteristicproperties
    ) AS use_counter_page_window_bluetoothcharacteristicproperties,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothdevice
    ) AS use_counter_page_window_bluetoothdevice,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothremotegattcharacteristic
    ) AS use_counter_page_window_bluetoothremotegattcharacteristic,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothremotegattdescriptor
    ) AS use_counter_page_window_bluetoothremotegattdescriptor,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothremotegattserver
    ) AS use_counter_page_window_bluetoothremotegattserver,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothremotegattservice
    ) AS use_counter_page_window_bluetoothremotegattservice,
    SUM(
      metrics.counter.use_counter_page_window_bluetoothuuid
    ) AS use_counter_page_window_bluetoothuuid,
    SUM(
      metrics.counter.use_counter_page_window_canvascapturemediastreamtrack
    ) AS use_counter_page_window_canvascapturemediastreamtrack,
    SUM(metrics.counter.use_counter_page_window_chrome) AS use_counter_page_window_chrome,
    SUM(
      metrics.counter.use_counter_page_window_clipboarditem
    ) AS use_counter_page_window_clipboarditem,
    SUM(
      metrics.counter.use_counter_page_window_cssimagevalue
    ) AS use_counter_page_window_cssimagevalue,
    SUM(
      metrics.counter.use_counter_page_window_csskeywordvalue
    ) AS use_counter_page_window_csskeywordvalue,
    SUM(
      metrics.counter.use_counter_page_window_cssmathclamp
    ) AS use_counter_page_window_cssmathclamp,
    SUM(
      metrics.counter.use_counter_page_window_cssmathinvert
    ) AS use_counter_page_window_cssmathinvert,
    SUM(metrics.counter.use_counter_page_window_cssmathmax) AS use_counter_page_window_cssmathmax,
    SUM(metrics.counter.use_counter_page_window_cssmathmin) AS use_counter_page_window_cssmathmin,
    SUM(
      metrics.counter.use_counter_page_window_cssmathnegate
    ) AS use_counter_page_window_cssmathnegate,
    SUM(
      metrics.counter.use_counter_page_window_cssmathproduct
    ) AS use_counter_page_window_cssmathproduct,
    SUM(metrics.counter.use_counter_page_window_cssmathsum) AS use_counter_page_window_cssmathsum,
    SUM(
      metrics.counter.use_counter_page_window_cssmathvalue
    ) AS use_counter_page_window_cssmathvalue,
    SUM(
      metrics.counter.use_counter_page_window_cssmatrixcomponent
    ) AS use_counter_page_window_cssmatrixcomponent,
    SUM(
      metrics.counter.use_counter_page_window_cssnumericarray
    ) AS use_counter_page_window_cssnumericarray,
    SUM(
      metrics.counter.use_counter_page_window_cssnumericvalue
    ) AS use_counter_page_window_cssnumericvalue,
    SUM(
      metrics.counter.use_counter_page_window_cssperspective
    ) AS use_counter_page_window_cssperspective,
    SUM(
      metrics.counter.use_counter_page_window_csspositionvalue
    ) AS use_counter_page_window_csspositionvalue,
    SUM(
      metrics.counter.use_counter_page_window_csspropertyrule
    ) AS use_counter_page_window_csspropertyrule,
    SUM(metrics.counter.use_counter_page_window_cssrotate) AS use_counter_page_window_cssrotate,
    SUM(metrics.counter.use_counter_page_window_cssscale) AS use_counter_page_window_cssscale,
    SUM(metrics.counter.use_counter_page_window_cssskew) AS use_counter_page_window_cssskew,
    SUM(metrics.counter.use_counter_page_window_cssskewx) AS use_counter_page_window_cssskewx,
    SUM(metrics.counter.use_counter_page_window_cssskewy) AS use_counter_page_window_cssskewy,
    SUM(
      metrics.counter.use_counter_page_window_cssstylevalue
    ) AS use_counter_page_window_cssstylevalue,
    SUM(
      metrics.counter.use_counter_page_window_csstransformcomponent
    ) AS use_counter_page_window_csstransformcomponent,
    SUM(
      metrics.counter.use_counter_page_window_csstransformvalue
    ) AS use_counter_page_window_csstransformvalue,
    SUM(
      metrics.counter.use_counter_page_window_csstranslate
    ) AS use_counter_page_window_csstranslate,
    SUM(
      metrics.counter.use_counter_page_window_cssunitvalue
    ) AS use_counter_page_window_cssunitvalue,
    SUM(
      metrics.counter.use_counter_page_window_cssunparsedvalue
    ) AS use_counter_page_window_cssunparsedvalue,
    SUM(
      metrics.counter.use_counter_page_window_cssvariablereferencevalue
    ) AS use_counter_page_window_cssvariablereferencevalue,
    SUM(
      metrics.counter.use_counter_page_window_defaultstatus
    ) AS use_counter_page_window_defaultstatus,
    SUM(
      metrics.counter.use_counter_page_window_devicemotioneventacceleration
    ) AS use_counter_page_window_devicemotioneventacceleration,
    SUM(
      metrics.counter.use_counter_page_window_devicemotioneventrotationrate
    ) AS use_counter_page_window_devicemotioneventrotationrate,
    SUM(metrics.counter.use_counter_page_window_domerror) AS use_counter_page_window_domerror,
    SUM(
      metrics.counter.use_counter_page_window_encodedvideochunk
    ) AS use_counter_page_window_encodedvideochunk,
    SUM(
      metrics.counter.use_counter_page_window_enterpictureinpictureevent
    ) AS use_counter_page_window_enterpictureinpictureevent,
    SUM(metrics.counter.use_counter_page_window_external) AS use_counter_page_window_external,
    SUM(
      metrics.counter.use_counter_page_window_federatedcredential
    ) AS use_counter_page_window_federatedcredential,
    SUM(metrics.counter.use_counter_page_window_gyroscope) AS use_counter_page_window_gyroscope,
    SUM(
      metrics.counter.use_counter_page_window_htmlcontentelement
    ) AS use_counter_page_window_htmlcontentelement,
    SUM(
      metrics.counter.use_counter_page_window_htmlshadowelement
    ) AS use_counter_page_window_htmlshadowelement,
    SUM(
      metrics.counter.use_counter_page_window_imagecapture
    ) AS use_counter_page_window_imagecapture,
    SUM(
      metrics.counter.use_counter_page_window_inputdevicecapabilities
    ) AS use_counter_page_window_inputdevicecapabilities,
    SUM(
      metrics.counter.use_counter_page_window_inputdeviceinfo
    ) AS use_counter_page_window_inputdeviceinfo,
    SUM(metrics.counter.use_counter_page_window_keyboard) AS use_counter_page_window_keyboard,
    SUM(
      metrics.counter.use_counter_page_window_keyboardlayoutmap
    ) AS use_counter_page_window_keyboardlayoutmap,
    SUM(
      metrics.counter.use_counter_page_window_linearaccelerationsensor
    ) AS use_counter_page_window_linearaccelerationsensor,
    SUM(
      metrics.counter.use_counter_page_window_mediasettingsrange
    ) AS use_counter_page_window_mediasettingsrange,
    SUM(metrics.counter.use_counter_page_window_midiaccess) AS use_counter_page_window_midiaccess,
    SUM(
      metrics.counter.use_counter_page_window_midiconnectionevent
    ) AS use_counter_page_window_midiconnectionevent,
    SUM(metrics.counter.use_counter_page_window_midiinput) AS use_counter_page_window_midiinput,
    SUM(
      metrics.counter.use_counter_page_window_midiinputmap
    ) AS use_counter_page_window_midiinputmap,
    SUM(
      metrics.counter.use_counter_page_window_midimessageevent
    ) AS use_counter_page_window_midimessageevent,
    SUM(metrics.counter.use_counter_page_window_midioutput) AS use_counter_page_window_midioutput,
    SUM(
      metrics.counter.use_counter_page_window_midioutputmap
    ) AS use_counter_page_window_midioutputmap,
    SUM(metrics.counter.use_counter_page_window_midiport) AS use_counter_page_window_midiport,
    SUM(
      metrics.counter.use_counter_page_window_networkinformation
    ) AS use_counter_page_window_networkinformation,
    SUM(
      metrics.counter.use_counter_page_window_offscreenbuffering
    ) AS use_counter_page_window_offscreenbuffering,
    SUM(
      metrics.counter.use_counter_page_window_onbeforeinstallprompt
    ) AS use_counter_page_window_onbeforeinstallprompt,
    SUM(metrics.counter.use_counter_page_window_oncancel) AS use_counter_page_window_oncancel,
    SUM(
      metrics.counter.use_counter_page_window_onmousewheel
    ) AS use_counter_page_window_onmousewheel,
    SUM(
      metrics.counter.use_counter_page_window_onorientationchange
    ) AS use_counter_page_window_onorientationchange,
    SUM(metrics.counter.use_counter_page_window_onsearch) AS use_counter_page_window_onsearch,
    SUM(
      metrics.counter.use_counter_page_window_onselectionchange
    ) AS use_counter_page_window_onselectionchange,
    SUM(
      metrics.counter.use_counter_page_window_open_empty_url
    ) AS use_counter_page_window_open_empty_url,
    SUM(
      metrics.counter.use_counter_page_window_opendatabase
    ) AS use_counter_page_window_opendatabase,
    SUM(metrics.counter.use_counter_page_window_orientation) AS use_counter_page_window_orientation,
    SUM(
      metrics.counter.use_counter_page_window_orientationsensor
    ) AS use_counter_page_window_orientationsensor,
    SUM(
      metrics.counter.use_counter_page_window_overconstrainederror
    ) AS use_counter_page_window_overconstrainederror,
    SUM(
      metrics.counter.use_counter_page_window_passwordcredential
    ) AS use_counter_page_window_passwordcredential,
    SUM(
      metrics.counter.use_counter_page_window_paymentaddress
    ) AS use_counter_page_window_paymentaddress,
    SUM(
      metrics.counter.use_counter_page_window_paymentinstruments
    ) AS use_counter_page_window_paymentinstruments,
    SUM(
      metrics.counter.use_counter_page_window_paymentmanager
    ) AS use_counter_page_window_paymentmanager,
    SUM(
      metrics.counter.use_counter_page_window_paymentmethodchangeevent
    ) AS use_counter_page_window_paymentmethodchangeevent,
    SUM(
      metrics.counter.use_counter_page_window_paymentrequest
    ) AS use_counter_page_window_paymentrequest,
    SUM(
      metrics.counter.use_counter_page_window_paymentrequestupdateevent
    ) AS use_counter_page_window_paymentrequestupdateevent,
    SUM(
      metrics.counter.use_counter_page_window_paymentresponse
    ) AS use_counter_page_window_paymentresponse,
    SUM(
      metrics.counter.use_counter_page_window_performancelongtasktiming
    ) AS use_counter_page_window_performancelongtasktiming,
    SUM(
      metrics.counter.use_counter_page_window_photocapabilities
    ) AS use_counter_page_window_photocapabilities,
    SUM(
      metrics.counter.use_counter_page_window_pictureinpictureevent
    ) AS use_counter_page_window_pictureinpictureevent,
    SUM(
      metrics.counter.use_counter_page_window_pictureinpicturewindow
    ) AS use_counter_page_window_pictureinpicturewindow,
    SUM(
      metrics.counter.use_counter_page_window_presentation
    ) AS use_counter_page_window_presentation,
    SUM(
      metrics.counter.use_counter_page_window_presentationavailability
    ) AS use_counter_page_window_presentationavailability,
    SUM(
      metrics.counter.use_counter_page_window_presentationconnection
    ) AS use_counter_page_window_presentationconnection,
    SUM(
      metrics.counter.use_counter_page_window_presentationconnectionavailableevent
    ) AS use_counter_page_window_presentationconnectionavailableevent,
    SUM(
      metrics.counter.use_counter_page_window_presentationconnectioncloseevent
    ) AS use_counter_page_window_presentationconnectioncloseevent,
    SUM(
      metrics.counter.use_counter_page_window_presentationconnectionlist
    ) AS use_counter_page_window_presentationconnectionlist,
    SUM(
      metrics.counter.use_counter_page_window_presentationreceiver
    ) AS use_counter_page_window_presentationreceiver,
    SUM(
      metrics.counter.use_counter_page_window_presentationrequest
    ) AS use_counter_page_window_presentationrequest,
    SUM(
      metrics.counter.use_counter_page_window_relativeorientationsensor
    ) AS use_counter_page_window_relativeorientationsensor,
    SUM(
      metrics.counter.use_counter_page_window_remoteplayback
    ) AS use_counter_page_window_remoteplayback,
    SUM(metrics.counter.use_counter_page_window_report) AS use_counter_page_window_report,
    SUM(metrics.counter.use_counter_page_window_reportbody) AS use_counter_page_window_reportbody,
    SUM(
      metrics.counter.use_counter_page_window_reportingobserver
    ) AS use_counter_page_window_reportingobserver,
    SUM(metrics.counter.use_counter_page_window_rtcerror) AS use_counter_page_window_rtcerror,
    SUM(
      metrics.counter.use_counter_page_window_rtcerrorevent
    ) AS use_counter_page_window_rtcerrorevent,
    SUM(
      metrics.counter.use_counter_page_window_rtcicetransport
    ) AS use_counter_page_window_rtcicetransport,
    SUM(
      metrics.counter.use_counter_page_window_rtcpeerconnectioniceerrorevent
    ) AS use_counter_page_window_rtcpeerconnectioniceerrorevent,
    SUM(metrics.counter.use_counter_page_window_sensor) AS use_counter_page_window_sensor,
    SUM(
      metrics.counter.use_counter_page_window_sensorerrorevent
    ) AS use_counter_page_window_sensorerrorevent,
    SUM(
      metrics.counter.use_counter_page_window_sidebar_getter
    ) AS use_counter_page_window_sidebar_getter,
    SUM(
      metrics.counter.use_counter_page_window_sidebar_setter
    ) AS use_counter_page_window_sidebar_setter,
    SUM(
      metrics.counter.use_counter_page_window_speechrecognitionalternative
    ) AS use_counter_page_window_speechrecognitionalternative,
    SUM(
      metrics.counter.use_counter_page_window_speechrecognitionresult
    ) AS use_counter_page_window_speechrecognitionresult,
    SUM(
      metrics.counter.use_counter_page_window_speechrecognitionresultlist
    ) AS use_counter_page_window_speechrecognitionresultlist,
    SUM(metrics.counter.use_counter_page_window_stylemedia) AS use_counter_page_window_stylemedia,
    SUM(
      metrics.counter.use_counter_page_window_stylepropertymap
    ) AS use_counter_page_window_stylepropertymap,
    SUM(
      metrics.counter.use_counter_page_window_stylepropertymapreadonly
    ) AS use_counter_page_window_stylepropertymapreadonly,
    SUM(
      metrics.counter.use_counter_page_window_svgdiscardelement
    ) AS use_counter_page_window_svgdiscardelement,
    SUM(metrics.counter.use_counter_page_window_syncmanager) AS use_counter_page_window_syncmanager,
    SUM(
      metrics.counter.use_counter_page_window_taskattributiontiming
    ) AS use_counter_page_window_taskattributiontiming,
    SUM(metrics.counter.use_counter_page_window_textevent) AS use_counter_page_window_textevent,
    SUM(metrics.counter.use_counter_page_window_touch) AS use_counter_page_window_touch,
    SUM(metrics.counter.use_counter_page_window_touchevent) AS use_counter_page_window_touchevent,
    SUM(metrics.counter.use_counter_page_window_touchlist) AS use_counter_page_window_touchlist,
    SUM(metrics.counter.use_counter_page_window_usb) AS use_counter_page_window_usb,
    SUM(
      metrics.counter.use_counter_page_window_usbalternateinterface
    ) AS use_counter_page_window_usbalternateinterface,
    SUM(
      metrics.counter.use_counter_page_window_usbconfiguration
    ) AS use_counter_page_window_usbconfiguration,
    SUM(
      metrics.counter.use_counter_page_window_usbconnectionevent
    ) AS use_counter_page_window_usbconnectionevent,
    SUM(metrics.counter.use_counter_page_window_usbdevice) AS use_counter_page_window_usbdevice,
    SUM(metrics.counter.use_counter_page_window_usbendpoint) AS use_counter_page_window_usbendpoint,
    SUM(
      metrics.counter.use_counter_page_window_usbinterface
    ) AS use_counter_page_window_usbinterface,
    SUM(
      metrics.counter.use_counter_page_window_usbintransferresult
    ) AS use_counter_page_window_usbintransferresult,
    SUM(
      metrics.counter.use_counter_page_window_usbisochronousintransferpacket
    ) AS use_counter_page_window_usbisochronousintransferpacket,
    SUM(
      metrics.counter.use_counter_page_window_usbisochronousintransferresult
    ) AS use_counter_page_window_usbisochronousintransferresult,
    SUM(
      metrics.counter.use_counter_page_window_usbisochronousouttransferpacket
    ) AS use_counter_page_window_usbisochronousouttransferpacket,
    SUM(
      metrics.counter.use_counter_page_window_usbisochronousouttransferresult
    ) AS use_counter_page_window_usbisochronousouttransferresult,
    SUM(
      metrics.counter.use_counter_page_window_usbouttransferresult
    ) AS use_counter_page_window_usbouttransferresult,
    SUM(
      metrics.counter.use_counter_page_window_useractivation
    ) AS use_counter_page_window_useractivation,
    SUM(
      metrics.counter.use_counter_page_window_videocolorspace
    ) AS use_counter_page_window_videocolorspace,
    SUM(
      metrics.counter.use_counter_page_window_videodecoder
    ) AS use_counter_page_window_videodecoder,
    SUM(
      metrics.counter.use_counter_page_window_videoencoder
    ) AS use_counter_page_window_videoencoder,
    SUM(metrics.counter.use_counter_page_window_videoframe) AS use_counter_page_window_videoframe,
    SUM(metrics.counter.use_counter_page_window_wakelock) AS use_counter_page_window_wakelock,
    SUM(
      metrics.counter.use_counter_page_window_wakelocksentinel
    ) AS use_counter_page_window_wakelocksentinel,
    SUM(
      metrics.counter.use_counter_page_window_webkitcancelanimationframe
    ) AS use_counter_page_window_webkitcancelanimationframe,
    SUM(
      metrics.counter.use_counter_page_window_webkitmediastream
    ) AS use_counter_page_window_webkitmediastream,
    SUM(
      metrics.counter.use_counter_page_window_webkitmutationobserver
    ) AS use_counter_page_window_webkitmutationobserver,
    SUM(
      metrics.counter.use_counter_page_window_webkitrequestanimationframe
    ) AS use_counter_page_window_webkitrequestanimationframe,
    SUM(
      metrics.counter.use_counter_page_window_webkitrequestfilesystem
    ) AS use_counter_page_window_webkitrequestfilesystem,
    SUM(
      metrics.counter.use_counter_page_window_webkitresolvelocalfilesystemurl
    ) AS use_counter_page_window_webkitresolvelocalfilesystemurl,
    SUM(
      metrics.counter.use_counter_page_window_webkitrtcpeerconnection
    ) AS use_counter_page_window_webkitrtcpeerconnection,
    SUM(
      metrics.counter.use_counter_page_window_webkitspeechgrammar
    ) AS use_counter_page_window_webkitspeechgrammar,
    SUM(
      metrics.counter.use_counter_page_window_webkitspeechgrammarlist
    ) AS use_counter_page_window_webkitspeechgrammarlist,
    SUM(
      metrics.counter.use_counter_page_window_webkitspeechrecognition
    ) AS use_counter_page_window_webkitspeechrecognition,
    SUM(
      metrics.counter.use_counter_page_window_webkitspeechrecognitionerror
    ) AS use_counter_page_window_webkitspeechrecognitionerror,
    SUM(
      metrics.counter.use_counter_page_window_webkitspeechrecognitionevent
    ) AS use_counter_page_window_webkitspeechrecognitionevent,
    SUM(
      metrics.counter.use_counter_page_window_webkitstorageinfo
    ) AS use_counter_page_window_webkitstorageinfo,
    SUM(
      metrics.counter.use_counter_page_workernavigator_permissions
    ) AS use_counter_page_workernavigator_permissions,
    SUM(metrics.counter.use_counter_page_wr_filter_fallback) AS use_counter_page_wr_filter_fallback,
    SUM(metrics.counter.use_counter_page_xslstylesheet) AS use_counter_page_xslstylesheet,
    SUM(
      metrics.counter.use_counter_page_xsltprocessor_constructor
    ) AS use_counter_page_xsltprocessor_constructor,
    SUM(
      metrics.counter.use_counter_page_you_tube_flash_embed
    ) AS use_counter_page_you_tube_flash_embed,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_assert
    ) AS use_counter_worker_dedicated_console_assert,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_clear
    ) AS use_counter_worker_dedicated_console_clear,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_count
    ) AS use_counter_worker_dedicated_console_count,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_countreset
    ) AS use_counter_worker_dedicated_console_countreset,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_debug
    ) AS use_counter_worker_dedicated_console_debug,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_dir
    ) AS use_counter_worker_dedicated_console_dir,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_dirxml
    ) AS use_counter_worker_dedicated_console_dirxml,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_error
    ) AS use_counter_worker_dedicated_console_error,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_exception
    ) AS use_counter_worker_dedicated_console_exception,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_group
    ) AS use_counter_worker_dedicated_console_group,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_groupcollapsed
    ) AS use_counter_worker_dedicated_console_groupcollapsed,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_groupend
    ) AS use_counter_worker_dedicated_console_groupend,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_info
    ) AS use_counter_worker_dedicated_console_info,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_log
    ) AS use_counter_worker_dedicated_console_log,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_profile
    ) AS use_counter_worker_dedicated_console_profile,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_profileend
    ) AS use_counter_worker_dedicated_console_profileend,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_table
    ) AS use_counter_worker_dedicated_console_table,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_time
    ) AS use_counter_worker_dedicated_console_time,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_timeend
    ) AS use_counter_worker_dedicated_console_timeend,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_timelog
    ) AS use_counter_worker_dedicated_console_timelog,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_timestamp
    ) AS use_counter_worker_dedicated_console_timestamp,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_trace
    ) AS use_counter_worker_dedicated_console_trace,
    SUM(
      metrics.counter.use_counter_worker_dedicated_console_warn
    ) AS use_counter_worker_dedicated_console_warn,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_caches_delete
    ) AS use_counter_worker_dedicated_private_browsing_caches_delete,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_caches_has
    ) AS use_counter_worker_dedicated_private_browsing_caches_has,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_caches_keys
    ) AS use_counter_worker_dedicated_private_browsing_caches_keys,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_caches_match
    ) AS use_counter_worker_dedicated_private_browsing_caches_match,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_caches_open
    ) AS use_counter_worker_dedicated_private_browsing_caches_open,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_idbfactory_delete_database
    ) AS use_counter_worker_dedicated_private_browsing_idbfactory_delete_database,
    SUM(
      metrics.counter.use_counter_worker_dedicated_private_browsing_idbfactory_open
    ) AS use_counter_worker_dedicated_private_browsing_idbfactory_open,
    SUM(
      metrics.counter.use_counter_worker_dedicated_pushmanager_subscribe
    ) AS use_counter_worker_dedicated_pushmanager_subscribe,
    SUM(
      metrics.counter.use_counter_worker_dedicated_pushsubscription_unsubscribe
    ) AS use_counter_worker_dedicated_pushsubscription_unsubscribe,
    SUM(
      metrics.counter.use_counter_worker_dedicated_scheduler_posttask
    ) AS use_counter_worker_dedicated_scheduler_posttask,
    SUM(
      metrics.counter.use_counter_worker_service_console_assert
    ) AS use_counter_worker_service_console_assert,
    SUM(
      metrics.counter.use_counter_worker_service_console_clear
    ) AS use_counter_worker_service_console_clear,
    SUM(
      metrics.counter.use_counter_worker_service_console_count
    ) AS use_counter_worker_service_console_count,
    SUM(
      metrics.counter.use_counter_worker_service_console_countreset
    ) AS use_counter_worker_service_console_countreset,
    SUM(
      metrics.counter.use_counter_worker_service_console_debug
    ) AS use_counter_worker_service_console_debug,
    SUM(
      metrics.counter.use_counter_worker_service_console_dir
    ) AS use_counter_worker_service_console_dir,
    SUM(
      metrics.counter.use_counter_worker_service_console_dirxml
    ) AS use_counter_worker_service_console_dirxml,
    SUM(
      metrics.counter.use_counter_worker_service_console_error
    ) AS use_counter_worker_service_console_error,
    SUM(
      metrics.counter.use_counter_worker_service_console_exception
    ) AS use_counter_worker_service_console_exception,
    SUM(
      metrics.counter.use_counter_worker_service_console_group
    ) AS use_counter_worker_service_console_group,
    SUM(
      metrics.counter.use_counter_worker_service_console_groupcollapsed
    ) AS use_counter_worker_service_console_groupcollapsed,
    SUM(
      metrics.counter.use_counter_worker_service_console_groupend
    ) AS use_counter_worker_service_console_groupend,
    SUM(
      metrics.counter.use_counter_worker_service_console_info
    ) AS use_counter_worker_service_console_info,
    SUM(
      metrics.counter.use_counter_worker_service_console_log
    ) AS use_counter_worker_service_console_log,
    SUM(
      metrics.counter.use_counter_worker_service_console_profile
    ) AS use_counter_worker_service_console_profile,
    SUM(
      metrics.counter.use_counter_worker_service_console_profileend
    ) AS use_counter_worker_service_console_profileend,
    SUM(
      metrics.counter.use_counter_worker_service_console_table
    ) AS use_counter_worker_service_console_table,
    SUM(
      metrics.counter.use_counter_worker_service_console_time
    ) AS use_counter_worker_service_console_time,
    SUM(
      metrics.counter.use_counter_worker_service_console_timeend
    ) AS use_counter_worker_service_console_timeend,
    SUM(
      metrics.counter.use_counter_worker_service_console_timelog
    ) AS use_counter_worker_service_console_timelog,
    SUM(
      metrics.counter.use_counter_worker_service_console_timestamp
    ) AS use_counter_worker_service_console_timestamp,
    SUM(
      metrics.counter.use_counter_worker_service_console_trace
    ) AS use_counter_worker_service_console_trace,
    SUM(
      metrics.counter.use_counter_worker_service_console_warn
    ) AS use_counter_worker_service_console_warn,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_caches_delete
    ) AS use_counter_worker_service_private_browsing_caches_delete,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_caches_has
    ) AS use_counter_worker_service_private_browsing_caches_has,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_caches_keys
    ) AS use_counter_worker_service_private_browsing_caches_keys,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_caches_match
    ) AS use_counter_worker_service_private_browsing_caches_match,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_caches_open
    ) AS use_counter_worker_service_private_browsing_caches_open,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_idbfactory_delete_database
    ) AS use_counter_worker_service_private_browsing_idbfactory_delete_database,
    SUM(
      metrics.counter.use_counter_worker_service_private_browsing_idbfactory_open
    ) AS use_counter_worker_service_private_browsing_idbfactory_open,
    SUM(
      metrics.counter.use_counter_worker_service_pushmanager_subscribe
    ) AS use_counter_worker_service_pushmanager_subscribe,
    SUM(
      metrics.counter.use_counter_worker_service_pushsubscription_unsubscribe
    ) AS use_counter_worker_service_pushsubscription_unsubscribe,
    SUM(
      metrics.counter.use_counter_worker_service_scheduler_posttask
    ) AS use_counter_worker_service_scheduler_posttask,
    SUM(
      metrics.counter.use_counter_worker_shared_console_assert
    ) AS use_counter_worker_shared_console_assert,
    SUM(
      metrics.counter.use_counter_worker_shared_console_clear
    ) AS use_counter_worker_shared_console_clear,
    SUM(
      metrics.counter.use_counter_worker_shared_console_count
    ) AS use_counter_worker_shared_console_count,
    SUM(
      metrics.counter.use_counter_worker_shared_console_countreset
    ) AS use_counter_worker_shared_console_countreset,
    SUM(
      metrics.counter.use_counter_worker_shared_console_debug
    ) AS use_counter_worker_shared_console_debug,
    SUM(
      metrics.counter.use_counter_worker_shared_console_dir
    ) AS use_counter_worker_shared_console_dir,
    SUM(
      metrics.counter.use_counter_worker_shared_console_dirxml
    ) AS use_counter_worker_shared_console_dirxml,
    SUM(
      metrics.counter.use_counter_worker_shared_console_error
    ) AS use_counter_worker_shared_console_error,
    SUM(
      metrics.counter.use_counter_worker_shared_console_exception
    ) AS use_counter_worker_shared_console_exception,
    SUM(
      metrics.counter.use_counter_worker_shared_console_group
    ) AS use_counter_worker_shared_console_group,
    SUM(
      metrics.counter.use_counter_worker_shared_console_groupcollapsed
    ) AS use_counter_worker_shared_console_groupcollapsed,
    SUM(
      metrics.counter.use_counter_worker_shared_console_groupend
    ) AS use_counter_worker_shared_console_groupend,
    SUM(
      metrics.counter.use_counter_worker_shared_console_info
    ) AS use_counter_worker_shared_console_info,
    SUM(
      metrics.counter.use_counter_worker_shared_console_log
    ) AS use_counter_worker_shared_console_log,
    SUM(
      metrics.counter.use_counter_worker_shared_console_profile
    ) AS use_counter_worker_shared_console_profile,
    SUM(
      metrics.counter.use_counter_worker_shared_console_profileend
    ) AS use_counter_worker_shared_console_profileend,
    SUM(
      metrics.counter.use_counter_worker_shared_console_table
    ) AS use_counter_worker_shared_console_table,
    SUM(
      metrics.counter.use_counter_worker_shared_console_time
    ) AS use_counter_worker_shared_console_time,
    SUM(
      metrics.counter.use_counter_worker_shared_console_timeend
    ) AS use_counter_worker_shared_console_timeend,
    SUM(
      metrics.counter.use_counter_worker_shared_console_timelog
    ) AS use_counter_worker_shared_console_timelog,
    SUM(
      metrics.counter.use_counter_worker_shared_console_timestamp
    ) AS use_counter_worker_shared_console_timestamp,
    SUM(
      metrics.counter.use_counter_worker_shared_console_trace
    ) AS use_counter_worker_shared_console_trace,
    SUM(
      metrics.counter.use_counter_worker_shared_console_warn
    ) AS use_counter_worker_shared_console_warn,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_caches_delete
    ) AS use_counter_worker_shared_private_browsing_caches_delete,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_caches_has
    ) AS use_counter_worker_shared_private_browsing_caches_has,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_caches_keys
    ) AS use_counter_worker_shared_private_browsing_caches_keys,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_caches_match
    ) AS use_counter_worker_shared_private_browsing_caches_match,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_caches_open
    ) AS use_counter_worker_shared_private_browsing_caches_open,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_idbfactory_delete_database
    ) AS use_counter_worker_shared_private_browsing_idbfactory_delete_database,
    SUM(
      metrics.counter.use_counter_worker_shared_private_browsing_idbfactory_open
    ) AS use_counter_worker_shared_private_browsing_idbfactory_open,
    SUM(
      metrics.counter.use_counter_worker_shared_pushmanager_subscribe
    ) AS use_counter_worker_shared_pushmanager_subscribe,
    SUM(
      metrics.counter.use_counter_worker_shared_pushsubscription_unsubscribe
    ) AS use_counter_worker_shared_pushsubscription_unsubscribe,
    SUM(
      metrics.counter.use_counter_worker_shared_scheduler_posttask
    ) AS use_counter_worker_shared_scheduler_posttask
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.use_counters` a
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2,
    3,
    4
  HAVING
    COUNT(DISTINCT(client_info.client_id)) >= 5000
),
firefox_desktop_pivoted_raw AS (
  SELECT
    *
  FROM
    firefox_desktop_use_counts_by_day_version_and_country_stg a UNPIVOT(
      cnt FOR metric IN (
        use_counter_css_doc_alignment_baseline,
        use_counter_css_doc_background_repeat_x,
        use_counter_css_doc_background_repeat_y,
        use_counter_css_doc_baseline_shift,
        use_counter_css_doc_buffered_rendering,
        use_counter_css_doc_color_rendering,
        use_counter_css_doc_css_accent_color,
        use_counter_css_doc_css_align_content,
        use_counter_css_doc_css_align_items,
        use_counter_css_doc_css_align_self,
        use_counter_css_doc_css_align_tracks,
        use_counter_css_doc_css_all,
        use_counter_css_doc_css_animation,
        use_counter_css_doc_css_animation_composition,
        use_counter_css_doc_css_animation_delay,
        use_counter_css_doc_css_animation_direction,
        use_counter_css_doc_css_animation_duration,
        use_counter_css_doc_css_animation_fill_mode,
        use_counter_css_doc_css_animation_iteration_count,
        use_counter_css_doc_css_animation_name,
        use_counter_css_doc_css_animation_play_state,
        use_counter_css_doc_css_animation_timeline,
        use_counter_css_doc_css_animation_timing_function,
        use_counter_css_doc_css_appearance,
        use_counter_css_doc_css_aspect_ratio,
        use_counter_css_doc_css_backdrop_filter,
        use_counter_css_doc_css_backface_visibility,
        use_counter_css_doc_css_background,
        use_counter_css_doc_css_background_attachment,
        use_counter_css_doc_css_background_blend_mode,
        use_counter_css_doc_css_background_clip,
        use_counter_css_doc_css_background_color,
        use_counter_css_doc_css_background_image,
        use_counter_css_doc_css_background_origin,
        use_counter_css_doc_css_background_position,
        use_counter_css_doc_css_background_position_x,
        use_counter_css_doc_css_background_position_y,
        use_counter_css_doc_css_background_repeat,
        use_counter_css_doc_css_background_size,
        use_counter_css_doc_css_baseline_source,
        use_counter_css_doc_css_block_size,
        use_counter_css_doc_css_border,
        use_counter_css_doc_css_border_block,
        use_counter_css_doc_css_border_block_color,
        use_counter_css_doc_css_border_block_end,
        use_counter_css_doc_css_border_block_end_color,
        use_counter_css_doc_css_border_block_end_style,
        use_counter_css_doc_css_border_block_end_width,
        use_counter_css_doc_css_border_block_start,
        use_counter_css_doc_css_border_block_start_color,
        use_counter_css_doc_css_border_block_start_style,
        use_counter_css_doc_css_border_block_start_width,
        use_counter_css_doc_css_border_block_style,
        use_counter_css_doc_css_border_block_width,
        use_counter_css_doc_css_border_bottom,
        use_counter_css_doc_css_border_bottom_color,
        use_counter_css_doc_css_border_bottom_left_radius,
        use_counter_css_doc_css_border_bottom_right_radius,
        use_counter_css_doc_css_border_bottom_style,
        use_counter_css_doc_css_border_bottom_width,
        use_counter_css_doc_css_border_collapse,
        use_counter_css_doc_css_border_color,
        use_counter_css_doc_css_border_end_end_radius,
        use_counter_css_doc_css_border_end_start_radius,
        use_counter_css_doc_css_border_image,
        use_counter_css_doc_css_border_image_outset,
        use_counter_css_doc_css_border_image_repeat,
        use_counter_css_doc_css_border_image_slice,
        use_counter_css_doc_css_border_image_source,
        use_counter_css_doc_css_border_image_width,
        use_counter_css_doc_css_border_inline,
        use_counter_css_doc_css_border_inline_color,
        use_counter_css_doc_css_border_inline_end,
        use_counter_css_doc_css_border_inline_end_color,
        use_counter_css_doc_css_border_inline_end_style,
        use_counter_css_doc_css_border_inline_end_width,
        use_counter_css_doc_css_border_inline_start,
        use_counter_css_doc_css_border_inline_start_color,
        use_counter_css_doc_css_border_inline_start_style,
        use_counter_css_doc_css_border_inline_start_width,
        use_counter_css_doc_css_border_inline_style,
        use_counter_css_doc_css_border_inline_width,
        use_counter_css_doc_css_border_left,
        use_counter_css_doc_css_border_left_color,
        use_counter_css_doc_css_border_left_style,
        use_counter_css_doc_css_border_left_width,
        use_counter_css_doc_css_border_radius,
        use_counter_css_doc_css_border_right,
        use_counter_css_doc_css_border_right_color,
        use_counter_css_doc_css_border_right_style,
        use_counter_css_doc_css_border_right_width,
        use_counter_css_doc_css_border_spacing,
        use_counter_css_doc_css_border_start_end_radius,
        use_counter_css_doc_css_border_start_start_radius,
        use_counter_css_doc_css_border_style,
        use_counter_css_doc_css_border_top,
        use_counter_css_doc_css_border_top_color,
        use_counter_css_doc_css_border_top_left_radius,
        use_counter_css_doc_css_border_top_right_radius,
        use_counter_css_doc_css_border_top_style,
        use_counter_css_doc_css_border_top_width,
        use_counter_css_doc_css_border_width,
        use_counter_css_doc_css_bottom,
        use_counter_css_doc_css_box_decoration_break,
        use_counter_css_doc_css_box_shadow,
        use_counter_css_doc_css_box_sizing,
        use_counter_css_doc_css_break_after,
        use_counter_css_doc_css_break_before,
        use_counter_css_doc_css_break_inside,
        use_counter_css_doc_css_caption_side,
        use_counter_css_doc_css_caret_color,
        use_counter_css_doc_css_clear,
        use_counter_css_doc_css_clip,
        use_counter_css_doc_css_clip_path,
        use_counter_css_doc_css_clip_rule,
        use_counter_css_doc_css_color,
        use_counter_css_doc_css_color_adjust,
        use_counter_css_doc_css_color_interpolation,
        use_counter_css_doc_css_color_interpolation_filters,
        use_counter_css_doc_css_color_scheme,
        use_counter_css_doc_css_column_count,
        use_counter_css_doc_css_column_fill,
        use_counter_css_doc_css_column_gap,
        use_counter_css_doc_css_column_rule,
        use_counter_css_doc_css_column_rule_color,
        use_counter_css_doc_css_column_rule_style,
        use_counter_css_doc_css_column_rule_width,
        use_counter_css_doc_css_column_span,
        use_counter_css_doc_css_column_width,
        use_counter_css_doc_css_columns,
        use_counter_css_doc_css_contain,
        use_counter_css_doc_css_contain_intrinsic_block_size,
        use_counter_css_doc_css_contain_intrinsic_height,
        use_counter_css_doc_css_contain_intrinsic_inline_size,
        use_counter_css_doc_css_contain_intrinsic_size,
        use_counter_css_doc_css_contain_intrinsic_width,
        use_counter_css_doc_css_container,
        use_counter_css_doc_css_container_name,
        use_counter_css_doc_css_container_type,
        use_counter_css_doc_css_content,
        use_counter_css_doc_css_content_visibility,
        use_counter_css_doc_css_counter_increment,
        use_counter_css_doc_css_counter_reset,
        use_counter_css_doc_css_counter_set,
        use_counter_css_doc_css_cursor,
        use_counter_css_doc_css_cx,
        use_counter_css_doc_css_cy,
        use_counter_css_doc_css_d,
        use_counter_css_doc_css_direction,
        use_counter_css_doc_css_display,
        use_counter_css_doc_css_dominant_baseline,
        use_counter_css_doc_css_empty_cells,
        use_counter_css_doc_css_fill,
        use_counter_css_doc_css_fill_opacity,
        use_counter_css_doc_css_fill_rule,
        use_counter_css_doc_css_filter,
        use_counter_css_doc_css_flex,
        use_counter_css_doc_css_flex_basis,
        use_counter_css_doc_css_flex_direction,
        use_counter_css_doc_css_flex_flow,
        use_counter_css_doc_css_flex_grow,
        use_counter_css_doc_css_flex_shrink,
        use_counter_css_doc_css_flex_wrap,
        use_counter_css_doc_css_float,
        use_counter_css_doc_css_flood_color,
        use_counter_css_doc_css_flood_opacity,
        use_counter_css_doc_css_font,
        use_counter_css_doc_css_font_family,
        use_counter_css_doc_css_font_feature_settings,
        use_counter_css_doc_css_font_kerning,
        use_counter_css_doc_css_font_language_override,
        use_counter_css_doc_css_font_optical_sizing,
        use_counter_css_doc_css_font_palette,
        use_counter_css_doc_css_font_size,
        use_counter_css_doc_css_font_size_adjust,
        use_counter_css_doc_css_font_stretch,
        use_counter_css_doc_css_font_style,
        use_counter_css_doc_css_font_synthesis,
        use_counter_css_doc_css_font_synthesis_position,
        use_counter_css_doc_css_font_synthesis_small_caps,
        use_counter_css_doc_css_font_synthesis_style,
        use_counter_css_doc_css_font_synthesis_weight,
        use_counter_css_doc_css_font_variant,
        use_counter_css_doc_css_font_variant_alternates,
        use_counter_css_doc_css_font_variant_caps,
        use_counter_css_doc_css_font_variant_east_asian,
        use_counter_css_doc_css_font_variant_emoji,
        use_counter_css_doc_css_font_variant_ligatures,
        use_counter_css_doc_css_font_variant_numeric,
        use_counter_css_doc_css_font_variant_position,
        use_counter_css_doc_css_font_variation_settings,
        use_counter_css_doc_css_font_weight,
        use_counter_css_doc_css_forced_color_adjust,
        use_counter_css_doc_css_gap,
        use_counter_css_doc_css_grid,
        use_counter_css_doc_css_grid_area,
        use_counter_css_doc_css_grid_auto_columns,
        use_counter_css_doc_css_grid_auto_flow,
        use_counter_css_doc_css_grid_auto_rows,
        use_counter_css_doc_css_grid_column,
        use_counter_css_doc_css_grid_column_end,
        use_counter_css_doc_css_grid_column_gap,
        use_counter_css_doc_css_grid_column_start,
        use_counter_css_doc_css_grid_gap,
        use_counter_css_doc_css_grid_row,
        use_counter_css_doc_css_grid_row_end,
        use_counter_css_doc_css_grid_row_gap,
        use_counter_css_doc_css_grid_row_start,
        use_counter_css_doc_css_grid_template,
        use_counter_css_doc_css_grid_template_areas,
        use_counter_css_doc_css_grid_template_columns,
        use_counter_css_doc_css_grid_template_rows,
        use_counter_css_doc_css_height,
        use_counter_css_doc_css_hyphenate_character,
        use_counter_css_doc_css_hyphens,
        use_counter_css_doc_css_image_orientation,
        use_counter_css_doc_css_image_rendering,
        use_counter_css_doc_css_ime_mode,
        use_counter_css_doc_css_initial_letter,
        use_counter_css_doc_css_inline_size,
        use_counter_css_doc_css_inset,
        use_counter_css_doc_css_inset_block,
        use_counter_css_doc_css_inset_block_end,
        use_counter_css_doc_css_inset_block_start,
        use_counter_css_doc_css_inset_inline,
        use_counter_css_doc_css_inset_inline_end,
        use_counter_css_doc_css_inset_inline_start,
        use_counter_css_doc_css_isolation,
        use_counter_css_doc_css_justify_content,
        use_counter_css_doc_css_justify_items,
        use_counter_css_doc_css_justify_self,
        use_counter_css_doc_css_justify_tracks,
        use_counter_css_doc_css_left,
        use_counter_css_doc_css_letter_spacing,
        use_counter_css_doc_css_lighting_color,
        use_counter_css_doc_css_line_break,
        use_counter_css_doc_css_line_height,
        use_counter_css_doc_css_list_style,
        use_counter_css_doc_css_list_style_image,
        use_counter_css_doc_css_list_style_position,
        use_counter_css_doc_css_list_style_type,
        use_counter_css_doc_css_margin,
        use_counter_css_doc_css_margin_block,
        use_counter_css_doc_css_margin_block_end,
        use_counter_css_doc_css_margin_block_start,
        use_counter_css_doc_css_margin_bottom,
        use_counter_css_doc_css_margin_inline,
        use_counter_css_doc_css_margin_inline_end,
        use_counter_css_doc_css_margin_inline_start,
        use_counter_css_doc_css_margin_left,
        use_counter_css_doc_css_margin_right,
        use_counter_css_doc_css_margin_top,
        use_counter_css_doc_css_marker,
        use_counter_css_doc_css_marker_end,
        use_counter_css_doc_css_marker_mid,
        use_counter_css_doc_css_marker_start,
        use_counter_css_doc_css_mask,
        use_counter_css_doc_css_mask_clip,
        use_counter_css_doc_css_mask_composite,
        use_counter_css_doc_css_mask_image,
        use_counter_css_doc_css_mask_mode,
        use_counter_css_doc_css_mask_origin,
        use_counter_css_doc_css_mask_position,
        use_counter_css_doc_css_mask_position_x,
        use_counter_css_doc_css_mask_position_y,
        use_counter_css_doc_css_mask_repeat,
        use_counter_css_doc_css_mask_size,
        use_counter_css_doc_css_mask_type,
        use_counter_css_doc_css_masonry_auto_flow,
        use_counter_css_doc_css_math_depth,
        use_counter_css_doc_css_math_style,
        use_counter_css_doc_css_max_block_size,
        use_counter_css_doc_css_max_height,
        use_counter_css_doc_css_max_inline_size,
        use_counter_css_doc_css_max_width,
        use_counter_css_doc_css_min_block_size,
        use_counter_css_doc_css_min_height,
        use_counter_css_doc_css_min_inline_size,
        use_counter_css_doc_css_min_width,
        use_counter_css_doc_css_mix_blend_mode,
        use_counter_css_doc_css_moz_animation,
        use_counter_css_doc_css_moz_animation_delay,
        use_counter_css_doc_css_moz_animation_direction,
        use_counter_css_doc_css_moz_animation_duration,
        use_counter_css_doc_css_moz_animation_fill_mode,
        use_counter_css_doc_css_moz_animation_iteration_count,
        use_counter_css_doc_css_moz_animation_name,
        use_counter_css_doc_css_moz_animation_play_state,
        use_counter_css_doc_css_moz_animation_timing_function,
        use_counter_css_doc_css_moz_appearance,
        use_counter_css_doc_css_moz_backface_visibility,
        use_counter_css_doc_css_moz_border_end,
        use_counter_css_doc_css_moz_border_end_color,
        use_counter_css_doc_css_moz_border_end_style,
        use_counter_css_doc_css_moz_border_end_width,
        use_counter_css_doc_css_moz_border_image,
        use_counter_css_doc_css_moz_border_start,
        use_counter_css_doc_css_moz_border_start_color,
        use_counter_css_doc_css_moz_border_start_style,
        use_counter_css_doc_css_moz_border_start_width,
        use_counter_css_doc_css_moz_box_align,
        use_counter_css_doc_css_moz_box_collapse,
        use_counter_css_doc_css_moz_box_direction,
        use_counter_css_doc_css_moz_box_flex,
        use_counter_css_doc_css_moz_box_ordinal_group,
        use_counter_css_doc_css_moz_box_orient,
        use_counter_css_doc_css_moz_box_pack,
        use_counter_css_doc_css_moz_box_sizing,
        use_counter_css_doc_css_moz_context_properties,
        use_counter_css_doc_css_moz_control_character_visibility,
        use_counter_css_doc_css_moz_default_appearance,
        use_counter_css_doc_css_moz_float_edge,
        use_counter_css_doc_css_moz_font_feature_settings,
        use_counter_css_doc_css_moz_font_language_override,
        use_counter_css_doc_css_moz_force_broken_image_icon,
        use_counter_css_doc_css_moz_hyphens,
        use_counter_css_doc_css_moz_inert,
        use_counter_css_doc_css_moz_margin_end,
        use_counter_css_doc_css_moz_margin_start,
        use_counter_css_doc_css_moz_math_variant,
        use_counter_css_doc_css_moz_min_font_size_ratio,
        use_counter_css_doc_css_moz_orient,
        use_counter_css_doc_css_moz_osx_font_smoothing,
        use_counter_css_doc_css_moz_padding_end,
        use_counter_css_doc_css_moz_padding_start,
        use_counter_css_doc_css_moz_perspective,
        use_counter_css_doc_css_moz_perspective_origin,
        use_counter_css_doc_css_moz_subtree_hidden_only_visually,
        use_counter_css_doc_css_moz_tab_size,
        use_counter_css_doc_css_moz_text_size_adjust,
        use_counter_css_doc_css_moz_theme,
        use_counter_css_doc_css_moz_top_layer,
        use_counter_css_doc_css_moz_transform,
        use_counter_css_doc_css_moz_transform_origin,
        use_counter_css_doc_css_moz_transform_style,
        use_counter_css_doc_css_moz_transition,
        use_counter_css_doc_css_moz_transition_delay,
        use_counter_css_doc_css_moz_transition_duration,
        use_counter_css_doc_css_moz_transition_property,
        use_counter_css_doc_css_moz_transition_timing_function,
        use_counter_css_doc_css_moz_user_focus,
        use_counter_css_doc_css_moz_user_input,
        use_counter_css_doc_css_moz_user_modify,
        use_counter_css_doc_css_moz_user_select,
        use_counter_css_doc_css_moz_window_dragging,
        use_counter_css_doc_css_moz_window_input_region_margin,
        use_counter_css_doc_css_moz_window_opacity,
        use_counter_css_doc_css_moz_window_shadow,
        use_counter_css_doc_css_moz_window_transform,
        use_counter_css_doc_css_moz_window_transform_origin,
        use_counter_css_doc_css_object_fit,
        use_counter_css_doc_css_object_position,
        use_counter_css_doc_css_offset,
        use_counter_css_doc_css_offset_anchor,
        use_counter_css_doc_css_offset_distance,
        use_counter_css_doc_css_offset_path,
        use_counter_css_doc_css_offset_position,
        use_counter_css_doc_css_offset_rotate,
        use_counter_css_doc_css_opacity,
        use_counter_css_doc_css_order,
        use_counter_css_doc_css_outline,
        use_counter_css_doc_css_outline_color,
        use_counter_css_doc_css_outline_offset,
        use_counter_css_doc_css_outline_style,
        use_counter_css_doc_css_outline_width,
        use_counter_css_doc_css_overflow,
        use_counter_css_doc_css_overflow_anchor,
        use_counter_css_doc_css_overflow_block,
        use_counter_css_doc_css_overflow_clip_box,
        use_counter_css_doc_css_overflow_clip_box_block,
        use_counter_css_doc_css_overflow_clip_box_inline,
        use_counter_css_doc_css_overflow_clip_margin,
        use_counter_css_doc_css_overflow_inline,
        use_counter_css_doc_css_overflow_wrap,
        use_counter_css_doc_css_overflow_x,
        use_counter_css_doc_css_overflow_y,
        use_counter_css_doc_css_overscroll_behavior,
        use_counter_css_doc_css_overscroll_behavior_block,
        use_counter_css_doc_css_overscroll_behavior_inline,
        use_counter_css_doc_css_overscroll_behavior_x,
        use_counter_css_doc_css_overscroll_behavior_y,
        use_counter_css_doc_css_padding,
        use_counter_css_doc_css_padding_block,
        use_counter_css_doc_css_padding_block_end,
        use_counter_css_doc_css_padding_block_start,
        use_counter_css_doc_css_padding_bottom,
        use_counter_css_doc_css_padding_inline,
        use_counter_css_doc_css_padding_inline_end,
        use_counter_css_doc_css_padding_inline_start,
        use_counter_css_doc_css_padding_left,
        use_counter_css_doc_css_padding_right,
        use_counter_css_doc_css_padding_top,
        use_counter_css_doc_css_page,
        use_counter_css_doc_css_page_break_after,
        use_counter_css_doc_css_page_break_before,
        use_counter_css_doc_css_page_break_inside,
        use_counter_css_doc_css_page_orientation,
        use_counter_css_doc_css_paint_order,
        use_counter_css_doc_css_perspective,
        use_counter_css_doc_css_perspective_origin,
        use_counter_css_doc_css_place_content,
        use_counter_css_doc_css_place_items,
        use_counter_css_doc_css_place_self,
        use_counter_css_doc_css_pointer_events,
        use_counter_css_doc_css_position,
        use_counter_css_doc_css_print_color_adjust,
        use_counter_css_doc_css_quotes,
        use_counter_css_doc_css_r,
        use_counter_css_doc_css_resize,
        use_counter_css_doc_css_right,
        use_counter_css_doc_css_rotate,
        use_counter_css_doc_css_row_gap,
        use_counter_css_doc_css_ruby_align,
        use_counter_css_doc_css_ruby_position,
        use_counter_css_doc_css_rx,
        use_counter_css_doc_css_ry,
        use_counter_css_doc_css_scale,
        use_counter_css_doc_css_scroll_behavior,
        use_counter_css_doc_css_scroll_margin,
        use_counter_css_doc_css_scroll_margin_block,
        use_counter_css_doc_css_scroll_margin_block_end,
        use_counter_css_doc_css_scroll_margin_block_start,
        use_counter_css_doc_css_scroll_margin_bottom,
        use_counter_css_doc_css_scroll_margin_inline,
        use_counter_css_doc_css_scroll_margin_inline_end,
        use_counter_css_doc_css_scroll_margin_inline_start,
        use_counter_css_doc_css_scroll_margin_left,
        use_counter_css_doc_css_scroll_margin_right,
        use_counter_css_doc_css_scroll_margin_top,
        use_counter_css_doc_css_scroll_padding,
        use_counter_css_doc_css_scroll_padding_block,
        use_counter_css_doc_css_scroll_padding_block_end,
        use_counter_css_doc_css_scroll_padding_block_start,
        use_counter_css_doc_css_scroll_padding_bottom,
        use_counter_css_doc_css_scroll_padding_inline,
        use_counter_css_doc_css_scroll_padding_inline_end,
        use_counter_css_doc_css_scroll_padding_inline_start,
        use_counter_css_doc_css_scroll_padding_left,
        use_counter_css_doc_css_scroll_padding_right,
        use_counter_css_doc_css_scroll_padding_top,
        use_counter_css_doc_css_scroll_snap_align,
        use_counter_css_doc_css_scroll_snap_stop,
        use_counter_css_doc_css_scroll_snap_type,
        use_counter_css_doc_css_scroll_timeline,
        use_counter_css_doc_css_scroll_timeline_axis,
        use_counter_css_doc_css_scroll_timeline_name,
        use_counter_css_doc_css_scrollbar_color,
        use_counter_css_doc_css_scrollbar_gutter,
        use_counter_css_doc_css_scrollbar_width,
        use_counter_css_doc_css_shape_image_threshold,
        use_counter_css_doc_css_shape_margin,
        use_counter_css_doc_css_shape_outside,
        use_counter_css_doc_css_shape_rendering,
        use_counter_css_doc_css_size,
        use_counter_css_doc_css_stop_color,
        use_counter_css_doc_css_stop_opacity,
        use_counter_css_doc_css_stroke,
        use_counter_css_doc_css_stroke_dasharray,
        use_counter_css_doc_css_stroke_dashoffset,
        use_counter_css_doc_css_stroke_linecap,
        use_counter_css_doc_css_stroke_linejoin,
        use_counter_css_doc_css_stroke_miterlimit,
        use_counter_css_doc_css_stroke_opacity,
        use_counter_css_doc_css_stroke_width,
        use_counter_css_doc_css_tab_size,
        use_counter_css_doc_css_table_layout,
        use_counter_css_doc_css_text_align,
        use_counter_css_doc_css_text_align_last,
        use_counter_css_doc_css_text_anchor,
        use_counter_css_doc_css_text_combine_upright,
        use_counter_css_doc_css_text_decoration,
        use_counter_css_doc_css_text_decoration_color,
        use_counter_css_doc_css_text_decoration_line,
        use_counter_css_doc_css_text_decoration_skip_ink,
        use_counter_css_doc_css_text_decoration_style,
        use_counter_css_doc_css_text_decoration_thickness,
        use_counter_css_doc_css_text_emphasis,
        use_counter_css_doc_css_text_emphasis_color,
        use_counter_css_doc_css_text_emphasis_position,
        use_counter_css_doc_css_text_emphasis_style,
        use_counter_css_doc_css_text_indent,
        use_counter_css_doc_css_text_justify,
        use_counter_css_doc_css_text_orientation,
        use_counter_css_doc_css_text_overflow,
        use_counter_css_doc_css_text_rendering,
        use_counter_css_doc_css_text_shadow,
        use_counter_css_doc_css_text_transform,
        use_counter_css_doc_css_text_underline_offset,
        use_counter_css_doc_css_text_underline_position,
        use_counter_css_doc_css_text_wrap,
        use_counter_css_doc_css_top,
        use_counter_css_doc_css_touch_action,
        use_counter_css_doc_css_transform,
        use_counter_css_doc_css_transform_box,
        use_counter_css_doc_css_transform_origin,
        use_counter_css_doc_css_transform_style,
        use_counter_css_doc_css_transition,
        use_counter_css_doc_css_transition_delay,
        use_counter_css_doc_css_transition_duration,
        use_counter_css_doc_css_transition_property,
        use_counter_css_doc_css_transition_timing_function,
        use_counter_css_doc_css_translate,
        use_counter_css_doc_css_unicode_bidi,
        use_counter_css_doc_css_user_select,
        use_counter_css_doc_css_vector_effect,
        use_counter_css_doc_css_vertical_align,
        use_counter_css_doc_css_view_timeline,
        use_counter_css_doc_css_view_timeline_axis,
        use_counter_css_doc_css_view_timeline_inset,
        use_counter_css_doc_css_view_timeline_name,
        use_counter_css_doc_css_visibility,
        use_counter_css_doc_css_webkit_align_content,
        use_counter_css_doc_css_webkit_align_items,
        use_counter_css_doc_css_webkit_align_self,
        use_counter_css_doc_css_webkit_animation,
        use_counter_css_doc_css_webkit_animation_delay,
        use_counter_css_doc_css_webkit_animation_direction,
        use_counter_css_doc_css_webkit_animation_duration,
        use_counter_css_doc_css_webkit_animation_fill_mode,
        use_counter_css_doc_css_webkit_animation_iteration_count,
        use_counter_css_doc_css_webkit_animation_name,
        use_counter_css_doc_css_webkit_animation_play_state,
        use_counter_css_doc_css_webkit_animation_timing_function,
        use_counter_css_doc_css_webkit_appearance,
        use_counter_css_doc_css_webkit_backface_visibility,
        use_counter_css_doc_css_webkit_background_clip,
        use_counter_css_doc_css_webkit_background_origin,
        use_counter_css_doc_css_webkit_background_size,
        use_counter_css_doc_css_webkit_border_bottom_left_radius,
        use_counter_css_doc_css_webkit_border_bottom_right_radius,
        use_counter_css_doc_css_webkit_border_image,
        use_counter_css_doc_css_webkit_border_radius,
        use_counter_css_doc_css_webkit_border_top_left_radius,
        use_counter_css_doc_css_webkit_border_top_right_radius,
        use_counter_css_doc_css_webkit_box_align,
        use_counter_css_doc_css_webkit_box_direction,
        use_counter_css_doc_css_webkit_box_flex,
        use_counter_css_doc_css_webkit_box_ordinal_group,
        use_counter_css_doc_css_webkit_box_orient,
        use_counter_css_doc_css_webkit_box_pack,
        use_counter_css_doc_css_webkit_box_shadow,
        use_counter_css_doc_css_webkit_box_sizing,
        use_counter_css_doc_css_webkit_clip_path,
        use_counter_css_doc_css_webkit_filter,
        use_counter_css_doc_css_webkit_flex,
        use_counter_css_doc_css_webkit_flex_basis,
        use_counter_css_doc_css_webkit_flex_direction,
        use_counter_css_doc_css_webkit_flex_flow,
        use_counter_css_doc_css_webkit_flex_grow,
        use_counter_css_doc_css_webkit_flex_shrink,
        use_counter_css_doc_css_webkit_flex_wrap,
        use_counter_css_doc_css_webkit_justify_content,
        use_counter_css_doc_css_webkit_line_clamp,
        use_counter_css_doc_css_webkit_mask,
        use_counter_css_doc_css_webkit_mask_clip,
        use_counter_css_doc_css_webkit_mask_composite,
        use_counter_css_doc_css_webkit_mask_image,
        use_counter_css_doc_css_webkit_mask_origin,
        use_counter_css_doc_css_webkit_mask_position,
        use_counter_css_doc_css_webkit_mask_position_x,
        use_counter_css_doc_css_webkit_mask_position_y,
        use_counter_css_doc_css_webkit_mask_repeat,
        use_counter_css_doc_css_webkit_mask_size,
        use_counter_css_doc_css_webkit_order,
        use_counter_css_doc_css_webkit_perspective,
        use_counter_css_doc_css_webkit_perspective_origin,
        use_counter_css_doc_css_webkit_text_fill_color,
        use_counter_css_doc_css_webkit_text_security,
        use_counter_css_doc_css_webkit_text_size_adjust,
        use_counter_css_doc_css_webkit_text_stroke,
        use_counter_css_doc_css_webkit_text_stroke_color,
        use_counter_css_doc_css_webkit_text_stroke_width,
        use_counter_css_doc_css_webkit_transform,
        use_counter_css_doc_css_webkit_transform_origin,
        use_counter_css_doc_css_webkit_transform_style,
        use_counter_css_doc_css_webkit_transition,
        use_counter_css_doc_css_webkit_transition_delay,
        use_counter_css_doc_css_webkit_transition_duration,
        use_counter_css_doc_css_webkit_transition_property,
        use_counter_css_doc_css_webkit_transition_timing_function,
        use_counter_css_doc_css_webkit_user_select,
        use_counter_css_doc_css_white_space,
        use_counter_css_doc_css_width,
        use_counter_css_doc_css_will_change,
        use_counter_css_doc_css_word_break,
        use_counter_css_doc_css_word_spacing,
        use_counter_css_doc_css_word_wrap,
        use_counter_css_doc_css_writing_mode,
        use_counter_css_doc_css_x,
        use_counter_css_doc_css_x_lang,
        use_counter_css_doc_css_x_span,
        use_counter_css_doc_css_x_text_scale,
        use_counter_css_doc_css_y,
        use_counter_css_doc_css_z_index,
        use_counter_css_doc_css_zoom,
        use_counter_css_doc_max_zoom,
        use_counter_css_doc_min_zoom,
        use_counter_css_doc_orientation,
        use_counter_css_doc_orphans,
        use_counter_css_doc_speak,
        use_counter_css_doc_text_size_adjust,
        use_counter_css_doc_user_zoom,
        use_counter_css_doc_webkit_app_region,
        use_counter_css_doc_webkit_border_after,
        use_counter_css_doc_webkit_border_after_color,
        use_counter_css_doc_webkit_border_after_style,
        use_counter_css_doc_webkit_border_after_width,
        use_counter_css_doc_webkit_border_before,
        use_counter_css_doc_webkit_border_before_color,
        use_counter_css_doc_webkit_border_before_style,
        use_counter_css_doc_webkit_border_before_width,
        use_counter_css_doc_webkit_border_end,
        use_counter_css_doc_webkit_border_end_color,
        use_counter_css_doc_webkit_border_end_style,
        use_counter_css_doc_webkit_border_end_width,
        use_counter_css_doc_webkit_border_horizontal_spacing,
        use_counter_css_doc_webkit_border_start,
        use_counter_css_doc_webkit_border_start_color,
        use_counter_css_doc_webkit_border_start_style,
        use_counter_css_doc_webkit_border_start_width,
        use_counter_css_doc_webkit_border_vertical_spacing,
        use_counter_css_doc_webkit_box_decoration_break,
        use_counter_css_doc_webkit_box_reflect,
        use_counter_css_doc_webkit_column_break_after,
        use_counter_css_doc_webkit_column_break_before,
        use_counter_css_doc_webkit_column_break_inside,
        use_counter_css_doc_webkit_column_count,
        use_counter_css_doc_webkit_column_gap,
        use_counter_css_doc_webkit_column_rule,
        use_counter_css_doc_webkit_column_rule_color,
        use_counter_css_doc_webkit_column_rule_style,
        use_counter_css_doc_webkit_column_rule_width,
        use_counter_css_doc_webkit_column_span,
        use_counter_css_doc_webkit_column_width,
        use_counter_css_doc_webkit_columns,
        use_counter_css_doc_webkit_font_feature_settings,
        use_counter_css_doc_webkit_font_size_delta,
        use_counter_css_doc_webkit_font_smoothing,
        use_counter_css_doc_webkit_highlight,
        use_counter_css_doc_webkit_hyphenate_character,
        use_counter_css_doc_webkit_line_break,
        use_counter_css_doc_webkit_locale,
        use_counter_css_doc_webkit_logical_height,
        use_counter_css_doc_webkit_logical_width,
        use_counter_css_doc_webkit_margin_after,
        use_counter_css_doc_webkit_margin_after_collapse,
        use_counter_css_doc_webkit_margin_before,
        use_counter_css_doc_webkit_margin_before_collapse,
        use_counter_css_doc_webkit_margin_bottom_collapse,
        use_counter_css_doc_webkit_margin_collapse,
        use_counter_css_doc_webkit_margin_end,
        use_counter_css_doc_webkit_margin_start,
        use_counter_css_doc_webkit_margin_top_collapse,
        use_counter_css_doc_webkit_mask_box_image,
        use_counter_css_doc_webkit_mask_box_image_outset,
        use_counter_css_doc_webkit_mask_box_image_repeat,
        use_counter_css_doc_webkit_mask_box_image_slice,
        use_counter_css_doc_webkit_mask_box_image_source,
        use_counter_css_doc_webkit_mask_box_image_width,
        use_counter_css_doc_webkit_mask_repeat_x,
        use_counter_css_doc_webkit_mask_repeat_y,
        use_counter_css_doc_webkit_max_logical_height,
        use_counter_css_doc_webkit_max_logical_width,
        use_counter_css_doc_webkit_min_logical_height,
        use_counter_css_doc_webkit_min_logical_width,
        use_counter_css_doc_webkit_opacity,
        use_counter_css_doc_webkit_padding_after,
        use_counter_css_doc_webkit_padding_before,
        use_counter_css_doc_webkit_padding_end,
        use_counter_css_doc_webkit_padding_start,
        use_counter_css_doc_webkit_perspective_origin_x,
        use_counter_css_doc_webkit_perspective_origin_y,
        use_counter_css_doc_webkit_print_color_adjust,
        use_counter_css_doc_webkit_rtl_ordering,
        use_counter_css_doc_webkit_ruby_position,
        use_counter_css_doc_webkit_shape_image_threshold,
        use_counter_css_doc_webkit_shape_margin,
        use_counter_css_doc_webkit_shape_outside,
        use_counter_css_doc_webkit_tap_highlight_color,
        use_counter_css_doc_webkit_text_combine,
        use_counter_css_doc_webkit_text_decorations_in_effect,
        use_counter_css_doc_webkit_text_emphasis,
        use_counter_css_doc_webkit_text_emphasis_color,
        use_counter_css_doc_webkit_text_emphasis_position,
        use_counter_css_doc_webkit_text_emphasis_style,
        use_counter_css_doc_webkit_text_orientation,
        use_counter_css_doc_webkit_transform_origin_x,
        use_counter_css_doc_webkit_transform_origin_y,
        use_counter_css_doc_webkit_transform_origin_z,
        use_counter_css_doc_webkit_user_drag,
        use_counter_css_doc_webkit_user_modify,
        use_counter_css_doc_webkit_writing_mode,
        use_counter_css_doc_widows,
        use_counter_css_page_alignment_baseline,
        use_counter_css_page_background_repeat_x,
        use_counter_css_page_background_repeat_y,
        use_counter_css_page_baseline_shift,
        use_counter_css_page_buffered_rendering,
        use_counter_css_page_color_rendering,
        use_counter_css_page_css_accent_color,
        use_counter_css_page_css_align_content,
        use_counter_css_page_css_align_items,
        use_counter_css_page_css_align_self,
        use_counter_css_page_css_align_tracks,
        use_counter_css_page_css_all,
        use_counter_css_page_css_animation,
        use_counter_css_page_css_animation_composition,
        use_counter_css_page_css_animation_delay,
        use_counter_css_page_css_animation_direction,
        use_counter_css_page_css_animation_duration,
        use_counter_css_page_css_animation_fill_mode,
        use_counter_css_page_css_animation_iteration_count,
        use_counter_css_page_css_animation_name,
        use_counter_css_page_css_animation_play_state,
        use_counter_css_page_css_animation_timeline,
        use_counter_css_page_css_animation_timing_function,
        use_counter_css_page_css_appearance,
        use_counter_css_page_css_aspect_ratio,
        use_counter_css_page_css_backdrop_filter,
        use_counter_css_page_css_backface_visibility,
        use_counter_css_page_css_background,
        use_counter_css_page_css_background_attachment,
        use_counter_css_page_css_background_blend_mode,
        use_counter_css_page_css_background_clip,
        use_counter_css_page_css_background_color,
        use_counter_css_page_css_background_image,
        use_counter_css_page_css_background_origin,
        use_counter_css_page_css_background_position,
        use_counter_css_page_css_background_position_x,
        use_counter_css_page_css_background_position_y,
        use_counter_css_page_css_background_repeat,
        use_counter_css_page_css_background_size,
        use_counter_css_page_css_baseline_source,
        use_counter_css_page_css_block_size,
        use_counter_css_page_css_border,
        use_counter_css_page_css_border_block,
        use_counter_css_page_css_border_block_color,
        use_counter_css_page_css_border_block_end,
        use_counter_css_page_css_border_block_end_color,
        use_counter_css_page_css_border_block_end_style,
        use_counter_css_page_css_border_block_end_width,
        use_counter_css_page_css_border_block_start,
        use_counter_css_page_css_border_block_start_color,
        use_counter_css_page_css_border_block_start_style,
        use_counter_css_page_css_border_block_start_width,
        use_counter_css_page_css_border_block_style,
        use_counter_css_page_css_border_block_width,
        use_counter_css_page_css_border_bottom,
        use_counter_css_page_css_border_bottom_color,
        use_counter_css_page_css_border_bottom_left_radius,
        use_counter_css_page_css_border_bottom_right_radius,
        use_counter_css_page_css_border_bottom_style,
        use_counter_css_page_css_border_bottom_width,
        use_counter_css_page_css_border_collapse,
        use_counter_css_page_css_border_color,
        use_counter_css_page_css_border_end_end_radius,
        use_counter_css_page_css_border_end_start_radius,
        use_counter_css_page_css_border_image,
        use_counter_css_page_css_border_image_outset,
        use_counter_css_page_css_border_image_repeat,
        use_counter_css_page_css_border_image_slice,
        use_counter_css_page_css_border_image_source,
        use_counter_css_page_css_border_image_width,
        use_counter_css_page_css_border_inline,
        use_counter_css_page_css_border_inline_color,
        use_counter_css_page_css_border_inline_end,
        use_counter_css_page_css_border_inline_end_color,
        use_counter_css_page_css_border_inline_end_style,
        use_counter_css_page_css_border_inline_end_width,
        use_counter_css_page_css_border_inline_start,
        use_counter_css_page_css_border_inline_start_color,
        use_counter_css_page_css_border_inline_start_style,
        use_counter_css_page_css_border_inline_start_width,
        use_counter_css_page_css_border_inline_style,
        use_counter_css_page_css_border_inline_width,
        use_counter_css_page_css_border_left,
        use_counter_css_page_css_border_left_color,
        use_counter_css_page_css_border_left_style,
        use_counter_css_page_css_border_left_width,
        use_counter_css_page_css_border_radius,
        use_counter_css_page_css_border_right,
        use_counter_css_page_css_border_right_color,
        use_counter_css_page_css_border_right_style,
        use_counter_css_page_css_border_right_width,
        use_counter_css_page_css_border_spacing,
        use_counter_css_page_css_border_start_end_radius,
        use_counter_css_page_css_border_start_start_radius,
        use_counter_css_page_css_border_style,
        use_counter_css_page_css_border_top,
        use_counter_css_page_css_border_top_color,
        use_counter_css_page_css_border_top_left_radius,
        use_counter_css_page_css_border_top_right_radius,
        use_counter_css_page_css_border_top_style,
        use_counter_css_page_css_border_top_width,
        use_counter_css_page_css_border_width,
        use_counter_css_page_css_bottom,
        use_counter_css_page_css_box_decoration_break,
        use_counter_css_page_css_box_shadow,
        use_counter_css_page_css_box_sizing,
        use_counter_css_page_css_break_after,
        use_counter_css_page_css_break_before,
        use_counter_css_page_css_break_inside,
        use_counter_css_page_css_caption_side,
        use_counter_css_page_css_caret_color,
        use_counter_css_page_css_clear,
        use_counter_css_page_css_clip,
        use_counter_css_page_css_clip_path,
        use_counter_css_page_css_clip_rule,
        use_counter_css_page_css_color,
        use_counter_css_page_css_color_adjust,
        use_counter_css_page_css_color_interpolation,
        use_counter_css_page_css_color_interpolation_filters,
        use_counter_css_page_css_color_scheme,
        use_counter_css_page_css_column_count,
        use_counter_css_page_css_column_fill,
        use_counter_css_page_css_column_gap,
        use_counter_css_page_css_column_rule,
        use_counter_css_page_css_column_rule_color,
        use_counter_css_page_css_column_rule_style,
        use_counter_css_page_css_column_rule_width,
        use_counter_css_page_css_column_span,
        use_counter_css_page_css_column_width,
        use_counter_css_page_css_columns,
        use_counter_css_page_css_contain,
        use_counter_css_page_css_contain_intrinsic_block_size,
        use_counter_css_page_css_contain_intrinsic_height,
        use_counter_css_page_css_contain_intrinsic_inline_size,
        use_counter_css_page_css_contain_intrinsic_size,
        use_counter_css_page_css_contain_intrinsic_width,
        use_counter_css_page_css_container,
        use_counter_css_page_css_container_name,
        use_counter_css_page_css_container_type,
        use_counter_css_page_css_content,
        use_counter_css_page_css_content_visibility,
        use_counter_css_page_css_counter_increment,
        use_counter_css_page_css_counter_reset,
        use_counter_css_page_css_counter_set,
        use_counter_css_page_css_cursor,
        use_counter_css_page_css_cx,
        use_counter_css_page_css_cy,
        use_counter_css_page_css_d,
        use_counter_css_page_css_direction,
        use_counter_css_page_css_display,
        use_counter_css_page_css_dominant_baseline,
        use_counter_css_page_css_empty_cells,
        use_counter_css_page_css_fill,
        use_counter_css_page_css_fill_opacity,
        use_counter_css_page_css_fill_rule,
        use_counter_css_page_css_filter,
        use_counter_css_page_css_flex,
        use_counter_css_page_css_flex_basis,
        use_counter_css_page_css_flex_direction,
        use_counter_css_page_css_flex_flow,
        use_counter_css_page_css_flex_grow,
        use_counter_css_page_css_flex_shrink,
        use_counter_css_page_css_flex_wrap,
        use_counter_css_page_css_float,
        use_counter_css_page_css_flood_color,
        use_counter_css_page_css_flood_opacity,
        use_counter_css_page_css_font,
        use_counter_css_page_css_font_family,
        use_counter_css_page_css_font_feature_settings,
        use_counter_css_page_css_font_kerning,
        use_counter_css_page_css_font_language_override,
        use_counter_css_page_css_font_optical_sizing,
        use_counter_css_page_css_font_palette,
        use_counter_css_page_css_font_size,
        use_counter_css_page_css_font_size_adjust,
        use_counter_css_page_css_font_stretch,
        use_counter_css_page_css_font_style,
        use_counter_css_page_css_font_synthesis,
        use_counter_css_page_css_font_synthesis_position,
        use_counter_css_page_css_font_synthesis_small_caps,
        use_counter_css_page_css_font_synthesis_style,
        use_counter_css_page_css_font_synthesis_weight,
        use_counter_css_page_css_font_variant,
        use_counter_css_page_css_font_variant_alternates,
        use_counter_css_page_css_font_variant_caps,
        use_counter_css_page_css_font_variant_east_asian,
        use_counter_css_page_css_font_variant_emoji,
        use_counter_css_page_css_font_variant_ligatures,
        use_counter_css_page_css_font_variant_numeric,
        use_counter_css_page_css_font_variant_position,
        use_counter_css_page_css_font_variation_settings,
        use_counter_css_page_css_font_weight,
        use_counter_css_page_css_forced_color_adjust,
        use_counter_css_page_css_gap,
        use_counter_css_page_css_grid,
        use_counter_css_page_css_grid_area,
        use_counter_css_page_css_grid_auto_columns,
        use_counter_css_page_css_grid_auto_flow,
        use_counter_css_page_css_grid_auto_rows,
        use_counter_css_page_css_grid_column,
        use_counter_css_page_css_grid_column_end,
        use_counter_css_page_css_grid_column_gap,
        use_counter_css_page_css_grid_column_start,
        use_counter_css_page_css_grid_gap,
        use_counter_css_page_css_grid_row,
        use_counter_css_page_css_grid_row_end,
        use_counter_css_page_css_grid_row_gap,
        use_counter_css_page_css_grid_row_start,
        use_counter_css_page_css_grid_template,
        use_counter_css_page_css_grid_template_areas,
        use_counter_css_page_css_grid_template_columns,
        use_counter_css_page_css_grid_template_rows,
        use_counter_css_page_css_height,
        use_counter_css_page_css_hyphenate_character,
        use_counter_css_page_css_hyphens,
        use_counter_css_page_css_image_orientation,
        use_counter_css_page_css_image_rendering,
        use_counter_css_page_css_ime_mode,
        use_counter_css_page_css_initial_letter,
        use_counter_css_page_css_inline_size,
        use_counter_css_page_css_inset,
        use_counter_css_page_css_inset_block,
        use_counter_css_page_css_inset_block_end,
        use_counter_css_page_css_inset_block_start,
        use_counter_css_page_css_inset_inline,
        use_counter_css_page_css_inset_inline_end,
        use_counter_css_page_css_inset_inline_start,
        use_counter_css_page_css_isolation,
        use_counter_css_page_css_justify_content,
        use_counter_css_page_css_justify_items,
        use_counter_css_page_css_justify_self,
        use_counter_css_page_css_justify_tracks,
        use_counter_css_page_css_left,
        use_counter_css_page_css_letter_spacing,
        use_counter_css_page_css_lighting_color,
        use_counter_css_page_css_line_break,
        use_counter_css_page_css_line_height,
        use_counter_css_page_css_list_style,
        use_counter_css_page_css_list_style_image,
        use_counter_css_page_css_list_style_position,
        use_counter_css_page_css_list_style_type,
        use_counter_css_page_css_margin,
        use_counter_css_page_css_margin_block,
        use_counter_css_page_css_margin_block_end,
        use_counter_css_page_css_margin_block_start,
        use_counter_css_page_css_margin_bottom,
        use_counter_css_page_css_margin_inline,
        use_counter_css_page_css_margin_inline_end,
        use_counter_css_page_css_margin_inline_start,
        use_counter_css_page_css_margin_left,
        use_counter_css_page_css_margin_right,
        use_counter_css_page_css_margin_top,
        use_counter_css_page_css_marker,
        use_counter_css_page_css_marker_end,
        use_counter_css_page_css_marker_mid,
        use_counter_css_page_css_marker_start,
        use_counter_css_page_css_mask,
        use_counter_css_page_css_mask_clip,
        use_counter_css_page_css_mask_composite,
        use_counter_css_page_css_mask_image,
        use_counter_css_page_css_mask_mode,
        use_counter_css_page_css_mask_origin,
        use_counter_css_page_css_mask_position,
        use_counter_css_page_css_mask_position_x,
        use_counter_css_page_css_mask_position_y,
        use_counter_css_page_css_mask_repeat,
        use_counter_css_page_css_mask_size,
        use_counter_css_page_css_mask_type,
        use_counter_css_page_css_masonry_auto_flow,
        use_counter_css_page_css_math_depth,
        use_counter_css_page_css_math_style,
        use_counter_css_page_css_max_block_size,
        use_counter_css_page_css_max_height,
        use_counter_css_page_css_max_inline_size,
        use_counter_css_page_css_max_width,
        use_counter_css_page_css_min_block_size,
        use_counter_css_page_css_min_height,
        use_counter_css_page_css_min_inline_size,
        use_counter_css_page_css_min_width,
        use_counter_css_page_css_mix_blend_mode,
        use_counter_css_page_css_moz_animation,
        use_counter_css_page_css_moz_animation_delay,
        use_counter_css_page_css_moz_animation_direction,
        use_counter_css_page_css_moz_animation_duration,
        use_counter_css_page_css_moz_animation_fill_mode,
        use_counter_css_page_css_moz_animation_iteration_count,
        use_counter_css_page_css_moz_animation_name,
        use_counter_css_page_css_moz_animation_play_state,
        use_counter_css_page_css_moz_animation_timing_function,
        use_counter_css_page_css_moz_appearance,
        use_counter_css_page_css_moz_backface_visibility,
        use_counter_css_page_css_moz_border_end,
        use_counter_css_page_css_moz_border_end_color,
        use_counter_css_page_css_moz_border_end_style,
        use_counter_css_page_css_moz_border_end_width,
        use_counter_css_page_css_moz_border_image,
        use_counter_css_page_css_moz_border_start,
        use_counter_css_page_css_moz_border_start_color,
        use_counter_css_page_css_moz_border_start_style,
        use_counter_css_page_css_moz_border_start_width,
        use_counter_css_page_css_moz_box_align,
        use_counter_css_page_css_moz_box_collapse,
        use_counter_css_page_css_moz_box_direction,
        use_counter_css_page_css_moz_box_flex,
        use_counter_css_page_css_moz_box_ordinal_group,
        use_counter_css_page_css_moz_box_orient,
        use_counter_css_page_css_moz_box_pack,
        use_counter_css_page_css_moz_box_sizing,
        use_counter_css_page_css_moz_context_properties,
        use_counter_css_page_css_moz_control_character_visibility,
        use_counter_css_page_css_moz_default_appearance,
        use_counter_css_page_css_moz_float_edge,
        use_counter_css_page_css_moz_font_feature_settings,
        use_counter_css_page_css_moz_font_language_override,
        use_counter_css_page_css_moz_force_broken_image_icon,
        use_counter_css_page_css_moz_hyphens,
        use_counter_css_page_css_moz_inert,
        use_counter_css_page_css_moz_margin_end,
        use_counter_css_page_css_moz_margin_start,
        use_counter_css_page_css_moz_math_variant,
        use_counter_css_page_css_moz_min_font_size_ratio,
        use_counter_css_page_css_moz_orient,
        use_counter_css_page_css_moz_osx_font_smoothing,
        use_counter_css_page_css_moz_padding_end,
        use_counter_css_page_css_moz_padding_start,
        use_counter_css_page_css_moz_perspective,
        use_counter_css_page_css_moz_perspective_origin,
        use_counter_css_page_css_moz_subtree_hidden_only_visually,
        use_counter_css_page_css_moz_tab_size,
        use_counter_css_page_css_moz_text_size_adjust,
        use_counter_css_page_css_moz_theme,
        use_counter_css_page_css_moz_top_layer,
        use_counter_css_page_css_moz_transform,
        use_counter_css_page_css_moz_transform_origin,
        use_counter_css_page_css_moz_transform_style,
        use_counter_css_page_css_moz_transition,
        use_counter_css_page_css_moz_transition_delay,
        use_counter_css_page_css_moz_transition_duration,
        use_counter_css_page_css_moz_transition_property,
        use_counter_css_page_css_moz_transition_timing_function,
        use_counter_css_page_css_moz_user_focus,
        use_counter_css_page_css_moz_user_input,
        use_counter_css_page_css_moz_user_modify,
        use_counter_css_page_css_moz_user_select,
        use_counter_css_page_css_moz_window_dragging,
        use_counter_css_page_css_moz_window_input_region_margin,
        use_counter_css_page_css_moz_window_opacity,
        use_counter_css_page_css_moz_window_shadow,
        use_counter_css_page_css_moz_window_transform,
        use_counter_css_page_css_moz_window_transform_origin,
        use_counter_css_page_css_object_fit,
        use_counter_css_page_css_object_position,
        use_counter_css_page_css_offset,
        use_counter_css_page_css_offset_anchor,
        use_counter_css_page_css_offset_distance,
        use_counter_css_page_css_offset_path,
        use_counter_css_page_css_offset_position,
        use_counter_css_page_css_offset_rotate,
        use_counter_css_page_css_opacity,
        use_counter_css_page_css_order,
        use_counter_css_page_css_outline,
        use_counter_css_page_css_outline_color,
        use_counter_css_page_css_outline_offset,
        use_counter_css_page_css_outline_style,
        use_counter_css_page_css_outline_width,
        use_counter_css_page_css_overflow,
        use_counter_css_page_css_overflow_anchor,
        use_counter_css_page_css_overflow_block,
        use_counter_css_page_css_overflow_clip_box,
        use_counter_css_page_css_overflow_clip_box_block,
        use_counter_css_page_css_overflow_clip_box_inline,
        use_counter_css_page_css_overflow_clip_margin,
        use_counter_css_page_css_overflow_inline,
        use_counter_css_page_css_overflow_wrap,
        use_counter_css_page_css_overflow_x,
        use_counter_css_page_css_overflow_y,
        use_counter_css_page_css_overscroll_behavior,
        use_counter_css_page_css_overscroll_behavior_block,
        use_counter_css_page_css_overscroll_behavior_inline,
        use_counter_css_page_css_overscroll_behavior_x,
        use_counter_css_page_css_overscroll_behavior_y,
        use_counter_css_page_css_padding,
        use_counter_css_page_css_padding_block,
        use_counter_css_page_css_padding_block_end,
        use_counter_css_page_css_padding_block_start,
        use_counter_css_page_css_padding_bottom,
        use_counter_css_page_css_padding_inline,
        use_counter_css_page_css_padding_inline_end,
        use_counter_css_page_css_padding_inline_start,
        use_counter_css_page_css_padding_left,
        use_counter_css_page_css_padding_right,
        use_counter_css_page_css_padding_top,
        use_counter_css_page_css_page,
        use_counter_css_page_css_page_break_after,
        use_counter_css_page_css_page_break_before,
        use_counter_css_page_css_page_break_inside,
        use_counter_css_page_css_page_orientation,
        use_counter_css_page_css_paint_order,
        use_counter_css_page_css_perspective,
        use_counter_css_page_css_perspective_origin,
        use_counter_css_page_css_place_content,
        use_counter_css_page_css_place_items,
        use_counter_css_page_css_place_self,
        use_counter_css_page_css_pointer_events,
        use_counter_css_page_css_position,
        use_counter_css_page_css_print_color_adjust,
        use_counter_css_page_css_quotes,
        use_counter_css_page_css_r,
        use_counter_css_page_css_resize,
        use_counter_css_page_css_right,
        use_counter_css_page_css_rotate,
        use_counter_css_page_css_row_gap,
        use_counter_css_page_css_ruby_align,
        use_counter_css_page_css_ruby_position,
        use_counter_css_page_css_rx,
        use_counter_css_page_css_ry,
        use_counter_css_page_css_scale,
        use_counter_css_page_css_scroll_behavior,
        use_counter_css_page_css_scroll_margin,
        use_counter_css_page_css_scroll_margin_block,
        use_counter_css_page_css_scroll_margin_block_end,
        use_counter_css_page_css_scroll_margin_block_start,
        use_counter_css_page_css_scroll_margin_bottom,
        use_counter_css_page_css_scroll_margin_inline,
        use_counter_css_page_css_scroll_margin_inline_end,
        use_counter_css_page_css_scroll_margin_inline_start,
        use_counter_css_page_css_scroll_margin_left,
        use_counter_css_page_css_scroll_margin_right,
        use_counter_css_page_css_scroll_margin_top,
        use_counter_css_page_css_scroll_padding,
        use_counter_css_page_css_scroll_padding_block,
        use_counter_css_page_css_scroll_padding_block_end,
        use_counter_css_page_css_scroll_padding_block_start,
        use_counter_css_page_css_scroll_padding_bottom,
        use_counter_css_page_css_scroll_padding_inline,
        use_counter_css_page_css_scroll_padding_inline_end,
        use_counter_css_page_css_scroll_padding_inline_start,
        use_counter_css_page_css_scroll_padding_left,
        use_counter_css_page_css_scroll_padding_right,
        use_counter_css_page_css_scroll_padding_top,
        use_counter_css_page_css_scroll_snap_align,
        use_counter_css_page_css_scroll_snap_stop,
        use_counter_css_page_css_scroll_snap_type,
        use_counter_css_page_css_scroll_timeline,
        use_counter_css_page_css_scroll_timeline_axis,
        use_counter_css_page_css_scroll_timeline_name,
        use_counter_css_page_css_scrollbar_color,
        use_counter_css_page_css_scrollbar_gutter,
        use_counter_css_page_css_scrollbar_width,
        use_counter_css_page_css_shape_image_threshold,
        use_counter_css_page_css_shape_margin,
        use_counter_css_page_css_shape_outside,
        use_counter_css_page_css_shape_rendering,
        use_counter_css_page_css_size,
        use_counter_css_page_css_stop_color,
        use_counter_css_page_css_stop_opacity,
        use_counter_css_page_css_stroke,
        use_counter_css_page_css_stroke_dasharray,
        use_counter_css_page_css_stroke_dashoffset,
        use_counter_css_page_css_stroke_linecap,
        use_counter_css_page_css_stroke_linejoin,
        use_counter_css_page_css_stroke_miterlimit,
        use_counter_css_page_css_stroke_opacity,
        use_counter_css_page_css_stroke_width,
        use_counter_css_page_css_tab_size,
        use_counter_css_page_css_table_layout,
        use_counter_css_page_css_text_align,
        use_counter_css_page_css_text_align_last,
        use_counter_css_page_css_text_anchor,
        use_counter_css_page_css_text_combine_upright,
        use_counter_css_page_css_text_decoration,
        use_counter_css_page_css_text_decoration_color,
        use_counter_css_page_css_text_decoration_line,
        use_counter_css_page_css_text_decoration_skip_ink,
        use_counter_css_page_css_text_decoration_style,
        use_counter_css_page_css_text_decoration_thickness,
        use_counter_css_page_css_text_emphasis,
        use_counter_css_page_css_text_emphasis_color,
        use_counter_css_page_css_text_emphasis_position,
        use_counter_css_page_css_text_emphasis_style,
        use_counter_css_page_css_text_indent,
        use_counter_css_page_css_text_justify,
        use_counter_css_page_css_text_orientation,
        use_counter_css_page_css_text_overflow,
        use_counter_css_page_css_text_rendering,
        use_counter_css_page_css_text_shadow,
        use_counter_css_page_css_text_transform,
        use_counter_css_page_css_text_underline_offset,
        use_counter_css_page_css_text_underline_position,
        use_counter_css_page_css_text_wrap,
        use_counter_css_page_css_top,
        use_counter_css_page_css_touch_action,
        use_counter_css_page_css_transform,
        use_counter_css_page_css_transform_box,
        use_counter_css_page_css_transform_origin,
        use_counter_css_page_css_transform_style,
        use_counter_css_page_css_transition,
        use_counter_css_page_css_transition_delay,
        use_counter_css_page_css_transition_duration,
        use_counter_css_page_css_transition_property,
        use_counter_css_page_css_transition_timing_function,
        use_counter_css_page_css_translate,
        use_counter_css_page_css_unicode_bidi,
        use_counter_css_page_css_user_select,
        use_counter_css_page_css_vector_effect,
        use_counter_css_page_css_vertical_align,
        use_counter_css_page_css_view_timeline,
        use_counter_css_page_css_view_timeline_axis,
        use_counter_css_page_css_view_timeline_inset,
        use_counter_css_page_css_view_timeline_name,
        use_counter_css_page_css_visibility,
        use_counter_css_page_css_webkit_align_content,
        use_counter_css_page_css_webkit_align_items,
        use_counter_css_page_css_webkit_align_self,
        use_counter_css_page_css_webkit_animation,
        use_counter_css_page_css_webkit_animation_delay,
        use_counter_css_page_css_webkit_animation_direction,
        use_counter_css_page_css_webkit_animation_duration,
        use_counter_css_page_css_webkit_animation_fill_mode,
        use_counter_css_page_css_webkit_animation_iteration_count,
        use_counter_css_page_css_webkit_animation_name,
        use_counter_css_page_css_webkit_animation_play_state,
        use_counter_css_page_css_webkit_animation_timing_function,
        use_counter_css_page_css_webkit_appearance,
        use_counter_css_page_css_webkit_backface_visibility,
        use_counter_css_page_css_webkit_background_clip,
        use_counter_css_page_css_webkit_background_origin,
        use_counter_css_page_css_webkit_background_size,
        use_counter_css_page_css_webkit_border_bottom_left_radius,
        use_counter_css_page_css_webkit_border_bottom_right_radius,
        use_counter_css_page_css_webkit_border_image,
        use_counter_css_page_css_webkit_border_radius,
        use_counter_css_page_css_webkit_border_top_left_radius,
        use_counter_css_page_css_webkit_border_top_right_radius,
        use_counter_css_page_css_webkit_box_align,
        use_counter_css_page_css_webkit_box_direction,
        use_counter_css_page_css_webkit_box_flex,
        use_counter_css_page_css_webkit_box_ordinal_group,
        use_counter_css_page_css_webkit_box_orient,
        use_counter_css_page_css_webkit_box_pack,
        use_counter_css_page_css_webkit_box_shadow,
        use_counter_css_page_css_webkit_box_sizing,
        use_counter_css_page_css_webkit_clip_path,
        use_counter_css_page_css_webkit_filter,
        use_counter_css_page_css_webkit_flex,
        use_counter_css_page_css_webkit_flex_basis,
        use_counter_css_page_css_webkit_flex_direction,
        use_counter_css_page_css_webkit_flex_flow,
        use_counter_css_page_css_webkit_flex_grow,
        use_counter_css_page_css_webkit_flex_shrink,
        use_counter_css_page_css_webkit_flex_wrap,
        use_counter_css_page_css_webkit_justify_content,
        use_counter_css_page_css_webkit_line_clamp,
        use_counter_css_page_css_webkit_mask,
        use_counter_css_page_css_webkit_mask_clip,
        use_counter_css_page_css_webkit_mask_composite,
        use_counter_css_page_css_webkit_mask_image,
        use_counter_css_page_css_webkit_mask_origin,
        use_counter_css_page_css_webkit_mask_position,
        use_counter_css_page_css_webkit_mask_position_x,
        use_counter_css_page_css_webkit_mask_position_y,
        use_counter_css_page_css_webkit_mask_repeat,
        use_counter_css_page_css_webkit_mask_size,
        use_counter_css_page_css_webkit_order,
        use_counter_css_page_css_webkit_perspective,
        use_counter_css_page_css_webkit_perspective_origin,
        use_counter_css_page_css_webkit_text_fill_color,
        use_counter_css_page_css_webkit_text_security,
        use_counter_css_page_css_webkit_text_size_adjust,
        use_counter_css_page_css_webkit_text_stroke,
        use_counter_css_page_css_webkit_text_stroke_color,
        use_counter_css_page_css_webkit_text_stroke_width,
        use_counter_css_page_css_webkit_transform,
        use_counter_css_page_css_webkit_transform_origin,
        use_counter_css_page_css_webkit_transform_style,
        use_counter_css_page_css_webkit_transition,
        use_counter_css_page_css_webkit_transition_delay,
        use_counter_css_page_css_webkit_transition_duration,
        use_counter_css_page_css_webkit_transition_property,
        use_counter_css_page_css_webkit_transition_timing_function,
        use_counter_css_page_css_webkit_user_select,
        use_counter_css_page_css_white_space,
        use_counter_css_page_css_width,
        use_counter_css_page_css_will_change,
        use_counter_css_page_css_word_break,
        use_counter_css_page_css_word_spacing,
        use_counter_css_page_css_word_wrap,
        use_counter_css_page_css_writing_mode,
        use_counter_css_page_css_x,
        use_counter_css_page_css_x_lang,
        use_counter_css_page_css_x_span,
        use_counter_css_page_css_x_text_scale,
        use_counter_css_page_css_y,
        use_counter_css_page_css_z_index,
        use_counter_css_page_css_zoom,
        use_counter_css_page_max_zoom,
        use_counter_css_page_min_zoom,
        use_counter_css_page_orientation,
        use_counter_css_page_orphans,
        use_counter_css_page_speak,
        use_counter_css_page_text_size_adjust,
        use_counter_css_page_user_zoom,
        use_counter_css_page_webkit_app_region,
        use_counter_css_page_webkit_border_after,
        use_counter_css_page_webkit_border_after_color,
        use_counter_css_page_webkit_border_after_style,
        use_counter_css_page_webkit_border_after_width,
        use_counter_css_page_webkit_border_before,
        use_counter_css_page_webkit_border_before_color,
        use_counter_css_page_webkit_border_before_style,
        use_counter_css_page_webkit_border_before_width,
        use_counter_css_page_webkit_border_end,
        use_counter_css_page_webkit_border_end_color,
        use_counter_css_page_webkit_border_end_style,
        use_counter_css_page_webkit_border_end_width,
        use_counter_css_page_webkit_border_horizontal_spacing,
        use_counter_css_page_webkit_border_start,
        use_counter_css_page_webkit_border_start_color,
        use_counter_css_page_webkit_border_start_style,
        use_counter_css_page_webkit_border_start_width,
        use_counter_css_page_webkit_border_vertical_spacing,
        use_counter_css_page_webkit_box_decoration_break,
        use_counter_css_page_webkit_box_reflect,
        use_counter_css_page_webkit_column_break_after,
        use_counter_css_page_webkit_column_break_before,
        use_counter_css_page_webkit_column_break_inside,
        use_counter_css_page_webkit_column_count,
        use_counter_css_page_webkit_column_gap,
        use_counter_css_page_webkit_column_rule,
        use_counter_css_page_webkit_column_rule_color,
        use_counter_css_page_webkit_column_rule_style,
        use_counter_css_page_webkit_column_rule_width,
        use_counter_css_page_webkit_column_span,
        use_counter_css_page_webkit_column_width,
        use_counter_css_page_webkit_columns,
        use_counter_css_page_webkit_font_feature_settings,
        use_counter_css_page_webkit_font_size_delta,
        use_counter_css_page_webkit_font_smoothing,
        use_counter_css_page_webkit_highlight,
        use_counter_css_page_webkit_hyphenate_character,
        use_counter_css_page_webkit_line_break,
        use_counter_css_page_webkit_locale,
        use_counter_css_page_webkit_logical_height,
        use_counter_css_page_webkit_logical_width,
        use_counter_css_page_webkit_margin_after,
        use_counter_css_page_webkit_margin_after_collapse,
        use_counter_css_page_webkit_margin_before,
        use_counter_css_page_webkit_margin_before_collapse,
        use_counter_css_page_webkit_margin_bottom_collapse,
        use_counter_css_page_webkit_margin_collapse,
        use_counter_css_page_webkit_margin_end,
        use_counter_css_page_webkit_margin_start,
        use_counter_css_page_webkit_margin_top_collapse,
        use_counter_css_page_webkit_mask_box_image,
        use_counter_css_page_webkit_mask_box_image_outset,
        use_counter_css_page_webkit_mask_box_image_repeat,
        use_counter_css_page_webkit_mask_box_image_slice,
        use_counter_css_page_webkit_mask_box_image_source,
        use_counter_css_page_webkit_mask_box_image_width,
        use_counter_css_page_webkit_mask_repeat_x,
        use_counter_css_page_webkit_mask_repeat_y,
        use_counter_css_page_webkit_max_logical_height,
        use_counter_css_page_webkit_max_logical_width,
        use_counter_css_page_webkit_min_logical_height,
        use_counter_css_page_webkit_min_logical_width,
        use_counter_css_page_webkit_opacity,
        use_counter_css_page_webkit_padding_after,
        use_counter_css_page_webkit_padding_before,
        use_counter_css_page_webkit_padding_end,
        use_counter_css_page_webkit_padding_start,
        use_counter_css_page_webkit_perspective_origin_x,
        use_counter_css_page_webkit_perspective_origin_y,
        use_counter_css_page_webkit_print_color_adjust,
        use_counter_css_page_webkit_rtl_ordering,
        use_counter_css_page_webkit_ruby_position,
        use_counter_css_page_webkit_shape_image_threshold,
        use_counter_css_page_webkit_shape_margin,
        use_counter_css_page_webkit_shape_outside,
        use_counter_css_page_webkit_tap_highlight_color,
        use_counter_css_page_webkit_text_combine,
        use_counter_css_page_webkit_text_decorations_in_effect,
        use_counter_css_page_webkit_text_emphasis,
        use_counter_css_page_webkit_text_emphasis_color,
        use_counter_css_page_webkit_text_emphasis_position,
        use_counter_css_page_webkit_text_emphasis_style,
        use_counter_css_page_webkit_text_orientation,
        use_counter_css_page_webkit_transform_origin_x,
        use_counter_css_page_webkit_transform_origin_y,
        use_counter_css_page_webkit_transform_origin_z,
        use_counter_css_page_webkit_user_drag,
        use_counter_css_page_webkit_user_modify,
        use_counter_css_page_webkit_writing_mode,
        use_counter_css_page_widows,
        use_counter_deprecated_ops_doc_ambient_light_event,
        use_counter_deprecated_ops_doc_app_cache,
        use_counter_deprecated_ops_doc_components,
        use_counter_deprecated_ops_doc_create_image_bitmap_canvas_rendering_context2_d,
        use_counter_deprecated_ops_doc_deprecated_testing_attribute,
        use_counter_deprecated_ops_doc_deprecated_testing_interface,
        use_counter_deprecated_ops_doc_deprecated_testing_method,
        use_counter_deprecated_ops_doc_document_release_capture,
        use_counter_deprecated_ops_doc_domquad_bounds_attr,
        use_counter_deprecated_ops_doc_draw_window_canvas_rendering_context2_d,
        use_counter_deprecated_ops_doc_element_release_capture,
        use_counter_deprecated_ops_doc_element_set_capture,
        use_counter_deprecated_ops_doc_external_add_search_provider,
        use_counter_deprecated_ops_doc_form_submission_untrusted_event,
        use_counter_deprecated_ops_doc_idbopen_dboptions_storage_type,
        use_counter_deprecated_ops_doc_image_bitmap_rendering_context_transfer_image_bitmap,
        use_counter_deprecated_ops_doc_import_xulinto_content,
        use_counter_deprecated_ops_doc_init_mouse_event,
        use_counter_deprecated_ops_doc_init_nsmouse_event,
        use_counter_deprecated_ops_doc_install_trigger_deprecated,
        use_counter_deprecated_ops_doc_install_trigger_install_deprecated,
        use_counter_deprecated_ops_doc_is_external_ctap2_security_key_supported,
        use_counter_deprecated_ops_doc_lenient_setter,
        use_counter_deprecated_ops_doc_lenient_this,
        use_counter_deprecated_ops_doc_math_ml_deprecated_math_space_value2,
        use_counter_deprecated_ops_doc_math_ml_deprecated_math_variant,
        use_counter_deprecated_ops_doc_math_ml_deprecated_stixgeneral_operator_stretching,
        use_counter_deprecated_ops_doc_motion_event,
        use_counter_deprecated_ops_doc_mouse_event_moz_pressure,
        use_counter_deprecated_ops_doc_moz_input_source,
        use_counter_deprecated_ops_doc_moz_request_full_screen_deprecated_prefix,
        use_counter_deprecated_ops_doc_mozfullscreenchange_deprecated_prefix,
        use_counter_deprecated_ops_doc_mozfullscreenerror_deprecated_prefix,
        use_counter_deprecated_ops_doc_mutation_event,
        use_counter_deprecated_ops_doc_navigator_get_user_media,
        use_counter_deprecated_ops_doc_node_iterator_detach,
        use_counter_deprecated_ops_doc_offscreen_canvas_to_blob,
        use_counter_deprecated_ops_doc_orientation_event,
        use_counter_deprecated_ops_doc_proximity_event,
        use_counter_deprecated_ops_doc_rtcpeer_connection_get_streams,
        use_counter_deprecated_ops_doc_size_to_content,
        use_counter_deprecated_ops_doc_svgdeselect_all,
        use_counter_deprecated_ops_doc_svgfarthest_viewport_element,
        use_counter_deprecated_ops_doc_svgnearest_viewport_element,
        use_counter_deprecated_ops_doc_sync_xmlhttp_request_deprecated,
        use_counter_deprecated_ops_doc_use_of_capture_events,
        use_counter_deprecated_ops_doc_use_of_release_events,
        use_counter_deprecated_ops_doc_webrtc_deprecated_prefix,
        use_counter_deprecated_ops_doc_window_cc_ontrollers,
        use_counter_deprecated_ops_doc_window_content_untrusted,
        use_counter_deprecated_ops_page_ambient_light_event,
        use_counter_deprecated_ops_page_app_cache,
        use_counter_deprecated_ops_page_components,
        use_counter_deprecated_ops_page_create_image_bitmap_canvas_rendering_context2_d,
        use_counter_deprecated_ops_page_deprecated_testing_attribute,
        use_counter_deprecated_ops_page_deprecated_testing_interface,
        use_counter_deprecated_ops_page_deprecated_testing_method,
        use_counter_deprecated_ops_page_document_release_capture,
        use_counter_deprecated_ops_page_domquad_bounds_attr,
        use_counter_deprecated_ops_page_draw_window_canvas_rendering_context2_d,
        use_counter_deprecated_ops_page_element_release_capture,
        use_counter_deprecated_ops_page_element_set_capture,
        use_counter_deprecated_ops_page_external_add_search_provider,
        use_counter_deprecated_ops_page_form_submission_untrusted_event,
        use_counter_deprecated_ops_page_idbopen_dboptions_storage_type,
        use_counter_deprecated_ops_page_image_bitmap_rendering_context_transfer_image_bitmap,
        use_counter_deprecated_ops_page_import_xulinto_content,
        use_counter_deprecated_ops_page_init_mouse_event,
        use_counter_deprecated_ops_page_init_nsmouse_event,
        use_counter_deprecated_ops_page_install_trigger_deprecated,
        use_counter_deprecated_ops_page_install_trigger_install_deprecated,
        use_counter_deprecated_ops_page_is_external_ctap2_security_key_supported,
        use_counter_deprecated_ops_page_lenient_setter,
        use_counter_deprecated_ops_page_lenient_this,
        use_counter_deprecated_ops_page_math_ml_deprecated_math_space_value2,
        use_counter_deprecated_ops_page_math_ml_deprecated_math_variant,
        use_counter_deprecated_ops_page_math_ml_deprecated_stixgeneral_operator_stretching,
        use_counter_deprecated_ops_page_motion_event,
        use_counter_deprecated_ops_page_mouse_event_moz_pressure,
        use_counter_deprecated_ops_page_moz_input_source,
        use_counter_deprecated_ops_page_moz_request_full_screen_deprecated_prefix,
        use_counter_deprecated_ops_page_mozfullscreenchange_deprecated_prefix,
        use_counter_deprecated_ops_page_mozfullscreenerror_deprecated_prefix,
        use_counter_deprecated_ops_page_mutation_event,
        use_counter_deprecated_ops_page_navigator_get_user_media,
        use_counter_deprecated_ops_page_node_iterator_detach,
        use_counter_deprecated_ops_page_offscreen_canvas_to_blob,
        use_counter_deprecated_ops_page_orientation_event,
        use_counter_deprecated_ops_page_proximity_event,
        use_counter_deprecated_ops_page_rtcpeer_connection_get_streams,
        use_counter_deprecated_ops_page_size_to_content,
        use_counter_deprecated_ops_page_svgdeselect_all,
        use_counter_deprecated_ops_page_svgfarthest_viewport_element,
        use_counter_deprecated_ops_page_svgnearest_viewport_element,
        use_counter_deprecated_ops_page_sync_xmlhttp_request_deprecated,
        use_counter_deprecated_ops_page_use_of_capture_events,
        use_counter_deprecated_ops_page_use_of_release_events,
        use_counter_deprecated_ops_page_webrtc_deprecated_prefix,
        use_counter_deprecated_ops_page_window_cc_ontrollers,
        use_counter_deprecated_ops_page_window_content_untrusted,
        use_counter_doc_clipboard_read,
        use_counter_doc_clipboard_readtext,
        use_counter_doc_clipboard_write,
        use_counter_doc_console_assert,
        use_counter_doc_console_clear,
        use_counter_doc_console_count,
        use_counter_doc_console_countreset,
        use_counter_doc_console_debug,
        use_counter_doc_console_dir,
        use_counter_doc_console_dirxml,
        use_counter_doc_console_error,
        use_counter_doc_console_exception,
        use_counter_doc_console_group,
        use_counter_doc_console_groupcollapsed,
        use_counter_doc_console_groupend,
        use_counter_doc_console_info,
        use_counter_doc_console_log,
        use_counter_doc_console_profile,
        use_counter_doc_console_profileend,
        use_counter_doc_console_table,
        use_counter_doc_console_time,
        use_counter_doc_console_timeend,
        use_counter_doc_console_timelog,
        use_counter_doc_console_timestamp,
        use_counter_doc_console_trace,
        use_counter_doc_console_warn,
        use_counter_doc_customelementregistry_define,
        use_counter_doc_customized_builtin,
        use_counter_doc_datatransfer_addelement,
        use_counter_doc_datatransfer_mozcleardataat,
        use_counter_doc_datatransfer_mozcursor_getter,
        use_counter_doc_datatransfer_mozcursor_setter,
        use_counter_doc_datatransfer_mozgetdataat,
        use_counter_doc_datatransfer_mozitemcount_getter,
        use_counter_doc_datatransfer_mozitemcount_setter,
        use_counter_doc_datatransfer_mozsetdataat,
        use_counter_doc_datatransfer_mozsourcenode_getter,
        use_counter_doc_datatransfer_mozsourcenode_setter,
        use_counter_doc_datatransfer_moztypesat,
        use_counter_doc_datatransfer_mozusercancelled_getter,
        use_counter_doc_datatransfer_mozusercancelled_setter,
        use_counter_doc_datatransferitem_webkitgetasentry,
        use_counter_doc_document_caretrangefrompoint,
        use_counter_doc_document_exec_command_content_read_only,
        use_counter_doc_document_exitpictureinpicture,
        use_counter_doc_document_featurepolicy,
        use_counter_doc_document_mozsetimageelement,
        use_counter_doc_document_onbeforecopy,
        use_counter_doc_document_onbeforecut,
        use_counter_doc_document_onbeforepaste,
        use_counter_doc_document_oncancel,
        use_counter_doc_document_onfreeze,
        use_counter_doc_document_onmousewheel,
        use_counter_doc_document_onresume,
        use_counter_doc_document_onsearch,
        use_counter_doc_document_onwebkitfullscreenchange,
        use_counter_doc_document_onwebkitfullscreenerror,
        use_counter_doc_document_open,
        use_counter_doc_document_pictureinpictureelement,
        use_counter_doc_document_pictureinpictureenabled,
        use_counter_doc_document_query_command_state_or_value_content_read_only,
        use_counter_doc_document_query_command_state_or_value_insert_br_on_return,
        use_counter_doc_document_query_command_supported_or_enabled_content_read_only,
        use_counter_doc_document_query_command_supported_or_enabled_insert_br_on_return,
        use_counter_doc_document_registerelement,
        use_counter_doc_document_wasdiscarded,
        use_counter_doc_document_webkitcancelfullscreen,
        use_counter_doc_document_webkitcurrentfullscreenelement,
        use_counter_doc_document_webkitexitfullscreen,
        use_counter_doc_document_webkitfullscreenelement,
        use_counter_doc_document_webkitfullscreenenabled,
        use_counter_doc_document_webkithidden,
        use_counter_doc_document_webkitisfullscreen,
        use_counter_doc_document_webkitvisibilitystate,
        use_counter_doc_document_xmlencoding,
        use_counter_doc_document_xmlstandalone,
        use_counter_doc_document_xmlversion,
        use_counter_doc_domparser_parsefromstring,
        use_counter_doc_element_attachshadow,
        use_counter_doc_element_computedstylemap,
        use_counter_doc_element_onmousewheel,
        use_counter_doc_element_releasecapture,
        use_counter_doc_element_releasepointercapture,
        use_counter_doc_element_scrollintoviewifneeded,
        use_counter_doc_element_setcapture,
        use_counter_doc_element_sethtml,
        use_counter_doc_element_setpointercapture,
        use_counter_doc_enumerate_devices_insec,
        use_counter_doc_enumerate_devices_unfocused,
        use_counter_doc_fe_blend,
        use_counter_doc_fe_color_matrix,
        use_counter_doc_fe_component_transfer,
        use_counter_doc_fe_composite,
        use_counter_doc_fe_convolve_matrix,
        use_counter_doc_fe_diffuse_lighting,
        use_counter_doc_fe_displacement_map,
        use_counter_doc_fe_flood,
        use_counter_doc_fe_gaussian_blur,
        use_counter_doc_fe_image,
        use_counter_doc_fe_merge,
        use_counter_doc_fe_morphology,
        use_counter_doc_fe_offset,
        use_counter_doc_fe_specular_lighting,
        use_counter_doc_fe_tile,
        use_counter_doc_fe_turbulence,
        use_counter_doc_filtered_cross_origin_iframe,
        use_counter_doc_get_user_media_insec,
        use_counter_doc_get_user_media_unfocused,
        use_counter_doc_htmlbuttonelement_popovertargetaction,
        use_counter_doc_htmlbuttonelement_popovertargetelement,
        use_counter_doc_htmldocument_named_getter_hit,
        use_counter_doc_htmlelement_attributestylemap,
        use_counter_doc_htmlelement_hidepopover,
        use_counter_doc_htmlelement_popover,
        use_counter_doc_htmlelement_showpopover,
        use_counter_doc_htmlelement_togglepopover,
        use_counter_doc_htmliframeelement_loading,
        use_counter_doc_htmlinputelement_capture,
        use_counter_doc_htmlinputelement_incremental,
        use_counter_doc_htmlinputelement_onsearch,
        use_counter_doc_htmlinputelement_popovertargetaction,
        use_counter_doc_htmlinputelement_popovertargetelement,
        use_counter_doc_htmlinputelement_webkitdirectory,
        use_counter_doc_htmlinputelement_webkitentries,
        use_counter_doc_htmlmediaelement_disableremoteplayback,
        use_counter_doc_htmlmediaelement_remote,
        use_counter_doc_htmlvideoelement_cancelvideoframecallback,
        use_counter_doc_htmlvideoelement_disablepictureinpicture,
        use_counter_doc_htmlvideoelement_onenterpictureinpicture,
        use_counter_doc_htmlvideoelement_onleavepictureinpicture,
        use_counter_doc_htmlvideoelement_playsinline,
        use_counter_doc_htmlvideoelement_requestpictureinpicture,
        use_counter_doc_htmlvideoelement_requestvideoframecallback,
        use_counter_doc_imagedata_colorspace,
        use_counter_doc_js_asmjs,
        use_counter_doc_js_late_weekday,
        use_counter_doc_js_wasm,
        use_counter_doc_location_ancestororigins,
        use_counter_doc_mediadevices_enumeratedevices,
        use_counter_doc_mediadevices_getdisplaymedia,
        use_counter_doc_mediadevices_getusermedia,
        use_counter_doc_mixed_content_not_upgraded_audio_failure,
        use_counter_doc_mixed_content_not_upgraded_audio_success,
        use_counter_doc_mixed_content_not_upgraded_image_failure,
        use_counter_doc_mixed_content_not_upgraded_image_success,
        use_counter_doc_mixed_content_not_upgraded_video_failure,
        use_counter_doc_mixed_content_not_upgraded_video_success,
        use_counter_doc_mixed_content_upgraded_audio_failure,
        use_counter_doc_mixed_content_upgraded_audio_success,
        use_counter_doc_mixed_content_upgraded_image_failure,
        use_counter_doc_mixed_content_upgraded_image_success,
        use_counter_doc_mixed_content_upgraded_video_failure,
        use_counter_doc_mixed_content_upgraded_video_success,
        use_counter_doc_moz_get_user_media_insec,
        use_counter_doc_navigator_canshare,
        use_counter_doc_navigator_clearappbadge,
        use_counter_doc_navigator_mozgetusermedia,
        use_counter_doc_navigator_setappbadge,
        use_counter_doc_navigator_share,
        use_counter_doc_navigator_useractivation,
        use_counter_doc_navigator_wakelock,
        use_counter_doc_onbounce,
        use_counter_doc_ondommousescroll,
        use_counter_doc_onfinish,
        use_counter_doc_onmozmousepixelscroll,
        use_counter_doc_onoverflow,
        use_counter_doc_onstart,
        use_counter_doc_onunderflow,
        use_counter_doc_percentage_stroke_width_in_svg,
        use_counter_doc_percentage_stroke_width_in_svgtext,
        use_counter_doc_private_browsing_caches_delete,
        use_counter_doc_private_browsing_caches_has,
        use_counter_doc_private_browsing_caches_keys,
        use_counter_doc_private_browsing_caches_match,
        use_counter_doc_private_browsing_caches_open,
        use_counter_doc_private_browsing_idbfactory_delete_database,
        use_counter_doc_private_browsing_idbfactory_open,
        use_counter_doc_private_browsing_navigator_service_worker,
        use_counter_doc_pushmanager_subscribe,
        use_counter_doc_pushsubscription_unsubscribe,
        use_counter_doc_range_createcontextualfragment,
        use_counter_doc_sanitizer_constructor,
        use_counter_doc_sanitizer_sanitize,
        use_counter_doc_scheduler_posttask,
        use_counter_doc_shadowroot_pictureinpictureelement,
        use_counter_doc_svgsvgelement_currentscale_getter,
        use_counter_doc_svgsvgelement_currentscale_setter,
        use_counter_doc_svgsvgelement_getelementbyid,
        use_counter_doc_window_absoluteorientationsensor,
        use_counter_doc_window_accelerometer,
        use_counter_doc_window_backgroundfetchmanager,
        use_counter_doc_window_backgroundfetchrecord,
        use_counter_doc_window_backgroundfetchregistration,
        use_counter_doc_window_beforeinstallpromptevent,
        use_counter_doc_window_bluetooth,
        use_counter_doc_window_bluetoothcharacteristicproperties,
        use_counter_doc_window_bluetoothdevice,
        use_counter_doc_window_bluetoothremotegattcharacteristic,
        use_counter_doc_window_bluetoothremotegattdescriptor,
        use_counter_doc_window_bluetoothremotegattserver,
        use_counter_doc_window_bluetoothremotegattservice,
        use_counter_doc_window_bluetoothuuid,
        use_counter_doc_window_canvascapturemediastreamtrack,
        use_counter_doc_window_chrome,
        use_counter_doc_window_clipboarditem,
        use_counter_doc_window_cssimagevalue,
        use_counter_doc_window_csskeywordvalue,
        use_counter_doc_window_cssmathclamp,
        use_counter_doc_window_cssmathinvert,
        use_counter_doc_window_cssmathmax,
        use_counter_doc_window_cssmathmin,
        use_counter_doc_window_cssmathnegate,
        use_counter_doc_window_cssmathproduct,
        use_counter_doc_window_cssmathsum,
        use_counter_doc_window_cssmathvalue,
        use_counter_doc_window_cssmatrixcomponent,
        use_counter_doc_window_cssnumericarray,
        use_counter_doc_window_cssnumericvalue,
        use_counter_doc_window_cssperspective,
        use_counter_doc_window_csspositionvalue,
        use_counter_doc_window_csspropertyrule,
        use_counter_doc_window_cssrotate,
        use_counter_doc_window_cssscale,
        use_counter_doc_window_cssskew,
        use_counter_doc_window_cssskewx,
        use_counter_doc_window_cssskewy,
        use_counter_doc_window_cssstylevalue,
        use_counter_doc_window_csstransformcomponent,
        use_counter_doc_window_csstransformvalue,
        use_counter_doc_window_csstranslate,
        use_counter_doc_window_cssunitvalue,
        use_counter_doc_window_cssunparsedvalue,
        use_counter_doc_window_cssvariablereferencevalue,
        use_counter_doc_window_defaultstatus,
        use_counter_doc_window_devicemotioneventacceleration,
        use_counter_doc_window_devicemotioneventrotationrate,
        use_counter_doc_window_domerror,
        use_counter_doc_window_encodedvideochunk,
        use_counter_doc_window_enterpictureinpictureevent,
        use_counter_doc_window_external,
        use_counter_doc_window_federatedcredential,
        use_counter_doc_window_gyroscope,
        use_counter_doc_window_htmlcontentelement,
        use_counter_doc_window_htmlshadowelement,
        use_counter_doc_window_imagecapture,
        use_counter_doc_window_inputdevicecapabilities,
        use_counter_doc_window_inputdeviceinfo,
        use_counter_doc_window_keyboard,
        use_counter_doc_window_keyboardlayoutmap,
        use_counter_doc_window_linearaccelerationsensor,
        use_counter_doc_window_mediasettingsrange,
        use_counter_doc_window_midiaccess,
        use_counter_doc_window_midiconnectionevent,
        use_counter_doc_window_midiinput,
        use_counter_doc_window_midiinputmap,
        use_counter_doc_window_midimessageevent,
        use_counter_doc_window_midioutput,
        use_counter_doc_window_midioutputmap,
        use_counter_doc_window_midiport,
        use_counter_doc_window_networkinformation,
        use_counter_doc_window_offscreenbuffering,
        use_counter_doc_window_onbeforeinstallprompt,
        use_counter_doc_window_oncancel,
        use_counter_doc_window_onmousewheel,
        use_counter_doc_window_onorientationchange,
        use_counter_doc_window_onsearch,
        use_counter_doc_window_onselectionchange,
        use_counter_doc_window_open_empty_url,
        use_counter_doc_window_opendatabase,
        use_counter_doc_window_orientation,
        use_counter_doc_window_orientationsensor,
        use_counter_doc_window_overconstrainederror,
        use_counter_doc_window_passwordcredential,
        use_counter_doc_window_paymentaddress,
        use_counter_doc_window_paymentinstruments,
        use_counter_doc_window_paymentmanager,
        use_counter_doc_window_paymentmethodchangeevent,
        use_counter_doc_window_paymentrequest,
        use_counter_doc_window_paymentrequestupdateevent,
        use_counter_doc_window_paymentresponse,
        use_counter_doc_window_performancelongtasktiming,
        use_counter_doc_window_photocapabilities,
        use_counter_doc_window_pictureinpictureevent,
        use_counter_doc_window_pictureinpicturewindow,
        use_counter_doc_window_presentation,
        use_counter_doc_window_presentationavailability,
        use_counter_doc_window_presentationconnection,
        use_counter_doc_window_presentationconnectionavailableevent,
        use_counter_doc_window_presentationconnectioncloseevent,
        use_counter_doc_window_presentationconnectionlist,
        use_counter_doc_window_presentationreceiver,
        use_counter_doc_window_presentationrequest,
        use_counter_doc_window_relativeorientationsensor,
        use_counter_doc_window_remoteplayback,
        use_counter_doc_window_report,
        use_counter_doc_window_reportbody,
        use_counter_doc_window_reportingobserver,
        use_counter_doc_window_rtcerror,
        use_counter_doc_window_rtcerrorevent,
        use_counter_doc_window_rtcicetransport,
        use_counter_doc_window_rtcpeerconnectioniceerrorevent,
        use_counter_doc_window_sensor,
        use_counter_doc_window_sensorerrorevent,
        use_counter_doc_window_sidebar_getter,
        use_counter_doc_window_sidebar_setter,
        use_counter_doc_window_speechrecognitionalternative,
        use_counter_doc_window_speechrecognitionresult,
        use_counter_doc_window_speechrecognitionresultlist,
        use_counter_doc_window_stylemedia,
        use_counter_doc_window_stylepropertymap,
        use_counter_doc_window_stylepropertymapreadonly,
        use_counter_doc_window_svgdiscardelement,
        use_counter_doc_window_syncmanager,
        use_counter_doc_window_taskattributiontiming,
        use_counter_doc_window_textevent,
        use_counter_doc_window_touch,
        use_counter_doc_window_touchevent,
        use_counter_doc_window_touchlist,
        use_counter_doc_window_usb,
        use_counter_doc_window_usbalternateinterface,
        use_counter_doc_window_usbconfiguration,
        use_counter_doc_window_usbconnectionevent,
        use_counter_doc_window_usbdevice,
        use_counter_doc_window_usbendpoint,
        use_counter_doc_window_usbinterface,
        use_counter_doc_window_usbintransferresult,
        use_counter_doc_window_usbisochronousintransferpacket,
        use_counter_doc_window_usbisochronousintransferresult,
        use_counter_doc_window_usbisochronousouttransferpacket,
        use_counter_doc_window_usbisochronousouttransferresult,
        use_counter_doc_window_usbouttransferresult,
        use_counter_doc_window_useractivation,
        use_counter_doc_window_videocolorspace,
        use_counter_doc_window_videodecoder,
        use_counter_doc_window_videoencoder,
        use_counter_doc_window_videoframe,
        use_counter_doc_window_wakelock,
        use_counter_doc_window_wakelocksentinel,
        use_counter_doc_window_webkitcancelanimationframe,
        use_counter_doc_window_webkitmediastream,
        use_counter_doc_window_webkitmutationobserver,
        use_counter_doc_window_webkitrequestanimationframe,
        use_counter_doc_window_webkitrequestfilesystem,
        use_counter_doc_window_webkitresolvelocalfilesystemurl,
        use_counter_doc_window_webkitrtcpeerconnection,
        use_counter_doc_window_webkitspeechgrammar,
        use_counter_doc_window_webkitspeechgrammarlist,
        use_counter_doc_window_webkitspeechrecognition,
        use_counter_doc_window_webkitspeechrecognitionerror,
        use_counter_doc_window_webkitspeechrecognitionevent,
        use_counter_doc_window_webkitstorageinfo,
        use_counter_doc_workernavigator_permissions,
        use_counter_doc_wr_filter_fallback,
        use_counter_doc_xslstylesheet,
        use_counter_doc_xsltprocessor_constructor,
        use_counter_doc_you_tube_flash_embed,
        use_counter_page_clipboard_read,
        use_counter_page_clipboard_readtext,
        use_counter_page_clipboard_write,
        use_counter_page_console_assert,
        use_counter_page_console_clear,
        use_counter_page_console_count,
        use_counter_page_console_countreset,
        use_counter_page_console_debug,
        use_counter_page_console_dir,
        use_counter_page_console_dirxml,
        use_counter_page_console_error,
        use_counter_page_console_exception,
        use_counter_page_console_group,
        use_counter_page_console_groupcollapsed,
        use_counter_page_console_groupend,
        use_counter_page_console_info,
        use_counter_page_console_log,
        use_counter_page_console_profile,
        use_counter_page_console_profileend,
        use_counter_page_console_table,
        use_counter_page_console_time,
        use_counter_page_console_timeend,
        use_counter_page_console_timelog,
        use_counter_page_console_timestamp,
        use_counter_page_console_trace,
        use_counter_page_console_warn,
        use_counter_page_customelementregistry_define,
        use_counter_page_customized_builtin,
        use_counter_page_datatransfer_addelement,
        use_counter_page_datatransfer_mozcleardataat,
        use_counter_page_datatransfer_mozcursor_getter,
        use_counter_page_datatransfer_mozcursor_setter,
        use_counter_page_datatransfer_mozgetdataat,
        use_counter_page_datatransfer_mozitemcount_getter,
        use_counter_page_datatransfer_mozitemcount_setter,
        use_counter_page_datatransfer_mozsetdataat,
        use_counter_page_datatransfer_mozsourcenode_getter,
        use_counter_page_datatransfer_mozsourcenode_setter,
        use_counter_page_datatransfer_moztypesat,
        use_counter_page_datatransfer_mozusercancelled_getter,
        use_counter_page_datatransfer_mozusercancelled_setter,
        use_counter_page_datatransferitem_webkitgetasentry,
        use_counter_page_document_caretrangefrompoint,
        use_counter_page_document_exec_command_content_read_only,
        use_counter_page_document_exitpictureinpicture,
        use_counter_page_document_featurepolicy,
        use_counter_page_document_mozsetimageelement,
        use_counter_page_document_onbeforecopy,
        use_counter_page_document_onbeforecut,
        use_counter_page_document_onbeforepaste,
        use_counter_page_document_oncancel,
        use_counter_page_document_onfreeze,
        use_counter_page_document_onmousewheel,
        use_counter_page_document_onresume,
        use_counter_page_document_onsearch,
        use_counter_page_document_onwebkitfullscreenchange,
        use_counter_page_document_onwebkitfullscreenerror,
        use_counter_page_document_open,
        use_counter_page_document_pictureinpictureelement,
        use_counter_page_document_pictureinpictureenabled,
        use_counter_page_document_query_command_state_or_value_content_read_only,
        use_counter_page_document_query_command_state_or_value_insert_br_on_return,
        use_counter_page_document_query_command_supported_or_enabled_content_read_only,
        use_counter_page_document_query_command_supported_or_enabled_insert_br_on_return,
        use_counter_page_document_registerelement,
        use_counter_page_document_wasdiscarded,
        use_counter_page_document_webkitcancelfullscreen,
        use_counter_page_document_webkitcurrentfullscreenelement,
        use_counter_page_document_webkitexitfullscreen,
        use_counter_page_document_webkitfullscreenelement,
        use_counter_page_document_webkitfullscreenenabled,
        use_counter_page_document_webkithidden,
        use_counter_page_document_webkitisfullscreen,
        use_counter_page_document_webkitvisibilitystate,
        use_counter_page_document_xmlencoding,
        use_counter_page_document_xmlstandalone,
        use_counter_page_document_xmlversion,
        use_counter_page_domparser_parsefromstring,
        use_counter_page_element_attachshadow,
        use_counter_page_element_computedstylemap,
        use_counter_page_element_onmousewheel,
        use_counter_page_element_releasecapture,
        use_counter_page_element_releasepointercapture,
        use_counter_page_element_scrollintoviewifneeded,
        use_counter_page_element_setcapture,
        use_counter_page_element_sethtml,
        use_counter_page_element_setpointercapture,
        use_counter_page_enumerate_devices_insec,
        use_counter_page_enumerate_devices_unfocused,
        use_counter_page_fe_blend,
        use_counter_page_fe_color_matrix,
        use_counter_page_fe_component_transfer,
        use_counter_page_fe_composite,
        use_counter_page_fe_convolve_matrix,
        use_counter_page_fe_diffuse_lighting,
        use_counter_page_fe_displacement_map,
        use_counter_page_fe_flood,
        use_counter_page_fe_gaussian_blur,
        use_counter_page_fe_image,
        use_counter_page_fe_merge,
        use_counter_page_fe_morphology,
        use_counter_page_fe_offset,
        use_counter_page_fe_specular_lighting,
        use_counter_page_fe_tile,
        use_counter_page_fe_turbulence,
        use_counter_page_filtered_cross_origin_iframe,
        use_counter_page_get_user_media_insec,
        use_counter_page_get_user_media_unfocused,
        use_counter_page_htmlbuttonelement_popovertargetaction,
        use_counter_page_htmlbuttonelement_popovertargetelement,
        use_counter_page_htmldocument_named_getter_hit,
        use_counter_page_htmlelement_attributestylemap,
        use_counter_page_htmlelement_hidepopover,
        use_counter_page_htmlelement_popover,
        use_counter_page_htmlelement_showpopover,
        use_counter_page_htmlelement_togglepopover,
        use_counter_page_htmliframeelement_loading,
        use_counter_page_htmlinputelement_capture,
        use_counter_page_htmlinputelement_incremental,
        use_counter_page_htmlinputelement_onsearch,
        use_counter_page_htmlinputelement_popovertargetaction,
        use_counter_page_htmlinputelement_popovertargetelement,
        use_counter_page_htmlinputelement_webkitdirectory,
        use_counter_page_htmlinputelement_webkitentries,
        use_counter_page_htmlmediaelement_disableremoteplayback,
        use_counter_page_htmlmediaelement_remote,
        use_counter_page_htmlvideoelement_cancelvideoframecallback,
        use_counter_page_htmlvideoelement_disablepictureinpicture,
        use_counter_page_htmlvideoelement_onenterpictureinpicture,
        use_counter_page_htmlvideoelement_onleavepictureinpicture,
        use_counter_page_htmlvideoelement_playsinline,
        use_counter_page_htmlvideoelement_requestpictureinpicture,
        use_counter_page_htmlvideoelement_requestvideoframecallback,
        use_counter_page_imagedata_colorspace,
        use_counter_page_js_asmjs,
        use_counter_page_js_late_weekday,
        use_counter_page_js_wasm,
        use_counter_page_location_ancestororigins,
        use_counter_page_mediadevices_enumeratedevices,
        use_counter_page_mediadevices_getdisplaymedia,
        use_counter_page_mediadevices_getusermedia,
        use_counter_page_mixed_content_not_upgraded_audio_failure,
        use_counter_page_mixed_content_not_upgraded_audio_success,
        use_counter_page_mixed_content_not_upgraded_image_failure,
        use_counter_page_mixed_content_not_upgraded_image_success,
        use_counter_page_mixed_content_not_upgraded_video_failure,
        use_counter_page_mixed_content_not_upgraded_video_success,
        use_counter_page_mixed_content_upgraded_audio_failure,
        use_counter_page_mixed_content_upgraded_audio_success,
        use_counter_page_mixed_content_upgraded_image_failure,
        use_counter_page_mixed_content_upgraded_image_success,
        use_counter_page_mixed_content_upgraded_video_failure,
        use_counter_page_mixed_content_upgraded_video_success,
        use_counter_page_moz_get_user_media_insec,
        use_counter_page_navigator_canshare,
        use_counter_page_navigator_clearappbadge,
        use_counter_page_navigator_mozgetusermedia,
        use_counter_page_navigator_setappbadge,
        use_counter_page_navigator_share,
        use_counter_page_navigator_useractivation,
        use_counter_page_navigator_wakelock,
        use_counter_page_onbounce,
        use_counter_page_ondommousescroll,
        use_counter_page_onfinish,
        use_counter_page_onmozmousepixelscroll,
        use_counter_page_onoverflow,
        use_counter_page_onstart,
        use_counter_page_onunderflow,
        use_counter_page_percentage_stroke_width_in_svg,
        use_counter_page_percentage_stroke_width_in_svgtext,
        use_counter_page_private_browsing_caches_delete,
        use_counter_page_private_browsing_caches_has,
        use_counter_page_private_browsing_caches_keys,
        use_counter_page_private_browsing_caches_match,
        use_counter_page_private_browsing_caches_open,
        use_counter_page_private_browsing_idbfactory_delete_database,
        use_counter_page_private_browsing_idbfactory_open,
        use_counter_page_private_browsing_navigator_service_worker,
        use_counter_page_pushmanager_subscribe,
        use_counter_page_pushsubscription_unsubscribe,
        use_counter_page_range_createcontextualfragment,
        use_counter_page_sanitizer_constructor,
        use_counter_page_sanitizer_sanitize,
        use_counter_page_scheduler_posttask,
        use_counter_page_shadowroot_pictureinpictureelement,
        use_counter_page_svgsvgelement_currentscale_getter,
        use_counter_page_svgsvgelement_currentscale_setter,
        use_counter_page_svgsvgelement_getelementbyid,
        use_counter_page_window_absoluteorientationsensor,
        use_counter_page_window_accelerometer,
        use_counter_page_window_backgroundfetchmanager,
        use_counter_page_window_backgroundfetchrecord,
        use_counter_page_window_backgroundfetchregistration,
        use_counter_page_window_beforeinstallpromptevent,
        use_counter_page_window_bluetooth,
        use_counter_page_window_bluetoothcharacteristicproperties,
        use_counter_page_window_bluetoothdevice,
        use_counter_page_window_bluetoothremotegattcharacteristic,
        use_counter_page_window_bluetoothremotegattdescriptor,
        use_counter_page_window_bluetoothremotegattserver,
        use_counter_page_window_bluetoothremotegattservice,
        use_counter_page_window_bluetoothuuid,
        use_counter_page_window_canvascapturemediastreamtrack,
        use_counter_page_window_chrome,
        use_counter_page_window_clipboarditem,
        use_counter_page_window_cssimagevalue,
        use_counter_page_window_csskeywordvalue,
        use_counter_page_window_cssmathclamp,
        use_counter_page_window_cssmathinvert,
        use_counter_page_window_cssmathmax,
        use_counter_page_window_cssmathmin,
        use_counter_page_window_cssmathnegate,
        use_counter_page_window_cssmathproduct,
        use_counter_page_window_cssmathsum,
        use_counter_page_window_cssmathvalue,
        use_counter_page_window_cssmatrixcomponent,
        use_counter_page_window_cssnumericarray,
        use_counter_page_window_cssnumericvalue,
        use_counter_page_window_cssperspective,
        use_counter_page_window_csspositionvalue,
        use_counter_page_window_csspropertyrule,
        use_counter_page_window_cssrotate,
        use_counter_page_window_cssscale,
        use_counter_page_window_cssskew,
        use_counter_page_window_cssskewx,
        use_counter_page_window_cssskewy,
        use_counter_page_window_cssstylevalue,
        use_counter_page_window_csstransformcomponent,
        use_counter_page_window_csstransformvalue,
        use_counter_page_window_csstranslate,
        use_counter_page_window_cssunitvalue,
        use_counter_page_window_cssunparsedvalue,
        use_counter_page_window_cssvariablereferencevalue,
        use_counter_page_window_defaultstatus,
        use_counter_page_window_devicemotioneventacceleration,
        use_counter_page_window_devicemotioneventrotationrate,
        use_counter_page_window_domerror,
        use_counter_page_window_encodedvideochunk,
        use_counter_page_window_enterpictureinpictureevent,
        use_counter_page_window_external,
        use_counter_page_window_federatedcredential,
        use_counter_page_window_gyroscope,
        use_counter_page_window_htmlcontentelement,
        use_counter_page_window_htmlshadowelement,
        use_counter_page_window_imagecapture,
        use_counter_page_window_inputdevicecapabilities,
        use_counter_page_window_inputdeviceinfo,
        use_counter_page_window_keyboard,
        use_counter_page_window_keyboardlayoutmap,
        use_counter_page_window_linearaccelerationsensor,
        use_counter_page_window_mediasettingsrange,
        use_counter_page_window_midiaccess,
        use_counter_page_window_midiconnectionevent,
        use_counter_page_window_midiinput,
        use_counter_page_window_midiinputmap,
        use_counter_page_window_midimessageevent,
        use_counter_page_window_midioutput,
        use_counter_page_window_midioutputmap,
        use_counter_page_window_midiport,
        use_counter_page_window_networkinformation,
        use_counter_page_window_offscreenbuffering,
        use_counter_page_window_onbeforeinstallprompt,
        use_counter_page_window_oncancel,
        use_counter_page_window_onmousewheel,
        use_counter_page_window_onorientationchange,
        use_counter_page_window_onsearch,
        use_counter_page_window_onselectionchange,
        use_counter_page_window_open_empty_url,
        use_counter_page_window_opendatabase,
        use_counter_page_window_orientation,
        use_counter_page_window_orientationsensor,
        use_counter_page_window_overconstrainederror,
        use_counter_page_window_passwordcredential,
        use_counter_page_window_paymentaddress,
        use_counter_page_window_paymentinstruments,
        use_counter_page_window_paymentmanager,
        use_counter_page_window_paymentmethodchangeevent,
        use_counter_page_window_paymentrequest,
        use_counter_page_window_paymentrequestupdateevent,
        use_counter_page_window_paymentresponse,
        use_counter_page_window_performancelongtasktiming,
        use_counter_page_window_photocapabilities,
        use_counter_page_window_pictureinpictureevent,
        use_counter_page_window_pictureinpicturewindow,
        use_counter_page_window_presentation,
        use_counter_page_window_presentationavailability,
        use_counter_page_window_presentationconnection,
        use_counter_page_window_presentationconnectionavailableevent,
        use_counter_page_window_presentationconnectioncloseevent,
        use_counter_page_window_presentationconnectionlist,
        use_counter_page_window_presentationreceiver,
        use_counter_page_window_presentationrequest,
        use_counter_page_window_relativeorientationsensor,
        use_counter_page_window_remoteplayback,
        use_counter_page_window_report,
        use_counter_page_window_reportbody,
        use_counter_page_window_reportingobserver,
        use_counter_page_window_rtcerror,
        use_counter_page_window_rtcerrorevent,
        use_counter_page_window_rtcicetransport,
        use_counter_page_window_rtcpeerconnectioniceerrorevent,
        use_counter_page_window_sensor,
        use_counter_page_window_sensorerrorevent,
        use_counter_page_window_sidebar_getter,
        use_counter_page_window_sidebar_setter,
        use_counter_page_window_speechrecognitionalternative,
        use_counter_page_window_speechrecognitionresult,
        use_counter_page_window_speechrecognitionresultlist,
        use_counter_page_window_stylemedia,
        use_counter_page_window_stylepropertymap,
        use_counter_page_window_stylepropertymapreadonly,
        use_counter_page_window_svgdiscardelement,
        use_counter_page_window_syncmanager,
        use_counter_page_window_taskattributiontiming,
        use_counter_page_window_textevent,
        use_counter_page_window_touch,
        use_counter_page_window_touchevent,
        use_counter_page_window_touchlist,
        use_counter_page_window_usb,
        use_counter_page_window_usbalternateinterface,
        use_counter_page_window_usbconfiguration,
        use_counter_page_window_usbconnectionevent,
        use_counter_page_window_usbdevice,
        use_counter_page_window_usbendpoint,
        use_counter_page_window_usbinterface,
        use_counter_page_window_usbintransferresult,
        use_counter_page_window_usbisochronousintransferpacket,
        use_counter_page_window_usbisochronousintransferresult,
        use_counter_page_window_usbisochronousouttransferpacket,
        use_counter_page_window_usbisochronousouttransferresult,
        use_counter_page_window_usbouttransferresult,
        use_counter_page_window_useractivation,
        use_counter_page_window_videocolorspace,
        use_counter_page_window_videodecoder,
        use_counter_page_window_videoencoder,
        use_counter_page_window_videoframe,
        use_counter_page_window_wakelock,
        use_counter_page_window_wakelocksentinel,
        use_counter_page_window_webkitcancelanimationframe,
        use_counter_page_window_webkitmediastream,
        use_counter_page_window_webkitmutationobserver,
        use_counter_page_window_webkitrequestanimationframe,
        use_counter_page_window_webkitrequestfilesystem,
        use_counter_page_window_webkitresolvelocalfilesystemurl,
        use_counter_page_window_webkitrtcpeerconnection,
        use_counter_page_window_webkitspeechgrammar,
        use_counter_page_window_webkitspeechgrammarlist,
        use_counter_page_window_webkitspeechrecognition,
        use_counter_page_window_webkitspeechrecognitionerror,
        use_counter_page_window_webkitspeechrecognitionevent,
        use_counter_page_window_webkitstorageinfo,
        use_counter_page_workernavigator_permissions,
        use_counter_page_wr_filter_fallback,
        use_counter_page_xslstylesheet,
        use_counter_page_xsltprocessor_constructor,
        use_counter_page_you_tube_flash_embed,
        use_counter_worker_dedicated_console_assert,
        use_counter_worker_dedicated_console_clear,
        use_counter_worker_dedicated_console_count,
        use_counter_worker_dedicated_console_countreset,
        use_counter_worker_dedicated_console_debug,
        use_counter_worker_dedicated_console_dir,
        use_counter_worker_dedicated_console_dirxml,
        use_counter_worker_dedicated_console_error,
        use_counter_worker_dedicated_console_exception,
        use_counter_worker_dedicated_console_group,
        use_counter_worker_dedicated_console_groupcollapsed,
        use_counter_worker_dedicated_console_groupend,
        use_counter_worker_dedicated_console_info,
        use_counter_worker_dedicated_console_log,
        use_counter_worker_dedicated_console_profile,
        use_counter_worker_dedicated_console_profileend,
        use_counter_worker_dedicated_console_table,
        use_counter_worker_dedicated_console_time,
        use_counter_worker_dedicated_console_timeend,
        use_counter_worker_dedicated_console_timelog,
        use_counter_worker_dedicated_console_timestamp,
        use_counter_worker_dedicated_console_trace,
        use_counter_worker_dedicated_console_warn,
        use_counter_worker_dedicated_private_browsing_caches_delete,
        use_counter_worker_dedicated_private_browsing_caches_has,
        use_counter_worker_dedicated_private_browsing_caches_keys,
        use_counter_worker_dedicated_private_browsing_caches_match,
        use_counter_worker_dedicated_private_browsing_caches_open,
        use_counter_worker_dedicated_private_browsing_idbfactory_delete_database,
        use_counter_worker_dedicated_private_browsing_idbfactory_open,
        use_counter_worker_dedicated_pushmanager_subscribe,
        use_counter_worker_dedicated_pushsubscription_unsubscribe,
        use_counter_worker_dedicated_scheduler_posttask,
        use_counter_worker_service_console_assert,
        use_counter_worker_service_console_clear,
        use_counter_worker_service_console_count,
        use_counter_worker_service_console_countreset,
        use_counter_worker_service_console_debug,
        use_counter_worker_service_console_dir,
        use_counter_worker_service_console_dirxml,
        use_counter_worker_service_console_error,
        use_counter_worker_service_console_exception,
        use_counter_worker_service_console_group,
        use_counter_worker_service_console_groupcollapsed,
        use_counter_worker_service_console_groupend,
        use_counter_worker_service_console_info,
        use_counter_worker_service_console_log,
        use_counter_worker_service_console_profile,
        use_counter_worker_service_console_profileend,
        use_counter_worker_service_console_table,
        use_counter_worker_service_console_time,
        use_counter_worker_service_console_timeend,
        use_counter_worker_service_console_timelog,
        use_counter_worker_service_console_timestamp,
        use_counter_worker_service_console_trace,
        use_counter_worker_service_console_warn,
        use_counter_worker_service_private_browsing_caches_delete,
        use_counter_worker_service_private_browsing_caches_has,
        use_counter_worker_service_private_browsing_caches_keys,
        use_counter_worker_service_private_browsing_caches_match,
        use_counter_worker_service_private_browsing_caches_open,
        use_counter_worker_service_private_browsing_idbfactory_delete_database,
        use_counter_worker_service_private_browsing_idbfactory_open,
        use_counter_worker_service_pushmanager_subscribe,
        use_counter_worker_service_pushsubscription_unsubscribe,
        use_counter_worker_service_scheduler_posttask,
        use_counter_worker_shared_console_assert,
        use_counter_worker_shared_console_clear,
        use_counter_worker_shared_console_count,
        use_counter_worker_shared_console_countreset,
        use_counter_worker_shared_console_debug,
        use_counter_worker_shared_console_dir,
        use_counter_worker_shared_console_dirxml,
        use_counter_worker_shared_console_error,
        use_counter_worker_shared_console_exception,
        use_counter_worker_shared_console_group,
        use_counter_worker_shared_console_groupcollapsed,
        use_counter_worker_shared_console_groupend,
        use_counter_worker_shared_console_info,
        use_counter_worker_shared_console_log,
        use_counter_worker_shared_console_profile,
        use_counter_worker_shared_console_profileend,
        use_counter_worker_shared_console_table,
        use_counter_worker_shared_console_time,
        use_counter_worker_shared_console_timeend,
        use_counter_worker_shared_console_timelog,
        use_counter_worker_shared_console_timestamp,
        use_counter_worker_shared_console_trace,
        use_counter_worker_shared_console_warn,
        use_counter_worker_shared_private_browsing_caches_delete,
        use_counter_worker_shared_private_browsing_caches_has,
        use_counter_worker_shared_private_browsing_caches_keys,
        use_counter_worker_shared_private_browsing_caches_match,
        use_counter_worker_shared_private_browsing_caches_open,
        use_counter_worker_shared_private_browsing_idbfactory_delete_database,
        use_counter_worker_shared_private_browsing_idbfactory_open,
        use_counter_worker_shared_pushmanager_subscribe,
        use_counter_worker_shared_pushsubscription_unsubscribe,
        use_counter_worker_shared_scheduler_posttask
      )
    )
),
firefox_desktop_staging AS (
  SELECT
    submission_date,
    version_major,
    geo_country,
    platform,
    use_counter_content_documents_destroyed,
    use_counter_top_level_content_documents_destroyed,
    use_counter_service_workers_destroyed,
    use_counter_shared_workers_destroyed,
    use_counter_dedicated_workers_destroyed,
    metric,
    cnt,
    CASE
      WHEN metric LIKE 'use_counter_css_doc_%'
        THEN SAFE_DIVIDE(cnt, use_counter_content_documents_destroyed)
      ELSE NULL
    END AS doc_rate,
    CASE
      WHEN metric LIKE 'use_counter_css_page_%'
        THEN SAFE_DIVIDE(cnt, use_counter_top_level_content_documents_destroyed)
      ELSE NULL
    END AS page_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_service_%'
        THEN SAFE_DIVIDE(cnt, use_counter_service_workers_destroyed)
      ELSE NULL
    END AS service_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_shared_%'
        THEN SAFE_DIVIDE(cnt, use_counter_shared_workers_destroyed)
      ELSE NULL
    END AS shared_rate,
    CASE
      WHEN metric LIKE 'use_counter_worker_dedicated_%'
        THEN SAFE_DIVIDE(cnt, use_counter_dedicated_workers_destroyed)
      ELSE NULL
    END AS dedicated_rate
  FROM
    firefox_desktop_pivoted_raw
)

SELECT
  submission_date,
  SAFE_CAST(version_major AS INT64) AS version_major,
  geo_country,
  platform,
  use_counter_content_documents_destroyed,
  use_counter_top_level_content_documents_destroyed,
  use_counter_service_workers_destroyed,
  use_counter_shared_workers_destroyed,
  use_counter_dedicated_workers_destroyed,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(metric, 'use_counter_css_doc_', 'use.counter.css.doc.'),
          'use_counter_css_page_',
          'use.counter.css.page.'
        ),
        'use_counter_worker_dedicated',
        'use.counter.worker.dedicated.'
      ),
      'use_counter_worker_shared_',
      'use.counter.worker.shared.'
    ),
    'use_counter_worker_service_',
    'use.counter.worker.service.'
  ) AS metric,
  cnt,
  COALESCE(doc_rate, page_rate, service_rate, shared_rate, dedicated_rate) AS rate
FROM
  firefox_desktop_staging
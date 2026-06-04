# UDF: Grid Type Determination for Firefox New Tab Layout

This User-Defined Function (UDF) determines the appropriate grid layout type for Firefox new tab pages based on various input parameters including whether the new tab is section-based, the browser version, and experiment enrollment metadata.

## 📥 Input Parameters

| Name          | Type          | Description                                                                                                                                                                         |
|---------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `is_section`  | BOOLEAN       | Indicates whether the new tab impressions are based on section layout.                                                                                                              |
| `app_version` | INTEGER       | Represents the Firefox major version number.                                                                                                                                        |
| `experiment`  | ARRAY<STRUCT> | An array of experimental configurations assigned to the client. Each entry is a `STRUCT<key STRING, value STRUCT<branch STRING, extra STRUCT<type STRING, enrollment_id STRING>>>`. |

## 📌 Evaluation Criteria

The UDF returns one of four possible values based on the following conditions:

### `NOVA`
Returned when:
- `app_version >= 151`

### `SECTION_GRID`
Returned when:
- `app_version < 151 AND is_section = TRUE`

### `NEW_GRID`
Returned when:
- `is_section = FALSE` AND
- One of the following holds:
  - `151 > app_version >= 136`
  - `app_version < 136` AND `experiment.key` matches one of the following:
    - `default-ui-experiment`
    - `new-tab-layout-variant-b-and-content-card-ui-rollout-global`
    - `new-tab-layout-variant-b-and-content-card-ui-release-rollout-global-v2`
    - `default-ui-experiment-logo-in-corner-rollout`

### `OLD_GRID`
Returned when:
- `is_section = FALSE` AND
- `app_version < 136` AND
- No matching experiment from the above list is found

## 🏁 Return Values

- `NOVA`
- `SECTION_GRID`
- `NEW_GRID`
- `OLD_GRID`

## 🛠 Example Usage

```sql
SELECT determine_grid_layout(
  TRUE,
  135,
  ARRAY[
    STRUCT('default-ui-experiment', STRUCT('branch1', STRUCT('type1', 'enroll123')))
  ]
) AS grid_type;
-- Returns: SECTION_GRID

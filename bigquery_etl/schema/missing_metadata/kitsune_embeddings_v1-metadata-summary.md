# Schema Enricher -- Metadata Summary

**Table:** `moz-fx-data-shared-prod.customer_experience_derived.kitsune_embeddings_v1`
**Run date:** 2026-04-02

---

## Phase Results

| Phase (Step) | Status | Notes |
|---|---|---|
| Discovery (Step 0) | Done | 1 base schema match (global.yaml); no app or dataset schema |
| Categorization (Step 1) | Done | 1 covered by base schema, 0 retained, 17 flagged for description fill |
| Description Fill (Step 2) | Done | 1 from base schema (locale), 17 from query context (priorities 5-6) |
| Column Validation (Step 3) | Skipped | Authentication error prevented dry-run validation |
| Quality Check (Step 4) | Done | 28 descriptions pass (18 top-level + 10 nested) |
| Write (Step 5) | Done | schema.yaml written |
| Verification (Step 5) | Done | All 28 fields confirmed with non-empty descriptions |

---

## Base Schema Coverage

| Column | Matched Field | Source File | Source Status | Alias Used |
|---|---|---|---|---|
| locale | locale | global.yaml | live | no |
| creation_date | -- | (none) | non-base-schema | -- |
| title | -- | (none) | non-base-schema | -- |
| content | -- | (none) | non-base-schema | -- |
| product | -- | (none) | non-base-schema | -- |
| topic | -- | (none) | non-base-schema | -- |
| type | -- | (none) | non-base-schema | -- |
| is_solved | -- | (none) | non-base-schema | -- |
| votes | -- | (none) | non-base-schema | -- |
| summary_generated | -- | (none) | non-base-schema | -- |
| category_generated | -- | (none) | non-base-schema | -- |
| language_generated | -- | (none) | non-base-schema | -- |
| entities_generated | -- | (none) | non-base-schema | -- |
| topics_generated | -- | (none) | non-base-schema | -- |
| sentiment_score | -- | (none) | non-base-schema | -- |
| embedding | -- | (none) | non-base-schema | -- |
| recency_score | -- | (none) | non-base-schema | -- |
| metadata | -- | (none) | non-base-schema | -- |

---

## Non-Base-Schema Descriptions

| Column | Description | Inference Basis | Recommended Base Schema |
|---|---|---|---|
| creation_date | Timestamp when the Kitsune support question was originally created. | query context | customer_experience_derived.yaml |
| title | Title of the support question as submitted by the user on Kitsune (SUMO). | query context | customer_experience_derived.yaml |
| content | Full text body of the support question as submitted by the user on Kitsune (SUMO). | query context | customer_experience_derived.yaml |
| product | Mozilla product that the Kitsune support question is associated with. | query context | customer_experience_derived.yaml |
| topic | Topic category assigned to the support question on Kitsune (SUMO). | query context | customer_experience_derived.yaml |
| type | Type of the Kitsune content record; currently always set to "question". | query context | customer_experience_derived.yaml |
| is_solved | Whether the support question has been marked as solved on Kitsune (SUMO). | query context | customer_experience_derived.yaml |
| votes | Number of votes the support question has received from Kitsune users. | query context | customer_experience_derived.yaml |
| summary_generated | AI-generated summary of the support question, max 20 words. | query context | customer_experience_derived.yaml |
| category_generated | AI-generated classification label for the support question. | query context | customer_experience_derived.yaml |
| language_generated | AI-detected ISO language code for the support question content. | query context | customer_experience_derived.yaml |
| entities_generated | Array of up to five AI-extracted normalized entities. | query context | customer_experience_derived.yaml |
| topics_generated | Array of up to three AI-generated normalized topic labels. | query context | customer_experience_derived.yaml |
| sentiment_score | AI-generated sentiment score from -1.0 to 1.0. | query context | customer_experience_derived.yaml |
| embedding | Dense vector embedding from gemini-embedding-001. | query context | customer_experience_derived.yaml |
| recency_score | Exponential decay score measuring content freshness. | query context | customer_experience_derived.yaml |
| metadata | Technical metadata about the AI analysis pipeline. | query context | customer_experience_derived.yaml |

---

## Column Validation Results

Column validation skipped -- authentication error prevented `bqetl query schema update` from running.

- **Columns added (missing from schema):** none (schema.yaml pre-existed)
- **Columns removed (not in query):** none
- **Type corrections:** none

---

## Output Files

- `sql/moz-fx-data-shared-prod/customer_experience_derived/kitsune_embeddings_v1/schema.yaml` -- enriched with all 28 descriptions (18 top-level + 10 nested)
- `bigquery_etl/schema/missing_metadata/kitsune_embeddings_v1_missing_metadata.yaml` -- 17 non-base-schema descriptions (query context)
- `bigquery_etl/schema/missing_metadata/kitsune_embeddings_v1-metadata-summary.md` -- this summary file

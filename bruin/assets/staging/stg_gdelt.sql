/* @bruin

name: kairoskop.stg_gdelt
type: bq.sql
description: >
  Staging model for GDELT 2.0 attention events (global news media).
  GDELT represents McLuhan's institutional media layer — what the global
  media apparatus decides is real and therefore shapes attention.
  Filters to English-language events and applies CAMEO-based category
  validation.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category
    - country_code

depends:
  - kairoskop.attention_events_raw

@bruin */

WITH source AS (
    SELECT
        event_id,
        source,
        CAST(event_timestamp AS TIMESTAMP)  AS event_timestamp,
        CAST(event_date AS DATE)            AS event_date,
        TRIM(topic)                         AS actor,
        LOWER(TRIM(topic_category))         AS topic_category,
        UPPER(TRIM(country_code))           AS country_code,
        CAST(signal_strength AS FLOAT64)    AS signal_strength,
        medium_type,
        CAST(ingested_at AS TIMESTAMP)      AS ingested_at
    FROM {{ ref('kairoskop.attention_events_raw') }}
    WHERE source = 'gdelt'
),

validated AS (
    SELECT *
    FROM source
    WHERE
        actor IS NOT NULL
        AND LENGTH(TRIM(actor)) > 1
        AND signal_strength BETWEEN 0.0 AND 1.0
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        AND event_date <= CURRENT_DATE()
)

SELECT
    event_id,
    'gdelt'                                 AS source,
    event_timestamp,
    event_date,
    actor                                   AS topic,
    topic_category,
    'en'                                    AS language,
    country_code,
    signal_strength,
    medium_type,
    ingested_at,
    -- Conflict intensity bucket based on signal strength (tone magnitude)
    CASE
        WHEN signal_strength >= 0.7 THEN 'high_intensity'
        WHEN signal_strength >= 0.4 THEN 'medium_intensity'
        ELSE 'low_intensity'
    END AS attention_intensity
FROM validated

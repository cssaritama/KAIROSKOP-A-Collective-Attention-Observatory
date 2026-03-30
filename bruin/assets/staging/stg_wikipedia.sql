/* @bruin

name: kairoskop.stg_wikipedia
type: bq.sql
description: >
  Staging model for Wikipedia attention events (pageviews + recent changes).
  Casts types, normalises column names, and filters out invalid rows.
  Both wiki_pageviews and wiki_changes sources are unified here — they share
  the same schema envelope from the Kafka producer.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category
    - medium_type

depends:
  - kairoskop.attention_events_raw

@bruin */

WITH source AS (
    SELECT
        event_id,
        source,
        CAST(event_timestamp AS TIMESTAMP)  AS event_timestamp,
        CAST(event_date AS DATE)            AS event_date,
        TRIM(topic)                         AS topic,
        LOWER(TRIM(topic_category))         AS topic_category,
        LOWER(TRIM(language))               AS language,
        UPPER(TRIM(country_code))           AS country_code,
        CAST(signal_strength AS FLOAT64)    AS signal_strength,
        LOWER(TRIM(medium_type))            AS medium_type,
        CAST(ingested_at AS TIMESTAMP)      AS ingested_at
    FROM {{ ref('kairoskop.attention_events_raw') }}
    WHERE source IN ('wiki_pageviews', 'wiki_changes')
),

validated AS (
    SELECT *
    FROM source
    WHERE
        -- Remove rows with no identifiable topic
        topic IS NOT NULL
        AND LENGTH(TRIM(topic)) > 2
        -- Remove rows with invalid signal strength
        AND signal_strength BETWEEN 0.0 AND 1.0
        -- Remove rows outside a reasonable date window (last 5 years)
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        AND event_date <= CURRENT_DATE()
)

SELECT
    event_id,
    source,
    event_timestamp,
    event_date,
    topic,
    topic_category,
    language,
    country_code,
    signal_strength,
    medium_type,
    ingested_at,
    -- Derived: edit magnitude bucket (for wiki_changes)
    CASE
        WHEN source = 'wiki_changes' AND signal_strength >= 0.8 THEN 'major_edit'
        WHEN source = 'wiki_changes' AND signal_strength >= 0.3 THEN 'moderate_edit'
        WHEN source = 'wiki_changes' THEN 'minor_edit'
        WHEN source = 'wiki_pageviews' AND signal_strength >= 0.7 THEN 'viral'
        WHEN source = 'wiki_pageviews' AND signal_strength >= 0.3 THEN 'trending'
        ELSE 'baseline'
    END AS attention_intensity
FROM validated

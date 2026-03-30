/* @bruin

name: kairoskop.fact_attention_events
type: bq.sql
description: >
  Central fact table for KAIROSKOP.  Unions all four staging models
  into a single analytical layer with a consistent schema.
  One row per attention event.  Used as the base for all metric models.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - source
    - topic_category

depends:
  - kairoskop.stg_wikipedia
  - kairoskop.stg_gdelt
  - kairoskop.stg_arxiv

@bruin */

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
    attention_intensity,
    ingested_at,

    -- Wilber consciousness level (computed in Spark enrichment layer,
    -- surfaced here as a first-class dimension for dashboard filtering)
    CASE
        WHEN source = 'arxiv'
             AND topic_category IN ('science', 'health', 'environment')
            THEN 'integral'
        WHEN source IN ('wiki_pageviews', 'wiki_changes')
             AND topic_category IN ('science', 'economics', 'environment')
            THEN 'worldcentric'
        WHEN source = 'gdelt'
             AND topic_category = 'conflict'
            THEN 'ethnocentric'
        ELSE 'worldcentric'
    END AS consciousness_level,

    -- McLuhan medium dominance dimension
    CASE
        WHEN source = 'arxiv'                                    THEN 'knowledge_led'
        WHEN source IN ('wiki_pageviews', 'wiki_changes')
             AND signal_strength >= 0.5                          THEN 'memory_led'
        WHEN source = 'gdelt' AND signal_strength >= 0.6        THEN 'media_led'
        ELSE 'ambient'
    END AS medium_dominance

FROM {{ ref('kairoskop.stg_wikipedia') }}

UNION ALL

SELECT
    event_id, source, event_timestamp, event_date, topic,
    topic_category, language, country_code, signal_strength,
    medium_type, attention_intensity, ingested_at,
    -- GDELT maps to ethnocentric for conflict, worldcentric otherwise
    CASE
        WHEN topic_category = 'conflict' THEN 'ethnocentric'
        ELSE 'worldcentric'
    END AS consciousness_level,
    CASE WHEN signal_strength >= 0.6 THEN 'media_led' ELSE 'ambient' END
FROM {{ ref('kairoskop.stg_gdelt') }}

UNION ALL

SELECT
    event_id, source, event_timestamp, event_date, topic,
    topic_category, language, country_code, signal_strength,
    medium_type, attention_intensity, ingested_at,
    'integral'       AS consciousness_level,
    'knowledge_led'  AS medium_dominance
FROM {{ ref('kairoskop.stg_arxiv') }}

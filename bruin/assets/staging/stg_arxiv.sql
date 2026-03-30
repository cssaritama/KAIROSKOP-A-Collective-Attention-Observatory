/* @bruin

name: kairoskop.stg_arxiv
type: bq.sql
description: >
  Staging model for arXiv academic paper attention signals.
  arXiv represents Teilhard de Chardin's formal knowledge layer of the
  noosphere — the growing edge of what humanity knows with rigour.
  Normalises categories, filters preprint noise, and computes a paper
  significance score.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category

depends:
  - kairoskop.attention_events_raw

@bruin */

WITH source AS (
    SELECT
        event_id,
        source,
        CAST(event_timestamp AS TIMESTAMP)  AS event_timestamp,
        CAST(event_date AS DATE)            AS event_date,
        TRIM(topic)                         AS paper_title,
        LOWER(TRIM(topic_category))         AS topic_category,
        CAST(signal_strength AS FLOAT64)    AS signal_strength,
        medium_type,
        CAST(ingested_at AS TIMESTAMP)      AS ingested_at
    FROM {{ ref('kairoskop.attention_events_raw') }}
    WHERE source = 'arxiv'
),

validated AS (
    SELECT *
    FROM source
    WHERE
        paper_title IS NOT NULL
        AND LENGTH(TRIM(paper_title)) > 5
        AND signal_strength BETWEEN 0.0 AND 1.0
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        AND event_date <= CURRENT_DATE()
        -- Filter out withdrawn papers (title often starts with [WITHDRAWN])
        AND paper_title NOT LIKE '[WITHDRAWN]%'
)

SELECT
    event_id,
    'arxiv'                                 AS source,
    event_timestamp,
    event_date,
    paper_title                             AS topic,
    topic_category,
    'en'                                    AS language,
    NULL                                    AS country_code,
    signal_strength,
    medium_type,
    ingested_at,
    -- Significance bucket (proxy: author count encoded in signal_strength)
    CASE
        WHEN signal_strength >= 0.8 THEN 'high_collaboration'   -- 16+ authors
        WHEN signal_strength >= 0.4 THEN 'standard'             -- 8–15 authors
        ELSE 'solo_or_small_team'                                -- < 8 authors
    END AS attention_intensity
FROM validated

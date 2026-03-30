/* @bruin

name: kairoskop.mart_dashboard
type: bq.sql
description: >
  Final mart that powers both Looker Studio dashboard tiles.

  TILE 1 — Collective Attention by Domain (Categorical Distribution):
    Aggregates event counts and signal strength by topic_category and
    medium_dominance for the trailing 30 days.  Answers: "Where is
    civilisation's attention concentrated right now — and which medium
    is leading it?"

  TILE 2 — Synchronicity Score Over Time (Temporal Distribution):
    Daily time series of the synchronicity_score and shadow_index
    for the trailing 90 days.  Kairos Events are flagged directly.
    Answers: "When did the collective mind converge — and what was it
    repressing while it did?"

  This mart is intentionally denormalised for direct dashboard
  consumption without additional joins.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category

depends:
  - kairoskop.fact_attention_events
  - kairoskop.synchronicity_score
  - kairoskop.shadow_index
  - kairoskop.consciousness_level

@bruin */

-- ─────────────────────────────────────────────────────────────────────
-- TILE 1: Categorical distribution (trailing 30 days)
-- ─────────────────────────────────────────────────────────────────────
WITH tile1_base AS (
    SELECT
        event_date,
        topic_category,
        medium_dominance,
        consciousness_level,
        COUNT(*)            AS event_count,
        SUM(signal_strength) AS total_signal,
        AVG(signal_strength) AS avg_signal
    FROM {{ ref('kairoskop.fact_attention_events') }}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 1, 2, 3, 4
),

tile1_totals AS (
    SELECT
        event_date,
        SUM(event_count) AS daily_total
    FROM tile1_base
    GROUP BY 1
),

tile1 AS (
    SELECT
        t.event_date,
        t.topic_category,
        t.medium_dominance,
        t.consciousness_level,
        t.event_count,
        t.total_signal,
        t.avg_signal,
        ROUND(SAFE_DIVIDE(t.event_count, tt.daily_total), 4) AS pct_of_daily_total,
        'categorical'   AS tile_type
    FROM tile1_base t
    JOIN tile1_totals tt USING (event_date)
),

-- ─────────────────────────────────────────────────────────────────────
-- TILE 2: Temporal distribution (trailing 90 days)
-- ─────────────────────────────────────────────────────────────────────
tile2 AS (
    SELECT
        s.event_date,
        s.topic_category,
        NULL                            AS medium_dominance,
        c.dominant_level                AS consciousness_level,

        -- Event volume for the secondary axis
        COALESCE(
            (SELECT SUM(event_count)
             FROM {{ ref('kairoskop.agg_daily_attention') }} a
             WHERE a.event_date = s.event_date
               AND a.topic_category = s.topic_category),
            0
        )                               AS event_count,

        -- Primary metric: Synchronicity Score
        s.synchronicity_score           AS total_signal,
        s.rolling_7d_synchronicity      AS avg_signal,
        sh.shadow_index_raw             AS shadow_index,
        sh.shadow_classification,
        s.is_kairos_event,

        -- Kairos Event label for annotation
        CASE WHEN s.is_kairos_event
            THEN CONCAT('⚡ Kairos: ', s.topic_category)
            ELSE NULL
        END                             AS kairos_label,

        NULL                            AS pct_of_daily_total,
        'temporal'                      AS tile_type
    FROM {{ ref('kairoskop.synchronicity_score') }} s
    LEFT JOIN {{ ref('kairoskop.shadow_index') }} sh
           ON s.event_date = sh.event_date
          AND s.topic_category = sh.topic_category
    LEFT JOIN {{ ref('kairoskop.consciousness_level') }} c
           ON s.event_date = c.event_date
    WHERE s.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
)

-- ─────────────────────────────────────────────────────────────────────
-- Final union with aligned schema
-- ─────────────────────────────────────────────────────────────────────
SELECT
    event_date,
    topic_category,
    medium_dominance,
    consciousness_level,
    event_count,
    total_signal,
    avg_signal,
    NULL        AS shadow_index,
    NULL        AS shadow_classification,
    FALSE       AS is_kairos_event,
    NULL        AS kairos_label,
    pct_of_daily_total,
    tile_type
FROM tile1

UNION ALL

SELECT
    event_date,
    topic_category,
    medium_dominance,
    consciousness_level,
    event_count,
    total_signal,
    avg_signal,
    shadow_index,
    shadow_classification,
    is_kairos_event,
    kairos_label,
    pct_of_daily_total,
    tile_type
FROM tile2
ORDER BY event_date DESC, topic_category

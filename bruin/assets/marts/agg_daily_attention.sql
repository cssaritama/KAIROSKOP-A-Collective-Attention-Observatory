/* @bruin

name: kairoskop.agg_daily_attention
type: bq.sql
description: >
  Daily aggregation of collective attention signals across all sources.
  One row per (event_date, topic_category, source) combination.
  Powers the temporal tile of the KAIROSKOP dashboard.

  Includes rolling 7-day averages for trend smoothing and
  the noospheric_density metric (Teilhard de Chardin) — the total
  volume of active collective thought per day across all layers.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category
    - source

depends:
  - kairoskop.fact_attention_events

@bruin */

WITH daily_base AS (
    SELECT
        event_date,
        topic_category,
        source,
        medium_dominance,
        COUNT(*)                        AS event_count,
        AVG(signal_strength)            AS avg_signal_strength,
        MAX(signal_strength)            AS peak_signal_strength,
        SUM(signal_strength)            AS total_signal_mass,
        COUNTIF(attention_intensity IN ('viral', 'major_edit', 'high_intensity',
                                        'high_collaboration'))
                                        AS high_intensity_count
    FROM {{ ref('kairoskop.fact_attention_events') }}
    GROUP BY 1, 2, 3, 4
),

with_rolling AS (
    SELECT
        *,
        -- 7-day rolling average of event count (trend smoothing)
        AVG(event_count) OVER (
            PARTITION BY topic_category, source
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_event_count,

        -- 7-day rolling average of signal strength
        AVG(avg_signal_strength) OVER (
            PARTITION BY topic_category, source
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_signal_strength,

        -- Week-over-week growth rate
        LAG(event_count, 7) OVER (
            PARTITION BY topic_category, source
            ORDER BY event_date
        ) AS event_count_7d_ago
    FROM daily_base
),

with_growth AS (
    SELECT
        *,
        SAFE_DIVIDE(
            event_count - event_count_7d_ago,
            NULLIF(event_count_7d_ago, 0)
        ) AS wow_growth_rate
    FROM with_rolling
)

SELECT
    event_date,
    topic_category,
    source,
    medium_dominance,
    event_count,
    avg_signal_strength,
    peak_signal_strength,
    total_signal_mass,
    high_intensity_count,
    rolling_7d_event_count,
    rolling_7d_signal_strength,
    event_count_7d_ago,
    wow_growth_rate,

    -- Noospheric density contribution for this day/category/source
    -- (Teilhard: weighted sum of all active thought signals)
    ROUND(total_signal_mass * LOG(1 + event_count), 4) AS noospheric_density
FROM with_growth

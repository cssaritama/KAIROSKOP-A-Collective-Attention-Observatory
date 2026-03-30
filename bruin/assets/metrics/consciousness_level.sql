/* @bruin

name: kairoskop.consciousness_level
type: bq.sql
description: >
  Computes the daily Consciousness Level distribution — the proportion
  of collective attention operating at each level of Ken Wilber's
  Integral (AQAL) developmental model.

  Theoretical grounding (Ken Wilber — Integral Theory):
  Wilber maps the evolution of individual and collective consciousness
  through a series of developmental levels, each transcending and
  including the previous:

    Egocentric   — attention centred on self, tribe, or nation
    Ethnocentric — attention defined by us-vs-them distinctions
    Worldcentric — attention that includes all humans or global systems
    Integral     — attention that holds multiple perspectives simultaneously

  In KAIROSKOP, the daily mix of these levels in collective attention
  patterns reveals what developmental "altitude" civilisation is
  operating from today — and whether it is evolving or regressing.

  One row per event_date, with columns for each level's share.

materialization:
  type: table
  partition_by: event_date

depends:
  - kairoskop.fact_attention_events

@bruin */

WITH daily_levels AS (
    SELECT
        event_date,
        consciousness_level,
        COUNT(*)        AS event_count,
        SUM(signal_strength) AS total_signal
    FROM {{ ref('kairoskop.fact_attention_events') }}
    WHERE consciousness_level IS NOT NULL
    GROUP BY 1, 2
),

daily_totals AS (
    SELECT
        event_date,
        SUM(event_count)    AS total_events,
        SUM(total_signal)   AS total_signal_mass
    FROM daily_levels
    GROUP BY 1
),

pivoted AS (
    SELECT
        dl.event_date,
        dt.total_events,
        dt.total_signal_mass,

        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'egocentric'
            THEN dl.event_count END), 0)   AS egocentric_count,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'ethnocentric'
            THEN dl.event_count END), 0)   AS ethnocentric_count,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'worldcentric'
            THEN dl.event_count END), 0)   AS worldcentric_count,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'integral'
            THEN dl.event_count END), 0)   AS integral_count,

        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'egocentric'
            THEN dl.total_signal END), 0)  AS egocentric_signal,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'ethnocentric'
            THEN dl.total_signal END), 0)  AS ethnocentric_signal,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'worldcentric'
            THEN dl.total_signal END), 0)  AS worldcentric_signal,
        COALESCE(MAX(CASE WHEN dl.consciousness_level = 'integral'
            THEN dl.total_signal END), 0)  AS integral_signal
    FROM daily_levels dl
    JOIN daily_totals dt USING (event_date)
    GROUP BY 1, 2, 3
)

SELECT
    event_date,
    total_events,
    total_signal_mass,

    -- Event count shares
    ROUND(SAFE_DIVIDE(egocentric_count,   total_events), 4) AS pct_egocentric,
    ROUND(SAFE_DIVIDE(ethnocentric_count, total_events), 4) AS pct_ethnocentric,
    ROUND(SAFE_DIVIDE(worldcentric_count, total_events), 4) AS pct_worldcentric,
    ROUND(SAFE_DIVIDE(integral_count,     total_events), 4) AS pct_integral,

    -- Signal-weighted shares (accounts for intensity, not just volume)
    ROUND(SAFE_DIVIDE(egocentric_signal,   total_signal_mass), 4) AS signal_pct_egocentric,
    ROUND(SAFE_DIVIDE(ethnocentric_signal, total_signal_mass), 4) AS signal_pct_ethnocentric,
    ROUND(SAFE_DIVIDE(worldcentric_signal, total_signal_mass), 4) AS signal_pct_worldcentric,
    ROUND(SAFE_DIVIDE(integral_signal,     total_signal_mass), 4) AS signal_pct_integral,

    -- Composite Consciousness Score: weighted average developmental level
    -- Egocentric=1, Ethnocentric=2, Worldcentric=3, Integral=4
    ROUND(
        SAFE_DIVIDE(
            (egocentric_count * 1.0
             + ethnocentric_count * 2.0
             + worldcentric_count * 3.0
             + integral_count    * 4.0),
            total_events
        ),
        4
    ) AS composite_consciousness_score,

    -- Dominant level for the day
    CASE
        WHEN GREATEST(egocentric_count, ethnocentric_count,
                      worldcentric_count, integral_count)
             = egocentric_count   THEN 'egocentric'
        WHEN GREATEST(egocentric_count, ethnocentric_count,
                      worldcentric_count, integral_count)
             = ethnocentric_count THEN 'ethnocentric'
        WHEN GREATEST(egocentric_count, ethnocentric_count,
                      worldcentric_count, integral_count)
             = worldcentric_count THEN 'worldcentric'
        ELSE                           'integral'
    END AS dominant_level,

    -- 7-day rolling composite score trend
    AVG(
        SAFE_DIVIDE(
            (egocentric_count * 1.0
             + ethnocentric_count * 2.0
             + worldcentric_count * 3.0
             + integral_count    * 4.0),
            total_events
        )
    ) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_consciousness_score
FROM pivoted

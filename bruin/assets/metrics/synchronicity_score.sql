/* @bruin

name: kairoskop.synchronicity_score
type: bq.sql
description: >
  Computes the KAIROSKOP Synchronicity Score — the degree to which
  collective attention across all four independent sources converges
  on the same topic domains simultaneously.

  Theoretical grounding (C.G. Jung):
  Synchronicity is defined as "the simultaneous occurrence of events
  that appear significantly related but have no discernible causal
  connection."  When Wikipedia edits, GDELT media events, arXiv papers,
  and Wikipedia pageviews all spike on the same domain within a 24-hour
  window — without any single coordinating actor — that convergence is
  what Jung called synchronicity, here made measurable for the first time.

  Score interpretation:
    0.0 – 0.2  →  Fragmented  (sources pointing in different directions)
    0.2 – 0.5  →  Coherent    (partial alignment across sources)
    0.5 – 0.8  →  Convergent  (strong multi-source alignment)
    0.8 – 1.0  →  Synchronic  (Kairos Event — potential historical moment)

  One row per (event_date, topic_category).

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category

depends:
  - kairoskop.agg_daily_attention

@bruin */

WITH source_signals AS (
    -- Pivot daily signals by source so we can compare them side by side
    SELECT
        event_date,
        topic_category,

        MAX(CASE WHEN source = 'wiki_pageviews' THEN avg_signal_strength END)
            AS wiki_pv_signal,
        MAX(CASE WHEN source = 'wiki_changes'   THEN avg_signal_strength END)
            AS wiki_ch_signal,
        MAX(CASE WHEN source = 'gdelt'          THEN avg_signal_strength END)
            AS gdelt_signal,
        MAX(CASE WHEN source = 'arxiv'          THEN avg_signal_strength END)
            AS arxiv_signal,

        MAX(CASE WHEN source = 'wiki_pageviews' THEN event_count END)
            AS wiki_pv_count,
        MAX(CASE WHEN source = 'wiki_changes'   THEN event_count END)
            AS wiki_ch_count,
        MAX(CASE WHEN source = 'gdelt'          THEN event_count END)
            AS gdelt_count,
        MAX(CASE WHEN source = 'arxiv'          THEN event_count END)
            AS arxiv_count,

        SUM(noospheric_density) AS total_noospheric_density
    FROM {{ ref('kairoskop.agg_daily_attention') }}
    GROUP BY 1, 2
),

with_scores AS (
    SELECT
        *,

        -- Count how many sources are "active" on this topic today
        -- (signal > 0.2 = meaningfully above background noise)
        (CASE WHEN COALESCE(wiki_pv_signal, 0) > 0.2 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(wiki_ch_signal, 0) > 0.2 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(gdelt_signal,  0) > 0.2 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(arxiv_signal,  0) > 0.2 THEN 1 ELSE 0 END)
            AS active_source_count,

        -- Mean signal across all available sources
        (COALESCE(wiki_pv_signal, 0)
         + COALESCE(wiki_ch_signal, 0)
         + COALESCE(gdelt_signal,  0)
         + COALESCE(arxiv_signal,  0)) / 4.0
            AS mean_signal,

        -- Signal variance (low variance + high mean = synchronicity)
        (
            POW(COALESCE(wiki_pv_signal, 0) - (COALESCE(wiki_pv_signal,0)+COALESCE(wiki_ch_signal,0)+COALESCE(gdelt_signal,0)+COALESCE(arxiv_signal,0))/4.0, 2)
          + POW(COALESCE(wiki_ch_signal, 0) - (COALESCE(wiki_pv_signal,0)+COALESCE(wiki_ch_signal,0)+COALESCE(gdelt_signal,0)+COALESCE(arxiv_signal,0))/4.0, 2)
          + POW(COALESCE(gdelt_signal,  0) - (COALESCE(wiki_pv_signal,0)+COALESCE(wiki_ch_signal,0)+COALESCE(gdelt_signal,0)+COALESCE(arxiv_signal,0))/4.0, 2)
          + POW(COALESCE(arxiv_signal,  0) - (COALESCE(wiki_pv_signal,0)+COALESCE(wiki_ch_signal,0)+COALESCE(gdelt_signal,0)+COALESCE(arxiv_signal,0))/4.0, 2)
        ) / 4.0 AS signal_variance
    FROM source_signals
)

SELECT
    event_date,
    topic_category,
    wiki_pv_signal,
    wiki_ch_signal,
    gdelt_signal,
    arxiv_signal,
    active_source_count,
    mean_signal,
    signal_variance,
    total_noospheric_density,

    -- Synchronicity Score: high mean signal, low variance, many active sources
    ROUND(
        LEAST(
            (mean_signal * active_source_count / 4.0)
            * (1.0 - LEAST(signal_variance * 2.0, 1.0)),
            1.0
        ),
        4
    ) AS synchronicity_score,

    -- Kairos Event flag: synchronicity >= 0.8
    (ROUND(
        LEAST(
            (mean_signal * active_source_count / 4.0)
            * (1.0 - LEAST(signal_variance * 2.0, 1.0)),
            1.0
        ), 4) >= 0.8
    ) AS is_kairos_event,

    -- Rolling 7-day average of synchronicity
    AVG(ROUND(
        LEAST(
            (mean_signal * active_source_count / 4.0)
            * (1.0 - LEAST(signal_variance * 2.0, 1.0)),
            1.0
        ), 4)
    ) OVER (
        PARTITION BY topic_category
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_synchronicity
FROM with_scores

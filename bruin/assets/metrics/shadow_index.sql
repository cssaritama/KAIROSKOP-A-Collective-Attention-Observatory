/* @bruin

name: kairoskop.shadow_index
type: bq.sql
description: >
  Computes the KAIROSKOP Shadow Index — the divergence between what
  a civilisation seeks privately (Wikipedia pageviews, driven by
  individual curiosity) and what the institutional media apparatus
  amplifies publicly (GDELT).

  Theoretical grounding (Freud / Lacan):
  Freud described repression as the psyche's mechanism for keeping
  threatening material out of conscious awareness.  In collective terms,
  the Shadow (Jung) is what a society cannot consciously acknowledge
  but cannot stop being drawn to.  Lacan added that the unconscious
  is "structured like a language" — it speaks in gaps, in what is
  NOT said in the official discourse.

  The Shadow Index operationalises this:
    High index → society privately seeks topics that media ignores.
                 The repressed is active.  Pay attention.
    Near zero  → private curiosity and public discourse are aligned.
    Negative   → media amplifies topics that people do not seek out.
                 Manufactured salience.

  One row per (event_date, topic_category).

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - topic_category

depends:
  - kairoskop.agg_daily_attention

@bruin */

WITH pivoted AS (
    SELECT
        event_date,
        topic_category,

        -- Private curiosity signal (Wikipedia pageviews = individual pull)
        MAX(CASE WHEN source = 'wiki_pageviews'
            THEN avg_signal_strength ELSE NULL END) AS private_signal,

        -- Public discourse signal (GDELT = institutional media push)
        MAX(CASE WHEN source = 'gdelt'
            THEN avg_signal_strength ELSE NULL END) AS public_signal,

        -- Formal knowledge signal (arXiv = expert attention)
        MAX(CASE WHEN source = 'arxiv'
            THEN avg_signal_strength ELSE NULL END) AS expert_signal,

        -- Active collective memory (Wikipedia edits = community investment)
        MAX(CASE WHEN source = 'wiki_changes'
            THEN avg_signal_strength ELSE NULL END) AS memory_signal
    FROM {{ ref('kairoskop.agg_daily_attention') }}
    GROUP BY 1, 2
),

with_index AS (
    SELECT
        *,

        -- Shadow Index = private curiosity minus public amplification
        -- Positive: people seek what media ignores (repressed content)
        -- Negative: media pushes what people don't organically seek
        ROUND(
            COALESCE(private_signal, 0) - COALESCE(public_signal, 0),
            4
        ) AS shadow_index_raw,

        -- Expert-Public Gap: what scientists study vs what media covers
        -- Positive: important scientific work happening below media radar
        ROUND(
            COALESCE(expert_signal, 0) - COALESCE(public_signal, 0),
            4
        ) AS expert_public_gap
    FROM pivoted
)

SELECT
    event_date,
    topic_category,
    private_signal,
    public_signal,
    expert_signal,
    memory_signal,
    shadow_index_raw,
    expert_public_gap,

    -- Normalised Shadow Index (–1 to +1 scale)
    ROUND(
        shadow_index_raw / NULLIF(
            GREATEST(ABS(private_signal), ABS(public_signal), 0.001),
            0
        ),
        4
    ) AS shadow_index_normalised,

    -- Classification
    CASE
        WHEN shadow_index_raw >  0.3 THEN 'strongly_repressed'
        WHEN shadow_index_raw >  0.1 THEN 'mildly_repressed'
        WHEN shadow_index_raw > -0.1 THEN 'aligned'
        WHEN shadow_index_raw > -0.3 THEN 'mildly_manufactured'
        ELSE                              'strongly_manufactured'
    END AS shadow_classification,

    -- 30-day rolling average of shadow index (structural pattern)
    AVG(shadow_index_raw) OVER (
        PARTITION BY topic_category
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_shadow_index
FROM with_index

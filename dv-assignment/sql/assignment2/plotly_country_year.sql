WITH base AS (
  SELECT
    EXTRACT(YEAR FROM fr.funded_at)::int AS year,
    COALESCE(NULLIF(o.country_code,''),'UNK') AS country,
    fr.raised_amount_usd
  FROM cb.funding_rounds fr
  JOIN cb.objects o ON (o.id = fr.object_id OR o.entity_id = fr.object_id)
  WHERE fr.funded_at IS NOT NULL
    AND fr.raised_amount_usd IS NOT NULL AND fr.raised_amount_usd > 0
    AND EXTRACT(YEAR FROM fr.funded_at)::int >= 2005
),
agg AS (
  SELECT year, country,
         SUM(raised_amount_usd) AS raised_usd,
         COUNT(*) AS deals
  FROM base
  GROUP BY 1,2
),
ranked AS (
  SELECT *,
         (raised_usd::numeric / NULLIF(deals,0)) AS avg_raised
  FROM agg
),
top10 AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY year ORDER BY raised_usd DESC) AS rn
  FROM ranked
)
SELECT year, country, deals, raised_usd, avg_raised
FROM top10
WHERE rn <= 10
ORDER BY year, raised_usd DESC;

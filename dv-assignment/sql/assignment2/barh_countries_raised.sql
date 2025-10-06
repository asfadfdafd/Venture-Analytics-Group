WITH fr_valid AS (
  SELECT fr.funding_round_id, fr.object_id, fr.funded_at::date AS dt, fr.raised_amount_usd
  FROM cb.funding_rounds fr
  WHERE fr.funded_at::date BETWEEN DATE '2005-01-01' AND DATE '2015-12-31'
)
SELECT
  COALESCE(NULLIF(o.country_code,''),'UNK') AS country,
  SUM(f.raised_amount_usd) AS raised_usd
FROM fr_valid f
JOIN cb.investments i ON i.funding_round_id = f.funding_round_id
JOIN cb.objects o ON (o.id = f.object_id OR o.entity_id = f.object_id)
GROUP BY 1
ORDER BY raised_usd DESC
LIMIT 10;

WITH deals AS (
  SELECT i.investor_object_id AS investor_id, COUNT(*) AS deals
  FROM cb.investments i
  JOIN cb.funding_rounds fr ON fr.funding_round_id = i.funding_round_id
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 5
)
SELECT
  EXTRACT(YEAR FROM fr.funded_at)::int AS year,
  COALESCE(NULLIF(oinv.name,''), oinv.id) AS investor,
  SUM(fr.raised_amount_usd) AS raised_usd
FROM deals d
JOIN cb.investments i   ON i.investor_object_id = d.investor_id
JOIN cb.funding_rounds fr ON fr.funding_round_id = i.funding_round_id
JOIN cb.objects oinv    ON oinv.id = d.investor_id
GROUP BY 1,2
ORDER BY 1,3 DESC;

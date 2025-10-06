SELECT
  COALESCE(NULLIF(oinv.entity_type,''),'Unknown') AS investor_type,
  COUNT(*) AS deals
FROM cb.investments i
JOIN cb.funding_rounds fr ON fr.funding_round_id = i.funding_round_id
JOIN cb.objects        oinv ON oinv.id = i.investor_object_id
GROUP BY 1
ORDER BY 2 DESC;

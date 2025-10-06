SELECT fr.raised_amount_usd
FROM cb.funding_rounds fr
JOIN cb.investments i ON i.funding_round_id = fr.funding_round_id
JOIN cb.objects o     ON (o.id = fr.object_id OR o.entity_id = fr.object_id)
WHERE fr.funding_round_type ILIKE 'series_a'
  AND COALESCE(o.country_code,'') = 'USA'
  AND fr.raised_amount_usd IS NOT NULL
  AND fr.raised_amount_usd > 0;

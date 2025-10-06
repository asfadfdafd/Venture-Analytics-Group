-- 5.1 количества строк по таблицам
SELECT 'objects',COUNT(*) FROM cb.objects UNION ALL
SELECT 'people',COUNT(*) FROM cb.people UNION ALL
SELECT 'offices',COUNT(*) FROM cb.offices UNION ALL
SELECT 'degrees',COUNT(*) FROM cb.degrees UNION ALL
SELECT 'milestones',COUNT(*) FROM cb.milestones UNION ALL
SELECT 'funds',COUNT(*) FROM cb.funds UNION ALL
SELECT 'funding_rounds',COUNT(*) FROM cb.funding_rounds UNION ALL
SELECT 'investments',COUNT(*) FROM cb.investments UNION ALL
SELECT 'acquisitions',COUNT(*) FROM cb.acquisitions UNION ALL
SELECT 'ipos',COUNT(*) FROM cb.ipos UNION ALL
SELECT 'relationships',COUNT(*) FROM cb.relationships;

-- 5.2 «висячие» ссылки
SELECT COUNT(*) AS bad_acq_buyers
FROM cb.acquisitions a LEFT JOIN cb.objects o ON o.entity_id = a.acquiring_object_id
WHERE o.entity_id IS NULL;

SELECT COUNT(*) AS bad_acq_targets
FROM cb.acquisitions a LEFT JOIN cb.objects o ON o.entity_id = a.acquired_object_id
WHERE o.entity_id IS NULL;

SELECT COUNT(*) AS bad_invest_round
FROM cb.investments i LEFT JOIN cb.funding_rounds fr ON fr.funding_round_id = i.funding_round_id
WHERE fr.funding_round_id IS NULL;

-- 5.3 пропуски по ключевым полям
SELECT
  SUM(CASE WHEN founded_at IS NULL THEN 1 ELSE 0 END) AS null_founded_at,
  SUM(CASE WHEN funding_total_usd IS NULL THEN 1 ELSE 0 END) AS null_funding_total_usd
FROM cb.objects;

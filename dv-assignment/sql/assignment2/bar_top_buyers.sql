SELECT
  COALESCE(NULLIF(buyer.name,''), buyer.id) AS buyer,
  COUNT(*) AS deals
FROM cb.acquisitions a
JOIN cb.objects buyer  ON buyer.id  = a.acquiring_object_id
JOIN cb.objects target ON target.id = a.acquired_object_id
GROUP BY 1
ORDER BY deals DESC
LIMIT 10;

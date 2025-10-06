WITH funding AS (
  SELECT
    o.id AS company_id,
    COALESCE(NULLIF(o.name,''), o.id) AS company_name,
    SUM(fr.raised_amount_usd) AS total_raised
  FROM cb.objects o
  JOIN cb.funding_rounds fr ON (o.id = fr.object_id OR o.entity_id = fr.object_id)
  GROUP BY 1,2
),
acq AS (
  SELECT a.acquired_object_id AS company_id,
         COUNT(*) AS acq_count
  FROM cb.acquisitions a
  JOIN cb.objects target ON target.id = a.acquired_object_id
  JOIN cb.objects buyer  ON buyer.id  = a.acquiring_object_id
  GROUP BY 1
)
SELECT f.company_name, f.total_raised, COALESCE(a.acq_count,0) AS acquisitions_as_target
FROM funding f
LEFT JOIN acq a ON a.company_id = f.company_id
WHERE f.total_raised IS NOT NULL AND f.total_raised > 0;

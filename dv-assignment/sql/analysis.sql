-- 6.1 Топ-20 компаний по общему финансированию (USD)
SELECT o.name, cf.total_raised_usd
FROM cb.v_company_funding cf
JOIN cb.objects o ON o.entity_id = cf.entity_id
ORDER BY cf.total_raised_usd DESC NULLS LAST
LIMIT 20;

-- 6.2 Объём привлечений по годам
SELECT * FROM cb.v_raised_by_year;

-- 6.3 Топ-20 инвесторов по числу сделок
SELECT * FROM cb.v_top_investors LIMIT 20;

-- 6.4 IPO по годам и странам
-- IPO по годам и странам (кастим public_at из TEXT в DATE на лету)
SELECT EXTRACT(YEAR FROM i.public_at::date)::int AS year,
       o.country_code,
       COUNT(*) AS ipo_count,
       SUM(i.raised_amount::numeric) AS raised_local_sum
FROM cb.ipos i
JOIN cb.objects o ON o.entity_id = i.object_id
WHERE i.public_at ~ '^\d{4}-\d{2}-\d{2}$'  -- страхуемся от кривых дат
GROUP BY 1,2
ORDER BY 1,3 DESC;


-- 6.5 Самые активные M&A-покупатели
SELECT
  COALESCE(NULLIF(buyer.name,''), NULLIF(buyer.permalink,''), buyer.id) AS buyer,
  COUNT(*) AS deals,
  SUM(a.price_amount) AS total_price
FROM cb.acquisitions a
JOIN cb.objects buyer  ON lower(replace(buyer.id,  chr(160), '')) =
                          lower(replace(btrim(a.acquiring_object_id), chr(160), ''))
JOIN cb.objects target ON lower(replace(target.id, chr(160), '')) =
                          lower(replace(btrim(a.acquired_object_id),  chr(160), ''))
GROUP BY 1
ORDER BY deals DESC NULLS LAST
LIMIT 20;



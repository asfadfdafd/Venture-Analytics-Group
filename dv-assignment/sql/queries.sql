-- 1) LIMIT 10 по таблицам (быстрый просмотр)
-- показывает первые 10 строк ключевых таблиц
SELECT * FROM cb.objects LIMIT 10;
SELECT * FROM cb.funding_rounds LIMIT 10;
SELECT * FROM cb.investments LIMIT 10;
SELECT * FROM cb.acquisitions LIMIT 10;
SELECT * FROM cb.ipos LIMIT 10;

-- 2) WHERE + ORDER BY
-- компании из США, отсортировано по updated_at по убыванию
SELECT name, country_code, updated_at
FROM cb.objects
WHERE country_code = 'USA'
ORDER BY updated_at DESC
LIMIT 20;

-- 3) GROUP BY + COUNT/MIN/MAX/AVG
-- по годам: сколько раундов и средняя сумма в USD
SELECT EXTRACT(YEAR FROM funded_at)::int AS year,
       COUNT(*) AS rounds_cnt,
       AVG(raised_amount_usd) AS avg_raised_usd,
       MIN(raised_amount_usd) AS min_raised_usd,
       MAX(raised_amount_usd) AS max_raised_usd
FROM cb.funding_rounds
GROUP BY 1
ORDER BY 1;

-- 4) Простой JOIN (1 минимум)
-- последние 20 раундов с названием компании
SELECT fr.funded_at, o.name, fr.funding_round_type, fr.raised_amount_usd
FROM cb.funding_rounds fr
JOIN cb.objects o ON o.entity_id = btrim(fr.object_id)
ORDER BY fr.funded_at DESC
LIMIT 20;

-- Тема 1: ТОП-20 компаний по общему финансированию (USD)
SELECT o.name, cf.total_raised_usd, cf.rounds_count, cf.last_round_at
FROM cb.v_company_funding cf
JOIN cb.objects o ON o.entity_id = cf.entity_id
ORDER BY cf.total_raised_usd DESC NULLS LAST
LIMIT 20;

-- Тема 2: Динамика привлечений по годам (USD)
SELECT * FROM cb.v_raised_by_year ORDER BY year;

-- Тема 3: ТОП-20 инвесторов по числу сделок (с фолбэком имени)
SELECT investor_id, investor_name, deals
FROM cb.v_top_investors
LIMIT 20;

-- Тема 4: ТОП-20 покупателей в M&A (сумма и число сделок)
SELECT
  COALESCE(NULLIF(buyer.name,''), NULLIF(buyer.permalink,''), buyer.entity_id) AS buyer,
  COUNT(*) AS deals,
  SUM(a.price_amount) AS total_price
FROM cb.acquisitions a
JOIN cb.objects buyer  ON buyer.entity_id  = a.acquiring_object_id
JOIN cb.objects target ON target.entity_id = a.acquired_object_id
GROUP BY 1
ORDER BY deals DESC NULLS LAST
LIMIT 20;

-- Тема 5: Средняя сумма раунда по типам (Seed/A/B/…)
SELECT funding_round_type,
       COUNT(*) AS rounds_cnt,
       AVG(raised_amount_usd) AS avg_usd
FROM cb.funding_rounds
GROUP BY 1
ORDER BY avg_usd DESC NULLS LAST;

-- Тема 6: ТОП-10 городов по числу компаний
SELECT city, COUNT(*) AS companies
FROM cb.objects
WHERE NULLIF(city,'') IS NOT NULL
GROUP BY city
ORDER BY companies DESC
LIMIT 10;

-- Тема 7: Сколько IPO по годам и странам (устойчивый парс дат)
WITH ip AS (
  SELECT
    i.object_id,
    COALESCE(
      NULLIF(i.public_at,'')::date,
      CASE WHEN i.public_at ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(i.public_at,'MM/DD/YYYY') END,
      CASE WHEN i.public_at ~ '^\d{4}/\d{2}/\d{2}$' THEN to_date(i.public_at,'YYYY/MM/DD') END
    ) AS public_dt
  FROM cb.ipos i
)
SELECT EXTRACT(YEAR FROM public_dt)::int AS year, o.country_code, COUNT(*) AS ipo_count
FROM ip
JOIN cb.objects o ON o.entity_id = ip.object_id
WHERE public_dt IS NOT NULL
GROUP BY 1,2
ORDER BY 1,3 DESC;

-- Тема 8: Средняя сумма привлечений по странам
SELECT o.country_code,
       COUNT(*) AS rounds_cnt,
       AVG(fr.raised_amount_usd) AS avg_raised_usd
FROM cb.funding_rounds fr
JOIN cb.objects o ON o.entity_id = btrim(fr.object_id)
GROUP BY o.country_code
ORDER BY avg_raised_usd DESC NULLS LAST
LIMIT 20;

-- Тема 9: Компании с > N раундов (пример: больше 5)
SELECT o.name, COUNT(*) AS rounds_cnt, SUM(fr.raised_amount_usd) AS total_raised_usd
FROM cb.funding_rounds fr
JOIN cb.objects o ON o.entity_id = btrim(fr.object_id)
GROUP BY o.name
HAVING COUNT(*) > 5
ORDER BY total_raised_usd DESC NULLS LAST
LIMIT 20;

-- Тема 10: Список «молодых» компаний (по вычисленной дате основания)
SELECT name, derived_founded_at, country_code
FROM cb.v_objects_with_derived_founded
WHERE derived_founded_at IS NOT NULL
ORDER BY derived_founded_at DESC
LIMIT 20;

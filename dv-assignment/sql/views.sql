CREATE SCHEMA IF NOT EXISTS util;

CREATE OR REPLACE FUNCTION util.norm_id(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT lower(
           regexp_replace(                             -- убираем управляющие символы
             replace(btrim(t), chr(160), ''),          -- убираем NBSP (0xA0) и крайние пробелы
             '[\x00-\x1F\x7F]', '', 'g'
           )
         )
$$;





-- 1) Объекты с вычислённой датой основания
CREATE OR REPLACE VIEW cb.v_objects_with_derived_founded AS
SELECT
  o.*,
  COALESCE(
    o.founded_at,
    LEAST(
      NULLIF(o.first_milestone_at, DATE '1900-01-01'),
      NULLIF(o.first_funding_at,  DATE '1900-01-01'),
      NULLIF(o.created_at::date,  DATE '1900-01-01')
    )
  ) AS derived_founded_at
FROM cb.objects o;

-- 2) Суммарное финансирование по компаниям
CREATE OR REPLACE VIEW cb.v_company_funding AS
SELECT
  o.id AS entity_id,                 -- было o.entity_id
  o.name,
  o.country_code,
  SUM(fr.raised_amount_usd) AS total_raised_usd,
  COUNT(*) AS rounds_count,
  MAX(fr.funded_at) AS last_round_at
FROM cb.objects o
JOIN cb.funding_rounds fr
  ON lower(replace(btrim(fr.object_id), chr(160), '')) =  -- стыкуем префиксы
     lower(replace(btrim(o.id),          chr(160), ''))
GROUP BY o.id, o.name, o.country_code;


-- 3) Топ инвесторов (с жёстким фолбэком имени)
DROP VIEW IF EXISTS cb.v_top_investors;

CREATE VIEW cb.v_top_investors AS
WITH deals AS (
  SELECT util.norm_id(investor_object_id) AS investor_id, COUNT(*) AS deals
  FROM cb.investments
  GROUP BY util.norm_id(investor_object_id)
),
meta_full AS (            -- имя по полному id (c префиксом)
  SELECT util.norm_id(id) AS key_full,
         COALESCE(NULLIF(btrim(name), ''),
                  NULLIF(btrim(permalink), ''),
                  NULLIF(btrim(normalized_name), ''),
                  NULLIF(btrim(domain), '')) AS base_name
  FROM cb.objects
),
meta_num AS (             -- имя по цифре (entity_id)
  SELECT btrim(entity_id) AS key_num,
         COALESCE(NULLIF(btrim(name), ''),
                  NULLIF(btrim(permalink), ''),
                  NULLIF(btrim(normalized_name), ''),
                  NULLIF(btrim(domain), '')) AS base_name
  FROM cb.objects
)
SELECT
  d.investor_id,
  COALESCE(
    mf.base_name,                                 -- 1) по полному id
    mn.base_name,                                 -- 2) по entity_id (цифрам)
    CASE left(d.investor_id,1)                    -- 3) фолбэк-текст
      WHEN 'f' THEN 'Financial org ' || split_part(d.investor_id,':',2)
      WHEN 'c' THEN 'Company '       || split_part(d.investor_id,':',2)
      WHEN 'p' THEN 'Person '        || split_part(d.investor_id,':',2)
      ELSE d.investor_id
    END
  ) AS investor_name,
  d.deals
FROM deals d
LEFT JOIN meta_full mf ON mf.key_full = d.investor_id
LEFT JOIN meta_num  mn ON mn.key_num  = split_part(d.investor_id,':',2)
ORDER BY d.deals DESC;




-- 4) Привлечения по годам
CREATE OR REPLACE VIEW cb.v_raised_by_year AS
SELECT
  EXTRACT(YEAR FROM fr.funded_at)::int AS year,
  SUM(fr.raised_amount_usd) AS raised_usd
FROM cb.funding_rounds fr
GROUP BY 1
ORDER BY 1;

#!/usr/bin/env python3
import os
import argparse
import logging
import psycopg2

# ---------- Конфиг по умолчанию (можно переопределить флагами) ----------
DB_CONFIG = {
    'host': 'localhost',
    'database': 'dv_project',
    'user': 'postgres',
    'password': '0000',
    'port': 5432,
}
DEFAULT_SCHEMA = 'cb'
DEFAULT_DATA_DIR = '/Users/asandauren/Downloads/archive'  # поменяй при запуске флагом --data-dir

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def qname(schema: str, table: str) -> str:
    return f'{schema}.{table}'

# ---------- SQL блоки: стейджинг + вставка для каждой таблицы ----------
SQL = {}

# objects
SQL["objects_stage_drop"] = "DROP TABLE IF EXISTS {sch}.objects_stage;"
SQL["objects_stage_create"] = """
CREATE TABLE {sch}.objects_stage (
  id TEXT, entity_type TEXT, entity_id TEXT, parent_id TEXT, name TEXT, normalized_name TEXT,
  permalink TEXT, category_code TEXT, status TEXT, founded_at TEXT, closed_at TEXT, domain TEXT,
  homepage_url TEXT, twitter_username TEXT, logo_url TEXT, logo_width TEXT, logo_height TEXT,
  short_description TEXT, description TEXT, overview TEXT, tag_list TEXT, country_code TEXT,
  state_code TEXT, city TEXT, region TEXT, first_investment_at TEXT, last_investment_at TEXT,
  investment_rounds TEXT, invested_companies TEXT, first_funding_at TEXT, last_funding_at TEXT,
  funding_rounds TEXT, funding_total_usd TEXT, first_milestone_at TEXT, last_milestone_at TEXT,
  milestones TEXT, relationships TEXT, created_by TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["objects_insert"] = """
INSERT INTO {sch}.objects(
  id, entity_type, entity_id, parent_id, name, normalized_name, permalink, category_code, status,
  founded_at, closed_at, domain, homepage_url, twitter_username, logo_url, logo_width, logo_height,
  short_description, description, overview, tag_list, country_code, state_code, city, region,
  first_investment_at, last_investment_at, investment_rounds, invested_companies, first_funding_at,
  last_funding_at, funding_rounds, funding_total_usd, first_milestone_at, last_milestone_at,
  milestones, relationships, created_by, created_at, updated_at
)
SELECT
  id, entity_type, entity_id, parent_id, name, normalized_name, permalink, category_code, status,
  NULLIF(founded_at,'')::date, NULLIF(closed_at,'')::date, domain, homepage_url, twitter_username,
  logo_url, NULLIF(logo_width,'')::int, NULLIF(logo_height,'')::int,
  short_description, description, overview, tag_list, country_code, state_code, city, region,
  NULLIF(first_investment_at,'')::date, NULLIF(last_investment_at,'')::date,
  NULLIF(investment_rounds,'')::int, NULLIF(invested_companies,'')::int,
  NULLIF(first_funding_at,'')::date, NULLIF(last_funding_at,'')::date,
  NULLIF(funding_rounds,'')::int, NULLIF(funding_total_usd,'')::numeric,
  NULLIF(first_milestone_at,'')::date, NULLIF(last_milestone_at,'')::date,
  NULLIF(milestones,'')::int, NULLIF(relationships,'')::int,
  created_by, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM (
  SELECT DISTINCT ON (entity_id) *
  FROM {sch}.objects_stage
  ORDER BY entity_id, COALESCE(NULLIF(updated_at,'')::timestamp, '1900-01-01'::timestamp) DESC
) s
ON CONFLICT (entity_id) DO NOTHING;
"""

# people
SQL["people_stage_drop"] = "DROP TABLE IF EXISTS {sch}.people_stage;"
SQL["people_stage_create"] = """
CREATE TABLE {sch}.people_stage (
  id TEXT, object_id TEXT, first_name TEXT, last_name TEXT, birthplace TEXT, affiliation_name TEXT
);
"""
SQL["people_insert"] = """
INSERT INTO {sch}.people(id, object_id, first_name, last_name, birthplace, affiliation_name)
SELECT
  NULLIF(id,'')::bigint, btrim(object_id), first_name, last_name, birthplace, affiliation_name
FROM {sch}.people_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id))
ON CONFLICT (object_id) DO NOTHING;
"""

# offices
SQL["offices_stage_drop"] = "DROP TABLE IF EXISTS {sch}.offices_stage;"
SQL["offices_stage_create"] = """
CREATE TABLE {sch}.offices_stage (
  id TEXT, object_id TEXT, office_id TEXT, description TEXT, region TEXT, address1 TEXT, address2 TEXT,
  city TEXT, zip_code TEXT, state_code TEXT, country_code TEXT, latitude TEXT, longitude TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["offices_insert"] = """
INSERT INTO {sch}.offices(
  id, object_id, office_id, description, region, address1, address2, city, zip_code, state_code,
  country_code, latitude, longitude, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, btrim(object_id), office_id, description, region, address1, address2, city, zip_code, state_code,
  country_code, NULLIF(latitude,'')::double precision, NULLIF(longitude,'')::double precision,
  NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.offices_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id));
"""

# degrees
SQL["degrees_stage_drop"] = "DROP TABLE IF EXISTS {sch}.degrees_stage;"
SQL["degrees_stage_create"] = """
CREATE TABLE {sch}.degrees_stage (
  id TEXT, object_id TEXT, degree_type TEXT, subject TEXT, institution TEXT, graduated_at TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["degrees_insert"] = """
INSERT INTO {sch}.degrees(
  id, object_id, degree_type, subject, institution, graduated_at, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, btrim(object_id), degree_type, subject, institution,
  NULLIF(graduated_at,'')::date, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.degrees_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id));
"""

# milestones
SQL["milestones_stage_drop"] = "DROP TABLE IF EXISTS {sch}.milestones_stage;"
SQL["milestones_stage_create"] = """
CREATE TABLE {sch}.milestones_stage (
  id TEXT, object_id TEXT, milestone_at TEXT, milestone_code TEXT, description TEXT,
  source_url TEXT, source_description TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["milestones_insert"] = """
INSERT INTO {sch}.milestones(
  id, object_id, milestone_at, milestone_code, description, source_url, source_description, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, btrim(object_id), NULLIF(milestone_at,'')::date, milestone_code, description,
  source_url, source_description, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.milestones_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id));
"""

# funds
SQL["funds_stage_drop"] = "DROP TABLE IF EXISTS {sch}.funds_stage;"
SQL["funds_stage_create"] = """
CREATE TABLE {sch}.funds_stage (
  id TEXT, fund_id TEXT, object_id TEXT, name TEXT, funded_at TEXT, raised_amount TEXT,
  raised_currency_code TEXT, created_at TEXT, updated_at TEXT, source_url TEXT, source_description TEXT
);
"""
SQL["funds_insert"] = """
INSERT INTO {sch}.funds(
  id, fund_id, object_id, name, funded_at, raised_amount, raised_currency_code, created_at, updated_at, source_url, source_description
)
SELECT
  NULLIF(id,'')::bigint, fund_id, btrim(object_id), name, NULLIF(funded_at,'')::date,
  NULLIF(raised_amount,'')::numeric, raised_currency_code,
  NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp, source_url, source_description
FROM {sch}.funds_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id))
ON CONFLICT (fund_id) DO NOTHING;
"""

# funding_rounds
SQL["funding_rounds_stage_drop"] = "DROP TABLE IF EXISTS {sch}.funding_rounds_stage;"
SQL["funding_rounds_stage_create"] = """
CREATE TABLE {sch}.funding_rounds_stage (
  id TEXT, funding_round_id TEXT, object_id TEXT, funded_at TEXT, funding_round_type TEXT, funding_round_code TEXT,
  raised_amount TEXT, raised_amount_usd TEXT, raised_currency_code TEXT,
  pre_money_valuation TEXT, pre_money_valuation_usd TEXT, pre_money_currency_code TEXT,
  post_money_valuation TEXT, post_money_valuation_usd TEXT, post_money_currency_code TEXT,
  participants TEXT, is_first_round TEXT, is_last_round TEXT, source_url TEXT, source_description TEXT,
  created_by TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["funding_rounds_insert"] = """
INSERT INTO {sch}.funding_rounds(
  id, funding_round_id, object_id, funded_at, funding_round_type, funding_round_code,
  raised_amount, raised_amount_usd, raised_currency_code,
  pre_money_valuation, pre_money_valuation_usd, pre_money_currency_code,
  post_money_valuation, post_money_valuation_usd, post_money_currency_code,
  participants, is_first_round, is_last_round, source_url, source_description, created_by, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, funding_round_id, btrim(object_id), NULLIF(funded_at,'')::date,
  funding_round_type, funding_round_code,
  NULLIF(raised_amount,'')::numeric, NULLIF(raised_amount_usd,'')::numeric, raised_currency_code,
  NULLIF(pre_money_valuation,'')::numeric, NULLIF(pre_money_valuation_usd,'')::numeric, pre_money_currency_code,
  NULLIF(post_money_valuation,'')::numeric, NULLIF(post_money_valuation_usd,'')::numeric, post_money_currency_code,
  NULLIF(participants,'')::int,
  CASE LOWER(COALESCE(is_first_round,'')) WHEN 't' THEN true WHEN 'true' THEN true WHEN '1' THEN true WHEN 'yes' THEN true ELSE false END,
  CASE LOWER(COALESCE(is_last_round,''))  WHEN 't' THEN true WHEN 'true' THEN true WHEN '1' THEN true WHEN 'yes' THEN true ELSE false END,
  source_url, source_description, created_by, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.funding_rounds_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id))
ON CONFLICT (funding_round_id) DO NOTHING;
"""

# investments
SQL["investments_stage_drop"] = "DROP TABLE IF EXISTS {sch}.investments_stage;"
SQL["investments_stage_create"] = """
CREATE TABLE {sch}.investments_stage (
  id TEXT, funding_round_id TEXT, funded_object_id TEXT, investor_object_id TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["investments_insert"] = """
INSERT INTO {sch}.investments(
  id, funding_round_id, funded_object_id, investor_object_id, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, funding_round_id, btrim(funded_object_id), btrim(investor_object_id),
  NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.investments_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.funding_rounds fr WHERE fr.funding_round_id = s.funding_round_id)
  AND EXISTS (SELECT 1 FROM {sch}.objects o1 WHERE o1.entity_id = btrim(s.funded_object_id))
  AND EXISTS (SELECT 1 FROM {sch}.objects o2 WHERE o2.entity_id = btrim(s.investor_object_id));
"""

# acquisitions
SQL["acq_stage_drop"] = "DROP TABLE IF EXISTS {sch}.acq_stage;"
SQL["acq_stage_create"] = """
CREATE TABLE {sch}.acq_stage (
  id TEXT, acquisition_id TEXT, acquiring_object_id TEXT, acquired_object_id TEXT, term_code TEXT,
  price_amount TEXT, price_currency_code TEXT, acquired_at TEXT, source_url TEXT,
  source_description TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["acq_insert"] = """
INSERT INTO {sch}.acquisitions(
  id, acquisition_id, acquiring_object_id, acquired_object_id, term_code, price_amount, price_currency_code,
  acquired_at, source_url, source_description, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, acquisition_id, btrim(acquiring_object_id), btrim(acquired_object_id), term_code,
  NULLIF(price_amount,'')::numeric, price_currency_code, NULLIF(acquired_at,'')::date,
  source_url, source_description, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.acq_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o1 WHERE o1.entity_id = btrim(s.acquiring_object_id))
  AND EXISTS (SELECT 1 FROM {sch}.objects o2 WHERE o2.entity_id = btrim(s.acquired_object_id))
ON CONFLICT (acquisition_id) DO NOTHING;
"""

# ipos
SQL["ipos_stage_drop"] = "DROP TABLE IF EXISTS {sch}.ipos_stage;"
SQL["ipos_stage_create"] = """
CREATE TABLE {sch}.ipos_stage (
  id TEXT, ipo_id TEXT, object_id TEXT, valuation_amount TEXT, valuation_currency_code TEXT,
  raised_amount TEXT, raised_currency_code TEXT, public_at TEXT, stock_symbol TEXT,
  source_url TEXT, source_description TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["ipos_insert"] = """
INSERT INTO {sch}.ipos(
  id, ipo_id, object_id, valuation_amount, valuation_currency_code, raised_amount, raised_currency_code,
  public_at, stock_symbol, source_url, source_description, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, ipo_id, btrim(object_id),
  NULLIF(valuation_amount,'')::numeric, valuation_currency_code,
  NULLIF(raised_amount,'')::numeric, raised_currency_code,
  NULLIF(public_at,'')::date, stock_symbol, source_url, source_description,
  NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.ipos_stage s
WHERE EXISTS (SELECT 1 FROM {sch}.objects o WHERE o.entity_id = btrim(s.object_id))
ON CONFLICT (ipo_id) DO NOTHING;
"""

# relationships
SQL["relationships_stage_drop"] = "DROP TABLE IF EXISTS {sch}.relationships_stage;"
SQL["relationships_stage_create"] = """
CREATE TABLE {sch}.relationships_stage (
  id TEXT, relationship_id TEXT, person_object_id TEXT, relationship_object_id TEXT,
  start_at TEXT, end_at TEXT, is_past TEXT, sequence TEXT, title TEXT, created_at TEXT, updated_at TEXT
);
"""
SQL["relationships_insert"] = """
INSERT INTO {sch}.relationships(
  id, relationship_id, person_object_id, relationship_object_id, start_at, end_at, is_past, sequence, title, created_at, updated_at
)
SELECT
  NULLIF(id,'')::bigint, relationship_id, btrim(person_object_id), btrim(relationship_object_id),
  NULLIF(start_at,'')::date, NULLIF(end_at,'')::date,
  CASE LOWER(COALESCE(is_past,'')) WHEN 't' THEN true WHEN 'true' THEN true WHEN '1' THEN true WHEN 'yes' THEN true ELSE false END,
  NULLIF(sequence,'')::int, title, NULLIF(created_at,'')::timestamp, NULLIF(updated_at,'')::timestamp
FROM {sch}.relationships_stage s
ON CONFLICT (relationship_id) DO NOTHING;
"""

# ---------- Лоадер ----------
class CBLoader:
    def __init__(self, conn, schema: str, data_dir: str):
        self.conn = conn
        self.schema = schema
        self.data_dir = data_dir

    def copy_csv(self, full_table: str, csv_path: str):
        sql = f"COPY {full_table} FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8', QUOTE '\"', ESCAPE '\"', NULL '');"
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            self.conn.cursor().copy_expert(sql, f)

    def run_step(self, name: str, drop_sql: str, create_sql: str, stage_table: str, csv_file: str, insert_sql: str):
        cur = self.conn.cursor()
        sch = self.schema
        csv_path = os.path.join(self.data_dir, csv_file)
        log.info("➡️  %s: staging %s", name, csv_path)
        cur.execute(drop_sql.format(sch=sch))
        cur.execute(create_sql.format(sch=sch))
        self.copy_csv(qname(sch, stage_table), csv_path)
        log.info("➡️  %s: inserting into %s", name, qname(sch, name if name != 'acq' else 'acquisitions'))
        cur.execute(insert_sql.format(sch=sch))
        self.conn.commit()
        cur.close()
        log.info("✅ %s done", name)

    def load_all(self):
        steps = [
            ("objects", SQL["objects_stage_drop"], SQL["objects_stage_create"], "objects_stage", "objects.csv", SQL["objects_insert"]),
            ("people", SQL["people_stage_drop"], SQL["people_stage_create"], "people_stage", "people.csv", SQL["people_insert"]),
            ("offices", SQL["offices_stage_drop"], SQL["offices_stage_create"], "offices_stage", "offices.csv", SQL["offices_insert"]),
            ("degrees", SQL["degrees_stage_drop"], SQL["degrees_stage_create"], "degrees_stage", "degrees.csv", SQL["degrees_insert"]),
            ("milestones", SQL["milestones_stage_drop"], SQL["milestones_stage_create"], "milestones_stage", "milestones.csv", SQL["milestones_insert"]),
            ("funds", SQL["funds_stage_drop"], SQL["funds_stage_create"], "funds_stage", "funds.csv", SQL["funds_insert"]),
            ("funding_rounds", SQL["funding_rounds_stage_drop"], SQL["funding_rounds_stage_create"], "funding_rounds_stage", "funding_rounds.csv", SQL["funding_rounds_insert"]),
            ("investments", SQL["investments_stage_drop"], SQL["investments_stage_create"], "investments_stage", "investments.csv", SQL["investments_insert"]),
            ("acq", SQL["acq_stage_drop"], SQL["acq_stage_create"], "acq_stage", "acquisitions.csv", SQL["acq_insert"]),
            ("ipos", SQL["ipos_stage_drop"], SQL["ipos_stage_create"], "ipos_stage", "ipos.csv", SQL["ipos_insert"]),
            ("relationships", SQL["relationships_stage_drop"], SQL["relationships_stage_create"], "relationships_stage", "relationships.csv", SQL["relationships_insert"]),
        ]
        for s in steps:
            self.run_step(*s)

def ensure_schema(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.commit()

def main():
    ap = argparse.ArgumentParser(description="Crunchbase-like CSV loader (staging + cast + FK-checked inserts)")
    ap.add_argument("--host", default=DB_CONFIG['host'])
    ap.add_argument("--port", type=int, default=DB_CONFIG['port'])
    ap.add_argument("--dbname", default=DB_CONFIG['database'])
    ap.add_argument("--user", default=DB_CONFIG['user'])
    ap.add_argument("--password", default=DB_CONFIG['password'])
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    args = ap.parse_args()

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password
    )
    try:
        ensure_schema(conn, args.schema)
        loader = CBLoader(conn, args.schema, args.data_dir)
        loader.load_all()
        log.info("🎉 All done!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

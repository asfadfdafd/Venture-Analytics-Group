#!/usr/bin/env python3
import os
import csv
import argparse
import psycopg2

def read_sql(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        if "--" in line:
            line = line[: line.index("--")]
        lines.append(line)
    text = "\n".join(lines)
    stmts = [s.strip() for s in text.split(";") if s.strip()]
    return stmts

def exec_file(cur, path: str, verbose_name: str):
    print(f"\n=== {verbose_name}: {path} ===")
    for stmt in read_sql(path):
        cur.execute(stmt)

def print_table(cur, title: str, limit: int = 20):
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    print(f"\n--- {title} (rows: {len(rows)}) ---")
    if not rows:
        print("(no rows)")
        return
    # заголовки
    print(" | ".join(cols))
    print("-" * min(120, max(20, sum(len(c) for c in cols) + 3*len(cols))))
    # строки (срез)
    for r in rows[:limit]:
        print(" | ".join(str(x) if x is not None else "" for x in r))
    if len(rows) > limit:
        print(f"... {len(rows) - limit} more")

def export_csv(cur, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow(r)
    print(f"saved: {path}")

def main():
    ap = argparse.ArgumentParser(description="Run assignment SQL: views, indices, checks, analysis, and export CSVs.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--dbname", default="dv_project")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--password", default=None)
    ap.add_argument("--project-dir", default="dv-assignment", help="корень проекта с папками sql/ и exports/")
    args = ap.parse_args()

    sql_dir = os.path.join(args.project_dir, "sql")
    exports_dir = os.path.join(args.project_dir, "exports")

    views_sql    = os.path.join(sql_dir, "views.sql")
    indices_sql  = os.path.join(sql_dir, "indices.sql")
    checks_sql   = os.path.join(sql_dir, "checks.sql")
    analysis_sql = os.path.join(sql_dir, "analysis.sql")

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) вьюхи и индексы
                exec_file(cur, views_sql, "CREATE/REPLACE VIEWS")
                exec_file(cur, indices_sql, "CREATE INDICES")

                # 2) проверки качества — печать в консоль
                print("\n>>> CHECKS")
                for stmt in read_sql(checks_sql):
                    cur.execute(stmt)
                    # заголовок = первые слова запроса
                    title = stmt.replace("\n", " ")[:60] + "..."
                    print_table(cur, title, limit=20)

                # 3) аналитика — печать и экспорт CSV
                print("\n>>> ANALYSIS (print preview)")
                for stmt in read_sql(analysis_sql):
                    cur.execute(stmt)
                    title = stmt.replace("\n", " ")[:60] + "..."
                    print_table(cur, title, limit=20)

                # 4) Обязательные выгрузки CSV (фиксируем три ключевых вывода)
                # Топ компаний по финансированию
                cur.execute("""
                    SELECT o.name, cf.total_raised_usd
                    FROM cb.v_company_funding cf
                    JOIN cb.objects o ON o.entity_id = cf.entity_id
                    ORDER BY cf.total_raised_usd DESC NULLS LAST
                    LIMIT 100;
                """)
                export_csv(cur, os.path.join(exports_dir, "company_funding_top100.csv"))

                # Топ инвесторов
                cur.execute("""
                    SELECT * FROM cb.v_top_investors
                    ORDER BY deals DESC
                    LIMIT 100;
                """)
                export_csv(cur, os.path.join(exports_dir, "top_investors_top100.csv"))

                # Привлечения по годам
                cur.execute("""SELECT * FROM cb.v_raised_by_year ORDER BY year;""")
                export_csv(cur, os.path.join(exports_dir, "raised_by_year.csv"))

        print("См. папку exports/ и лог консоли.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

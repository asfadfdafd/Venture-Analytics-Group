# Venture-Analytics-Group
компания анализирует рынок стартапов и венчурных инвестиций

## Содержание

* [Структура проекта](#структура-проекта)
* [Предварительные требования](#предварительные-требования)
* [Установка и запуск](#установка-и-запуск)
* [Схема БД и источники](#схема-бд-и-источники)
* [Очистка и нормализация](#очистка-и-нормализация)
* [Представления (views)](#представления-views)
* [Экспорты (CSV)](#экспорты-csv)
* [Проверки и базовые запросы](#проверки-и-базовые-запросы)
* [Аналитические темы (10+ запросов)](#аналитические-темы-10-запросов)
* [Трюки и частые ошибки](#трюки-и-частые-ошибки)
* [Лицензия и источники](#лицензия-и-источники)

---

## Структура проекта

```
dv-assignment/
├─ sql/
│  ├─ views.sql          # CREATE OR REPLACE VIEW … (v_objects_with_derived_founded, v_company_funding, v_top_investors, v_raised_by_year)
│  ├─ indices.sql        # индексы для ускорения join/anti-join/нормализации
│  └─ queries.sql        # 10+ аналитических SQL–запросов с комментариями
├─ analysis.sql          # выборки для отчёта + печать результатов в консоль
├─ exports/
│  ├─ top_investors_top100.csv
│  ├─ raised_by_year.csv
│  └─ company_funding_top200.csv
├─ run_assignment.py     # скрипт: применяет views/indices, запускает проверки и делает экспорт CSV
└─ README.md             # этот файл
```mermaid
erDiagram
  objects {
    TEXT id PK
    TEXT name
    TEXT entity_type
    TEXT entity_id
    TEXT parent_id
    TEXT permalink
    TEXT category_code
    TEXT status
    TIMESTAMP founded_at
    TIMESTAMP closed_at
    NUMERIC funding_total_usd
  }

  people {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    TEXT first_name
    TEXT last_name
    TEXT affiliation_name
  }

  degrees {
    TEXT id PK
    TEXT object_id FK "→ people.id"
    TEXT degree_type
    TEXT institution
    TIMESTAMP graduated_at
  }

  relationships {
    TEXT id PK
    TEXT person_object_id FK "→ people.id"
    TEXT relationship_object_id FK "→ objects.id"
    BOOLEAN is_past
    TEXT title
  }

  offices {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    TEXT city
    TEXT country_code
    FLOAT latitude
    FLOAT longitude
  }

  acquisitions {
    TEXT id PK
    TEXT acquiring_object_id FK "→ objects.id"
    TEXT acquired_object_id  FK "→ objects.id"
    NUMERIC price_amount
    TIMESTAMP acquired_at
  }

  funding_rounds {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    TEXT funding_round_type
    NUMERIC raised_amount
    TIMESTAMP funded_at
  }

  investments {
    TEXT id PK
    TEXT funding_round_id FK "→ funding_rounds.id"
    TEXT funded_object_id  FK "→ objects.id"
    TEXT investor_object_id FK "→ objects.id"
    TIMESTAMP created_at
  }

  funds {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    NUMERIC raised_amount
    TIMESTAMP funded_at
  }

  ipos {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    NUMERIC valuation_amount
    NUMERIC raised_amount
    TIMESTAMP public_at
    TEXT stock_symbol
  }

  milestones {
    TEXT id PK
    TEXT object_id FK "→ objects.id"
    TIMESTAMP milestone_at
    TEXT milestone_code
  }

  %% связи
  objects ||--o{ people : "people.object_id"
  people  ||--o{ degrees : "degrees.object_id"
  people  ||--o{ relationships : "relationships.person_object_id"
  objects ||--o{ relationships : "relationships.relationship_object_id"
  objects ||--o{ offices : "offices.object_id"
  objects ||--o{ acquisitions : "acquisitions.acquiring_object_id"
  objects ||--o{ acquisitions : "acquisitions.acquired_object_id"
  objects ||--o{ funding_rounds : "funding_rounds.object_id"
  funding_rounds ||--o{ investments : "investments.funding_round_id"
  objects ||--o{ investments : "investments.funded_object_id"
  objects ||--o{ investments : "investments.investor_object_id"
  objects ||--o{ funds : "funds.object_id"
  objects ||--o{ ipos : "ipos.object_id"
  objects ||--o{ milestones : "milestones.object_id"

## Предварительные требования

* **PostgreSQL 14+** (локально). Порт по умолчанию: `5432`.
* **Python 3.10+** и пакет:

  ```
  pip install psycopg2-binary
  ```
* (Опционально) **pgAdmin 4** для выполнения одиночных запросов/проверок.

---

## Установка и запуск

1. Создай БД и схему (если ещё не созданы):

   ```sql
   CREATE DATABASE dv_project;
   \c dv_project
   CREATE SCHEMA IF NOT EXISTS cb;
   ```

2. Запусти основной скрипт:

   ```bash
   python3 dv-assignment/run_assignment.py \
     --host localhost --port 5432 \
     --dbname dv_project --user postgres --password 0000 \
     --project-dir dv-assignment
   ```

Скрипт:

* применит `sql/views.sql` и `sql/indices.sql`,
* выполнит проверки в консоли,
* сохранит CSV в `dv-assignment/exports/`.

---

## Схема БД и источники

Все таблицы лежат в схеме **`cb`**. Ключевые:

* `cb.objects(id, entity_type, entity_id, name, …)` — справочник всех сущностей.
  Важно: `id` — **с префиксом** (`f:10`, `c:2520`), `entity_id` — **без префикса** (`10`, `2520`).
* `cb.funding_rounds(object_id, funded_at, raised_amount_usd, …)` — раунды финансирования (object_id обычно указывает на компанию/организацию).
* `cb.investments(funding_round_id, funded_object_id, investor_object_id, …)` — факты участия инвесторов в раундах.
* `cb.acquisitions(acquiring_object_id, acquired_object_id, price_amount, term_code, …)` — M&A.

Поля типов `date`, `numeric` и ссылки приведены к корректным типам в процессе загрузки/очистки.

---

## Очистка и нормализация

Чтобы стабильно стыковать ссылки:

* обрезаем пробелы и **NBSP** (неразрывный пробел, `0xA0`), убираем управляющие символы;
* используем функцию `util.norm_id(text)` (создаётся в `views.sql`) для нормализации ID.

Ключевые принципы join:

* там, где ссылки хранятся **с префиксом** (`f:/c:/p:`), стыкуем по `objects.id`;
* если по данным отсутствует запись с полным id, применяем безопасный «фолбэк имени» (см. ниже).

---

## Представления (views)

`sql/views.sql` создаёт:

1. **`cb.v_objects_with_derived_founded`**
   Вычисляет `derived_founded_at` из набора дат (milestone/funding/created_at), если `founded_at` пуст.

2. **`cb.v_company_funding`**
   Суммарные привлечения по компаниям: `total_raised_usd`, `rounds_count`, `last_round_at`.
   Join: `funding_rounds.object_id` ↔ `objects` (нормализовано).

3. **`cb.v_top_investors`**
   Топ инвесторов по числу сделок.

   * Счётчик сделок из `cb.investments`.
   * Имя берём из `objects` по **точному** `id` (префикс + цифра).
   * Если в справочнике записи нет, используем фолбэк:
     `Financial org N` / `Company N` / `Person N`.
   * (Опционально) поддерживает ручные алиасы через маленькую таблицу `cb.investor_alias`.

4. **`cb.v_raised_by_year`**
   Сумма `raised_amount_usd` по годам (`funded_at`).

---

## Экспорты (CSV)

Скрипт пишет итоговые файлы в `exports/`:

* `top_investors_top100.csv` — `investor_id, investor_name, deals`
* `raised_by_year.csv` — `year, raised_usd`
* `company_funding_top100.csv` — `name, total_raised_usd` (или `entity_id,name,total_raised_usd`)

Пример проверки:

```bash
head -n 10 dv-assignment/exports/top_investors_top100.csv
```

---

## Проверки и базовые запросы

Запусти в **pgAdmin → Query Tool**:

```sql
-- 1. Подсчёты строк (быстрая сверка загрузки)
SELECT 'objects'         AS t, COUNT(*) FROM cb.objects         UNION ALL
SELECT 'funding_rounds',      COUNT(*) FROM cb.funding_rounds   UNION ALL
SELECT 'investments',         COUNT(*) FROM cb.investments      UNION ALL
SELECT 'acquisitions',        COUNT(*) FROM cb.acquisitions;

-- 2. JOIN funding_rounds → objects (должно быть > 0)
SELECT COUNT(*) AS join_rows_now
FROM cb.funding_rounds fr
JOIN cb.objects o ON o.entity_id = fr.object_id;

-- 3. «Висячие» в M&A (может оставаться часть из-за дырок в источнике)
SELECT COUNT(*) AS bad_acq_buyers
FROM cb.acquisitions a LEFT JOIN cb.objects o ON o.id = a.acquiring_object_id
WHERE o.id IS NULL;

SELECT COUNT(*) AS bad_acq_targets
FROM cb.acquisitions a LEFT JOIN cb.objects o ON o.id = a.acquired_object_id
WHERE o.id IS NULL;

-- 4. Пустых имён после фолбэков быть не должно
SELECT COUNT(*) AS blanks_left
FROM cb.v_top_investors
WHERE investor_name IS NULL OR investor_name = '';

-- 5. Базовые «WHERE + ORDER BY»
SELECT * FROM cb.objects
WHERE country_code = 'USA'
ORDER BY created_at DESC
LIMIT 10;

-- 6. Агрегации (COUNT/AVG/MIN/MAX)
SELECT funding_round_type,
       COUNT(*) AS n,
       AVG(raised_amount_usd) AS avg_usd,
       MIN(raised_amount_usd) AS min_usd,
       MAX(raised_amount_usd) AS max_usd
FROM cb.funding_rounds
GROUP BY 1
ORDER BY 2 DESC;
```

---

## Аналитические темы (10+ запросов)

Все собраны в `sql/queries.sql` (с комментариями к каждой теме), среди них:

1. Топ стран по total raised.
2. Средний/мин/макс размер раунда по типам.
3. Количество раундов по годам.
4. Топ-20 покупателей M&A.
5. Компании с наибольшим total raised.
6. Активность инвесторов по годам.
7. Средняя сумма раунда по странам.
8. Доля компаний без `founded_at` (используем `derived_founded_at`).
9. ТОП типов раундов по общей сумме.
10. Топ-инвесторы (`v_top_investors`).

> Результаты корректны на основе текущего состояния БД; часть ссылок в исходном датасете отсутствует — это отражено фолбэками.

---

## Трюки и частые ошибки

* **`password authentication failed`** — проверь пользователя/пароль Postgres.
* **`connection refused`** — не запущен сервер (`pg_ctl`/`brew services`).
* **`VACUUM cannot run inside a transaction block`** — выполняй `VACUUM` отдельной командой (не внутри `BEGIN`).
* **`SUM(text)` / `EXTRACT(... text)`** — нужны явные приведения типов (`::numeric`, `::date`).
* **`invalid input syntax for type numeric: ""`** — при `ALTER TYPE` используй `USING NULLIF(col,'')::numeric`.
* **Фантомные пробелы/NBSP в id** — нормализуем через `util.norm_id(text)` перед JOIN.
* **Почему «Financial org 367»?** — в `objects` нет `id='f:367'`; фолбэк — ожидаемое поведение.

---

## Лицензия и источники

Учебный проект. Данные взяты из предоставленного набора CSV (Crunchbase-like).
Используемое ПО: PostgreSQL, Python (`psycopg2-binary`), pgAdmin.

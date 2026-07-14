# Learning dbt with a Retail Sales Project

A hands-on learning project built while following a dbt (data build tool) course on YouTube. The goal was to go beyond watching tutorials and actually build a working dbt project end-to-end — sources, a medallion (Bronze/Silver/Gold) transformation layer, custom macros, tests, seeds, and a snapshot — connected to a real cloud warehouse (Databricks SQL).

This is documented as a **learning log**, not a polished portfolio piece: the intent is to show what was practiced, why each piece exists, and what dbt concept it demonstrates.

**Course followed:** [dbt YouTube playlist](https://www.youtube.com/watch?v=B8uwFmVt4sU&list=PL2bT_3_kYqnfpPwm8AuGpKCkN-nHE1hK4)

---

## 1. What this project is

A retail sales dataset (customers, products, stores, dates, sales, returns, and an "items" feed) is modeled through three layers using dbt on top of a Databricks SQL Warehouse:

```
Source tables (Databricks catalog)
        │
        ▼
   BRONZE  →  1:1 copies of source tables, lightly exposed via dbt sources/models
        │
        ▼
   SILVER  →  cleaned, joined, and aggregated business-level tables
        │
        ▼
    GOLD   →  deduplicated, analysis-ready tables + a Type-2 snapshot for history
```

This mirrors the medallion architecture that's common in modern cloud data warehouses (Databricks, Snowflake, BigQuery), which is why it was chosen as the practice structure instead of a flat set of models.

## 2. Tech stack

| Layer | Tool |
|---|---|
| Transformation framework | [dbt-core](https://github.com/dbt-labs/dbt-core) v1.11 |
| Warehouse adapter | [dbt-databricks](https://github.com/databricks/dbt-databricks) v1.12 |
| Warehouse | Databricks SQL Warehouse (Unity Catalog) |
| Language | SQL + Jinja (dbt's templating layer) |
| Python / package management | Python 3.13, managed with [`uv`](https://docs.astral.sh/uv/) (`pyproject.toml` + `uv.lock`) |

## 3. Project structure

```
Learning-dbt-with-Project/
├── main.py                        # placeholder entry point from the uv project scaffold (not part of the dbt pipeline)
├── pyproject.toml / uv.lock        # Python environment definition
├── logs/dbt.log                    # sample dbt run log (kept to show a real `dbt debug` execution against Databricks)
└── my_first_dbt_project/           # the actual dbt project
    ├── dbt_project.yml             # project config: per-layer materialization + schema routing
    ├── models/
    │   ├── source/sources.yml      # declares the raw source tables dbt reads from
    │   ├── bronze/                 # bronze_customer, bronze_date, bronze_product,
    │   │                           # bronze_returns, bronze_sales, bronze_store
    │   │                           # + properties.yml (schema tests on this layer)
    │   ├── silver/silver_salesinfo.sql   # joined + aggregated sales-by-category-and-gender
    │   └── gold/source_gold_items.sql    # deduplicated "items" table
    ├── snapshots/gold_items.yml    # Type-2 snapshot on top of the gold items model
    ├── seeds/lookup.csv            # small static CSV loaded into the warehouse as a table
    ├── macros/
    │   ├── multiply.sql            # reusable Jinja macro for a SQL expression
    │   └── generate_schema.sql     # overrides dbt's default schema-naming behaviour
    ├── analyses/                   # ad hoc / exploratory SQL, not part of the build DAG
    └── tests/
        ├── non_negative_test.sql          # singular test
        └── generic/generic_non_negative.sql  # reusable generic test definition
```

## 4. Layer-by-layer walkthrough

### Sources (`models/source/sources.yml`)
Rather than hardcoding table names, every raw table (`dim_customer`, `dim_date`, `dim_product`, `dim_store`, `fact_sales`, `fact_returns`, `items`) is declared as a dbt **source**. Bronze models then read from `{{ source('source', 'table_name') }}` instead of the table directly — this is what lets dbt track lineage back to the raw data and lets the source location change (schema/catalog) without touching model SQL.

### Bronze layer (`models/bronze/`)
Each bronze model is a thin 1:1 pull from its source table (e.g. `bronze_sales` = `SELECT * FROM {{ source('source', 'fact_sales') }}`). This layer exists to give every downstream model a single, versioned, testable reference point instead of querying raw tables directly. Bronze models are configured in `dbt_project.yml` to build as **tables** in a dedicated `bronze` schema (with two exceptions — `bronze_date` and `bronze_product` — overridden to materialize as **views** in `properties.yml`, as practice with per-model materialization overrides).

### Silver layer (`models/silver/silver_salesinfo.sql`)
This is where the real transformation logic lives. It:
- Uses CTEs to isolate `sales`, `products`, and `customer` bronze models before joining them
- Calls the custom `multiply()` macro to recompute a `calculated_gross_amount` from `unit_price * quantity`, alongside the original `gross_amount` — practice for cross-checking calculated vs. source values
- Joins sales to products and customers on their surrogate keys
- Aggregates to **total sales by product category and customer gender**, filtering out null genders and ordering by total sales descending

### Gold layer (`models/gold/source_gold_items.sql`)
Takes the raw `items` source and **deduplicates** it using a windowed `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updateDATE DESC)`, keeping only the latest record per `id`. This is a common real-world pattern for handling source feeds that land multiple versions of the same record over time.

### Snapshot (`snapshots/gold_items.yml`)
Sits on top of `source_gold_items` and implements a **Type-2 Slowly Changing Dimension** using dbt's `timestamp` strategy (tracked via the `updateDATE` column). This was built specifically to practice dbt's snapshot feature — capturing how a row's attributes change over time by inserting new versioned rows instead of overwriting history, with `dbt_valid_to_current` set to a far-future sentinel date.

### Seed (`seeds/lookup.csv`)
A small static customer lookup file (`customer_id`, `customer_name`, `customer_email`) loaded into the warehouse via `dbt seed`, used both as a standalone example of seeding and as the source for the `analyses/1_explore.sql` query.

## 5. Macros, tests, and Jinja practice

- **`multiply(col1, col2)`** — a minimal custom macro that returns a SQL multiplication expression, used in the silver model. Practice for writing and calling reusable Jinja macros instead of repeating SQL logic.
- **`generate_schema_name`** — overrides dbt's default macro of the same name so that a model's configured `+schema` value (e.g. `bronze`, `silver`, `gold`) is used directly as the schema name, rather than being appended to the target schema as dbt does by default.
- **Generic custom test — `generic_non_negative`** (`tests/generic/`) — a reusable parameterized test (`{% test %}` block) that flags any row where a given column is negative. Applied to `bronze_sales.gross_amount` via `properties.yml`.
- **Singular test — `non_negative_test.sql`** — a one-off SQL test checking that sales rows aren't negative on both `gross_amount` and `net_amount` simultaneously.
- **Built-in generic tests** — `unique` / `not_null` on `bronze_sales.sales_id` and `bronze_store.store_sk`, plus `accepted_values` tests (with `severity: warn` so the pipeline doesn't hard-fail) validating that `store_name` and `country` only contain expected values — practice for tuning test severity rather than always failing the build.
- **Analyses folder** — used for ad hoc, non-materialized SQL: a plain select off the `lookup` seed, and a query calling the `multiply()` macro directly to sanity-check its output before wiring it into a real model.

## 6. Configuration highlights (`dbt_project.yml`)

Materialization and schema are set **per layer** rather than per model:

```yaml
models:
  my_first_dbt_project:
    bronze:
      +materialized: table
      +schema: bronze
    silver:
      +materialized: table
      +schema: silver
    gold:
      +materialized: table
      +schema: gold
```

This was deliberate practice with dbt's config inheritance — every model in a folder picks up these settings automatically unless overridden locally (as done for `bronze_date` / `bronze_product`).

## 7. What I practiced / learned

- Structuring a dbt project around a medallion architecture (Bronze → Silver → Gold)
- Declaring and referencing **sources** vs. using `ref()` for lineage between models
- Writing and calling **custom Jinja macros**, including overriding a built-in dbt macro (`generate_schema_name`)
- Writing both **generic (parameterized)** and **singular** custom tests, and using built-in tests (`unique`, `not_null`, `accepted_values`) with different severities
- Implementing a **Type-2 snapshot** for change tracking using the `timestamp` strategy
- **Deduplication** using window functions (`ROW_NUMBER`) inside a model
- Connecting dbt to a real cloud warehouse (Databricks SQL Warehouse over Unity Catalog) and running `dbt debug` / `dbt run` / `dbt test` / `dbt snapshot` against it
- Managing the Python/dbt environment with `uv` instead of plain `pip`/`venv`

## 8. Running it locally

This project targets Databricks and needs a `~/.dbt/profiles.yml` with your own workspace credentials (not included in this repo). Rough steps:

```bash
# 1. Install dependencies
uv sync

# 2. Configure your Databricks connection in ~/.dbt/profiles.yml
#    (host, http_path, access token, catalog, schema)

# 3. From the my_first_dbt_project/ directory:
dbt debug      # verify the warehouse connection
dbt seed       # load seeds/lookup.csv
dbt run        # build bronze -> silver -> gold models
dbt test       # run generic + singular tests
dbt snapshot   # run the Type-2 snapshot on gold_items
```

## 9. Known limitations

Since this was built purely for learning, a few things are intentionally left simple and would need work before this pattern was reused on a real dataset:
- No CI/CD (e.g. GitHub Actions running `dbt build` on PRs)
- No `dbt docs` generation/publishing included in the repo
- Test coverage is illustrative (a handful of columns), not comprehensive
- `main.py` is leftover project scaffolding from `uv init` and isn't part of the dbt pipeline

## 10. Possible next steps

- Add a CI workflow that runs `dbt build` against a dev target on every push
- Expand schema tests across all bronze/silver/gold columns
- Generate and publish `dbt docs` (lineage graph) alongside this repo
- Extend the silver layer with a returns-vs-sales reconciliation model

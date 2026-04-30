# Review and dive deeper to dbt-20260419

- [data-engineering-zoomcamp/04-analytics-engineering->Review __class_notes and videos__](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering)


## Tutorial for setup (DuckDB + dbt core) \
### The course setup — two paths:
#### Option A: BigQuery + dbt Cloud (recommended)
- Data warehouse: BigQuery (assuming you set this up in previous weeks)
- dbt: dbt Cloud Developer plan (free account, web IDE)
- No local installation needed \
This is the path most of the videos will follow. It's the fastest way to get started and closest to how teams actually use dbt in production.
#### Option B: DuckDB + dbt Core
- Data warehouse: DuckDB (local or however you've got it set up)
- dbt: dbt Core installed locally
- Dev environment: your own IDE (VS Code, etc.)
- Orchestration: you'll need to handle this separately (Airflow, Prefect, whatever) \
This path gives you more hands-on control but requires more setup.
### The project flow
By the time we get to the end of the module, here's what we'll have built: \
1. Raw data sitting in the warehouse — trip data from previous weeks, plus a lookup table to demonstrate joining multiple sources
2. dbt transformations that turn that raw data into properly modeled tables following the dimensional modeling concepts from 4.1.1
3. Dashboards that consume the final output and make it useful for business stakeholders






## Below are from class_notes
[data-engineering-zoomcamp/04-analytics-engineering/class_notesclass_notes](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/class_notes)
### DE Zoomcamp 4.1.1 — Analytics Engineering Basics
- Why analytics engineering exists \
  A few shifts in the data world created a gap that nobody was filling: \
  - **Cloud data warehouses** (BigQuery, Snowflake, Redshift) made storage and compute cheap. You no longer have to be surgical about what data you load.
  - **EL tools** like Fivetran and Stitch made getting data into the warehouse almost trivial — the extract and load steps are basically automated now.
  - **SQL-first BI tools** like Looker brought version control into the data workflow. And tools like Mode enabled self-service analytics for business users.
  - **Data governance** became a bigger conversation as more people started touching data. \
  All of this changed how data teams work and how stakeholders consume data. But it left a gap between the people building the infrastructure and the people using the data.
- The gap \
  Analysts and scientists are writing more code, but they weren't trained for it. Engineers are great at building systems, but they don't always know how the data gets consumed downstream. Nobody was bridging that gap.
- Analytics Engineer \
  The analytics engineer is the bridge. They bring software engineering best practices — version control, testing, documentation, modularity — into the work that analysts and scientists are already doing. It's a role that sits at the intersection of the data engineer and the data analyst.
  - In terms of the toolchain, an analytics engineer might touch:
    - __Data loading__ — tools like __Fivetran, Stitch (the EL layer)__
    - __Data storing__ — cloud data warehouses, shared territory with data engineers
    - __Data modeling — this is the core of it__. Tools like dbt or Dataform. This is where most of Module 4 lives.
    - __Data presentation — BI tools like Google Looker Studio__. The end product that business users actually see. \
  The focus this week is on modeling and presentation — everything in between "data is in the warehouse" and "business user sees a dashboard."
- ETL vs ELT — a quick recap \
  Two philosophies for getting data transformed and ready:
  - **ETL (Extract → Transform → Load)** — you transform the data *before* it hits the warehouse. Takes longer to set up because the transformation logic has to be built first, but the data in the warehouse is clean and stable from day one.
  - **ELT (Extract → Load → Transform)** — you load the raw data first, then transform it *inside* the warehouse. Faster and more flexible. This is the approach that cloud warehouses made possible — storage is cheap, so just load everything and figure out the transformations later. \
    __ELT is the dominant approach now__, and it's the one we'll be working with. dbt fits squarely into the "T" of ELT — it runs transformations inside the warehouse using SQL.
- __Dimensional Modeling — the key concepts__ \
  - Fact tables vs Dimension tables (Star Schema) \
    - **Fact tables** — measurements, metrics, business events. Think of them as **verbs**. "A sale happened." "An order was placed." They correspond to a business process.
    - **Dimension tables** — the context around those facts. Think of them as **nouns**. "Who bought it? What product? When?" They correspond to a business entity like a customer or a product. \
    Together they form a **star schema** — the fact table in the center, dimension tables radiating out around it. It's the classic layout you'll see in most data warehouses.
  - __Read this book: Kimball's Dimensional Modeling: *The Data Warehouse Toolkit* (Ralph Kimball & Margy Ross)__

### DE Zoomcamp 4.1.2 — What is dbt?
- What is dbt? \
  dbt is a transformation workflow tool. It sits on top of your data warehouse and helps you turn raw data into something useful for downstream consumers (analysts, BI tools, ML pipelines, whatever needs clean, structured data). \
  You write SQL (or Python) to define your transformations, and dbt handles the rest: compiling it, running it against the warehouse, managing dependencies, and persisting the results as tables or views. \
  In a real company setup, you'd have data flowing in from all over the place — backend systems, frontend apps, third-party APIs like weather data. All of that gets loaded into your warehouse (BigQuery, Snowflake, Databricks, whatever), and dbt is the layer that transforms that raw data into something the business can actually consume.
- What problems it solves? \
  The transformation step has always existed. What dbt brings to the table is **software engineering best practices for analytics code**. Things that software engineers have been doing for years but didn't have a clear path into the analytics world: \
  - **Version control** — your transformations live in git, just like any other code
  - **Modularity** — break complex logic into reusable pieces instead of massive spaghetti queries
  - **Testing** — automated data quality checks that run with every deployment
  - **Documentation** — generated from your code, not a separate wiki that gets out of date
  - **Environments** — separate dev and prod. Each developer gets their own sandbox to work in without stepping on each other's toes
  - **CI/CD** — automated deployments with validation and rollback \
  The result is higher-quality pipelines that are easier to maintain and less prone to breaking in production.
- __How it works__ — the mechanics \
  __You write a SQL file. It looks like a normal `SELECT` statement__. __dbt takes that file, figures out where it should go in the warehouse__ (which schema, which dataset, what environment), __wraps it in the necessary DDL/DML, compiles it with any Jinja templating you've used, and runs it__.
  - __When you run `dbt run`__, it:
    1. __Compiles your SQL__ (resolves `ref()` calls, `source()` calls, Jinja macros, everything)
    2. __Sends the compiled SQL to your warehouse__
    3. __Materializes the result as a table, view, incremental table, or ephemeral CTE — whatever you configured__ \
  _You don't write `CREATE TABLE` statements yourself. You just write the `SELECT`, and dbt handles the rest._
- __dbt Core vs dbt Cloud__ \
  There are two ways to use dbt, and it's worth understanding the difference:
  - dbt Core \
    Open source. Free. You __install it locally__ on your machine (or wherever) and run commands from the terminal. You're responsible for:
    - Setting up your dev environment
    - Orchestrating production runs (Airflow, cron jobs, whatever you want)
    - Hosting documentation if you want it accessible
    - Managing logs and metadata \
    It's the raw engine. You get full control, but you also have to build the surrounding infrastructure yourself.
  - dbt Cloud \
    __SaaS product__ that runs dbt Core under the hood. It gives you:
    - A web-based IDE for writing transformations (or you can use a Cloud CLI if you prefer local development)
    - Environment management — dev/staging/prod, all handled for you
    - Built-in orchestration (job scheduling, triggers, dependencies)
    - Hosted documentation (automatically generated and served)
    - Logging and observability
    - APIs for administration and metadata access
    - A semantic layer for metrics (if you need it) \
    __There's a free Developer plan that works for small teams or individual learning__. For anything bigger, it's a paid product.
  - [more details about dbt core and cloud](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/class_notes/4_2_1_dbt_core_vs_dbt_cloud.md)
---
### DE Zoomcamp 4.3.1 — dbt Project Structure
> 📄 Video: [dbt Project Structure](https://www.youtube.com/watch?v=2dYDS4OQbT0)  
> 📄 Official docs: [About dbt projects](https://docs.getdbt.com/docs/build/projects)  
> 📄 __Best practices__: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)

#### Top-Level Files & Folders
- `analysis/`
  - A place for **ad-hoc SQL scripts** that you don't necessarily want to share with stakeholders
  - Not heavily used by everyone, but handy for things like **data quality reports** or **administrative checks**
  - Think of it as a scratchpad — if you want to investigate how bad a data quality issue is, drop a SQL script here
- __`dbt_project.yml`__
  - **The most important file in a dbt project**
  - Every time you run a dbt command, dbt looks for this file first — if it's missing, the command fails
  - Key things it contains:
    - Project name
    - Profile name (must match your `profiles.yml` — critical for dbt Core users)
    - Default materializations
    - Variables
  - Also a place to set project-wide defaults and configuration \
  > 📄 [dbt_project.yml reference](https://docs.getdbt.com/reference/dbt_project.yml)
- __`macros/`__
  - Macros behave like **reusable functions** (similar to Python functions or UDFs)
  - Use them when you find yourself **repeating the same SQL logic** in multiple places, or when you want to **encapsulate a piece of logic** in one place
  - Benefits:
    - Easier to test (you're testing a small, isolated chunk)
    - If a definition changes, you only update it in one place
  - Common use cases:
    - **Calendar conversions** (e.g. converting standard dates to a company's fiscal calendar)
    - **Tax rates or regulatory definitions** that might change over time
    - Any reusable business logic that shouldn't be duplicated across models \
    > 📄 [Jinja and macros](https://docs.getdbt.com/docs/build/jinja-macros)
- `models/`: The **most important directory** — this is where all your SQL transformation logic lives. dbt suggests breaking it into **three subfolders** (see below)
- `README.md`
  - Standard project documentation — the first thing someone sees when they open your project
  - dbt creates a default one, but most teams customize it
  - Good things to include:
    - How to run the project
    - Whether you need credentials or onboarding
    - Contact information
    - Installation/setup guides
- `seeds/`
  - A place to **upload CSV or flat files** and ingest them as dbt models in your database
  - Considered a **quick-and-dirty** approach — if you have the option, it's better to load data properly at the source
  - Useful for:
    - **Lookup tables**
    - Quick experiments or prototypes
    - Showing a stakeholder something before fully committing to a data load
  - Use when you don't have the right permissions, or the data is expected to change frequently during experimentation \
  > 📄 [Seeds](https://docs.getdbt.com/docs/build/seeds)
- `snapshots/`
  - Solves a specific problem: a source table has a column that **overwrites itself**, but you need to **keep the history**
  - Example: an `orders` table with a `current_status` column that only ever shows the latest status. For analytics, you want to know *when* each status changed
  - How it works: a snapshot takes a **"picture" of a table at a point in time**. Each time you run it, if a value has changed, a new row is recorded with a timestamp — without overwriting the previous value
  - Like seeds, this is a **workaround** — ideally you'd solve this at the source. But if you don't control the source, snapshots work well \
  > 📄 [Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- `tests/`
  - A place for **singular tests** written as SQL assertions
  - The logic is simple: **if the query returns more than zero rows, the dbt build fails**
  - Example from the course: a client needed to ensure that vehicle timestamps always covered exactly 24 hours per day. A test query checked for any day where the total hours deviated from 24 — catching logic errors like accidental filters or bad joins early
  - This is one of several ways to test in dbt, but singular tests are especially good for **custom business rules** that don't fit standard schema tests \
  > 📄 [Data tests (singular & generic)](https://docs.getdbt.com/docs/build/data-tests)
#### The `models/` Subfolders \
dbt suggests organizing models into three layers:
- `staging/`
  - Contains two things:
    - **Source definitions** — telling dbt where your raw data lives in the database
    - **Staging models** — a **1:1 copy** of each source table with only **minimal cleaning** applied
  - Minimal cleaning means things like:
    - Fixing data types
    - Renaming columns
    - Filtering out clearly empty rows
    - Removing unnecessary columns
    - Standardizing values
  - Keep it **1:1** — same number of rows and columns as the raw source. Breaking this rule is occasionally convenient but should be the exception
- `intermediate/`
  - Everything that is **not raw** and **not ready to expose** to end users
  - A catch-all for:
    - Complex joins
    - Heavy-duty cleaning or standardization
    - Data quality processing
  - No strict guidelines on what goes here — if it doesn't fit neatly into staging or marts, it belongs in intermediate
- `marts/`
  - Where all the **final, consumption-ready** tables live
  - If it's in marts, it's **ready for end users**
  - In a well-governed dbt project, **only marts tables should be exposed** to BI tools, analysts, and business stakeholders — nothing else
  - Typically contains:
    - Tables ready for dashboards
    - Properly modeled, clean tables
    - Often star schemas, but not necessarily
#### A Note on Conventions
The `staging → intermediate → marts` structure is dbt's recommendation, but it's not mandatory. The instructor has seen teams use:
- **Medallion architecture** naming: `bronze`, `silver`, `gold`
- Numbered layers: `first`, `second`, `third`, `last`
- Other custom conventions
---

### DE Zoomcamp 4.3.2 — dbt Sources

### DE Zoomcamp 4.4.1 — dbt Models
### DE Zoomcamp 4.4.2 — dbt Seeds and Macros
### DE Zoomcamp 4.5.1 — Documentation
### DE Zoomcamp 4.5.2 — dbt Tests
### DE Zoomcamp 4.5.3 — dbt Packages
### DE Zoomcamp 4.6.1 — dbt Commands



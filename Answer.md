# Task 1 - Data modelling 

## Task 1.1. - Clearly explain your choice of fact and dimension tables.

We use a star-like schema with two fact tables, three conformed dimensions, and a simple bridge.

* fct_fund_event: supports NAV calculation and the ownership numerator (cumulative commitments).
* fct_company_valuation: baseline for company NAV before ownership scaling.
* dim_fund: conformed attributes for slicing and to supply the denominator in the ownership calculation.
* dim_company: consistent company attributes across funds (a company can appear in multiple funds).
* bridge_fund_company: quick relationship checks, exploration, and referential integrity tests

### From a technical standpoint, what are your observations about the data extract?
* Missing a data dictionnary 
* Avoid Excel as data source, priviledge API / DB or, if flat file, consider parquet (size, data type...)
* Multiple valuations per date: need a business rule → keep highest transaction_index.
* Mixed transactional & static data: fund_size is static (“latest”), while events are transactional.
* Sparse dates: events are not daily; use as-of logic or construct a date spine for daily reporting
* We need to define a process to refresh data 


### What potential challenges might arise when working with this data?
* Ambiguity on same-day valuations (handled with transaction_index).
* Changing fund_size over time (brief says “latest”; if not true in reality, ownership will drift).
* Entity resolution: fund names may vary; we would need a fund id.
* Country/region standardization: handled via seed mapping, but coverage must be maintained.
* Currency: not present; if multi-currency shows up later, NAVs and ownership must be FX-aware

### Can you identify any data quality issues or inconsistencies?
* Inconsistent casing/whitespace (fixed in staging).
* Occasional missing data for company.
* Extra spaces or punctuation in headers (Excel exports).
* Index as float, Int expected 

### If you were responsible for designing this data extract, what additional information would you include to improve usability?
* Currencies for all amounts + FX rates/effective dates (or a separate FX table).
* Explicit “as_of” valuation timestamps and a business “version” field to avoid relying solely on transaction_index.
* Historical fund_size (effective dates)
* Unique IDs for funds (natural keys can be brittle).
* Data lineage/ingest metadata (file name, load timestamp).
* Transaction source/system code for traceability.
* not Excel 

### What would the end-to-end pipeline look like for ingesting this data into a Snowflake data warehouse?

**NOTE: I have not worked hands-on with Snowflake yet; the design below is based on gen-AI assistance, common sense from data warehousing, and quick research. **
1. Landing
    * Drop CSVs from Excel into S3 (or directly into Snowflake stages if preferred).
    * Ideally go away from Excel CSV to API / DB / parquet

2. Externalization / Ingestion 
    * Create external stage (S3) and external tables for fund_data_ext, company_data_ext.

3. dbt Staging (stg_fund, stg_company)
    * Clean/trim/cast types; normalize transaction types; enforce signs; add ingest metadata.
    * Standardize country/region with seed mapping.

4. dbt Intermediate
    * Build dim_fund, dim_company, bridge_fund_company.
    * Build fct_fund_event, fct_company_valuation.
    * Deduplicate by max transaction_index as needed.

5. dbt Marts
    * Fund NAV (latest valuation + flows since anchor).
    * Company NAV using ownership = commitments_to_date / fund_size.
    
6. Tests & Docs
    * create relevant tests: e.g. not_null, accepted_values, relationships, uniqueness (“unique combination of columns”).
    * Generate dbt docs; publish via dbt Cloud.

7. Orchestration
    * methodology TBD
    * Alerts on failures 
    * report on source freshness 

8. Consumption
    * Dahsboard to report costs 





# Task 2 - SQL Exercises

## Question 2.1 
Assumptions 
* All amounts are in a single currency.
* fund_size is the latest snapshot and acceptable for ownership.
* The rule “pick max transaction_index per day” reflects the intended business logic for multiple transaction.
* Flows include Calls (capital calls), Distributions (cash returned), and optionally Commitments
* Distributions should be negative, Calls positive
* Calendar granularity: reporting on the union of event dates


## Question 2.4
difference exist on Summitvale Equity Group fordate 31/12/2020 and 31/03/2021

NAV: most recent valuation of the fund + calls - distributions 

Method 1 — Per-company NAV = (sum. Commitments up to date ÷ latest Fund Size) × company valuation
Method 2 — Per-company NAV = (Fund NAV ÷ Σ company valuations on that date) x company valuation

Method 1 bases company NAV only on commitments and fund size. It ignores the impact of calls and distributions on the fund’s NAV, so the sum of company NAVs may not match the actual reported fund NAV.

Method 2 (scaled to fund NAV) takes the reported fund NAV (which already reflects calls, distributions, and other cash movements) and proportionally scales company valuations so that they reconcile exactly.


# Task 3 - Testing and Data Quality in dbt
## What other data tests would you recommend for this dataset?
* Controlled lists: sectors, countries → accepted_values or check against seed lookups.
* Country mapping coverage: rows in staging not matched by country_map seed.
* Freshness (error if last refresh older than... )
* Distributions should be negative, Calls positive

## How would you monitor data quality over time?
* Automate dbt jobs in Cloud (Prod): dbt deps → seed → build → test → docs generate.
* Alerting on failures (email/Slack/webhooks/...).
* Track failing rows with singular tests that materialize into QA views (easy to inspect).
* track  anomaly detection, volume drift, schema changes.
* Dashboards to track quality, freshness, volume, costs...

## What edge cases should be considered in testing?
* fund_size zero or null → ownership% forced to 0; surface as QA.
* Signs: Distributions must be negative; commitments/calls ≥ 0.
* Whitespace/case diffs in fund_name/company_name → normalize in staging and test trim()/case rules if needed.
* “N/A” strings → null mapping verified.
* Transaction_index dust (e.g., 1.00000021) → round/ceil in staging and assert integer positivity.
* Currency: if multi-currency ever appears, add currency codes + FX conformity tests. Check FX across lines for one date 

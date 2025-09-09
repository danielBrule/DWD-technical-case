# Task 1 - Data modelling 

## Task 1.1. - Clearly explain your choice of fact and dimension tables.

We use a star-like schema with two fact tables, three conformed dimensions, and a simple bridge.

### Fact tables

fct_fund_event
* Grain: one row per (fund, transaction_date, transaction_type, transaction_index).
* Content: all fund-level events — Valuation, Commitment/Call, Distribution.
* Purpose: supports NAV calculation and the ownership numerator (cumulative commitments).
* Key columns: fund_key, transaction_date, transaction_type, transaction_index, amount.

**Notes**: enforce signs (Distributions < 0; Commitments/Calls > 0). Deduplicate using the highest transaction_index per day/type.

fct_company_valuation

Grain: one row per (fund, company, valuation_date, transaction_index).

Content: reported company-level valuation amounts.

Purpose: baseline for company NAV before ownership scaling.

Key columns: fund_key, company_id, company_name, valuation_date, valuation_amount, transaction_index.

Notes: deduplicate multiple same-day valuations with the highest transaction_index.

(Optional performance layer: precompute daily NAV in fct_fund_nav_daily or monthly company NAV in fct_company_nav_m1. These can always be derived on the fly from the two facts above.)
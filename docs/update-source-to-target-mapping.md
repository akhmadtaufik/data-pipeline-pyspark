# Source to Target Mapping

This document outlines the transformation process from staging tables to data warehouse tables, including handling of missing values identified in the data profiling.

## ETL Process Overview

1. Extract from source systems (database, CSV)
2. Load data to staging area
3. Extract from staging area
4. Transform data (handling missing values, data type conversions, lookups)
5. Load to data warehouse

## Dimension Tables

### Company Dimension

| Source Table: `staging.company` | Target Table: `warehouse.dim_company` | Description | Missing Value Handling |
|--------------------------------|-------------------------------------|-------------|------------------------|
| - | `company_id` (UUID) | Generated UUID as surrogate key | N/A |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `description` (TEXT) | `description` (TEXT) | Direct mapping | No missing values (0%) |
| `region` (VARCHAR) | `region` (VARCHAR) | Direct mapping | No missing values (0%) |
| `city` (VARCHAR) | `city` (VARCHAR) | Direct mapping | No missing values (0%) |
| `state_code` (VARCHAR) | `state_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `country_code` (VARCHAR) | `country_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) | N/A |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) | N/A |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Location Dimension

| Source Table: `staging.company` | Target Table: `warehouse.dim_location` | Description | Missing Value Handling |
|--------------------------------|--------------------------------------|-------------|------------------------|
| - | `location_id` (UUID) | Generated UUID as surrogate key | N/A |
| `office_id` (VARCHAR) | `office_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `region` (VARCHAR) | `region` (VARCHAR) | Direct mapping | No missing values (0%) |
| `address1` (TEXT) | `address1` (TEXT) | Direct mapping | No missing values (0%) |
| `address2` (TEXT) | `address2` (TEXT) | Direct mapping | No missing values (0%) |
| `city` (VARCHAR) | `city` (VARCHAR) | Direct mapping | No missing values (0%) |
| `zip_code` (VARCHAR) | `zip_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `state_code` (VARCHAR) | `state_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `country_code` (VARCHAR) | `country_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `latitude` (DECIMAL) | `latitude` (DECIMAL) | Direct mapping | No missing values (0%) |
| `longitude` (DECIMAL) | `longitude` (DECIMAL) | Direct mapping | No missing values (0%) |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) | N/A |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) | N/A |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Person Dimension

| Source Table: `staging.people` | Target Table: `warehouse.dim_person` | Description | Missing Value Handling |
|-------------------------------|----------------------------------|-------------|------------------------|
| - | `person_id` (UUID) | Generated UUID as surrogate key | N/A |
| `people_id` (VARCHAR) | `people_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `first_name` (VARCHAR) | `first_name` (VARCHAR) | Direct mapping | No missing values (0%) |
| `last_name` (VARCHAR) | `last_name` (VARCHAR) | Direct mapping | No missing values (0%) |
| CONCAT(`first_name`, ' ', `last_name`) | `full_name` (VARCHAR) | Derived field | Derived from non-missing fields |
| `birthplace` (VARCHAR) | `birthplace` (VARCHAR) | Direct mapping | Set to NULL if missing (87.61% missing) |
| `affiliation_name` (VARCHAR) | `affiliation_name` (VARCHAR) | Direct mapping | Set to NULL if missing (0.01% missing) |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) | N/A |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) | N/A |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | Default to current timestamp if missing |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | Default to current timestamp if missing |

### Fund Dimension

| Source Table: `staging.funds` | Target Table: `warehouse.dim_fund` | Description | Missing Value Handling |
|------------------------------|--------------------------------|-------------|------------------------|
| - | `fund_id` (UUID) | Generated UUID as surrogate key | N/A |
| `fund_id` (VARCHAR) | `fund_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `name` (VARCHAR) | `name` (VARCHAR) | Direct mapping | No missing values (0%) |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping | No missing values (0%) |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping | No missing values (0%) |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) | N/A |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) | N/A |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Investor Dimension

| Source Table: `staging.investments` | Target Table: `warehouse.dim_investor` | Description | Missing Value Handling |
|------------------------------------|-----------------------------------|-------------|------------------------|
| - | `investor_id` (UUID) | Generated UUID as surrogate key | N/A |
| `investor_object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| - | `investor_type` (VARCHAR) | Derived from joining with other tables | Set to 'Unknown' if not determinable |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) | N/A |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) | N/A |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Relationship Type Dimension

| Source Table: `staging.relations` | Target Table: `warehouse.dim_relationship_type` | Description | Missing Value Handling |
|--------------------------------------|-------------------------------------------|-------------|------------------------|
| - | `relationship_type_id` (UUID) | Generated UUID as surrogate key | N/A |
| DISTINCT `title` (VARCHAR) | `title` (VARCHAR) | Unique values extracted | Filter out NULL values (4.03% missing) |
| - | `description` (TEXT) | Default NULL (can be updated later) | N/A |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) | N/A |

### Milestone Type Dimension

| Source Table: `staging.milestones` | Target Table: `warehouse.dim_milestone_type` | Description | Missing Value Handling |
|------------------------------------|----------------------------------------|-------------|------------------------|
| - | `milestone_type_id` (UUID) | Generated UUID as surrogate key | N/A |
| DISTINCT `milestone_code` (VARCHAR) | `milestone_code` (VARCHAR) | Unique values extracted | N/A - Not in profiling data |
| - | `description` (TEXT) | Default NULL (can be updated later) | N/A |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) | N/A |

### Round Type Dimension

| Source Table: `staging.funding_rounds` | Target Table: `warehouse.dim_round_type` | Description | Missing Value Handling |
|---------------------------------------|-------------------------------------|-------------|------------------------|
| - | `round_type_id` (UUID) | Generated UUID as surrogate key | N/A |
| DISTINCT `funding_round_type` (VARCHAR) | `funding_round_type` (VARCHAR) | Unique values extracted | No missing values (0%) |
| DISTINCT `funding_round_code` (VARCHAR) | `funding_round_code` (VARCHAR) | Unique values extracted | No missing values (0%) |
| - | `description` (TEXT) | Default NULL (can be updated later) | N/A |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) | N/A |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) | N/A |

### Date Dimension

| Source | Target Table: `warehouse.dim_date` | Description | Missing Value Handling |
|--------|--------------------------------|-------------|------------------------|
| Generated | `date_id` (INT) | Formatted as YYYYMMDD | N/A |
| Generated | `date_actual` (DATE) | Generated date value | N/A |
| Generated | `year` (INT) | Extracted year from date | N/A |
| Generated | `quarter` (INT) | Extracted quarter from date | N/A |
| Generated | `month` (INT) | Extracted month from date | N/A |
| Generated | `month_name` (VARCHAR) | Extracted month name from date | N/A |
| Generated | `day` (INT) | Extracted day from date | N/A |
| Generated | `day_of_week` (INT) | Calculated day of week | N/A |
| Generated | `day_name` (VARCHAR) | Calculated day name | N/A |
| Generated | `is_weekend` (BOOLEAN) | Calculated based on day of week | N/A |
| Generated | `week_of_year` (INT) | Calculated week number | N/A |
| Generated | `is_leap_year` (BOOLEAN) | Calculated based on year | N/A |
| Generated | `created_at` (TIMESTAMP) | Default value (current timestamp) | N/A |
| Generated | `updated_at` (TIMESTAMP) | Default value (current timestamp) | N/A |

## Fact Tables

### Funding Round Fact

| Source Table: `staging.funding_rounds` | Target Table: `warehouse.fact_funding_round` | Description | Missing Value Handling |
|---------------------------------------|----------------------------------------|-------------|------------------------|
| - | `funding_round_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `funding_round_id` (VARCHAR) | `funding_round_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| `funding_round_type`, `funding_round_code` | `round_type_id` (UUID) | Lookup from dim_round_type | No missing values (0%) |
| TO_CHAR(`funded_at`, 'YYYYMMDD')::INT | `funded_date_id` (INT) | Converted to date_id format | Set to NULL if missing (41% missing), consider adding a "Unknown Date" record in dim_date |
| `raised_amount_usd` (DECIMAL) | `raised_amount_usd` (DECIMAL) | Direct mapping | No missing values (0%) |
| `raised_amount` (DECIMAL) | `raised_amount` (DECIMAL) | Direct mapping | No missing values (0%) |
| `raised_currency_code` (VARCHAR) | `raised_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `pre_money_valuation_usd` (DECIMAL) | `pre_money_valuation_usd` (DECIMAL) | Direct mapping | No missing values (0%) |
| `pre_money_valuation` (DECIMAL) | `pre_money_valuation` (DECIMAL) | Direct mapping | No missing values (0%) |
| `pre_money_currency_code` (VARCHAR) | `pre_money_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `post_money_valuation_usd` (DECIMAL) | `post_money_valuation_usd` (DECIMAL) | Direct mapping | No missing values (0%) |
| `post_money_valuation` (DECIMAL) | `post_money_valuation` (DECIMAL) | Direct mapping | No missing values (0%) |
| `post_money_currency_code` (VARCHAR) | `post_money_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `participants` (INTEGER) | `participants` (INTEGER) | Direct mapping, convert string to integer | No missing values (0%) |
| `is_first_round` (BOOLEAN) | `is_first_round` (BOOLEAN) | Direct mapping | No missing values (0%) |
| `is_last_round` (BOOLEAN) | `is_last_round` (BOOLEAN) | Direct mapping | No missing values (0%) |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping | No missing values (0%) |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping | No missing values (0%) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Investment Fact

| Source Table: `staging.investments` | Target Table: `warehouse.fact_investment` | Description | Missing Value Handling |
|------------------------------------|-------------------------------------|-------------|------------------------|
| - | `investment_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `investment_id` (VARCHAR) | `investment_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `funding_round_id` (VARCHAR) | `funding_round_fact_id` (UUID) | Lookup from fact_funding_round | No missing values (0%) |
| `investor_object_id` (VARCHAR) | `investor_id` (UUID) | Lookup from dim_investor | No missing values (0%) |
| `funded_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| JOIN with funding_rounds | `investment_date_id` (INT) | Derived from funding round date | Set to NULL if missing (dependent on funding_rounds.funded_at which has 41% missing) |
| - | `investment_amount` (DECIMAL) | NULL (data not available) | Set to NULL |
| - | `investment_currency_code` (VARCHAR) | NULL (data not available) | Set to NULL |
| - | `source_url` (TEXT) | NULL (data not available) | Set to NULL |
| - | `source_description` (TEXT) | NULL (data not available) | Set to NULL |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Acquisition Fact

| Source Table: `staging.acquisition` | Target Table: `warehouse.fact_acquisition` | Description | Missing Value Handling |
|------------------------------------|-------------------------------------|-------------|------------------------|
| - | `acquisition_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `acquisition_id` (VARCHAR) | `acquisition_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `acquiring_object_id` (VARCHAR) | `acquiring_company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| `acquired_object_id` (VARCHAR) | `acquired_company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| TO_CHAR(`acquired_at`, 'YYYYMMDD')::INT | `acquisition_date_id` (INT) | Converted to date_id format | Set to NULL if missing (16% missing), consider adding a "Unknown Date" record in dim_date |
| `term_code` (VARCHAR) | `term_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `price_amount` (DECIMAL) | `price_amount` (DECIMAL) | Direct mapping | No missing values (0%) |
| `price_currency_code` (VARCHAR) | `price_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping | No missing values (0%) |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping | No missing values (0%) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### IPO Fact

| Source Table: `staging.ipos` | Target Table: `warehouse.fact_ipo` | Description | Missing Value Handling |
|------------------------------|------------------------------|-------------|------------------------|
| - | `ipo_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `ipo_id` (VARCHAR) | `ipo_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| TO_CHAR(`public_at`, 'YYYYMMDD')::INT | `ipo_date_id` (INT) | Converted to date_id format | Set to NULL if missing (47.82% missing), consider adding a "Unknown Date" record in dim_date |
| `valuation_amount` (DECIMAL) | `valuation_amount` (DECIMAL) | Direct mapping | No missing values (0%) |
| `valuation_currency_code` (VARCHAR) | `valuation_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `raised_amount` (DECIMAL) | `raised_amount` (DECIMAL) | Direct mapping | No missing values (0%) |
| `raised_currency_code` (VARCHAR) | `raised_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `stock_symbol` (VARCHAR) | `stock_symbol` (VARCHAR) | Direct mapping | No missing values (0%) |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping | No missing values (0%) |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping | No missing values (0%) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Relationship Fact

| Source Table: `staging.relations` | Target Table: `warehouse.fact_relationship` | Description | Missing Value Handling |
|--------------------------------------|--------------------------------------|-------------|------------------------|
| - | `relationship_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `relationship_id` (VARCHAR) | `relationship_id_nk` (VARCHAR) | Renamed as natural key | No missing values (0%) |
| `person_object_id` (VARCHAR) | `person_id` (UUID) | Lookup from dim_person | No missing values (0%) |
| `relationship_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| `title` (VARCHAR) | `relationship_type_id` (UUID) | Lookup from dim_relationship_type | Set to "Unknown" relationship type if missing (4.03% missing) |
| TO_CHAR(`start_at`, 'YYYYMMDD')::INT | `start_date_id` (INT) | Converted to date_id format | Set to NULL if missing (53.48% missing), consider adding a "Unknown Date" record in dim_date |
| TO_CHAR(`end_at`, 'YYYYMMDD')::INT | `end_date_id` (INT) | Converted to date_id format | Set to NULL if missing (85.71% missing) |
| `is_past` (BOOLEAN) | `is_past` (BOOLEAN) | Direct mapping | No missing values (0%) |
| `sequence` (INTEGER) | `sequence` (INTEGER) | Direct mapping | No missing values (0%) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

### Fund Fact

| Source Table: `staging.funds` | Target Table: `warehouse.fact_fund` | Description | Missing Value Handling |
|------------------------------|------------------------------|-------------|------------------------|
| - | `fund_fact_id` (UUID) | Generated UUID as surrogate key | N/A |
| `fund_id` (VARCHAR) | `fund_id` (UUID) | Lookup from dim_fund | No missing values (0%) |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company | No missing values (0%) |
| TO_CHAR(`funded_at`, 'YYYYMMDD')::INT | `funded_date_id` (INT) | Converted to date_id format | Set to NULL if missing (7.5% missing), consider adding a "Unknown Date" record in dim_date |
| `raised_amount` (DECIMAL) | `raised_amount` (DECIMAL) | Direct mapping | No missing values (0%) |
| `raised_currency_code` (VARCHAR) | `raised_currency_code` (VARCHAR) | Direct mapping | No missing values (0%) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping | No missing values (0%) |

## ETL Processing Order

For optimal loading with proper referential integrity, follow this order:

1. **Dimension Tables (Independent)**:
   - `warehouse.dim_date` (Generate dates from 2000-01-01 to 2024-12-31)
   - `warehouse.dim_company` (from staging.company)
   - `warehouse.dim_location` (from staging.company)
   - `warehouse.dim_person` (from staging.people)
   - `warehouse.dim_fund` (from staging.funds)
   - `warehouse.dim_relationship_type` (from staging.relations)
   - `warehouse.dim_round_type` (from staging.funding_rounds)

2. **Fact Tables (Dependent on Dimensions)**:
   - `warehouse.fact_funding_round` (from staging.funding_rounds)
   - `warehouse.dim_investor` (from staging.investments, after funding_round creation)
   - `warehouse.fact_ipo` (from staging.ipos)
   - `warehouse.fact_acquisition` (from staging.acquisition)
   - `warehouse.fact_relationship` (from staging.relations)
   - `warehouse.fact_fund` (from staging.funds)

3. **Fact Tables (Dependent on Other Facts)**:
   - `warehouse.fact_investment` (from staging.investments, after fact_funding_round creation)

## Missing Value Handling Summary

Based on profiling, the following fields have significant missing values that require handling:

1. **High Missing Values (>50%)**:
   - `end_at` in staging.relations (85.71%)
   - `birthplace` in staging.people (87.61%)
   - `start_at` in staging.relations (53.48%)

2. **Moderate Missing Values (10-50%)**:
   - `public_at` in staging.ipos (47.82%)
   - `funded_at` in staging.funding_rounds (41%)

3. **Low Missing Values (<10%)**:
   - `acquired_at` in staging.acquisition (16%)
   - `funded_at` in staging.funds (7.5%)
   - `title` in staging.relations (4.03%)
   - `affiliation_name` in staging.people (0.01%)

**Special Considerations:**

1. Create an "Unknown Date" record in dim_date (e.g., date_id = 19000101) to handle missing dates
2. Create an "Unknown" record in relationship type dimension for missing titles
3. Document all missing value handling in ETL logs for auditing purposes

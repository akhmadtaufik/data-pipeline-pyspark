# Source to Target Mapping

This document outlines the transformation process from staging tables to data warehouse tables.

## Dimension Tables

### Company Dimension

| Source Table: `staging.company` | Target Table: `warehouse.dim_company` | Description |
|--------------------------------|-------------------------------------|-------------|
| - | `company_id` (UUID) | Generated UUID as surrogate key |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key |
| `description` (TEXT) | `description` (TEXT) | Direct mapping |
| `region` (VARCHAR) | `region` (VARCHAR) | Direct mapping |
| `city` (VARCHAR) | `city` (VARCHAR) | Direct mapping |
| `state_code` (VARCHAR) | `state_code` (VARCHAR) | Direct mapping |
| `country_code` (VARCHAR) | `country_code` (VARCHAR) | Direct mapping |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Location Dimension

| Source Table: `staging.company` | Target Table: `warehouse.dim_location` | Description |
|--------------------------------|--------------------------------------|-------------|
| - | `location_id` (UUID) | Generated UUID as surrogate key |
| `office_id` (VARCHAR) | `office_id_nk` (VARCHAR) | Renamed as natural key |
| `region` (VARCHAR) | `region` (VARCHAR) | Direct mapping |
| `address1` (TEXT) | `address1` (TEXT) | Direct mapping |
| `address2` (TEXT) | `address2` (TEXT) | Direct mapping |
| `city` (VARCHAR) | `city` (VARCHAR) | Direct mapping |
| `zip_code` (VARCHAR) | `zip_code` (VARCHAR) | Direct mapping |
| `state_code` (VARCHAR) | `state_code` (VARCHAR) | Direct mapping |
| `country_code` (VARCHAR) | `country_code` (VARCHAR) | Direct mapping |
| `latitude` (DECIMAL) | `latitude` (DECIMAL) | Direct mapping |
| `longitude` (DECIMAL) | `longitude` (DECIMAL) | Direct mapping |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Person Dimension

| Source Table: `staging.people` | Target Table: `warehouse.dim_person` | Description |
|-------------------------------|----------------------------------|-------------|
| - | `person_id` (UUID) | Generated UUID as surrogate key |
| `people_id` (VARCHAR) | `people_id_nk` (VARCHAR) | Renamed as natural key |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key |
| `first_name` (VARCHAR) | `first_name` (VARCHAR) | Direct mapping |
| `last_name` (VARCHAR) | `last_name` (VARCHAR) | Direct mapping |
| CONCAT(`first_name`, ' ', `last_name`) | `full_name` (VARCHAR) | Derived field |
| `birthplace` (VARCHAR) | `birthplace` (VARCHAR) | Direct mapping |
| `affiliation_name` (VARCHAR) | `affiliation_name` (VARCHAR) | Direct mapping |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Fund Dimension

| Source Table: `staging.funds` | Target Table: `warehouse.dim_fund` | Description |
|------------------------------|--------------------------------|-------------|
| - | `fund_id` (UUID) | Generated UUID as surrogate key |
| `fund_id` (VARCHAR) | `fund_id_nk` (VARCHAR) | Renamed as natural key |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key |
| `name` (VARCHAR) | `name` (VARCHAR) | Direct mapping |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Investor Dimension

| Source Table: `staging.investments` | Target Table: `warehouse.dim_investor` | Description |
|------------------------------------|-----------------------------------|-------------|
| - | `investor_id` (UUID) | Generated UUID as surrogate key |
| `investor_object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Renamed as natural key |
| - | `investor_type` (VARCHAR) | Derived from joining with other tables |
| - | `valid_from` (TIMESTAMP) | Default value (current timestamp) |
| - | `valid_to` (TIMESTAMP) | Default value (NULL for current record) |
| - | `is_current` (BOOLEAN) | Default value (TRUE for current record) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Relationship Type Dimension

| Source Table: `staging.relationships` | Target Table: `warehouse.dim_relationship_type` | Description |
|--------------------------------------|-------------------------------------------|-------------|
| - | `relationship_type_id` (UUID) | Generated UUID as surrogate key |
| DISTINCT `title` (VARCHAR) | `title` (VARCHAR) | Unique values extracted |
| - | `description` (TEXT) | Default NULL (can be updated later) |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) |

### Milestone Type Dimension

| Source Table: `staging.milestones` | Target Table: `warehouse.dim_milestone_type` | Description |
|------------------------------------|----------------------------------------|-------------|
| - | `milestone_type_id` (UUID) | Generated UUID as surrogate key |
| DISTINCT `milestone_code` (VARCHAR) | `milestone_code` (VARCHAR) | Unique values extracted |
| - | `description` (TEXT) | Default NULL (can be updated later) |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) |

### Round Type Dimension

| Source Table: `staging.funding_rounds` | Target Table: `warehouse.dim_round_type` | Description |
|---------------------------------------|-------------------------------------|-------------|
| - | `round_type_id` (UUID) | Generated UUID as surrogate key |
| DISTINCT `funding_round_type` (VARCHAR) | `funding_round_type` (VARCHAR) | Unique values extracted |
| DISTINCT `funding_round_code` (VARCHAR) | `funding_round_code` (VARCHAR) | Unique values extracted |
| - | `description` (TEXT) | Default NULL (can be updated later) |
| - | `created_at` (TIMESTAMP) | Default value (current timestamp) |
| - | `updated_at` (TIMESTAMP) | Default value (current timestamp) |

### Date Dimension

| Source | Target Table: `warehouse.dim_date` | Description |
|--------|--------------------------------|-------------|
| Generated | `date_id` (INT) | Formatted as YYYYMMDD |
| Generated | `date_actual` (DATE) | Generated date value |
| Generated | `year` (INT) | Extracted year from date |
| Generated | `quarter` (INT) | Extracted quarter from date |
| Generated | `month` (INT) | Extracted month from date |
| Generated | `month_name` (VARCHAR) | Extracted month name from date |
| Generated | `day` (INT) | Extracted day from date |
| Generated | `day_of_week` (INT) | Calculated day of week |
| Generated | `day_name` (VARCHAR) | Calculated day name |
| Generated | `is_weekend` (BOOLEAN) | Calculated based on day of week |
| Generated | `week_of_year` (INT) | Calculated week number |
| Generated | `is_leap_year` (BOOLEAN) | Calculated based on year |
| Generated | `created_at` (TIMESTAMP) | Default value (current timestamp) |
| Generated | `updated_at` (TIMESTAMP) | Default value (current timestamp) |

## Fact Tables

### Funding Round Fact

| Source Table: `staging.funding_rounds` | Target Table: `warehouse.fact_funding_round` | Description |
|---------------------------------------|----------------------------------------|-------------|
| - | `funding_round_fact_id` (UUID) | Generated UUID as surrogate key |
| `funding_round_id` (VARCHAR) | `funding_round_id_nk` (VARCHAR) | Renamed as natural key |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company |
| `funding_round_type`, `funding_round_code` | `round_type_id` (UUID) | Lookup from dim_round_type |
| TO_CHAR(`funded_at`, 'YYYYMMDD')::INT | `funded_date_id` (INT) | Converted to date_id format |
| `raised_amount_usd` (DECIMAL) | `raised_amount_usd` (DECIMAL) | Direct mapping |
| `raised_amount` (DECIMAL) | `raised_amount` (DECIMAL) | Direct mapping |
| `raised_currency_code` (VARCHAR) | `raised_currency_code` (VARCHAR) | Direct mapping |
| `pre_money_valuation_usd` (DECIMAL) | `pre_money_valuation_usd` (DECIMAL) | Direct mapping |
| `pre_money_valuation` (DECIMAL) | `pre_money_valuation` (DECIMAL) | Direct mapping |
| `pre_money_currency_code` (VARCHAR) | `pre_money_currency_code` (VARCHAR) | Direct mapping |
| `post_money_valuation_usd` (DECIMAL) | `post_money_valuation_usd` (DECIMAL) | Direct mapping |
| `post_money_valuation` (DECIMAL) | `post_money_valuation` (DECIMAL) | Direct mapping |
| `post_money_currency_code` (VARCHAR) | `post_money_currency_code` (VARCHAR) | Direct mapping |
| `participants` (INTEGER) | `participants` (INTEGER) | Direct mapping |
| `is_first_round` (BOOLEAN) | `is_first_round` (BOOLEAN) | Direct mapping |
| `is_last_round` (BOOLEAN) | `is_last_round` (BOOLEAN) | Direct mapping |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Investment Fact

| Source Table: `staging.investments` | Target Table: `warehouse.fact_investment` | Description |
|------------------------------------|-------------------------------------|-------------|
| - | `investment_fact_id` (UUID) | Generated UUID as surrogate key |
| `investment_id` (VARCHAR) | `investment_id_nk` (VARCHAR) | Renamed as natural key |
| `funding_round_id` (VARCHAR) | `funding_round_fact_id` (UUID) | Lookup from fact_funding_round |
| `investor_object_id` (VARCHAR) | `investor_id` (UUID) | Lookup from dim_investor |
| `funded_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company |
| JOIN with funding_rounds | `investment_date_id` (INT) | Derived from funding round date |
| - | `investment_amount` (DECIMAL) | NULL (data not available) |
| - | `investment_currency_code` (VARCHAR) | NULL (data not available) |
| - | `source_url` (TEXT) | NULL (data not available) |
| - | `source_description` (TEXT) | NULL (data not available) |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Acquisition Fact

| Source Table: `staging.acquisition` | Target Table: `warehouse.fact_acquisition` | Description |
|------------------------------------|-------------------------------------|-------------|
| - | `acquisition_fact_id` (UUID) | Generated UUID as surrogate key |
| `acquisition_id` (VARCHAR) | `acquisition_id_nk` (VARCHAR) | Renamed as natural key |
| `acquiring_object_id` (VARCHAR) | `acquiring_company_id` (UUID) | Lookup from dim_company |
| `acquired_object_id` (VARCHAR) | `acquired_company_id` (UUID) | Lookup from dim_company |
| TO_CHAR(`acquired_at`, 'YYYYMMDD')::INT | `acquisition_date_id` (INT) | Converted to date_id format |
| `term_code` (VARCHAR) | `term_code` (VARCHAR) | Direct mapping |
| `price_amount` (DECIMAL) | `price_amount` (DECIMAL) | Direct mapping |
| `price_currency_code` (VARCHAR) | `price_currency_code` (VARCHAR) | Direct mapping |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### IPO Fact

| Source Table: `staging.ipos` | Target Table: `warehouse.fact_ipo` | Description |
|------------------------------|------------------------------|-------------|
| - | `ipo_fact_id` (UUID) | Generated UUID as surrogate key |
| `ipo_id` (VARCHAR) | `ipo_id_nk` (VARCHAR) | Renamed as natural key |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company |
| TO_CHAR(`public_at`, 'YYYYMMDD')::INT | `ipo_date_id` (INT) | Converted to date_id format |
| `valuation_amount` (DECIMAL) | `valuation_amount` (DECIMAL) | Direct mapping |
| `valuation_currency_code` (VARCHAR) | `valuation_currency_code` (VARCHAR) | Direct mapping |
| `raised_amount` (DECIMAL) | `raised_amount` (DECIMAL) | Direct mapping |
| `raised_currency_code` (VARCHAR) | `raised_currency_code` (VARCHAR) | Direct mapping |
| `stock_symbol` (VARCHAR) | `stock_symbol` (VARCHAR) | Direct mapping |
| `source_url` (TEXT) | `source_url` (TEXT) | Direct mapping |
| `source_description` (TEXT) | `source_description` (TEXT) | Direct mapping |
| `created_at` (TIMESTAMP) | `created_at` (TIMESTAMP) | Direct mapping |
| `updated_at` (TIMESTAMP) | `updated_at` (TIMESTAMP) | Direct mapping |

### Relationship Fact

| Source Table: `staging.relationships` | Target Table: `warehouse.fact_relationship` | Description |
|--------------------------------------|--------------------------------------|-------------|
| - | `relationship_fact_id` (UUID) | Generated UUID as surrogate key |
| `relationship_id` (VARCHAR) | `relationship_id_nk` (VARCHAR) | Renamed as natural key |
| `person_object_id` (VARCHAR) | `person_id` (UUID) | Lookup from dim_person |
| `relationship_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from dim_company |
| `title` (VARCHAR) | `relationship_type_id` (UUID) | Lookup from dim_relationship_type |
| TO_CHAR(`start_at`, 'YYYYMMDD')::INT | `start_date_id` (INT) | Converted to date_id format |
| TO_CHAR(`end_at`, 'YYYYMMDD')::INT | `end_date_id` (INT) | Converted to date_id format |

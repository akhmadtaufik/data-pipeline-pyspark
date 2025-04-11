# Source to Target Mapping

This document outlines the transformation process from staging tables to data warehouse tables.

## Dimension Tables

### Company Dimension

| Source Table: `staging.company` | Target Table: `warehouse.dim_company` | Description |
|--------------------------------|-------------------------------------|-------------|
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Natural key (renamed) |
| `description` (TEXT) | `description` (TEXT) | Direct mapping |
| `region` (VARCHAR) | `region` (VARCHAR) | Direct mapping |
| `city` (VARCHAR) | `city` (VARCHAR) | Direct mapping |
| `state_code` (VARCHAR) | `state_code` (VARCHAR) | Direct mapping |
| `country_code` (VARCHAR) | `country_code` (VARCHAR) | Direct mapping |
| - | `valid_from` (TIMESTAMP) | Default: `CURRENT_TIMESTAMP` |
| - | `valid_to` (TIMESTAMP) | Default: `NULL` (current record) |

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
| `people_id` (VARCHAR) | `people_id_nk` (VARCHAR) | Natural key |
| `object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Natural key |
| `first_name` (VARCHAR) | `first_name` (VARCHAR) | Direct mapping |
| `last_name` (VARCHAR) | `last_name` (VARCHAR) | Direct mapping |
| CONCAT(`first_name`, ' ', `last_name`) | `full_name` (VARCHAR) | Derived field |
| `affiliation_name` (VARCHAR) | `affiliation_name` (VARCHAR) | Direct mapping |
| ➤ **Removed** | ~~`birthplace`~~ | **87.61% missing data** |
| - | `valid_from` (TIMESTAMP) | Default: `CURRENT_TIMESTAMP` |

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
| `investor_object_id` (VARCHAR) | `object_id_nk` (VARCHAR) | Natural key |
| ➤ **Removed** | ~~`investor_type`~~ | **No direct source data** |
| - | `valid_from` (TIMESTAMP) | Default: `CURRENT_TIMESTAMP` |

### Relationship Type Dimension

| Source Table: `staging.relationships` | Target Table: `warehouse.dim_relationship_type` | Description |
|--------------------------------------|-------------------------------------------|-------------|
| DISTINCT `title` (VARCHAR) | `title` (VARCHAR) | Unique relationship roles |

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
| `funding_round_id` (VARCHAR) | `funding_round_id_nk` (VARCHAR) | Natural key |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from `dim_company` |
| `funding_round_type`, `funding_round_code` | `round_type_id` (UUID) | Lookup from `dim_round_type` |
| TO_CHAR(`funded_at`, 'YYYYMMDD')::INT | `funded_date_id` (INT) | Convert to `YYYYMMDD` format |
| `participants` (STRING) | `participants` (TEXT) | **➤ Tipe data sesuai sumber** |

### Investment Fact

| Source Table: `staging.investments` | Target Table: `warehouse.fact_investment` | Description |
|------------------------------------|-------------------------------------|-------------|
| `investment_id` (VARCHAR) | `investment_id_nk` (VARCHAR) | Natural key |
| `funding_round_id` (VARCHAR) | `funding_round_fact_id` (UUID) | Lookup from `fact_funding_round` |
| `investor_object_id` (VARCHAR) | `investor_id` (UUID) | Lookup from `dim_investor` |
| `funded_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from `dim_company` |
| ➤ **Removed** | ~~`investment_amount`~~, ~~`investment_currency_code`~~, ~~`source_url`~~, ~~`source_description`~~ | **No source data** |

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
| `ipo_id` (VARCHAR) | `ipo_id_nk` (VARCHAR) | Natural key |
| `object_id` (VARCHAR) | `company_id` (UUID) | Lookup from `dim_company` |
| TO_CHAR(`public_at`, 'YYYYMMDD')::INT | `ipo_date_id` (INT) | Convert to `YYYYMMDD` format |

### Relationship Fact

| Source Table: `staging.relationships` | Target Table: `warehouse.fact_relationship` | Description |
|--------------------------------------|--------------------------------------|-------------|
| `relationship_id` (VARCHAR) | `relationship_id_nk` (VARCHAR) | Natural key |
| `person_object_id` (VARCHAR) | `person_id` (UUID) | Lookup from `dim_person` |
| `relationship_object_id` (VARCHAR) | `company_id` (UUID) | Lookup from `dim_company` |
| `title` (VARCHAR) | `relationship_type_id` (UUID) | Lookup from `dim_relationship_type` |
| TO_CHAR(`start_at`, 'YYYYMMDD')::INT | `start_date_id` (INT) | **Default: `19000101` jika NULL** |
| ➤ **Removed** | ~~`end_date_id`~~ | **85.71% missing data** |

---

## Summary of Removed Columns

| Table | Removed Column | Reason |
|-------|----------------|--------|
| `dim_person` | `birthplace` | 87.61% missing data |
| `fact_investment` | `investment_amount`, `investment_currency_code`, `source_url`, `source_description` | No source data |
| `fact_relationship` | `end_date_id` | 85.71% missing data |
| `dim_investor` | `investor_type` | No direct source mapping |

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

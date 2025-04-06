-- Create warehouse schema
DROP SCHEMA IF EXISTS warehouse CASCADE;
CREATE SCHEMA warehouse;

-- Dimension tables
-- dim_company - Company dimension
CREATE TABLE warehouse.dim_company (
    company_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id_nk VARCHAR(255) NOT NULL,
    description TEXT,
    region VARCHAR(255),
    city VARCHAR(255),
    state_code VARCHAR(50),
    country_code VARCHAR(50),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_location - Location dimension
CREATE TABLE warehouse.dim_location (
    location_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    office_id_nk VARCHAR(255) NOT NULL,
    region VARCHAR(255),
    address1 TEXT,
    address2 TEXT,
    city VARCHAR(255),
    zip_code VARCHAR(50),
    state_code VARCHAR(50),
    country_code VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_person - Person dimension
CREATE TABLE warehouse.dim_person (
    person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    people_id_nk VARCHAR(255) NOT NULL,
    object_id_nk VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(511),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_date - Date dimension
CREATE TABLE warehouse.dim_date (
    date_id INT PRIMARY KEY,
    date_actual DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    week_of_year INT NOT NULL,
    is_leap_year BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_fund - Fund dimension
CREATE TABLE warehouse.dim_fund (
    fund_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id_nk VARCHAR(255) NOT NULL,
    object_id_nk VARCHAR(255),
    name VARCHAR(255),
    source_url TEXT,
    source_description TEXT,
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_investor - Investor dimension
CREATE TABLE warehouse.dim_investor (
    investor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id_nk VARCHAR(255) NOT NULL,
    investor_type VARCHAR(50),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_relationship_type - Relationship type dimension
CREATE TABLE warehouse.dim_relationship_type (
    relationship_type_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_milestone_type - Milestone type dimension
CREATE TABLE warehouse.dim_milestone_type (
    milestone_type_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    milestone_code VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- dim_round_type - Funding round type dimension
CREATE TABLE warehouse.dim_round_type (
    round_type_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    funding_round_type VARCHAR(100) NOT NULL,
    funding_round_code VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Fact tables
-- fact_funding_round - Funding round facts
CREATE TABLE warehouse.fact_funding_round (
    funding_round_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    funding_round_id_nk VARCHAR(255) NOT NULL,
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    round_type_id UUID REFERENCES warehouse.dim_round_type(round_type_id),
    funded_date_id INT REFERENCES warehouse.dim_date(date_id),
    raised_amount_usd DECIMAL(18, 2),
    raised_amount DECIMAL(18, 2),
    raised_currency_code VARCHAR(10),
    pre_money_valuation_usd DECIMAL(18, 2),
    pre_money_valuation DECIMAL(18, 2),
    pre_money_currency_code VARCHAR(10),
    post_money_valuation_usd DECIMAL(18, 2),
    post_money_valuation DECIMAL(18, 2),
    post_money_currency_code VARCHAR(10),
    participants INTEGER,
    is_first_round BOOLEAN,
    is_last_round BOOLEAN,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_investment - Investment facts
CREATE TABLE warehouse.fact_investment (
    investment_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investment_id_nk VARCHAR(255) NOT NULL,
    funding_round_fact_id UUID REFERENCES warehouse.fact_funding_round(funding_round_fact_id),
    investor_id UUID REFERENCES warehouse.dim_investor(investor_id),
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    investment_date_id INT REFERENCES warehouse.dim_date(date_id),
    investment_amount DECIMAL(18, 2),
    investment_currency_code VARCHAR(10),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_acquisition - Acquisition facts
CREATE TABLE warehouse.fact_acquisition (
    acquisition_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    acquisition_id_nk VARCHAR(255) NOT NULL,
    acquiring_company_id UUID REFERENCES warehouse.dim_company(company_id),
    acquired_company_id UUID REFERENCES warehouse.dim_company(company_id),
    acquisition_date_id INT REFERENCES warehouse.dim_date(date_id),
    term_code VARCHAR(50),
    price_amount DECIMAL(18, 2),
    price_currency_code VARCHAR(10),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_ipo - IPO facts
CREATE TABLE warehouse.fact_ipo (
    ipo_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ipo_id_nk VARCHAR(255) NOT NULL,
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    ipo_date_id INT REFERENCES warehouse.dim_date(date_id),
    valuation_amount DECIMAL(18, 2),
    valuation_currency_code VARCHAR(10),
    raised_amount DECIMAL(18, 2),
    raised_currency_code VARCHAR(10),
    stock_symbol VARCHAR(50),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_relationship - Relationship facts
CREATE TABLE warehouse.fact_relationship (
    relationship_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    relationship_id_nk VARCHAR(255) NOT NULL,
    person_id UUID REFERENCES warehouse.dim_person(person_id),
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    relationship_type_id UUID REFERENCES warehouse.dim_relationship_type(relationship_type_id),
    start_date_id INT REFERENCES warehouse.dim_date(date_id),
    end_date_id INT REFERENCES warehouse.dim_date(date_id),
    is_past BOOLEAN,
    sequence INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_milestone - Milestone facts
CREATE TABLE warehouse.fact_milestone (
    milestone_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    milestone_id_nk VARCHAR(255) NOT NULL,
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    milestone_type_id UUID REFERENCES warehouse.dim_milestone_type(milestone_type_id),
    milestone_date_id INT REFERENCES warehouse.dim_date(date_id),
    description TEXT,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- fact_fund - Fund facts
CREATE TABLE warehouse.fact_fund (
    fund_fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fund_id UUID REFERENCES warehouse.dim_fund(fund_id),
    company_id UUID REFERENCES warehouse.dim_company(company_id),
    funded_date_id INT REFERENCES warehouse.dim_date(date_id),
    raised_amount DECIMAL(18, 2),
    raised_currency_code VARCHAR(10),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_dim_company_object_id ON warehouse.dim_company(object_id_nk);
CREATE INDEX idx_dim_person_object_id ON warehouse.dim_person(object_id_nk);
CREATE INDEX idx_fact_funding_round_company ON warehouse.fact_funding_round(company_id);
CREATE INDEX idx_fact_investment_company ON warehouse.fact_investment(company_id);
CREATE INDEX idx_fact_acquisition_acquiring ON warehouse.fact_acquisition(acquiring_company_id);
CREATE INDEX idx_fact_acquisition_acquired ON warehouse.fact_acquisition(acquired_company_id);
CREATE INDEX idx_fact_ipo_company ON warehouse.fact_ipo(company_id);
CREATE INDEX idx_fact_relationship_person ON warehouse.fact_relationship(person_id);
CREATE INDEX idx_fact_relationship_company ON warehouse.fact_relationship(company_id);
CREATE INDEX idx_fact_milestone_company ON warehouse.fact_milestone(company_id);
CREATE INDEX idx_fact_fund_company ON warehouse.fact_fund(company_id);

-- Create a function to populate date dimension
CREATE OR REPLACE FUNCTION warehouse.populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    loop_date DATE := start_date;
BEGIN
    WHILE loop_date <= end_date LOOP
        INSERT INTO warehouse.dim_date (
            date_id,
            date_actual,
            year,
            quarter,
            month,
            month_name,
            day,
            day_of_week,
            day_name,
            is_weekend,
            week_of_year,
            is_leap_year
        )
        SELECT
            TO_CHAR(loop_date, 'YYYYMMDD')::INT,
            loop_date,
            EXTRACT(YEAR FROM loop_date),
            EXTRACT(QUARTER FROM loop_date),
            EXTRACT(MONTH FROM loop_date),
            TO_CHAR(loop_date, 'Month'),
            EXTRACT(DAY FROM loop_date),
            EXTRACT(DOW FROM loop_date),
            TO_CHAR(loop_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM loop_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            EXTRACT(WEEK FROM loop_date),
            CASE WHEN (EXTRACT(YEAR FROM loop_date) % 4 = 0 AND EXTRACT(YEAR FROM loop_date) % 100 != 0) OR (EXTRACT(YEAR FROM loop_date) % 400 = 0) THEN TRUE ELSE FALSE END
        ON CONFLICT (date_id) DO NOTHING;

        loop_date := loop_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for 20 years
SELECT warehouse.populate_dim_date('2000-01-01', '2024-12-31');

-- Create log table for ETL process
CREATE TABLE IF NOT EXISTS warehouse.etl_log (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_count INTEGER,
    target_count INTEGER,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

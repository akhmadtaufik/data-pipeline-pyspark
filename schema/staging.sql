DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

-- Company table
CREATE TABLE IF NOT EXISTS staging.company (
    office_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    description TEXT,
    region VARCHAR(255),
    address1 TEXT,
    address2 TEXT,
    city VARCHAR(255),
    zip_code VARCHAR(50),
    state_code VARCHAR(50),
    country_code VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Funds table
CREATE TABLE IF NOT EXISTS staging.funds (
    fund_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    name VARCHAR(255),
    funded_at DATE,
    raised_amount DECIMAL(18, 2),
    raised_currency_code VARCHAR(10),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Acquisition table
CREATE TABLE IF NOT EXISTS staging.acquisition (
    acquisition_id VARCHAR(255) PRIMARY KEY,
    acquiring_object_id VARCHAR(255),
    acquired_object_id VARCHAR(255),
    term_code VARCHAR(50),
    price_amount DECIMAL(18, 2),
    price_currency_code VARCHAR(10),
    acquired_at TIMESTAMP,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Funding Rounds table
CREATE TABLE IF NOT EXISTS staging.funding_rounds (
    funding_round_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    funded_at DATE,
    funding_round_type VARCHAR(100),
    funding_round_code VARCHAR(50),
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
    created_by VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Investment table
CREATE TABLE IF NOT EXISTS staging.investment (
    investment_id VARCHAR(255) PRIMARY KEY,
    funding_round_id VARCHAR(255),
    funded_object_id VARCHAR(255),
    investor_object_id VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- IPOs table
CREATE TABLE IF NOT EXISTS staging.ipos (
    ipo_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    valuation_amount DECIMAL(18, 2),
    valuation_currency_code VARCHAR(10),
    raised_amount DECIMAL(18, 2),
    raised_currency_code VARCHAR(10),
    public_at TIMESTAMP,
    stock_symbol VARCHAR(50),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- People table
CREATE TABLE IF NOT EXISTS staging.people (
    people_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Relationship table
CREATE TABLE IF NOT EXISTS staging.relationships (
    relationship_id VARCHAR(255) PRIMARY KEY,
    person_object_id VARCHAR(255),
    relationship_object_id VARCHAR(255),
    start_at DATE,
    end_at DATE,
    is_past BOOLEAN,
    sequence INTEGER,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

-- Milestones table
CREATE TABLE IF NOT EXISTS staging.milestones (
    milestone_id VARCHAR(255) PRIMARY KEY,
    object_id VARCHAR(255),
    description TEXT,
    milestone_at DATE,
    milestone_code VARCHAR(50),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_date TIMESTAMP
);

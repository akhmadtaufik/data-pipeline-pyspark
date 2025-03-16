DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

------------------------ SEQUENCES ------------------------
CREATE SEQUENCE staging.company_office_id_seq;
CREATE SEQUENCE staging.funds_fund_id_seq;
CREATE SEQUENCE staging.acquisition_acquisition_id_seq;
CREATE SEQUENCE staging.funding_rounds_funding_round_id_seq;
CREATE SEQUENCE staging.investment_investment_id_seq;
CREATE SEQUENCE staging.ipos_ipo_id_seq;
CREATE SEQUENCE staging.people_people_id_seq;
CREATE SEQUENCE staging.relationship_relationship_id_seq;

------------------------ TABEL STAGING ------------------------
-- COMPANY
CREATE TABLE staging.company (
    office_id INT PRIMARY KEY DEFAULT nextval('staging.company_office_id_seq'),
    object_id VARCHAR(50),
    description TEXT,
    region VARCHAR(100),
    address1 VARCHAR(255),
    address2 VARCHAR(255),
    city VARCHAR(100),
    zip_code VARCHAR(20),
    state_code VARCHAR(5),
    country_code VARCHAR(5),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- FUNDS
CREATE TABLE staging.funds (
    fund_id INT PRIMARY KEY DEFAULT nextval('staging.funds_fund_id_seq'),
    object_id VARCHAR(50) REFERENCES staging.company(object_id),
    name VARCHAR(255),
    funded_at DATE,
    raised_amount DECIMAL(18,2),
    raised_currency_code VARCHAR(3),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- ACQUISITION
CREATE TABLE staging.acquisition (
    acquisition_id INT PRIMARY KEY DEFAULT nextval('staging.acquisition_acquisition_id_seq'),
    acquiring_object_id VARCHAR(50),
    acquired_object_id VARCHAR(50),
    term_code VARCHAR(50),
    price_amount DECIMAL(18,2),
    price_currency_code VARCHAR(3),
    acquired_at TIMESTAMPTZ,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- FUNDING_ROUNDS
CREATE TABLE staging.funding_rounds (
    funding_round_id INT PRIMARY KEY DEFAULT nextval('staging.funding_rounds_funding_round_id_seq'),
    object_id VARCHAR(50) REFERENCES staging.company(object_id),
    funded_at DATE,
    funding_round_type VARCHAR(50),
    funding_round_code VARCHAR(20),
    raised_amount_usd DECIMAL(18,2),
    raised_amount DECIMAL(18,2),
    raised_currency_code VARCHAR(3),
    pre_money_valuation_usd DECIMAL(18,2),
    pre_money_valuation DECIMAL(18,2),
    pre_money_currency_code VARCHAR(3),
    post_money_valuation_usd DECIMAL(18,2),
    post_money_valuation DECIMAL(18,2),
    post_money_currency_code VARCHAR(3),
    participants TEXT,
    is_first_round BOOLEAN,
    is_last_round BOOLEAN,
    source_url TEXT,
    source_description TEXT,
    created_by VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- INVESTMENT
CREATE TABLE staging.investment (
    investment_id INT PRIMARY KEY DEFAULT nextval('staging.investment_investment_id_seq'),
    funding_round_id INT REFERENCES staging.funding_rounds(funding_round_id),
    funded_object_id VARCHAR(50),
    investor_object_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- IPOS
CREATE TABLE staging.ipos (
    ipo_id INT PRIMARY KEY DEFAULT nextval('staging.ipos_ipo_id_seq'),
    object_id VARCHAR(50) REFERENCES staging.company(object_id),
    valuation_amount DECIMAL(18,2),
    valuation_currency_code VARCHAR(3),
    raised_amount DECIMAL(18,2),
    raised_currency_code VARCHAR(3),
    public_at TIMESTAMPTZ,
    stock_symbol VARCHAR(50),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- PEOPLE
CREATE TABLE staging.people (
    people_id INT PRIMARY KEY DEFAULT nextval('staging.people_people_id_seq'),
    object_id VARCHAR(50) REFERENCES staging.company(object_id),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- RELATIONSHIP
CREATE TABLE staging.relationship (
    relationship_id INT PRIMARY KEY DEFAULT nextval('staging.relationship_relationship_id_seq'),
    person_object_id VARCHAR(50) REFERENCES staging.people(people_id),
    relationship_object_id VARCHAR(50) REFERENCES staging.company(object_id),
    start_at DATE,
    end_at DATE,
    is_past BOOLEAN,
    sequence INT,
    title VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- MILESTONES (Dari API)
CREATE TABLE staging.milestones (
    milestone_id INT PRIMARY KEY,
    object_id VARCHAR(50) REFERENCES staging.company(object_id),
    created_at TIMESTAMPTZ,
    description TEXT,
    milestone_at DATE,
    milestone_code VARCHAR(50),
    source_description TEXT,
    source_url TEXT,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

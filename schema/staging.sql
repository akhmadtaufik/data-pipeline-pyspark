-- DROP SCHEMA staging;

CREATE SCHEMA staging AUTHORIZATION postgres;
-- staging.acquisition definition

-- Drop table

-- DROP TABLE staging.acquisition;

CREATE TABLE staging.acquisition (
	acquisition_id int4 NULL,
	acquiring_object_id text NULL,
	acquired_object_id text NULL,
	term_code text NULL,
	price_amount numeric(15, 2) NULL,
	price_currency_code text NULL,
	acquired_at timestamp NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.company definition

-- Drop table

-- DROP TABLE staging.company;

CREATE TABLE staging.company (
	office_id int4 NULL,
	object_id text NULL,
	description text NULL,
	region text NULL,
	address1 text NULL,
	address2 text NULL,
	city text NULL,
	zip_code text NULL,
	state_code text NULL,
	country_code text NULL,
	latitude numeric(9, 6) NULL,
	longitude numeric(9, 6) NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.funding_rounds definition

-- Drop table

-- DROP TABLE staging.funding_rounds;

CREATE TABLE staging.funding_rounds (
	funding_round_id int4 NULL,
	object_id text NULL,
	funded_at date NULL,
	funding_round_type text NULL,
	funding_round_code text NULL,
	raised_amount_usd numeric(15, 2) NULL,
	raised_amount numeric(15, 2) NULL,
	raised_currency_code text NULL,
	pre_money_valuation_usd numeric(15, 2) NULL,
	pre_money_valuation numeric(15, 2) NULL,
	pre_money_currency_code text NULL,
	post_money_valuation_usd numeric(15, 2) NULL,
	post_money_valuation numeric(15, 2) NULL,
	post_money_currency_code text NULL,
	participants text NULL,
	is_first_round bool NULL,
	is_last_round bool NULL,
	source_url text NULL,
	source_description text NULL,
	created_by text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.funds definition

-- Drop table

-- DROP TABLE staging.funds;

CREATE TABLE staging.funds (
	fund_id text NULL,
	object_id text NULL,
	"name" text NULL,
	funded_at date NULL,
	raised_amount numeric(15, 2) NULL,
	raised_currency_code text NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.investments definition

-- Drop table

-- DROP TABLE staging.investments;

CREATE TABLE staging.investments (
	investment_id int4 NULL,
	funding_round_id int4 NULL,
	funded_object_id text NULL,
	investor_object_id text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.ipos definition

-- Drop table

-- DROP TABLE staging.ipos;

CREATE TABLE staging.ipos (
	ipo_id text NULL,
	object_id text NULL,
	valuation_amount numeric(15, 2) NULL,
	valuation_currency_code text NULL,
	raised_amount numeric(15, 2) NULL,
	raised_currency_code text NULL,
	public_at timestamp NULL,
	stock_symbol text NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);


-- staging.milestones definition

-- Drop table

-- DROP TABLE staging.milestones;

CREATE TABLE staging.milestones (
	created_at timestamp NULL,
	description text NULL,
	milestone_at text NULL,
	milestone_code text NULL,
	milestone_id int8 NULL,
	object_id text NULL,
	source_description text NULL,
	source_url text NULL,
	updated_at text NULL
);


-- staging.people definition

-- Drop table

-- DROP TABLE staging.people;

CREATE TABLE staging.people (
	people_id int4 NULL,
	object_id text NULL,
	first_name text NULL,
	last_name text NULL,
	birthplace text NULL,
	affiliation_name text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);


-- staging.relationships definition

-- Drop table

-- DROP TABLE staging.relationships;

CREATE TABLE staging.relationships (
	relationship_id int4 NULL,
	person_object_id text NULL,
	relationship_object_id text NULL,
	start_at timestamp NULL,
	end_at timestamp NULL,
	is_past bool NULL,
	"sequence" int4 NULL,
	title text NULL,
	created_at timestamp NULL,
	updated_at timestamp NULL
);

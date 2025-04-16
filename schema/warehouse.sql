-- Create warehouse schema
-- DROP SCHEMA IF EXISTS warehouse CASCADE;

CREATE SCHEMA warehouse AUTHORIZATION postgres;
-- warehouse.dim_company definition

-- Drop table

-- DROP TABLE warehouse.dim_company;

CREATE TABLE warehouse.dim_company (
	company_id uuid DEFAULT gen_random_uuid() NOT NULL,
	object_id_nk varchar(255) NOT NULL,
	description text NULL,
	region varchar(255) NULL,
	city varchar(255) NULL,
	state_code varchar(50) NULL,
	country_code varchar(50) NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_to timestamp NULL,
	is_current bool DEFAULT true NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_company_pkey PRIMARY KEY (company_id)
);
CREATE INDEX idx_dim_company_object_id ON warehouse.dim_company USING btree (object_id_nk);


-- warehouse.dim_date definition

-- Drop table

-- DROP TABLE warehouse.dim_date;

CREATE TABLE warehouse.dim_date (
	date_id int4 NOT NULL,
	date_actual date NOT NULL,
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	month_name varchar(9) NOT NULL,
	"day" int4 NOT NULL,
	day_of_week int4 NOT NULL,
	day_name varchar(9) NOT NULL,
	is_weekend bool NOT NULL,
	week_of_year int4 NOT NULL,
	is_leap_year bool NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);


-- warehouse.dim_fund definition

-- Drop table

-- DROP TABLE warehouse.dim_fund;

CREATE TABLE warehouse.dim_fund (
	fund_id uuid DEFAULT gen_random_uuid() NOT NULL,
	fund_id_nk varchar(255) NOT NULL,
	object_id_nk varchar(255) NULL,
	"name" varchar(255) NULL,
	source_url text NULL,
	source_description text NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_to timestamp NULL,
	is_current bool DEFAULT true NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_fund_pkey PRIMARY KEY (fund_id)
);


-- warehouse.dim_investor definition

-- Drop table

-- DROP TABLE warehouse.dim_investor;

CREATE TABLE warehouse.dim_investor (
	investor_id uuid DEFAULT gen_random_uuid() NOT NULL,
	object_id_nk varchar(255) NOT NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_to timestamp NULL,
	is_current bool DEFAULT true NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_investor_pkey PRIMARY KEY (investor_id)
);


-- warehouse.dim_location definition

-- Drop table

-- DROP TABLE warehouse.dim_location;

CREATE TABLE warehouse.dim_location (
	location_id uuid DEFAULT gen_random_uuid() NOT NULL,
	office_id_nk varchar(255) NOT NULL,
	region varchar(255) NULL,
	address1 text NULL,
	address2 text NULL,
	city varchar(255) NULL,
	zip_code varchar(50) NULL,
	state_code varchar(50) NULL,
	country_code varchar(50) NULL,
	latitude numeric(10, 6) NULL,
	longitude numeric(10, 6) NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_to timestamp NULL,
	is_current bool DEFAULT true NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_location_pkey PRIMARY KEY (location_id)
);


-- warehouse.dim_milestone_type definition

-- Drop table

-- DROP TABLE warehouse.dim_milestone_type;

CREATE TABLE warehouse.dim_milestone_type (
	milestone_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
	milestone_code varchar(50) NOT NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_milestone_type_pkey PRIMARY KEY (milestone_type_id)
);


-- warehouse.dim_person definition

-- Drop table

-- DROP TABLE warehouse.dim_person;

CREATE TABLE warehouse.dim_person (
	person_id uuid DEFAULT gen_random_uuid() NOT NULL,
	people_id_nk varchar(255) NOT NULL,
	object_id_nk varchar(255) NULL,
	first_name varchar(255) NULL,
	last_name varchar(255) NULL,
	full_name varchar(511) NULL,
	affiliation_name varchar(255) NULL,
	valid_from timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	valid_to timestamp NULL,
	is_current bool DEFAULT true NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_person_pkey PRIMARY KEY (person_id)
);
CREATE INDEX idx_dim_person_object_id ON warehouse.dim_person USING btree (object_id_nk);


-- warehouse.dim_relationship_type definition

-- Drop table

-- DROP TABLE warehouse.dim_relationship_type;

CREATE TABLE warehouse.dim_relationship_type (
	relationship_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_relationship_type_pkey PRIMARY KEY (relationship_type_id)
);


-- warehouse.dim_round_type definition

-- Drop table

-- DROP TABLE warehouse.dim_round_type;

CREATE TABLE warehouse.dim_round_type (
	round_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
	funding_round_type varchar(100) NOT NULL,
	funding_round_code varchar(50) NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT dim_round_type_pkey PRIMARY KEY (round_type_id)
);


-- warehouse.fact_acquisition definition

-- Drop table

-- DROP TABLE warehouse.fact_acquisition;

CREATE TABLE warehouse.fact_acquisition (
	acquisition_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	acquisition_id_nk varchar(255) NOT NULL,
	acquiring_company_id uuid NULL,
	acquired_company_id uuid NULL,
	acquisition_date_id int4 NULL,
	term_code varchar(50) NULL,
	price_amount numeric(18, 2) NULL,
	price_currency_code varchar(10) NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_acquisition_pkey PRIMARY KEY (acquisition_fact_id),
	CONSTRAINT uniq_acquisition_nk UNIQUE (acquisition_id_nk),
	CONSTRAINT fact_acquisition_acquired_company_id_fkey FOREIGN KEY (acquired_company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_acquisition_acquiring_company_id_fkey FOREIGN KEY (acquiring_company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_acquisition_acquisition_date_id_fkey FOREIGN KEY (acquisition_date_id) REFERENCES warehouse.dim_date(date_id)
);
CREATE INDEX idx_fact_acquisition_acquired ON warehouse.fact_acquisition USING btree (acquired_company_id);
CREATE INDEX idx_fact_acquisition_acquiring ON warehouse.fact_acquisition USING btree (acquiring_company_id);


-- warehouse.fact_fund definition

-- Drop table

-- DROP TABLE warehouse.fact_fund;

CREATE TABLE warehouse.fact_fund (
	fund_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	fund_id uuid NULL,
	company_id uuid NULL,
	funded_date_id int4 NULL,
	raised_amount numeric(18, 2) NULL,
	raised_currency_code varchar(10) NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_fund_pkey PRIMARY KEY (fund_fact_id),
	CONSTRAINT fact_fund_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_fund_fund_id_fkey FOREIGN KEY (fund_id) REFERENCES warehouse.dim_fund(fund_id),
	CONSTRAINT fact_fund_funded_date_id_fkey FOREIGN KEY (funded_date_id) REFERENCES warehouse.dim_date(date_id)
);
CREATE INDEX idx_fact_fund_company ON warehouse.fact_fund USING btree (company_id);


-- warehouse.fact_funding_round definition

-- Drop table

-- DROP TABLE warehouse.fact_funding_round;

CREATE TABLE warehouse.fact_funding_round (
	funding_round_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	funding_round_id_nk varchar(255) NOT NULL,
	company_id uuid NULL,
	round_type_id uuid NULL,
	funded_date_id int4 NULL,
	raised_amount_usd numeric(18, 2) NULL,
	raised_amount numeric(18, 2) NULL,
	raised_currency_code varchar(10) NULL,
	pre_money_valuation_usd numeric(18, 2) NULL,
	pre_money_valuation numeric(18, 2) NULL,
	pre_money_currency_code varchar(10) NULL,
	post_money_valuation_usd numeric(18, 2) NULL,
	post_money_valuation numeric(18, 2) NULL,
	post_money_currency_code varchar(10) NULL,
	participants text NULL,
	is_first_round bool NULL,
	is_last_round bool NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_funding_round_pkey PRIMARY KEY (funding_round_fact_id),
	CONSTRAINT uniq_funding_round_nk UNIQUE (funding_round_id_nk),
	CONSTRAINT fact_funding_round_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_funding_round_funded_date_id_fkey FOREIGN KEY (funded_date_id) REFERENCES warehouse.dim_date(date_id),
	CONSTRAINT fact_funding_round_round_type_id_fkey FOREIGN KEY (round_type_id) REFERENCES warehouse.dim_round_type(round_type_id)
);
CREATE INDEX idx_fact_funding_round_company ON warehouse.fact_funding_round USING btree (company_id);


-- warehouse.fact_investment definition

-- Drop table

-- DROP TABLE warehouse.fact_investment;

CREATE TABLE warehouse.fact_investment (
	investment_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	investment_id_nk varchar(255) NOT NULL,
	funding_round_fact_id uuid NULL,
	investor_id uuid NULL,
	company_id uuid NULL,
	investment_date_id int4 NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_investment_pkey PRIMARY KEY (investment_fact_id),
	CONSTRAINT uniq_investment_nk UNIQUE (investment_id_nk),
	CONSTRAINT fact_investment_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_investment_funding_round_fact_id_fkey FOREIGN KEY (funding_round_fact_id) REFERENCES warehouse.fact_funding_round(funding_round_fact_id),
	CONSTRAINT fact_investment_investment_date_id_fkey FOREIGN KEY (investment_date_id) REFERENCES warehouse.dim_date(date_id),
	CONSTRAINT fact_investment_investor_id_fkey FOREIGN KEY (investor_id) REFERENCES warehouse.dim_investor(investor_id)
);
CREATE INDEX idx_fact_investment_company ON warehouse.fact_investment USING btree (company_id);


-- warehouse.fact_ipo definition

-- Drop table

-- DROP TABLE warehouse.fact_ipo;

CREATE TABLE warehouse.fact_ipo (
	ipo_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	ipo_id_nk varchar(255) NOT NULL,
	company_id uuid NULL,
	ipo_date_id int4 NULL,
	valuation_amount numeric(18, 2) NULL,
	valuation_currency_code varchar(10) NULL,
	raised_amount numeric(18, 2) NULL,
	raised_currency_code varchar(10) NULL,
	stock_symbol varchar(50) NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_ipo_pkey PRIMARY KEY (ipo_fact_id),
	CONSTRAINT uniq_ipo_nk UNIQUE (ipo_id_nk),
	CONSTRAINT fact_ipo_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_ipo_ipo_date_id_fkey FOREIGN KEY (ipo_date_id) REFERENCES warehouse.dim_date(date_id)
);
CREATE INDEX idx_fact_ipo_company ON warehouse.fact_ipo USING btree (company_id);


-- warehouse.fact_milestone definition

-- Drop table

-- DROP TABLE warehouse.fact_milestone;

CREATE TABLE warehouse.fact_milestone (
	milestone_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	milestone_id_nk varchar(255) NOT NULL,
	company_id uuid NULL,
	milestone_type_id uuid NULL,
	milestone_date_id int4 NULL,
	description text NULL,
	source_url text NULL,
	source_description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_milestone_pkey PRIMARY KEY (milestone_fact_id),
	CONSTRAINT uniq_milestone_nk UNIQUE (milestone_id_nk),
	CONSTRAINT fact_milestone_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_milestone_milestone_date_id_fkey FOREIGN KEY (milestone_date_id) REFERENCES warehouse.dim_date(date_id),
	CONSTRAINT fact_milestone_milestone_type_id_fkey FOREIGN KEY (milestone_type_id) REFERENCES warehouse.dim_milestone_type(milestone_type_id)
);
CREATE INDEX idx_fact_milestone_company ON warehouse.fact_milestone USING btree (company_id);


-- warehouse.fact_relationship definition

-- Drop table

-- DROP TABLE warehouse.fact_relationship;

CREATE TABLE warehouse.fact_relationship (
	relationship_fact_id uuid DEFAULT gen_random_uuid() NOT NULL,
	relationship_id_nk varchar(255) NOT NULL,
	person_id uuid NULL,
	company_id uuid NULL,
	relationship_type_id uuid NULL,
	start_date_id int4 DEFAULT 19000101 NULL,
	is_past bool NULL,
	"sequence" int4 NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fact_relationship_pkey PRIMARY KEY (relationship_fact_id),
	CONSTRAINT uniq_relationship_nk UNIQUE (relationship_id_nk),
	CONSTRAINT fact_relationship_company_id_fkey FOREIGN KEY (company_id) REFERENCES warehouse.dim_company(company_id),
	CONSTRAINT fact_relationship_person_id_fkey FOREIGN KEY (person_id) REFERENCES warehouse.dim_person(person_id),
	CONSTRAINT fact_relationship_relationship_type_id_fkey FOREIGN KEY (relationship_type_id) REFERENCES warehouse.dim_relationship_type(relationship_type_id),
	CONSTRAINT fact_relationship_start_date_id_fkey FOREIGN KEY (start_date_id) REFERENCES warehouse.dim_date(date_id)
);
CREATE INDEX idx_fact_relationship_company ON warehouse.fact_relationship USING btree (company_id);
CREATE INDEX idx_fact_relationship_person ON warehouse.fact_relationship USING btree (person_id);



-- DROP FUNCTION warehouse.populate_dim_date(date, date);

CREATE OR REPLACE FUNCTION warehouse.populate_dim_date(start_date date, end_date date)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
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
$function$
;

-- Populate date dimension for 50 years
SELECT warehouse.populate_dim_date('1960-01-01', '2025-12-31');



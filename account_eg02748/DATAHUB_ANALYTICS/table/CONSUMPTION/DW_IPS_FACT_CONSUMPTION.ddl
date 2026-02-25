-- datahub_analytics.dw_ips_fact_consumption definition

-- Drop table

DROP TABLE IF EXISTS datahub_analytics.dw_ips_fact_consumption;

CREATE TABLE IF NOT EXISTS datahub_analytics.dw_ips_fact_consumption
(
	target_ips_consumption_key  INTEGER IDENTITY NOT NULL 
	,account_key INTEGER  
	,account_service_key INTEGER  
	,address_key INTEGER 
	,comp_key INTEGER 
	,month INTEGER 
	,act_daily_avg DOUBLE PRECISION 
	,actual_days INTEGER  
	,actual_volume DOUBLE PRECISION
	,full_month_flag VARCHAR(1) 
	,est_daily_avg DOUBLE PRECISION
	,est_days INTEGER 
	,est_volume DOUBLE PRECISION
	,total_volume DOUBLE PRECISION
	,estimate_flag VARCHAR(10) 
	,estimate_method VARCHAR(30) 
	,consumption_percentage DOUBLE PRECISION
	,etl_load_datetime TIMESTAMP 
	,etl_source VARCHAR(50) 
)

;
--ALTER TABLE datahub_target.dw_ips_fact_consumption owner to wslmaster;
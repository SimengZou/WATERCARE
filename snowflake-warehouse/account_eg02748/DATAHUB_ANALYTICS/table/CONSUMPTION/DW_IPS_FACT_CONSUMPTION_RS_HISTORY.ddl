-- datahub_analytics.dw_ips_fact_consumption definition

-- Drop table

DROP TABLE IF EXISTS datahub_analytics.dw_ips_fact_consumption_rs_history;

CREATE TABLE IF NOT EXISTS datahub_analytics.dw_ips_fact_consumption_rs_history
(
	target_ips_consumption_key  INTEGER 
	,account_key INTEGER  
	,account_service_key INTEGER  
	,address_key INTEGER 
	,comp_key INTEGER 
	,month INTEGER 
	,act_daily_avg NUMBER(38,4) 
	,actual_days INTEGER  
	,actual_volume NUMBER(38,4)
	,full_month_flag VARCHAR(1) 
	,est_daily_avg NUMBER(38,4)
	,est_days INTEGER 
	,est_volume NUMBER(38,4)
	,total_volume NUMBER(38,4)
	,estimate_flag VARCHAR(10) 
	,estimate_method VARCHAR(30) 
	,consumption_percentage NUMBER(38,4)
	,etl_load_datetime TIMESTAMP 
	,etl_source VARCHAR(50) 
)

;

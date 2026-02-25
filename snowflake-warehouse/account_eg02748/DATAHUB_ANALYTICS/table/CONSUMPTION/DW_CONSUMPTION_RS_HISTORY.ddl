DROP TABLE IF EXISTS datahub_analytics.dw_consumption_RS_history;
CREATE TABLE datahub_analytics.dw_consumption_RS_history
(
	cal_year INTEGER 
	,cal_month_no INTEGER 
	,suburb VARCHAR(100)  
	,comp_key INTEGER  
	,unit_id VARCHAR(50)   
	,account_number VARCHAR(50)   
	,account_type VARCHAR(12) NOT NULL 
	,customer_description VARCHAR(200)  
	,account_class_desc VARCHAR(100)  
	,account_class_code VARCHAR(100) NOT NULL  
	,lno VARCHAR(50) 
	,water_meter_address_key INTEGER   
	,full_address VARCHAR(300) 
	,act_daily_avg NUMERIC(19,9)  
	,est_daily_avg NUMERIC(19,9)   
	,actual_days INTEGER   
	,act_monthly_avg NUMERIC(19,9) 
	,est_monthly_avg NUMERIC(19,9)  
	,estimate_flag VARCHAR(10) NOT NULL  
	,full_month_flag VARCHAR(1) 
	,etl_src_from VARCHAR(50)   
	,etl_load_datetime TIMESTAMP
)
;
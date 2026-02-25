DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_accountservices_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_accountservices_rs_history
(
	dw_ips_dim_accountservice_key INTEGER
	,account_key INTEGER   
	,account_service_key INTEGER   
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP   
	,address_service_key INTEGER   
	,area VARCHAR(10)   
	,consumption_percentage DOUBLE PRECISION   
	,impervious_percentage DOUBLE PRECISION   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP   
	,service_class1 VARCHAR(10)   
	,service_class2 VARCHAR(10)   
	,service_field1_value DOUBLE PRECISION   
	,service_field2_value DOUBLE PRECISION   
	,service_field3_value DOUBLE PRECISION   
	,service_options_key INTEGER   
	,start_datetime TIMESTAMP   
	,status VARCHAR(10)   
	,status_datetime TIMESTAMP   
	,stop_datetime TIMESTAMP   
	,suspend_from_date TIMESTAMP   
	,suspend_to_date TIMESTAMP   
	,winter_avg_calculation_date TIMESTAMP   
	,winter_average DOUBLE PRECISION   
	,etl_load_datetime TIMESTAMP   
)
;

DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_compwmtr_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_compwmtr_rs_history
(
	stage_ips_compwmtr_key INTEGER
	,account_number VARCHAR(24)   
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP    
	,address_key INTEGER   
	,address_qualifier VARCHAR(300)   
	,area VARCHAR(10)   
	,asbuilt VARCHAR(10)   
	,average_monthly_usage DOUBLE PRECISION   
	,average_monthly_usage_uom VARCHAR(10)   
	,budget_number VARCHAR(24)   
	,building_floor INTEGER   
	,building_room INTEGER   
	,budget_key INTEGER   
	,bypass VARCHAR(1)   
	,comp_key INTEGER   
	,complex_key INTEGER   
	,component INTEGER   
	,district VARCHAR(10)   
	,expired_by VARCHAR(12)   
	,expire_date TIMESTAMP    
	,gis_static INTEGER   
	,high_register INTEGER   
	,installed_date TIMESTAMP    
	,interaction_key INTEGER   
	,last_reading_export_schedule_key INTEGER   
	,last_reading_import_schedule_key INTEGER   
	,location VARCHAR(10)   
	,low_register INTEGER   
	,main_key INTEGER   
	,map_no VARCHAR(14)   
	,meter_size DOUBLE PRECISION   
	,meter_size_uom VARCHAR(10)   
	,manufactured_date TIMESTAMP    
	,manufacturer_key INTEGER   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP    
	,model_number VARCHAR(20)   
	,out_for_activity_flag VARCHAR(1)   
	,own VARCHAR(10)   
	,position INTEGER   
	,parcel_key INTEGER   
	,pressure_zone VARCHAR(10)   
	,purchased_date TIMESTAMP    
	,segment_key INTEGER   
	,serial_number VARCHAR(20)   
	,service_status VARCHAR(10)   
	,site_key INTEGER   
	,service_line_key INTEGER   
	,special_instructions VARCHAR(8000)   
	,service_type VARCHAR(10)   
	,subarea VARCHAR(10)   
	,unit_description VARCHAR(300)   
	,unit_id VARCHAR(50)   
	,unit_type VARCHAR(10)   
	,usage_area_key INTEGER   
	,usage_date TIMESTAMP    
	,usage_total DOUBLE PRECISION   
	,usage_total_uom VARCHAR(10)   
	,xcoordinate DOUBLE PRECISION   
	,ycoordinate DOUBLE PRECISION   
	,zcoordinate DOUBLE PRECISION   
	,etl_load_datetime TIMESTAMP    
	,etl_source VARCHAR(50)   
)
;

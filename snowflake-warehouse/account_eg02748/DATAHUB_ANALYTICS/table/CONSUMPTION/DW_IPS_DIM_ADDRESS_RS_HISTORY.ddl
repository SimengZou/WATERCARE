DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_address_rs_history;
CREATE TABLE IF NOT EXISTS datahub_analytics.dw_ips_dim_address_rs_history
(
	stage_ips_address_key INTEGER
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP   
	,address_status VARCHAR(10)   
	,address_key INTEGER   
	,allow_duplicate_services VARCHAR(1)   
	,area VARCHAR(10)   
	,block VARCHAR(15)   
	,cass_is_validation VARCHAR(1)   
	,cass_validation_description VARCHAR(50)   
	,cass_validation_date TIMESTAMP   
	,cass_validation_status VARCHAR(10)   
	,city_limits VARCHAR(1)   
	,current_address VARCHAR(1)   
	,effective_date TIMESTAMP   
	,expire_date TIMESTAMP   
	,full_street_number VARCHAR(25)   
	,gps_x_coordinate DOUBLE PRECISION   
	,gps_y_coordinate DOUBLE PRECISION   
	,gps_z_coordinate DOUBLE PRECISION   
	,in_out_service_area VARCHAR(10)   
	,legal_description VARCHAR(8000)   
	,legal_owner VARCHAR(300)   
	,lot VARCHAR(15)   
	,management_group INTEGER   
	,map_no VARCHAR(14)   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP   
	,address_type VARCHAR(1)   
	,compass_direction VARCHAR(3)   
	,flat VARCHAR(10)   
	,house_number_high VARCHAR(10)   
	,house_number_sort_as INTEGER   
	,house_number_sort_as_high INTEGER   
	,house_number VARCHAR(10)   
	,post_code VARCHAR(5)   
	,street2_name VARCHAR(28)   
	,street2_type VARCHAR(4)   
	,street3_name VARCHAR(28)   
	,street3_type VARCHAR(4)   
	,state VARCHAR(4)   
	,street_name VARCHAR(28)   
	,street_type VARCHAR(4)   
	,suburb VARCHAR(30)   
	,option_a VARCHAR(10)   
	,option_b VARCHAR(10)   
	,option_c VARCHAR(10)   
	,option_d INTEGER   
	,property_use VARCHAR(30)   
	,subdivision_code VARCHAR(10)   
	,subdivision_description VARCHAR(300)   
	,township VARCHAR(30)   
	,version INTEGER   
	,etl_load_datetime TIMESTAMP   
	,etl_source VARCHAR(50)   
)

;

DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_account_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_account_rs_history
(
	stage_ips_account_key INTEGER
	,accept_checks_flag VARCHAR(1)   
	,account_key INTEGER   
	,account_number VARCHAR(27)   
	,account_status VARCHAR(6)   
	,account_type INTEGER   
	,account_class VARCHAR(10)   
	,account_group VARCHAR(10)   
	,account_subgroup VARCHAR(10)   
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP    
	,address_key INTEGER   
	,aggregate_flag VARCHAR(1)   
	,aggregate_key INTEGER   
	,allow_check_overrides_flag VARCHAR(1)   
	,area VARCHAR(10)   
	,arranged_flag VARCHAR(1)   
	,assigned VARCHAR(12)   
	,bankrupt_flag VARCHAR(1)   
	,billing_cycle VARCHAR(10)   
	,billing_print_exception VARCHAR(10)   
	,billing_status VARCHAR(10)   
	,billing_datetime TIMESTAMP    
	,budget_billing_disallowed VARCHAR(1)   
	,budget_billing_flag VARCHAR(1)   
	,business_type VARCHAR(10)   
	,character_cons_value1 VARCHAR(10)   
	,character_cons_value2 VARCHAR(10)   
	,character_cons_value3 VARCHAR(10)   
	,character_cons_value4 VARCHAR(10)   
	,character_cons_value5 VARCHAR(10)   
	,character_cons_value6 VARCHAR(10)   
	,character_cons_value7 VARCHAR(10)   
	,character_cons_value8 VARCHAR(10)   
	,close_date TIMESTAMP    
	,collections_eligible_flag VARCHAR(1)   
	,collections_flag VARCHAR(1)   
	,collections_exempt VARCHAR(1)   
	,comments_key INTEGER   
	,credit_flag VARCHAR(1)   
	,customer_description VARCHAR(85)   
	,delinquency_exempt VARCHAR(10)   
	,delinquency_exempt_flag VARCHAR(1)   
	,delinquency_flag VARCHAR(1)   
	,deposit_paid_off TIMESTAMP    
	,do_not_direct_debit VARCHAR(1)   
	,eligible_for_refund_flag VARCHAR(1)   
	,final_bill_flag VARCHAR(1)   
	,frequency VARCHAR(1)   
	,identity_key INTEGER   
	,initiated_by VARCHAR(12)   
	,initiated_datetime TIMESTAMP    
	,legacy_account_number VARCHAR(27)   
	,lien_eligible_flag VARCHAR(1)   
	,lien_exempt VARCHAR(1)   
	,lien_flag VARCHAR(1)   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP    
	,moved_out_account_key INTEGER   
	,move_in_admin_hold_by VARCHAR(12)   
	,move_in_admin_hold_datetime TIMESTAMP    
	,move_in_admin_hold_reason VARCHAR(10)   
	,move_in_date TIMESTAMP    
	,move_in_exception INTEGER   
	,move_in_status INTEGER   
	,move_out_admin_hold_by VARCHAR(12)   
	,move_out_admin_hold_datetime TIMESTAMP    
	,move_out_admin_hold_reason VARCHAR(10)   
	,move_out_date TIMESTAMP    
	,move_out_exception INTEGER   
	,move_out_status INTEGER   
	,do_not_arrange VARCHAR(1)   
	,do_not_send_bills VARCHAR(1)   
	,do_not_disconnect VARCHAR(1)   
	,do_not_send_notices VARCHAR(1)   
	,notice_datetime TIMESTAMP    
	,numeric_cons_value1 DOUBLE PRECISION   
	,numeric_cons_value2 DOUBLE PRECISION   
	,numeric_cons_value3 DOUBLE PRECISION   
	,numeric_cons_value4 DOUBLE PRECISION   
	,numeric_cons_value5 DOUBLE PRECISION   
	,numeric_cons_value6 DOUBLE PRECISION   
	,numeric_cons_value7 DOUBLE PRECISION   
	,numeric_cons_value8 DOUBLE PRECISION   
	,occupant VARCHAR(10)   
	,oneoff_charge_flag VARCHAR(1)   
	,open_date TIMESTAMP    
	,parcel_key INTEGER   
	,penalty_exempt VARCHAR(10)   
	,preselected_for_bill_run VARCHAR(1)   
	,preselected_for_interim_bill VARCHAR(1)   
	,ready_flag VARCHAR(1)   
	,responsibility VARCHAR(10)   
	,status_by VARCHAR(12)   
	,status_datetime TIMESTAMP    
	,suspended_by VARCHAR(12)   
	,suspended_datetime TIMESTAMP    
	,suspended_flag VARCHAR(1)   
	,tax_exempt_falg VARCHAR(1)   
	,transferred_to INTEGER   
	,writeoff_eligible_flag VARCHAR(1)   
	,etl_load_datetime TIMESTAMP    
	,etl_source VARCHAR(50)   
)
;

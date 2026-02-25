CREATE OR REPLACE PROCEDURE "SP_STG_DIM_CUSTOMER_ACCOUNT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    
//  Variables
    
    var debug = "True";                                 // do we want debug messages?
    var result = "";                                    // return status of this proc call
    const process_name = Object.keys(this)[0];          // name of currently executing process
    const buffer_extract_hour = 1;                      // allow extra hour when extracting latest modifications from source data
    var debug_statement = "";                           // debug message statement
    var number_of_rows_inserted = 0;                             // track number of rows we have inserted
	var number_of_rows_updated = 0;                             // track number of rows we have updated
	var default_extraction_timestamp = ''2000-01-01 00:00:00''; //this time will be used if no value is obtained
    
    
//  Step 1.

//  Select MAX process ID for this process
    
    var log_sql_command;
    log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_process ";
    log_sql_command += "where process_name = ''" + process_name + "''";
    
    var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
    log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
    var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

    //  Increment MAX process ID to log a new run of this process
    
    if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
        {process_execution_id = 1;}
    else
        {process_execution_id = process_execution_id + 1;}
    
    //  log debug message
    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Started process, new ID = " + process_execution_id.toString() + "'');";
        snowflake.execute({sqlText: debug_statement}); 
        }
        
    
//  Step 2.

//  Get status of previous execution 
//      - for this we check if any previous execution is still running, where the status is "Running"

    var process_previous_status = "OK";
    log_sql_command = "select process_status from datahub_logging.etl_log_process ";
    log_sql_command += "where process_name = ''" + process_name + "'' and process_status = ''Running'';";
    var log_status_result = snowflake.execute({sqlText: log_sql_command});                                      // execute select from log table
    if (log_status_result.getRowCount() > 0) 
        {
        log_status_result.next();                                                                               // expecting a maximum of 1 row
        process_previous_status = log_status_result.getColumnValue(1);                                          // and only 1 return value
        } 

    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Running status = " + process_previous_status + "'');";
        snowflake.execute({sqlText: debug_statement});
        }
        
        
    //  If the previous process execution is still running, then quit this run
    
    if(process_previous_status === "Running")       
        {
        //  Create a log entry to say we found a previous running process and we are quitting
        
        log_sql_command = "call datahub_logging.sp_etl_log_process(''Insert'',''" + process_name + "'',''Failed'',''Previous process execution still Running. Quitting.'', " + process_execution_id.toString() + ",0,0);";
        snowflake.execute({sqlText: log_sql_command});
        return "Failed";
        }
    
    //  log debug message
    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''No Running process found'');";
        snowflake.execute({sqlText: debug_statement}); 
        }
        

//  Step 3.

//  Start execution - log start

    log_sql_command = "call datahub_logging.sp_etl_log_process(''Insert'',''" + process_name + "'',''Running'',''Started process execution.'', " + process_execution_id.toString() + ",0,0);";
    snowflake.execute({sqlText: log_sql_command});


    try
        {
        
        snowflake.execute ({sqlText: "begin transaction"});
        

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Begin Transaction'');";
            snowflake.execute({sqlText: debug_statement}); 
            }

       
//  Step 4.       

//  Get the last date that was extracted  - for each source table

//update the latest time included in extract for ACCOUNT table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNT ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNT'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
//update the latest time included in extract for ACCOUNTEXTN table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_WSLBILLING_ACCOUNTEXTN) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_WSLBILLING_ACCOUNTEXTN'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
		
//update the latest time included in extract for IPS_BILLING_DELINQUENCYEXEMPTION table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_DELINQUENCYEXEMPTION) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_DELINQUENCYEXEMPTION'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 		
		
		
//update the latest time included in extract for IPS_BILLING_ACCOUNTTYPESETUP table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNTTYPESETUP) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNTTYPESETUP'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for IPS_PROPERTY_ADDRESS table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_PROPERTY_ADDRESS) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_PROPERTY_ADDRESS'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for IPS_BILLING_ACCOUNTSERVICE table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNTSERVICE) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNTSERVICE'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for IPS_CUSTOMER_GROUPING table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_STAGING.IPS_CUSTOMER_GROUPING) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_CUSTOMER_GROUPING'' AND schema_name=''DATAHUB_STAGING'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for IPS_BILLING_ACCOUNTDELINQUENCY table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNTDELINQUENCY) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNTDELINQUENCY'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for IPS_BILLING_DELINQUENCYMILESTONE table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_DELINQUENCYMILESTONE) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_DELINQUENCYMILESTONE'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for STG1_DIM_CUSTOMER_ACCOUNT table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_SOURCE_DATETIME) FROM DATAHUB_STAGING.STG1_DIM_CUSTOMER_ACCOUNT) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''STG1_DIM_CUSTOMER_ACCOUNT'' AND schema_name=''DATAHUB_STAGING'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	
		

//update the latest time included in extract for IPS_BILLING_BILL table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_BILL) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_BILL'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 



//update the latest time included in extract for IPS_BILLING_SENDBILLTO table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_SENDBILLTO) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_SENDBILLTO'' AND schema_name=''DATAHUB_TARGET_HISTORY'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
//update the latest time included in extract for IPS_RESOURCES_CONTACT table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_RESOURCES_CONTACT) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_RESOURCES_CONTACT'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 

//update the latest time included in extract for IPS_RESOURCES_CNTCTID table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_RESOURCES_CNTCTID) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_RESOURCES_CNTCTID'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
	
		
		
	        
	if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Date updated for incremental extract for all ACCOUNT source tables.'');";
            snowflake.execute({sqlText: debug_statement}); 
            }	
            

            
//  Step 5.

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG_DIM_CUSTOMER_ACCOUNT"});
   
       
    
//  Step 6.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG_DIM_CUSTOMER_ACCOUNT (  ";
				sql_command += "  ACCOUNT_KEY, ";
				sql_command += "  ACCOUNT_NUMBER, ";
				sql_command += "  DUNNING_LEVEL_CODE, ";
				sql_command += "  MOST_RECENT_BILL_DATE, ";
				sql_command += "  ACCOUNT_DESCRIPTION, ";
				sql_command += "  CUSTOMER_GROUP_NAME, ";
				sql_command += "  ACCOUNT_STATUS, ";
				sql_command += "  ACCOUNT_TYPE_CODE, ";
				sql_command += "  ACCOUNT_CLASS, ";
				sql_command += "  ACCOUNT_SUBGROUP, ";
				sql_command += "  ACCOUNT_ADDED_DATE, ";
				sql_command += "  BANKRUPT_FLAG, ";
				sql_command += "  BILLING_CYCLE, ";
				sql_command += "  BILLING_STATUS, ";
				sql_command += "  BILLING_STATUS_DATE, ";
				sql_command += "  BUSINESS_TYPE, ";
				sql_command += "  ACCOUNT_CLOSED_DATE, ";
				sql_command += "  CUSTOMER_DESCRIPTION, ";
				sql_command += "  DELINQUENCY_EXEMPT, ";
				sql_command += "  DELINQUENCY_EXEMPT_FLAG, ";
				sql_command += "  ACCOUNT_IN_DELINQUENCY, ";
				sql_command += "  ACCOUNT_MODIFIED_DATE, ";
				sql_command += "  PROPERTY_MOVE_IN_DATE, ";
				sql_command += "  MOVE_IN_STATUS, ";
				sql_command += "  MOVE_IN_ADMIN_HOLD_REASON, ";
				sql_command += "  PROPERTY_MOVE_OUT_DATE, ";
				sql_command += "  MOVE_OUT_STATUS, ";
				sql_command += "  ACCOUNT_OPEN_DATE, ";
				sql_command += "  RESPONSIBILITY, ";
				sql_command += "  ACCOUNT_STATUS_DATE, ";
				sql_command += "  ACCOUNT_SUSPEND_FLAG, ";
				sql_command += "  RESIDENTIAL_STATUS, ";
				sql_command += "  ENTITY_CODE, ";
				sql_command += "  DUNNING_EXEMPTION_DESCRIPTION, ";
				sql_command += "  ACCOUNT_TYPE, ";
				sql_command += "  PROPERTY_ADDRESS, ";
				sql_command += "  FLAT, ";
				sql_command += "  HOUSE_NO, ";
				sql_command += "  STREET_NAME, ";
				sql_command += "  STREET_TYPE, ";
				sql_command += "  SUBURB, ";
				sql_command += "  POSTCODE, ";
				sql_command += "  LNO_AREA, ";
				sql_command += "  SERVICE_AREA_CODE, ";
				sql_command += "  CUSTOMER_NUMBER,";
				sql_command += "  EXTERNAL_ACCOUNT_TYPE,";
				sql_command += "  CUSTOMER_REFERENCE, ";
				sql_command += "  ACCOUNT_OWNER, ";
				sql_command += "  IGC_ON_ACCOUNT_FLAG, ";
				sql_command += "  COMPLIANCE_ON_ACCOUNT_FLAG, ";
				sql_command += "  SERVICES_ATTACHED, ";
				sql_command += "  ETL_SOURCE_DATETIME, ";
				sql_command += "  ETL_RECORD_CREATED, ";
				sql_command += "  ETL_RECORD_UPDATED";
				sql_command += "  )";
				sql_command += "  SELECT ";
				sql_command += "  ACCOUNT.ACCOUNTKEY AS Account_Key,";
				sql_command += "  ACCOUNT.ACCOUNTNUMBER AS Account_Number,";
				sql_command += "  DELINQUENCYMILESTONE.CODE AS DUNNING_LEVEL_CODE, ";
				sql_command += "  BILL.MOST_RECENT_BILL_DATE, ";
				sql_command += "  ACCOUNT.ACCOUNTNUMBER || '' '' || TRIM(ACCOUNT.CUSTDESC) AS Account_Description,";
				sql_command += "  CUSTGROUPING.customername AS CUSTOMER_GROUP_NAME, ";
				sql_command += "  CASE WHEN ACCOUNT.ACCOUNTSTATUS  =''A'' THEN ''Active (A)''";
				sql_command += "  WHEN ACCOUNT.ACCOUNTSTATUS  =''C'' THEN ''Closed (C)''";
				sql_command += "  WHEN ACCOUNT.ACCOUNTSTATUS  =''F'' THEN ''Final (F)''";
				sql_command += "  WHEN ACCOUNT.ACCOUNTSTATUS  =''I'' THEN ''Initiated (I)''";
				sql_command += "  WHEN ACCOUNT.ACCOUNTSTATUS  =''V'' THEN ''Voided (V)''";
				sql_command += "  END AS Account_Status,";
				sql_command += "  ACCOUNT.ACCOUNTTYPE AS Account_Type_Code,";
				sql_command += "  ACCOUNT.ACCTCLASS AS Account_Class,";
				sql_command += "  ACCOUNT.ACCTSUBGRP AS Account_Subgroup,";
				sql_command += "  ACCOUNT.ADDDTTM AS Account_Added_Date,";
				sql_command += "  ACCOUNT.BANKRUPTFLAG AS BANKRUPT_FLAG,";
				sql_command += "  ACCOUNT.BILLINGCYCLE AS Billing_Cycle,";
				sql_command += "  ACCOUNT.BLNGSTAT AS Billing_Status,";
				sql_command += "  ACCOUNT.BLNGSTDTTM AS Billing_Status_Date,";
				sql_command += "  ACCOUNT.BUSINESSTYPE AS Business_Type,";
				sql_command += "  ACCOUNT.CLOSEDATE AS Account_Close_Date,";
				sql_command += "  TRIM(ACCOUNT.CUSTDESC) AS Customer_Description,";
				sql_command += "  ACCOUNT.DELINQUENCYEXEMPT AS Delinquency_Exempt,";
				sql_command += "  ACCOUNT.DELINQUENCYEXEMPTFLAG AS Delinquency_Exempt_Flag,";
				sql_command += "  ACCOUNT.DELINQUENCYFLAG AS ACCOUNT_IN_DELINQUENCY,";
				sql_command += "  ACCOUNT.MODDTTM AS Account_Modified_Date,";
				sql_command += "  ACCOUNT.MOVEINDATE AS Property_Move_In_Date,";
				sql_command += "  ACCOUNT.MOVEINSTATUS AS MOVE_IN_STATUS, ";
				sql_command += "  ACCOUNT.MOVEINADMINHOLDREASON AS MOVE_IN_ADMIN_HOLD_REASON,";
				sql_command += "  ACCOUNT.MOVEOUTDATE AS PROPERTY_MOVE_OUT_DATE, ";
				sql_command += "  ACCOUNT.MOVEOUTSTATUS AS MOVE_OUT_STATUS, ";
				sql_command += "  ACCOUNT.OPENDATE AS Account_Open_Date,";
				sql_command += "  ACCOUNT.RESPONSIBILITY AS RESPONSIBILITY,";
				sql_command += "  ACCOUNT.STATDTTM AS Account_Status_Date,";
				sql_command += "  ACCOUNT.SUSPENDFLAG AS ACCOUNT_SUSPEND_FLAG,";
				sql_command += "  case when account.ACCTCLASS in (''Domestic'',''Dialysis'') then ''Residential''";
				sql_command += "  else ''Non Residential'' end AS Residential_Status,";
				sql_command += "  ACCOUNTEXTN.ACENTITY AS Entity_Code,";
				sql_command += "  DELINQUENCYEXEMPTION.DESCRIPT AS Dunning_Exemption_Desc,";
				sql_command += "  ACCOUNTTYPESETUP.ACCOUNTTYPESETUP AS Account_Type,";
				sql_command += "  CASE WHEN  ADDRESS.NZFLAT IS NULL THEN  ADDRESS.NZHOUSENO||'', ''||ADDRESS.NZSTNAME||'' ''||ADDRESS.NZSTTYPE||'', ''||ADDRESS.NZSUBURB||'', ''||ADDRESS.NZPOSTCODE";
				sql_command += "  ELSE  ADDRESS.NZFLAT||'', ''|| ADDRESS.NZHOUSENO ||'', ''||ADDRESS.NZSTNAME||'' ''||ADDRESS.NZSTTYPE||'', ''||ADDRESS.NZSUBURB||'', ''||ADDRESS.NZPOSTCODE";
				sql_command += "  END AS Property_Address,";
				sql_command += "  ADDRESS.NZFLAT AS Flat,";
				sql_command += "  ADDRESS.NZHOUSENO AS House_No,";
				sql_command += "  ADDRESS.NZSTNAME AS Street_Name,";
				sql_command += "  ADDRESS.NZSTTYPE AS Street_Type,";
				sql_command += "  ADDRESS.NZSUBURB AS Suburb,";
				sql_command += "  ADDRESS.NZPOSTCODE AS Postcode,";
				sql_command += "  ADDRESS.SUBDIVCODE AS LNO_Area,";
				sql_command += "  ADDRESS.INOUTSERVICEAREA AS Service_Area_Code,";
				sql_command += "  CNTCTID.CUSTOMERNO AS CUSTOMER_NUMBER,";
				sql_command += "  NULL AS EXTERNAL_ACCOUNT_TYPE,";
				sql_command += "  NULL AS Customer_Reference,";
				sql_command += "  CASE WHEN TRIM(ACCOUNT.CUSTDESC) = ''Kainga Ora'' THEN  ''Kainga Ora''";
				sql_command += "  WHEN TRIM(ACCOUNT.CUSTDESC) = ''Auckland Council'' THEN ''Auckland Council'' ";
				sql_command += "  ELSE  ''Other''";
				sql_command += "  END AS Account_Owner,";
				sql_command += "  CASE WHEN STG1_ACCOUNT.IGC_FLAG>0 THEN ''Y'' ELSE ''N'' END AS IGC_ON_ACCOUNT_FLAG, ";
				sql_command += "  CASE WHEN STG1_ACCOUNT.COMPLIANCE_FLAG>0 THEN ''Y'' ELSE ''N'' END AS COMPLIANCE_ON_ACCOUNT_FLAG, ";
				sql_command += "  CASE WHEN ACCOUNTSERVICE.accountkey IS NULL THEN ''N'' ELSE ''Y'' END  AS SERVICES_ATTACHED,";
				sql_command += "  GREATEST(ACCOUNT.etl_load_datetime, COALESCE(ACCOUNTEXTN.etl_load_datetime,ACCOUNT.etl_load_datetime), ";
				sql_command += "          COALESCE(DELINQUENCYEXEMPTION.etl_load_datetime, ACCOUNT.etl_load_datetime), COALESCE(ACCOUNTTYPESETUP.etl_load_datetime, ACCOUNT.etl_load_datetime), ";
				sql_command += "          COALESCE(ADDRESS.etl_load_datetime,ACCOUNT.etl_load_datetime),";
				sql_command += "          COALESCE(ACCOUNTDELINQUENCY.etl_load_datetime, ACCOUNT.etl_load_datetime), COALESCE(DELINQUENCYMILESTONE.etl_load_datetime,ACCOUNT.etl_load_datetime), ";
				sql_command += "          COALESCE(STG1_ACCOUNT.etl_source_datetime, ACCOUNT.etl_load_datetime), coalesce( BILL.etl_load_datetime,ACCOUNT.etl_load_datetime), ";
				sql_command += "          COALESCE(ACCOUNTSERVICE.etl_load_datetime, ACCOUNT.etl_load_datetime),coalesce( SENDBILLTO.etl_load_datetime,ACCOUNT.etl_load_datetime), ";
				sql_command += "          COALESCE(CONTACT.etl_load_datetime, ACCOUNT.etl_load_datetime),coalesce( CNTCTID.etl_load_datetime,ACCOUNT.etl_load_datetime) )  AS ETL_SOURCE_DATETIME, ";
				sql_command += "  current_timestamp() AS ETL_RECORD_CREATED, ";
				sql_command += "  current_timestamp() AS ETL_RECORD_UPDATED";
				sql_command += "  from DATAHUB_TARGET.IPS_BILLING_ACCOUNT ACCOUNT";
				sql_command += "  left join DATAHUB_TARGET.IPS_WSLBILLING_ACCOUNTEXTN ACCOUNTEXTN on account.accountkey=accountextn.accountkey ";
				sql_command += "  left join DATAHUB_TARGET.IPS_BILLING_DELINQUENCYEXEMPTION DELINQUENCYEXEMPTION on ACCOUNT.DELINQUENCYEXEMPT=DELINQUENCYEXEMPTION.CODE";
				sql_command += "  left join DATAHUB_TARGET.IPS_BILLING_ACCOUNTTYPESETUP ACCOUNTTYPESETUP on ACCOUNT.ACCOUNTTYPE=ACCOUNTTYPESETUP.ACCOUNTTYPESETUPKEY ";
				sql_command += "  LEFT JOIN DATAHUB_TARGET.IPS_PROPERTY_ADDRESS ADDRESS ON ACCOUNT.ADDRKEY = ADDRESS.ADDRKEY";
				sql_command += "  LEFT JOIN (SELECT accountkey , max(ETL_LOAD_DATETIME) AS ETL_LOAD_DATETIME FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNTSERVICE GROUP BY accountkey) ACCOUNTSERVICE ON ACCOUNT.accountkey=ACCOUNTSERVICE.accountkey";
				sql_command += "  LEFT JOIN DATAHUB_STAGING.IPS_CUSTOMER_GROUPING CUSTGROUPING ON to_char(ACCOUNT.accountkey) =trim(CUSTGROUPING.ACCOUNTKEY)";
				sql_command += "  LEFT JOIN ( SELECT ACCOUNTKEY, MILESTONEKEY, row_number() OVER (PARTITION BY ACCOUNTKEY ORDER BY ETL_LOAD_DATETIME DESC) AS last_record, ETL_LOAD_DATETIME";
				sql_command += "  FROM DATAHUB_TARGET.IPS_BILLING_ACCOUNTDELINQUENCY ) ACCOUNTDELINQUENCY ";
				sql_command += "  ON ACCOUNT.ACCOUNTKEY=ACCOUNTDELINQUENCY.ACCOUNTKEY AND LAST_RECORD=1";
				sql_command += "  LEFT JOIN DATAHUB_TARGET.IPS_BILLING_DELINQUENCYMILESTONE DELINQUENCYMILESTONE ON ACCOUNTDELINQUENCY.MILESTONEKEY = DELINQUENCYMILESTONE.MILESTONEKEY";
				sql_command += "  LEFT JOIN (SELECT accountkey, sum(IGC_Flag) AS IGC_Flag, sum(Compliance_Flag) AS Compliance_flag, max(etl_source_datetime) AS etl_source_datetime ";
				sql_command += "  FROM  DATAHUB_STAGING.STG1_DIM_CUSTOMER_ACCOUNT GROUP BY ACCOUNTKEY ) STG1_ACCOUNT ";
				sql_command += "  ON ACCOUNT.accountkey=stg1_account.accountkey";
				sql_command += "  LEFT JOIN (SELECT accountkey, max(billdate) AS MOST_RECENT_BILL_DATE, max(ETL_LOAD_DATETIME) AS ETL_LOAD_DATETIME ";
				sql_command += "  FROM DATAHUB_TARGET.IPS_BILLING_BILL";
				sql_command += "  GROUP BY accountkey) BILL";
				sql_command += "  ON ACCOUNT.accountkey=BILL.accountkey      ";
				sql_command += "  LEFT JOIN (SELECT row_number() OVER (PARTITION BY ACCOUNTKEY ORDER BY EFFECTIVEDATE DESC) AS latest_record , *";
				sql_command += "    FROM DATAHUB_TARGET_HISTORY.IPS_BILLING_SENDBILLTO SENDBILLTO WHERE not etl_is_deleted) SENDBILLTO    ";
				sql_command += "    ON ACCOUNT.ACCOUNTKEY=SENDBILLTO.ACCOUNTKEY AND SENDBILLTO.latest_record=1";
				sql_command += "  LEFT JOIN DATAHUB_TARGET.IPS_RESOURCES_CONTACT CONTACT    ON SENDBILLTO.ADDRESSCONTACTKEY = CONTACT.CNTCTKEY";
				sql_command += "  LEFT JOIN DATAHUB_TARGET.IPS_RESOURCES_CNTCTID CNTCTID    ON CONTACT.IDKEY = CNTCTID.IDKEY ";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNT_CTRL ";
				sql_command += "  ON ACCOUNT_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ACCOUNT_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNT'' AND ACCOUNT_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''   ";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTEXTN_CTRL ";
				sql_command += "  ON ACCOUNTEXTN_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ACCOUNTEXTN_CTRL.TABLE_NAME=''IPS_WSLBILLING_ACCOUNTEXTN'' AND ACCOUNTEXTN_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB DELINQUENCYEXEMPTION_CTRL ";
				sql_command += "  ON DELINQUENCYEXEMPTION_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND DELINQUENCYEXEMPTION_CTRL.TABLE_NAME=''IPS_BILLING_DELINQUENCYEXEMPTION'' AND DELINQUENCYEXEMPTION_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTTYPESETUP_CTRL ";
				sql_command += "  ON ACCOUNTTYPESETUP_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ACCOUNTTYPESETUP_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNTTYPESETUP'' AND ACCOUNTTYPESETUP_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ADDRESS_CTRL ";
				sql_command += "  ON ADDRESS_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ADDRESS_CTRL.TABLE_NAME=''IPS_PROPERTY_ADDRESS'' AND ADDRESS_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTSERVICE_CTRL ";
				sql_command += "  ON ACCOUNTSERVICE_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ACCOUNTSERVICE_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNTSERVICE'' AND ACCOUNTSERVICE_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB CUSTGROUPING_CTRL ";
				sql_command += "  ON CUSTGROUPING_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND CUSTGROUPING_CTRL.TABLE_NAME=''IPS_CUSTOMER_GROUPING'' AND CUSTGROUPING_CTRL.SCHEMA_NAME=''DATAHUB_STAGING''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTDELINQUENCY_CTRL ";
				sql_command += "  ON ACCOUNTDELINQUENCY_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND ACCOUNTDELINQUENCY_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNTDELINQUENCY'' AND ACCOUNTDELINQUENCY_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB DELINQUENCYMILESTONE_CTRL ";
				sql_command += "  ON DELINQUENCYMILESTONE_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND DELINQUENCYMILESTONE_CTRL.TABLE_NAME=''IPS_BILLING_DELINQUENCYMILESTONE'' AND DELINQUENCYMILESTONE_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB STG1_ACCOUNT_CTRL ";
				sql_command += "  ON STG1_ACCOUNT_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND STG1_ACCOUNT_CTRL.TABLE_NAME=''STG1_DIM_CUSTOMER_ACCOUNT'' AND STG1_ACCOUNT_CTRL.SCHEMA_NAME=''DATAHUB_STAGING''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB BILL_CTRL ";
				sql_command += "  ON BILL_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND BILL_CTRL.TABLE_NAME=''IPS_BILLING_BILL'' AND BILL_CTRL.SCHEMA_NAME=''DATAHUB_TARGET'' ";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB SENDBILLTO_CTRL ";
				sql_command += "  ON SENDBILLTO_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND SENDBILLTO_CTRL.TABLE_NAME=''IPS_BILLING_SENDBILLTO'' AND SENDBILLTO_CTRL.SCHEMA_NAME=''DATAHUB_TARGET_HISTORY''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB CONTACT_CTRL ";
				sql_command += "  ON CONTACT_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND CONTACT_CTRL.TABLE_NAME=''IPS_RESOURCES_CONTACT'' AND CONTACT_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB CNTCTID_CTRL ";
				sql_command += "  ON CNTCTID_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_CUSTOMER_ACCOUNT'' AND CNTCTID_CTRL.TABLE_NAME=''IPS_RESOURCES_CNTCTID'' AND CNTCTID_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
				sql_command += "  WHERE  (ACCOUNT.ETL_LOAD_DATETIME>ACCOUNT_CTRL.INC_ETL_LOAD_DATETIME ";
				sql_command += "  OR ACCOUNTEXTN.ETL_LOAD_DATETIME> ACCOUNTEXTN_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR DELINQUENCYEXEMPTION.ETL_LOAD_DATETIME> DELINQUENCYEXEMPTION_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR ACCOUNTTYPESETUP.ETL_LOAD_DATETIME> ACCOUNTTYPESETUP_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR ADDRESS.ETL_LOAD_DATETIME> ADDRESS_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR ACCOUNTSERVICE.ETL_LOAD_DATETIME> ACCOUNTSERVICE_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR CUSTGROUPING.ETL_LOAD_DATETIME> CUSTGROUPING_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR ACCOUNTDELINQUENCY.ETL_LOAD_DATETIME> ACCOUNTDELINQUENCY_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR DELINQUENCYMILESTONE.ETL_LOAD_DATETIME> DELINQUENCYMILESTONE_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR STG1_ACCOUNT.ETL_SOURCE_DATETIME> STG1_ACCOUNT_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR BILL.ETL_LOAD_DATETIME> BILL_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR SENDBILLTO.ETL_LOAD_DATETIME> SENDBILLTO_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR CONTACT.ETL_LOAD_DATETIME> CONTACT_CTRL.INC_ETL_LOAD_DATETIME";
				sql_command += "  OR CNTCTID.ETL_LOAD_DATETIME> CNTCTID_CTRL.INC_ETL_LOAD_DATETIME);";


		
	 //   snowflake.execute({sqlText: sql_command});
    
        //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
            var rs = sqlStmt.execute();
            var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
            var number_of_rows_updated =  sqlStmt.getNumRowsUpdated(); 
            
            }
         catch (err)
         {

            
            if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Failed to get number of rows inserted, err = " + err + "'');";
            snowflake.execute({sqlText: debug_statement}); 
            }
            
          //  Create a log entry to say INSERT STATEMENT failed
        
	        log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''Failed to insert the rows. Quitting.'', " + process_execution_id.toString() + ",0,0);";
	        snowflake.execute({sqlText: log_sql_command});
	        return "Failed";         
   
         }
    
    
        //  Mark execution as successful
        
        result = "Success"; 


//  Step 7.

//  Completed execution - log end

        log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        snowflake.execute ({sqlText: "commit"});


        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Commit Transaction'');";
            snowflake.execute({sqlText: debug_statement}); 
            }
        }
        
    
//  Error handling

    catch (err)  
        {
        snowflake.execute ({sqlText: "rollback"});
		
		//  Create a log entry to say INSERT STATEMENT failed
        
	    log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''The insert job has failed. Rollback.'', " + process_execution_id.toString() + ",0,0);";
	    snowflake.execute({sqlText: log_sql_command});
		
		if( debug === "True" ) 
			{
			debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Failed to get number of rows inserted, err = " + err + "'');";
			snowflake.execute({sqlText: debug_statement}); 
			}
		 
        return "Failed: " + err;   // Return error indicator.
        }

//    Return SP status to caller
  
    return result;
  
  
    ';
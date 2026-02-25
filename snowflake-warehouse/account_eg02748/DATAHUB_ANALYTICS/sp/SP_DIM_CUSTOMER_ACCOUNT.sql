CREATE OR REPLACE PROCEDURE "SP_DIM_CUSTOMER_ACCOUNT"()
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

//  Run merge statement
    
		
        var sql_command = "MERGE INTO DATAHUB_ANALYTICS.DIM_CUSTOMER_ACCOUNT target USING (SELECT  ";
		sql_command += " ACCOUNT_KEY, ";
		sql_command += " ACCOUNT_NUMBER, ";
		sql_command += " DUNNING_LEVEL_CODE, ";
		sql_command += " MOST_RECENT_BILL_DATE, ";
		sql_command += " ACCOUNT_DESCRIPTION, ";
		sql_command += " CUSTOMER_GROUP_NAME, ";
		sql_command += " ACCOUNT_STATUS, ";
		sql_command += " ACCOUNT_TYPE_CODE, ";
		sql_command += " ACCOUNT_CLASS, ";
		sql_command += " ACCOUNT_SUBGROUP, ";
		sql_command += " ACCOUNT_ADDED_DATE, ";
		sql_command += " BANKRUPT_FLAG, ";
		sql_command += " BILLING_CYCLE, ";
		sql_command += " BILLING_STATUS, ";
		sql_command += " BILLING_STATUS_DATE, ";
		sql_command += " BUSINESS_TYPE, ";
		sql_command += " ACCOUNT_CLOSED_DATE, ";
		sql_command += " CUSTOMER_DESCRIPTION, ";
		sql_command += " DELINQUENCY_EXEMPT, ";
		sql_command += " DELINQUENCY_EXEMPT_FLAG, ";
		sql_command += " ACCOUNT_IN_DELINQUENCY, ";
		sql_command += " ACCOUNT_MODIFIED_DATE, ";
		sql_command += " PROPERTY_MOVE_IN_DATE, ";
		sql_command += " MOVE_IN_STATUS, ";
		sql_command += " MOVE_IN_ADMIN_HOLD_REASON, ";
		sql_command += " PROPERTY_MOVE_OUT_DATE, ";
		sql_command += " MOVE_OUT_STATUS, ";
		sql_command += " ACCOUNT_OPEN_DATE, ";
		sql_command += " RESPONSIBILITY, ";
		sql_command += " ACCOUNT_STATUS_DATE, ";
		sql_command += " ACCOUNT_SUSPEND_FLAG, ";
		sql_command += " RESIDENTIAL_STATUS, ";
		sql_command += " ENTITY_CODE, ";
		sql_command += " DUNNING_EXEMPTION_DESCRIPTION, ";
		sql_command += " ACCOUNT_TYPE, ";
		sql_command += " PROPERTY_ADDRESS, ";
		sql_command += " FLAT, ";
		sql_command += " HOUSE_NO, ";
		sql_command += " STREET_NAME, ";
		sql_command += " STREET_TYPE, ";
		sql_command += " SUBURB, ";
		sql_command += " POSTCODE, ";
		sql_command += " LNO_AREA, ";
		sql_command += " SERVICE_AREA_CODE, ";
		sql_command += " CUSTOMER_NUMBER, ";
		sql_command += " EXTERNAL_ACCOUNT_TYPE, ";
		sql_command += " CUSTOMER_REFERENCE, ";
		sql_command += " ACCOUNT_OWNER, ";
		sql_command += " IGC_ON_ACCOUNT_FLAG, ";
		sql_command += " COMPLIANCE_ON_ACCOUNT_FLAG, ";
		sql_command += " SERVICES_ATTACHED, ";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG_DIM_CUSTOMER_ACCOUNT";
		sql_command += " ) AS b ON target.account_key =b.account_key";
		sql_command += " WHEN MATCHED THEN UPDATE ";
		sql_command += "  SET target.ACCOUNT_NUMBER = b.ACCOUNT_NUMBER,";
		sql_command += " target.DUNNING_LEVEL_CODE =b.DUNNING_LEVEL_CODE ,";
		sql_command += " target.MOST_RECENT_BILL_DATE =b.MOST_RECENT_BILL_DATE ,";
		sql_command += " target.ACCOUNT_DESCRIPTION =b.ACCOUNT_DESCRIPTION ,";
		sql_command += " target.CUSTOMER_GROUP_NAME =b.CUSTOMER_GROUP_NAME ,";
		sql_command += " target.ACCOUNT_STATUS =b.ACCOUNT_STATUS ,";
		sql_command += " target.ACCOUNT_TYPE_CODE =b.ACCOUNT_TYPE_CODE ,";
		sql_command += " target.ACCOUNT_CLASS =b.ACCOUNT_CLASS ,";
		sql_command += " target.ACCOUNT_SUBGROUP =b.ACCOUNT_SUBGROUP ,";
		sql_command += " target.ACCOUNT_ADDED_DATE =b.ACCOUNT_ADDED_DATE ,";
		sql_command += " target.BANKRUPT_FLAG =b.BANKRUPT_FLAG ,";
		sql_command += " target.BILLING_CYCLE =b.BILLING_CYCLE ,";
		sql_command += " target.BILLING_STATUS =b.BILLING_STATUS ,";
		sql_command += " target.BILLING_STATUS_DATE =b.BILLING_STATUS_DATE ,";
		sql_command += " target.BUSINESS_TYPE =b.BUSINESS_TYPE ,";
		sql_command += " target.ACCOUNT_CLOSED_DATE =b.ACCOUNT_CLOSED_DATE ,";
		sql_command += " target.CUSTOMER_DESCRIPTION =b.CUSTOMER_DESCRIPTION ,";
		sql_command += " target.DELINQUENCY_EXEMPT =b.DELINQUENCY_EXEMPT ,";
		sql_command += " target.DELINQUENCY_EXEMPT_FLAG =b.DELINQUENCY_EXEMPT_FLAG ,";
		sql_command += " target.ACCOUNT_IN_DELINQUENCY =b.ACCOUNT_IN_DELINQUENCY ,";
		sql_command += " target.ACCOUNT_MODIFIED_DATE =b.ACCOUNT_MODIFIED_DATE ,";
		sql_command += " target.PROPERTY_MOVE_IN_DATE =b.PROPERTY_MOVE_IN_DATE ,";
		sql_command += " target.MOVE_IN_STATUS =b.MOVE_IN_STATUS ,";
		sql_command += " target.MOVE_IN_ADMIN_HOLD_REASON =b.MOVE_IN_ADMIN_HOLD_REASON ,";
		sql_command += " target.PROPERTY_MOVE_OUT_DATE =b.PROPERTY_MOVE_OUT_DATE ,";
		sql_command += " target.MOVE_OUT_STATUS =b.MOVE_OUT_STATUS ,";
		sql_command += " target.ACCOUNT_OPEN_DATE =b.ACCOUNT_OPEN_DATE ,";
		sql_command += " target.RESPONSIBILITY =b.RESPONSIBILITY ,";
		sql_command += " target.ACCOUNT_STATUS_DATE =b.ACCOUNT_STATUS_DATE ,";
		sql_command += " target.ACCOUNT_SUSPEND_FLAG =b.ACCOUNT_SUSPEND_FLAG ,";
		sql_command += " target.RESIDENTIAL_STATUS =b.RESIDENTIAL_STATUS ,";
		sql_command += " target.ENTITY_CODE =b.ENTITY_CODE ,";
		sql_command += " target.DUNNING_EXEMPTION_DESCRIPTION =b.DUNNING_EXEMPTION_DESCRIPTION ,";
		sql_command += " target.ACCOUNT_TYPE =b.ACCOUNT_TYPE ,";
		sql_command += " target.PROPERTY_ADDRESS =b.PROPERTY_ADDRESS ,";
		sql_command += " target.FLAT =b.FLAT ,";
		sql_command += " target.HOUSE_NO =b.HOUSE_NO ,";
		sql_command += " target.STREET_NAME =b.STREET_NAME ,";
		sql_command += " target.STREET_TYPE =b.STREET_TYPE ,";
		sql_command += " target.SUBURB =b.SUBURB ,";
		sql_command += " target.POSTCODE =b.POSTCODE ,";
		sql_command += " target.LNO_AREA =b.LNO_AREA ,";
		sql_command += " target.SERVICE_AREA_CODE =b.SERVICE_AREA_CODE ,";
		sql_command += " target.CUSTOMER_NUMBER =b.CUSTOMER_NUMBER ,";
		sql_command += " target.EXTERNAL_ACCOUNT_TYPE =b.EXTERNAL_ACCOUNT_TYPE ,";
		sql_command += " target.CUSTOMER_REFERENCE =b.CUSTOMER_REFERENCE ,";
		sql_command += " target.ACCOUNT_OWNER =b.ACCOUNT_OWNER ,";
		sql_command += " target.IGC_ON_ACCOUNT_FLAG =b.IGC_ON_ACCOUNT_FLAG ,";
		sql_command += " target.COMPLIANCE_ON_ACCOUNT_FLAG =b.COMPLIANCE_ON_ACCOUNT_FLAG ,";
		sql_command += " target.SERVICES_ATTACHED =b.SERVICES_ATTACHED ,";
		sql_command += " target.ETL_SOURCE_DATETIME=b.ETL_SOURCE_DATETIME , ";
		sql_command += " target.ETL_RECORD_UPDATED =current_timestamp()";
		sql_command += " WHEN NOT MATCHED THEN INSERT (ACCOUNT_KEY, ";
		sql_command += " ACCOUNT_NUMBER, ";
		sql_command += " DUNNING_LEVEL_CODE, ";
		sql_command += " MOST_RECENT_BILL_DATE, ";
		sql_command += " ACCOUNT_DESCRIPTION, ";
		sql_command += " CUSTOMER_GROUP_NAME, ";
		sql_command += " ACCOUNT_STATUS, ";
		sql_command += " ACCOUNT_TYPE_CODE, ";
		sql_command += " ACCOUNT_CLASS, ";
		sql_command += " ACCOUNT_SUBGROUP, ";
		sql_command += " ACCOUNT_ADDED_DATE, ";
		sql_command += " BANKRUPT_FLAG, ";
		sql_command += " BILLING_CYCLE, ";
		sql_command += " BILLING_STATUS, ";
		sql_command += " BILLING_STATUS_DATE, ";
		sql_command += " BUSINESS_TYPE, ";
		sql_command += " ACCOUNT_CLOSED_DATE, ";
		sql_command += " CUSTOMER_DESCRIPTION, ";
		sql_command += " DELINQUENCY_EXEMPT, ";
		sql_command += " DELINQUENCY_EXEMPT_FLAG, ";
		sql_command += " ACCOUNT_IN_DELINQUENCY, ";
		sql_command += " ACCOUNT_MODIFIED_DATE, ";
		sql_command += " PROPERTY_MOVE_IN_DATE, ";
		sql_command += " MOVE_IN_STATUS, ";
		sql_command += " MOVE_IN_ADMIN_HOLD_REASON, ";
		sql_command += " PROPERTY_MOVE_OUT_DATE, ";
		sql_command += " MOVE_OUT_STATUS, ";
		sql_command += " ACCOUNT_OPEN_DATE, ";
		sql_command += " RESPONSIBILITY, ";
		sql_command += " ACCOUNT_STATUS_DATE, ";
		sql_command += " ACCOUNT_SUSPEND_FLAG, ";
		sql_command += " RESIDENTIAL_STATUS, ";
		sql_command += " ENTITY_CODE, ";
		sql_command += " DUNNING_EXEMPTION_DESCRIPTION, ";
		sql_command += " ACCOUNT_TYPE, ";
		sql_command += " PROPERTY_ADDRESS, ";
		sql_command += " FLAT, ";
		sql_command += " HOUSE_NO, ";
		sql_command += " STREET_NAME, ";
		sql_command += " STREET_TYPE, ";
		sql_command += " SUBURB, ";
		sql_command += " POSTCODE, ";
		sql_command += " LNO_AREA, ";
		sql_command += " SERVICE_AREA_CODE, ";
		sql_command += " CUSTOMER_NUMBER, ";
		sql_command += " EXTERNAL_ACCOUNT_TYPE, ";
		sql_command += " CUSTOMER_REFERENCE, ";
		sql_command += " ACCOUNT_OWNER, ";
		sql_command += " IGC_ON_ACCOUNT_FLAG, ";
		sql_command += " COMPLIANCE_ON_ACCOUNT_FLAG, ";
		sql_command += " SERVICES_ATTACHED, ";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " VALUES (b.ACCOUNT_KEY, ";
		sql_command += " b.ACCOUNT_NUMBER ,";
		sql_command += " b.DUNNING_LEVEL_CODE ,";
		sql_command += " b.MOST_RECENT_BILL_DATE ,";
		sql_command += " b.ACCOUNT_DESCRIPTION ,";
		sql_command += " b.CUSTOMER_GROUP_NAME ,";
		sql_command += " b.ACCOUNT_STATUS ,";
		sql_command += " b.ACCOUNT_TYPE_CODE ,";
		sql_command += " b.ACCOUNT_CLASS ,";
		sql_command += " b.ACCOUNT_SUBGROUP ,";
		sql_command += " b.ACCOUNT_ADDED_DATE ,";
		sql_command += " b.BANKRUPT_FLAG ,";
		sql_command += " b.BILLING_CYCLE ,";
		sql_command += " b.BILLING_STATUS ,";
		sql_command += " b.BILLING_STATUS_DATE ,";
		sql_command += " b.BUSINESS_TYPE ,";
		sql_command += " b.ACCOUNT_CLOSED_DATE ,";
		sql_command += " b.CUSTOMER_DESCRIPTION ,";
		sql_command += " b.DELINQUENCY_EXEMPT ,";
		sql_command += " b.DELINQUENCY_EXEMPT_FLAG ,";
		sql_command += " b.ACCOUNT_IN_DELINQUENCY ,";
		sql_command += " b.ACCOUNT_MODIFIED_DATE ,";
		sql_command += " b.PROPERTY_MOVE_IN_DATE ,";
		sql_command += " b.MOVE_IN_STATUS ,";
		sql_command += " b.MOVE_IN_ADMIN_HOLD_REASON ,";
		sql_command += " b.PROPERTY_MOVE_OUT_DATE ,";
		sql_command += " b.MOVE_OUT_STATUS ,";
		sql_command += " b.ACCOUNT_OPEN_DATE ,";
		sql_command += " b.RESPONSIBILITY ,";
		sql_command += " b.ACCOUNT_STATUS_DATE ,";
		sql_command += " b.ACCOUNT_SUSPEND_FLAG ,";
		sql_command += " b.RESIDENTIAL_STATUS ,";
		sql_command += " b.ENTITY_CODE ,";
		sql_command += " b.DUNNING_EXEMPTION_DESCRIPTION ,";
		sql_command += " b.ACCOUNT_TYPE ,";
		sql_command += " b.PROPERTY_ADDRESS ,";
		sql_command += " b.FLAT ,";
		sql_command += " b.HOUSE_NO ,";
		sql_command += " b.STREET_NAME ,";
		sql_command += " b.STREET_TYPE ,";
		sql_command += " b.SUBURB ,";
		sql_command += " b.POSTCODE ,";
		sql_command += " b.LNO_AREA ,";
		sql_command += " b.SERVICE_AREA_CODE ,";
		sql_command += " b.CUSTOMER_NUMBER ,";
		sql_command += " b.EXTERNAL_ACCOUNT_TYPE ,";
		sql_command += " b.CUSTOMER_REFERENCE ,";
		sql_command += " b.ACCOUNT_OWNER ,";
		sql_command += " b.IGC_ON_ACCOUNT_FLAG ,";
		sql_command += " b.COMPLIANCE_ON_ACCOUNT_FLAG ,";
		sql_command += " b.SERVICES_ATTACHED ,";
		sql_command += " b.ETL_SOURCE_DATETIME, ";
		sql_command += " b.ETL_RECORD_CREATED, ";
		sql_command += " b.ETL_RECORD_UPDATED);";


	  //  snowflake.execute({sqlText: sql_command});
    
    //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
            var rs = sqlStmt.execute();
            var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
         //   var number_of_rows_updated =  sqlStmt.getNumRowsUpdated(); 
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

//  Step 5.

//  Update last loaded date in control table
     
     control_sql_command = "call datahub_logging.sp_etl_load_control_job(''" + process_name + "'' );";
        snowflake.execute({sqlText: control_sql_command});
     
      if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Updated etl_load_control_job table with new extract dates'');";
            snowflake.execute({sqlText: debug_statement}); 
            }


//  Step 6.

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
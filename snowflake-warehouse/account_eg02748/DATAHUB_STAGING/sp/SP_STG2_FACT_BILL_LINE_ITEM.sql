CREATE OR REPLACE PROCEDURE "SP_STG2_FACT_BILL_LINE_ITEM"()
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

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG2_FACT_BILL_LINE_ITEM"});
         
    
//  Step 5.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG2_FACT_BILL_LINE_ITEM (  ";
		
		sql_command += " ACCOUNT_SID, ";
		sql_command += " ACCOUNT_SERVICE_SID, ";
		sql_command += " BILL_SID, ";
		sql_command += " GL_MAPPING_SID, ";
		sql_command += " BILL_LINE_ITEM_SETUP_SID, ";
		sql_command += " BILL_LINE_ITEM_MODIFIED_DATE_SID, ";
		sql_command += " BILL_DATE_SID, ";
		sql_command += " BILL_LINE_ITEM_ADDED_DATE_SID, ";
		sql_command += " BILL_DUE_DATE_SID, ";
		sql_command += " BILL_PAID_DATE_SID, ";
		sql_command += " BILL_POSTED_DATE_SID, ";
		sql_command += " BILL_CREATED_DATE_SID, ";
		sql_command += " BILL_FROM_DATE_SID, ";
		sql_command += " BILL_TO_DATE_SID, ";
		sql_command += " BILL_LINE_ITEM_KEY, ";
		sql_command += " ACCOUNT_KEY, ";
		sql_command += " BILL_LINE_ITEM_MODIFIED_DATE, ";
		sql_command += " BILL_DATE, ";
		sql_command += " BILL_LINE_ITEM_ADDED_DATE, ";
		sql_command += " BILL_DUE_DATE, ";
		sql_command += " BILL_PAID_DATE, ";
		sql_command += " BILL_POSTED_DATE, ";
		sql_command += " BILL_CREATED_DATE, ";
		sql_command += " BILL_KEY, ";
		sql_command += " BILL_LINE_ITEM_SETUP_KEY, ";
		sql_command += " BILL_LINE_ITEM_PAID_STATUS, ";
		sql_command += " ACCOUNT_SERVICE_KEY, ";
		sql_command += " BILL_LINE_ITEM_TEXT, ";
		sql_command += " PENDING_TYPE, ";
		sql_command += " BILL_FROM_DATE, ";
		sql_command += " BILL_TO_DATE, ";
		sql_command += " BILL_LINE_ITEM_AMOUNT, ";
		sql_command += " BILL_LINE_ITEM_ACTUAL_AMOUNT, ";
		sql_command += " BUDGETED_AMOUNT_FLAG, ";
		sql_command += " BILLABLE_UNITS, ";
		sql_command += " BILLABLE_USAGE, ";
		sql_command += " PENALTY_DATE, ";
		sql_command += " LINEITEM_CODE,";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " SELECT ";
		sql_command += " CASE WHEN LINEITEM.ACCOUNT_KEY IS NULL OR ACCOUNT.ACCOUNT_KEY IS NULL THEN 0 ELSE ACCOUNT.ACCOUNT_SID END AS ACCOUNT_SID, ";
		sql_command += " CASE WHEN LINEITEM.ACCOUNT_SERVICE_KEY IS NULL OR ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY IS NULL THEN 0 ELSE ACCOUNTSERVICE.ACCOUNT_SERVICE_SID END AS ACCOUNT_SERVICE_SID,";
		sql_command += " CASE WHEN LINEITEM.BILL_KEY IS NULL OR BILL.BILL_KEY IS NULL THEN 0 ELSE BILL.BILL_SID END AS BILL_SID, ";
		sql_command += " CASE WHEN (LINEITEM.LINEITEM_CODE IS NULL OR GLMAPPING.GL_MAPPING_CODE IS NULL) THEN 0 ELSE GLMAPPING.GL_MAPPING_SID END AS GL_MAPPING_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_LINE_ITEM_SETUP_KEY IS NULL OR LINEITEMSETUP.BILL_LINE_ITEM_SETUP_KEY IS NULL THEN 0 ELSE LINEITEMSETUP.BILL_LINE_ITEM_SETUP_SID END AS BILL_LINE_ITEM_SETUP_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_LINE_ITEM_MODIFIED_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_LINE_ITEM_MODIFIED_DATE,''YYYYMMDD'') END as BILL_LINE_ITEM_MODIFIED_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_DATE,''YYYYMMDD'') END AS BILL_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_LINE_ITEM_ADDED_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_LINE_ITEM_ADDED_DATE,''YYYYMMDD'') END AS BILL_LINE_ITEM_ADDED_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_DUE_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_DUE_DATE,''YYYYMMDD'') END AS BILL_DUE_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_PAID_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_PAID_DATE,''YYYYMMDD'') END AS BILL_PAID_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_POSTED_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_POSTED_DATE,''YYYYMMDD'') END AS BILL_POSTED_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_CREATED_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_CREATED_DATE,''YYYYMMDD'') END AS BILL_CREATED_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_FROM_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_FROM_DATE,''YYYYMMDD'') END AS BILL_FROM_DATE_SID, ";
		sql_command += " CASE WHEN LINEITEM.BILL_TO_DATE IS NULL THEN 0 ELSE to_char(LINEITEM.BILL_TO_DATE,''YYYYMMDD'') END AS BILL_TO_DATE_SID, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_KEY, ";
		sql_command += " LINEITEM.ACCOUNT_KEY, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_MODIFIED_DATE, ";
		sql_command += " LINEITEM.BILL_DATE, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_ADDED_DATE, ";
		sql_command += " LINEITEM.BILL_DUE_DATE, ";
		sql_command += " LINEITEM.BILL_PAID_DATE, ";
		sql_command += " LINEITEM.BILL_POSTED_DATE, ";
		sql_command += " LINEITEM.BILL_CREATED_DATE, ";
		sql_command += " LINEITEM.BILL_KEY, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_SETUP_KEY, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_PAID_STATUS, ";
		sql_command += " LINEITEM.ACCOUNT_SERVICE_KEY, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_TEXT, ";
		sql_command += " LINEITEM.PENDING_TYPE, ";
		sql_command += " LINEITEM.BILL_FROM_DATE, ";
		sql_command += " LINEITEM.BILL_TO_DATE, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_AMOUNT, ";
		sql_command += " LINEITEM.BILL_LINE_ITEM_ACTUAL_AMOUNT, ";
		sql_command += " LINEITEM.BUDGETED_AMOUNT_FLAG, ";
		sql_command += " LINEITEM.BILLABLE_UNITS, ";
		sql_command += " LINEITEM.BILLABLE_USAGE, ";
		sql_command += " LINEITEM.PENALTY_DATE, ";
		sql_command += " LINEITEM.LINEITEM_CODE,";
		sql_command += " LINEITEM.ETL_SOURCE_DATETIME, ";
		sql_command += " LINEITEM.ETL_RECORD_CREATED, ";
		sql_command += " LINEITEM.ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG_FACT_BILL_LINE_ITEM LINEITEM";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_CUSTOMER_ACCOUNT ACCOUNT";
		sql_command += "     ON LINEITEM.ACCOUNT_KEY =ACCOUNT.ACCOUNT_KEY ";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_ACCOUNT_SERVICE ACCOUNTSERVICE";
		sql_command += "     ON LINEITEM.ACCOUNT_SERVICE_KEY =ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY ";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_BILL BILL";
		sql_command += "     ON LINEITEM.BILL_KEY =BILL.BILL_KEY ";
		sql_command += "   LEFT JOIN (SELECT max(GL_MAPPING_SID) GL_MAPPING_SID, upper(GL_MAPPING_CODE) GL_MAPPING_CODE FROM DATAHUB_ANALYTICS.DIM_GL_MAPPING_INFORMATION GLMAPPING group by upper(GL_MAPPING_CODE)) GLMAPPING ";
		sql_command += "     ON upper(LINEITEM.LINEITEM_CODE) = GL_MAPPING_CODE ";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_BILL_LINE_ITEM_SETUP LINEITEMSETUP";
		sql_command += "     ON LINEITEM.BILL_LINE_ITEM_SETUP_KEY =LINEITEMSETUP.BILL_LINE_ITEM_SETUP_KEY ;";


		
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
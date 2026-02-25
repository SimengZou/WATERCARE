CREATE OR REPLACE PROCEDURE "SP_STG2_FACT_ACCOUNT_TRANSACTION"()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG2_FACT_ACCOUNT_TRANSACTION"});
         
    
//  Step 5.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG2_FACT_ACCOUNT_TRANSACTION (  ";
		sql_command += " ACCOUNT_SID, ";
		sql_command += " ACCOUNT_SERVICE_SID, ";
		sql_command += " BILL_SID, ";
		sql_command += " PAYMENT_SID, ";
		sql_command += " ADJUSTMENT_SID, ";
		sql_command += " REFUND_SID, ";
		sql_command += " TRANSACTION_COMMENTS_SID, ";
		sql_command += " TRANSACTION_BY_EMPLOYEE_SID, ";
		sql_command += " TRANSACTION_TYPE_SID, ";
		sql_command += " TRANSACTION_DATE_SID, ";
		sql_command += " TRANSACTION_ADDED_DATE_SID, ";
		sql_command += " TRANSACTION_MODIFIED_DATE_SID, ";
		sql_command += " BILL_DATE_SID, ";
		sql_command += " ADJUSTMENT_STATUS_DATE_SID, ";
		sql_command += " ADJUSTMENT_DATE_SID, ";
		sql_command += " ACCOUNT_KEY, ";
		sql_command += " ACCOUNT_SERVICE_KEY, ";
		sql_command += " BILL_KEY, ";
		sql_command += " PAY_KEY, ";
		sql_command += " ADJUSTMENT_KEY, ";
		sql_command += " REFUND_KEY, ";
		sql_command += " TRANSACTION_COMMENTS_KEY, ";
		sql_command += " TRANSACTION_BY_EMPLOYEE_KEY, ";
		sql_command += " TRANSACTION_ADDED_BY_EMPLOYEE_KEY, ";
		sql_command += " TRANSACTION_MODIFIED_BY_EMPLOYEE_KEY, ";
		sql_command += " TRANSACTION_TYPE_CODE, ";
		sql_command += " TRANSACTION_DESIGNATOR_CODE, ";
		sql_command += " TRANSACTION_NUMBER, ";
		sql_command += " TRANSACTION_DATE, ";
		sql_command += " TRANSACTION_ADDED_DATE, ";
		sql_command += " TRANSACTION_MODIFIED_DATE, ";
		sql_command += " BILL_DATE, ";
		sql_command += " ADJUSTMENT_STATUS_DATE, ";
		sql_command += " ADJUSTMENT_DATE, ";
		sql_command += " INTERNAL_FLAG, ";
		sql_command += " TRANSACTION_AMOUNT, ";
		sql_command += " NEEDS_JOURNAL, ";
		sql_command += " BILL_LINE_ITEM_KEY, ";
		sql_command += " TRANSACTION_SOURCE_CODE, ";
		sql_command += " TRANSACTION_SOURCE_TYPE, ";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " SELECT";
		sql_command += " CASE WHEN TRAN.ACCOUNT_KEY IS NULL OR ACCOUNT.ACCOUNT_KEY IS NULL THEN 0 ELSE ACCOUNT.ACCOUNT_SID END AS ACCOUNT_SID,";
		sql_command += " CASE WHEN TRAN.ACCOUNT_SERVICE_KEY IS NULL OR ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY IS NULL THEN 0 ELSE ACCOUNTSERVICE.ACCOUNT_SERVICE_SID END AS ACCOUNT_SERVICE_SID,";
		sql_command += " CASE WHEN TRAN.BILL_KEY IS NULL OR BILL.BILL_KEY IS NULL THEN 0 ELSE BILL.BILL_SID END AS BILL_SID, ";

		sql_command += " CASE WHEN TRAN.PAY_KEY IS NULL OR PAYMENT.PAY_KEY IS NULL THEN 0 ELSE PAYMENT.PAYMENT_SID END AS PAYMENT_SID,";
		sql_command += " CASE WHEN TRAN.ADJUSTMENT_KEY IS NULL OR ADJUSTMENT.ADJUSTMENT_KEY IS NULL THEN 0 ELSE ADJUSTMENT.ADJUSTMENT_SID END AS ADJUSTMENT_SID,";
		sql_command += " CASE WHEN TRAN.REFUND_KEY IS NULL OR REFUND.REFUND_KEY IS NULL THEN 0 ELSE REFUND.REFUND_SID END AS REFUND_SID,";
		sql_command += " CASE WHEN TRAN.TRANSACTION_COMMENTS_KEY IS NULL OR TRANSACTION_COMMENTS.TRANSACTION_COMMENTS_KEY IS NULL THEN 0 ELSE TRANSACTION_COMMENTS.TRANSACTION_COMMENTS_SID END AS TRANSACTION_COMMENTS_SID,";

		sql_command += " CASE WHEN TRAN.TRANSACTION_BY_EMPLOYEE_KEY IS NULL OR TRANSACTION_BY_EMPLOYEE.EMPLOYEE_CODE IS NULL THEN 0 ELSE TRANSACTION_BY_EMPLOYEE.TRANSACTION_BY_EMPLOYEE_SID END AS TRANSACTION_BY_EMPLOYEE_SID,";
		sql_command += " CASE WHEN TRANSACTION_TYPE.TRANSACTION_TYPE_SID is null  THEN 0 ELSE TRANSACTION_TYPE.TRANSACTION_TYPE_SID END AS TRANSACTION_TYPE_SID,";
	// Transaction type SID should never be null - as codes are generated from account transaction table   

		sql_command += " CASE WHEN TRAN.TRANSACTION_DATE IS NULL THEN 0 ELSE to_char(TRAN.TRANSACTION_DATE,''YYYYMMDD'') END AS TRANSACTION_DATE_SID,";

		sql_command += " CASE WHEN TRAN.TRANSACTION_ADDED_DATE IS NULL THEN 0 ELSE to_char(TRAN.TRANSACTION_ADDED_DATE,''YYYYMMDD'') END AS TRANSACTION_ADDED_DATE_SID,";
		sql_command += " CASE WHEN TRAN.TRANSACTION_MODIFIED_DATE IS NULL THEN 0 ELSE to_char(TRAN.TRANSACTION_MODIFIED_DATE,''YYYYMMDD'') END AS TRANSACTION_MODIFIED_DATE_SID,";
		sql_command += " CASE WHEN TRAN.BILL_DATE IS NULL THEN 0 ELSE to_char(TRAN.BILL_DATE,''YYYYMMDD'') END AS BILL_DATE_SID,";
		sql_command += " CASE WHEN TRAN.ADJUSTMENT_STATUS_DATE IS NULL THEN 0 ELSE to_char(TRAN.ADJUSTMENT_STATUS_DATE,''YYYYMMDD'') END AS ADJUSTMENT_STATUS_DATE_SID,";
		sql_command += " CASE WHEN TRAN.ADJUSTMENT_DATE IS NULL THEN 0 ELSE to_char(TRAN.ADJUSTMENT_DATE,''YYYYMMDD'') END AS ADJUSTMENT_DATE_SID,";

		sql_command += " TRAN.ACCOUNT_KEY, ";
		sql_command += " TRAN.ACCOUNT_SERVICE_KEY, ";
		sql_command += " TRAN.BILL_KEY, ";
		sql_command += " TRAN.PAY_KEY, ";
		sql_command += " TRAN.ADJUSTMENT_KEY, ";
		sql_command += " TRAN.REFUND_KEY, ";
		sql_command += " TRAN.TRANSACTION_COMMENTS_KEY, ";
		sql_command += " TRAN.TRANSACTION_BY_EMPLOYEE_KEY, ";
		sql_command += " TRAN.TRANSACTION_ADDED_BY_EMPLOYEE_KEY, ";
		sql_command += " TRAN.TRANSACTION_MODIFIED_BY_EMPLOYEE_KEY, ";
		sql_command += " TRAN.TRANSACTION_TYPE_CODE, ";
		sql_command += " TRAN.TRANSACTION_DESIGNATOR_CODE, ";
		sql_command += " TRAN.TRANSACTION_NUMBER, ";
		sql_command += " TRAN.TRANSACTION_DATE, ";
		sql_command += " TRAN.TRANSACTION_ADDED_DATE, ";
		sql_command += " TRAN.TRANSACTION_MODIFIED_DATE, ";
		sql_command += " TRAN.BILL_DATE, ";
		sql_command += " TRAN.ADJUSTMENT_STATUS_DATE, ";
		sql_command += " TRAN.ADJUSTMENT_DATE, ";
		sql_command += " TRAN.INTERNAL_FLAG, ";
		sql_command += " TRAN.TRANSACTION_AMOUNT, ";
		sql_command += " TRAN.NEEDS_JOURNAL, ";
		sql_command += " TRAN.BILL_LINE_ITEM_KEY, ";
		sql_command += " TRAN.TRANSACTION_SOURCE_CODE, ";
		sql_command += " TRAN.TRANSACTION_SOURCE_TYPE, ";
		sql_command += " TRAN.ETL_SOURCE_DATETIME, ";
		sql_command += " TRAN.ETL_RECORD_CREATED, ";
		sql_command += " TRAN.ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG_FACT_ACCOUNT_TRANSACTION TRAN";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_CUSTOMER_ACCOUNT ACCOUNT";
		sql_command += "   ON TRAN.ACCOUNT_KEY =ACCOUNT.ACCOUNT_KEY";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_ACCOUNT_SERVICE ACCOUNTSERVICE";
		sql_command += "   ON TRAN.ACCOUNT_SERVICE_KEY =ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_BILL BILL";
		sql_command += "   ON TRAN.BILL_KEY =BILL.BILL_KEY";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_PAYMENT PAYMENT";
		sql_command += "   ON TRAN.PAY_KEY =PAYMENT.PAY_KEY ";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_ADJUSTMENT ADJUSTMENT";
		sql_command += "   ON TRAN.ADJUSTMENT_KEY =ADJUSTMENT.ADJUSTMENT_KEY  ";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_REFUND REFUND";
		sql_command += "   ON TRAN.REFUND_KEY = REFUND.REFUND_KEY  ";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_TRANSACTION_COMMENTS TRANSACTION_COMMENTS";
		sql_command += "   ON TRAN.TRANSACTION_COMMENTS_KEY = TRANSACTION_COMMENTS.TRANSACTION_COMMENTS_KEY   ";
		sql_command += "   ";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_TRANSACTION_BY_EMPLOYEE TRANSACTION_BY_EMPLOYEE";
		sql_command += "   ON upper(TRAN.TRANSACTION_BY_EMPLOYEE_KEY) = upper(TRANSACTION_BY_EMPLOYEE.EMPLOYEE_CODE)  ";
		sql_command += "  LEFT JOIN DATAHUB_ANALYTICS.DIM_TRANSACTION_TYPE TRANSACTION_TYPE";
		sql_command += "   ON coalesce(TRAN.TRANSACTION_TYPE_CODE,-1) = coalesce(TRANSACTION_TYPE.TRANSACTION_TYPE_CODE,-1)  ";
		sql_command += "      AND COALESCE(TRAN.TRANSACTION_DESIGNATOR_CODE,-1)= COALESCE(TRANSACTION_TYPE.TRANSACTION_DESIGNATOR_CODE,-1);";


		
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
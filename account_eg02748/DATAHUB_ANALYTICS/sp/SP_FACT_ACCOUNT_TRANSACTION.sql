CREATE OR REPLACE PROCEDURE "SP_FACT_ACCOUNT_TRANSACTION"()
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
    
		
        var sql_command = "MERGE INTO DATAHUB_ANALYTICS.FACT_ACCOUNT_TRANSACTION target USING (SELECT  ";
		
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
				sql_command += " ETL_RECORD_UPDATED";

				sql_command += " FROM DATAHUB_STAGING.STG2_FACT_ACCOUNT_TRANSACTION";
				sql_command += " ) AS b ON target.TRANSACTION_NUMBER =b.TRANSACTION_NUMBER";
				sql_command += " WHEN MATCHED THEN UPDATE";
				sql_command += " SET target.ACCOUNT_SID =b.ACCOUNT_SID ,";	
				sql_command += " target.ACCOUNT_SERVICE_SID =b.ACCOUNT_SERVICE_SID ,";
				sql_command += " target.BILL_SID =b.BILL_SID ,";
				sql_command += " target.PAYMENT_SID =b.PAYMENT_SID ,";
				sql_command += " target.ADJUSTMENT_SID =b.ADJUSTMENT_SID ,";
				sql_command += " target.REFUND_SID =b.REFUND_SID ,";
				sql_command += " target.TRANSACTION_COMMENTS_SID =b.TRANSACTION_COMMENTS_SID ,";
				sql_command += " target.TRANSACTION_BY_EMPLOYEE_SID =b.TRANSACTION_BY_EMPLOYEE_SID ,";
				sql_command += " target.TRANSACTION_TYPE_SID =b.TRANSACTION_TYPE_SID ,";
				sql_command += " target.TRANSACTION_DATE_SID =b.TRANSACTION_DATE_SID ,";
				sql_command += " target.TRANSACTION_ADDED_DATE_SID =b.TRANSACTION_ADDED_DATE_SID ,";
				sql_command += " target.TRANSACTION_MODIFIED_DATE_SID =b.TRANSACTION_MODIFIED_DATE_SID ,";
				sql_command += " target.BILL_DATE_SID =b.BILL_DATE_SID ,";
				sql_command += " target.ADJUSTMENT_STATUS_DATE_SID =b.ADJUSTMENT_STATUS_DATE_SID ,";
				sql_command += " target.ADJUSTMENT_DATE_SID =b.ADJUSTMENT_DATE_SID ,";
				sql_command += " target.TRANSACTION_DATE =b.TRANSACTION_DATE ,";
				sql_command += " target.TRANSACTION_ADDED_DATE =b.TRANSACTION_ADDED_DATE ,";
				sql_command += " target.TRANSACTION_MODIFIED_DATE =b.TRANSACTION_MODIFIED_DATE ,";
				sql_command += " target.BILL_DATE =b.BILL_DATE ,";
				sql_command += " target.ADJUSTMENT_STATUS_DATE =b.ADJUSTMENT_STATUS_DATE ,";
				sql_command += " target.ADJUSTMENT_DATE =b.ADJUSTMENT_DATE ,";
				sql_command += " target.INTERNAL_FLAG =b.INTERNAL_FLAG ,";
				sql_command += " target.TRANSACTION_AMOUNT =b.TRANSACTION_AMOUNT ,";
				sql_command += " target.NEEDS_JOURNAL =b.NEEDS_JOURNAL ,";
				sql_command += " target.BILL_LINE_ITEM_KEY =b.BILL_LINE_ITEM_KEY , ";
				sql_command += " target.TRANSACTION_SOURCE_CODE =b.TRANSACTION_SOURCE_CODE , ";
				sql_command += " target.TRANSACTION_SOURCE_TYPE =b.TRANSACTION_SOURCE_TYPE , ";	
				sql_command += " target.ETL_SOURCE_DATETIME=b.ETL_SOURCE_DATETIME , ";
				sql_command += " target.ETL_RECORD_UPDATED =current_timestamp()";
				sql_command += " WHEN NOT MATCHED THEN INSERT ( ";
				
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
				sql_command += " VALUES (  ";
				
				sql_command += " b.ACCOUNT_SID ,";
				sql_command += " b.ACCOUNT_SERVICE_SID ,";
				sql_command += " b.BILL_SID ,";
				sql_command += " b.PAYMENT_SID ,";
				sql_command += " b.ADJUSTMENT_SID ,";
				sql_command += " b.REFUND_SID ,";
				sql_command += " b.TRANSACTION_COMMENTS_SID ,";
				sql_command += " b.TRANSACTION_BY_EMPLOYEE_SID ,";
				sql_command += " b.TRANSACTION_TYPE_SID ,";
				sql_command += " b.TRANSACTION_DATE_SID ,";
				sql_command += " b.TRANSACTION_ADDED_DATE_SID ,";
				sql_command += " b.TRANSACTION_MODIFIED_DATE_SID ,";
				sql_command += " b.BILL_DATE_SID ,";
				sql_command += " b.ADJUSTMENT_STATUS_DATE_SID ,";
				sql_command += " b.ADJUSTMENT_DATE_SID ,";
				sql_command += " b.TRANSACTION_NUMBER ,";
				sql_command += " b.TRANSACTION_DATE ,";
				sql_command += " b.TRANSACTION_ADDED_DATE ,";
				sql_command += " b.TRANSACTION_MODIFIED_DATE ,";
				sql_command += " b.BILL_DATE ,";
				sql_command += " b.ADJUSTMENT_STATUS_DATE ,";
				sql_command += " b.ADJUSTMENT_DATE ,";
				sql_command += " b.INTERNAL_FLAG ,";
				sql_command += " b.TRANSACTION_AMOUNT ,";
				sql_command += " b.NEEDS_JOURNAL ,";
				sql_command += " b.BILL_LINE_ITEM_KEY ,";
				sql_command += " b.TRANSACTION_SOURCE_CODE ,";
				sql_command += " b.TRANSACTION_SOURCE_TYPE ,";
				sql_command += "  b.ETL_SOURCE_DATETIME, ";
				sql_command += "  b.ETL_RECORD_CREATED, ";
				sql_command += "  b.ETL_RECORD_UPDATED);";



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
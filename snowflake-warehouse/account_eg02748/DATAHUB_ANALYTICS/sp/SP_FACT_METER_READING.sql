CREATE OR REPLACE PROCEDURE "SP_FACT_METER_READING"()
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
    
		
        var sql_command = "MERGE INTO DATAHUB_ANALYTICS.FACT_METER_READING target USING (SELECT  ";
		sql_command += " WATER_METER_SID, ";
		sql_command += " READ_DATE_SID, ";
		sql_command += " READING_ADDED_SID, ";
		sql_command += " READ_SOURCE_SID, ";
		sql_command += " CUSTOMER_PROBLEM_SID, ";
		sql_command += " READER_CODE_SID, ";
		sql_command += " EMPLOYEE_SID, ";
		sql_command += " READ_REASON_SID, ";
		sql_command += " FIELD_NOTES, ";
		sql_command += " READING_KEY, ";
		sql_command += " BILLABLE_USAGE, ";
		sql_command += " READING, ";
		sql_command += " USAGE, ";
		sql_command += " METER_READING_ESTIMATE_FLAG, ";
		sql_command += " READY_FLAG, ";
		sql_command += " FINAL_FLAG, ";
		sql_command += " NO_READ_FLAG, ";
		sql_command += " WORK_ORDER, ";
		sql_command += " INSPECTION, ";
		sql_command += " ADDED_BY, ";
		sql_command += " LAST_MODIFIED_BY, ";
		sql_command += " READING_ADDED_DATE, ";
		sql_command += " READ_DATE, ";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG2_FACT_METER_READING";
		sql_command += " ) AS b ON target.READING_KEY =b.READING_KEY";
		sql_command += " WHEN MATCHED THEN UPDATE";
		sql_command += "       SET target.WATER_METER_SID=b.WATER_METER_SID,";
		sql_command += "         target.READ_DATE_SID = b.READ_DATE_SID,";
		sql_command += " target.READING_ADDED_SID = b.READING_ADDED_SID,";
		sql_command += " target.READ_SOURCE_SID = b.READ_SOURCE_SID,";
		sql_command += " target.CUSTOMER_PROBLEM_SID  = b.CUSTOMER_PROBLEM_SID,";
		sql_command += " target.READER_CODE_SID  = b.READER_CODE_SID,";
		sql_command += " target.EMPLOYEE_SID  = b.EMPLOYEE_SID,";
		sql_command += " target.READ_REASON_SID  = b.READ_REASON_SID,";
		sql_command += " target.FIELD_NOTES  = b.FIELD_NOTES,";
		sql_command += " target.BILLABLE_USAGE  = b.BILLABLE_USAGE,";
		sql_command += " target.READING  = b.READING,";
		sql_command += " target.USAGE  = b.USAGE,";
		sql_command += " target.METER_READING_ESTIMATE_FLAG  = b.METER_READING_ESTIMATE_FLAG,";
		sql_command += " target.READY_FLAG  = b.READY_FLAG,";
		sql_command += " target.FINAL_FLAG  = b.FINAL_FLAG,";
		sql_command += " target.NO_READ_FLAG  = b.NO_READ_FLAG,";
		sql_command += " target.WORK_ORDER  = b.WORK_ORDER,";
		sql_command += " target.INSPECTION  = b.INSPECTION,";
		sql_command += " target.ADDED_BY  = b.ADDED_BY,";
		sql_command += " target.LAST_MODIFIED_BY  = b.LAST_MODIFIED_BY,";
		sql_command += " target.READING_ADDED_DATE  = b.READING_ADDED_DATE,";
		sql_command += " target.READ_DATE  = b.READ_DATE,";
		sql_command += " target.ETL_SOURCE_DATETIME=b.ETL_SOURCE_DATETIME , ";
		sql_command += " target.ETL_RECORD_UPDATED =current_timestamp()";
		sql_command += " WHEN NOT MATCHED THEN INSERT (WATER_METER_SID, ";
		sql_command += " READ_DATE_SID, ";
		sql_command += " READING_ADDED_SID, ";
		sql_command += " READ_SOURCE_SID, ";
		sql_command += " CUSTOMER_PROBLEM_SID, ";
		sql_command += " READER_CODE_SID, ";
		sql_command += " EMPLOYEE_SID, ";
		sql_command += " READ_REASON_SID, ";
		sql_command += " FIELD_NOTES, ";
		sql_command += " READING_KEY, ";
		sql_command += " BILLABLE_USAGE, ";
		sql_command += " READING, ";
		sql_command += " USAGE, ";
		sql_command += " METER_READING_ESTIMATE_FLAG, ";
		sql_command += " READY_FLAG, ";
		sql_command += " FINAL_FLAG, ";
		sql_command += " NO_READ_FLAG, ";
		sql_command += " WORK_ORDER, ";
		sql_command += " INSPECTION, ";
		sql_command += " ADDED_BY, ";
		sql_command += " LAST_MODIFIED_BY, ";
		sql_command += " READING_ADDED_DATE, ";
		sql_command += " READ_DATE, ";
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " VALUES ( b.WATER_METER_SID, ";
		sql_command += "  b.READ_DATE_SID, ";
		sql_command += "  b.READING_ADDED_SID, ";
		sql_command += "  b.READ_SOURCE_SID, ";
		sql_command += "  b.CUSTOMER_PROBLEM_SID, ";
		sql_command += "  b.READER_CODE_SID, ";
		sql_command += "  b.EMPLOYEE_SID, ";
		sql_command += "  b.READ_REASON_SID, ";
		sql_command += "  b.FIELD_NOTES, ";
		sql_command += "  b.READING_KEY, ";
		sql_command += "  b.BILLABLE_USAGE, ";
		sql_command += "  b.READING, ";
		sql_command += "  b.USAGE, ";
		sql_command += "  b.METER_READING_ESTIMATE_FLAG, ";
		sql_command += "  b.READY_FLAG, ";
		sql_command += "  b.FINAL_FLAG, ";
		sql_command += "  b.NO_READ_FLAG, ";
		sql_command += "  b.WORK_ORDER, ";
		sql_command += "  b.INSPECTION, ";
		sql_command += "  b.ADDED_BY, ";
		sql_command += "  b.LAST_MODIFIED_BY, ";
		sql_command += "  b.READING_ADDED_DATE, ";
		sql_command += "  b.READ_DATE, ";
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

            log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''Failed to get number of rows. Quitting.'', " + process_execution_id.toString() + ",0,0);";
            snowflake.execute({sqlText: log_sql_command});            
		
            if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Failed to get number of rows inserted, err = " + err + "'');";
            snowflake.execute({sqlText: debug_statement}); 
			 return "Failed"; 
            }
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
 

        snowflake.execute ({sqlText: "commit"});
		
		 log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});


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
		 log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''Failed to get number of rows. Rolling back. Quitting.'', " + process_execution_id.toString() + ",0,0);";
	     snowflake.execute({sqlText: log_sql_command}); 
		 if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Failed to get number of rows inserted, err = " + err + "'');";
            snowflake.execute({sqlText: debug_statement}); 
            }
		
        snowflake.execute ({sqlText: "rollback"});
        return "Failed: " + err;   // Return error indicator.
        }


//    Return SP status to caller
  
    return result;
  
  
    ';
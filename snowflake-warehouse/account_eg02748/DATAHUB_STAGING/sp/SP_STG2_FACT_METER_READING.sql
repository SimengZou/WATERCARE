CREATE OR REPLACE PROCEDURE "SP_STG2_FACT_METER_READING"()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG2_FACT_METER_READING"});
         
    
//  Step 5.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG2_FACT_METER_READING (  ";
		sql_command += " WATER_METER_SID, ";
		sql_command += " READ_DATE_SID,";
		sql_command += " READING_ADDED_SID,";
		sql_command += " READ_SOURCE_SID, ";
		sql_command += " CUSTOMER_PROBLEM_SID, ";
		sql_command += " READER_CODE_SID, ";
		sql_command += " EMPLOYEE_SID, ";
		sql_command += " READ_REASON_SID, ";
		sql_command += " COMP_KEY, ";
		sql_command += " READ_DATE, ";
		sql_command += " READING_ADDED_DATE, ";
		sql_command += " READ_SOURCE, ";
		sql_command += " CUSTOMER_PROBLEM_NUMBER, ";
		sql_command += " READER_CODE, ";
		sql_command += " READ_BY, ";
		sql_command += " READ_REASON, ";
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
		sql_command += " ETL_SOURCE_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " SELECT ";
		sql_command += "        case when METERREADING.comp_key is null or meter.comp_key is null then 0 else WATER_METER_SID end as WATER_METER_SID,";
		sql_command += "        case when METERREADING.read_date is null then 0 else to_char(METERREADING.read_date,''YYYYMMDD'') end as READ_DATE_SID,";
		sql_command += "        case when METERREADING.reading_added_date is null then 0 else to_char(METERREADING.reading_added_date,''YYYYMMDD'') end as READING_ADDED_SID,";
		sql_command += "        case when METERREADING.READ_SOURCE is null or READ_SOURCE.READ_SOURCE_CODE is null then 0 else READ_SOURCE_SID end as READ_SOURCE_SID,";
		sql_command += "        case when METERREADING.CUSTOMER_PROBLEM_NUMBER is null or CUSTOMER_PROBLEM.CUSTOMER_PROBLEM_NUMBER is null then 0 else CUSTOMER_PROBLEM_SID end as CUSTOMER_PROBLEM_SID,";
		sql_command += "        case when METERREADING.READER_CODE is null or READER_CODE.READER_CODE is null then 0 else READER_CODE_SID end as READER_CODE_SID,";
		sql_command += "        case when METERREADING.READ_BY is null or EMPLOYEE.EMPLOYEE_CODE is null then 0 else EMPLOYEE_SID end as EMPLOYEE_SID,";
		sql_command += "        case when METERREADING.READ_REASON is null or READ_REASON.READ_REASON_CODE is null then 0 else READ_REASON_SID end as READ_REASON_SID,";
		sql_command += "        METERREADING.COMP_KEY, ";
		sql_command += "        METERREADING.READ_DATE, ";
		sql_command += "        METERREADING.READING_ADDED_DATE, ";
		sql_command += "        METERREADING.READ_SOURCE, ";
		sql_command += "        METERREADING.CUSTOMER_PROBLEM_NUMBER, ";
		sql_command += "        METERREADING.READER_CODE, ";
		sql_command += "        METERREADING.READ_BY, ";
		sql_command += "        METERREADING.READ_REASON, ";
		sql_command += "        METERREADING.FIELD_NOTES, ";
		sql_command += "        METERREADING.READING_KEY, ";
		sql_command += "        METERREADING.BILLABLE_USAGE, ";
		sql_command += "        METERREADING.READING, ";
		sql_command += "        METERREADING.USAGE, ";
		sql_command += "        METERREADING.METER_READING_ESTIMATE_FLAG, ";
		sql_command += "        METERREADING.READY_FLAG, ";
		sql_command += "        METERREADING.FINAL_FLAG, ";
		sql_command += "        METERREADING.NO_READ_FLAG, ";
		sql_command += "        METERREADING.WORK_ORDER, ";
		sql_command += "        METERREADING.INSPECTION, ";
		sql_command += "        METERREADING.ADDED_BY, ";
		sql_command += "        METERREADING.LAST_MODIFIED_BY, ";
		sql_command += "        METERREADING.ETL_SOURCE_DATETIME,";
		sql_command += "        METERREADING.ETL_RECORD_CREATED,";
		sql_command += "        METERREADING.ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG_FACT_METER_READING METERREADING";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_READER_CODE  READER_CODE";
		sql_command += "     ON METERREADING.READER_CODE=READER_CODE.READER_CODE";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_READ_REASON  READ_REASON";
		sql_command += "     ON METERREADING.READ_REASON=READ_REASON.READ_REASON_CODE";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_READ_SOURCE  READ_SOURCE";
		sql_command += "     ON METERREADING.READ_SOURCE=READ_SOURCE.READ_SOURCE_CODE";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_CUSTOMER_PROBLEM CUSTOMER_PROBLEM ";
		sql_command += "     ON METERREADING.CUSTOMER_PROBLEM_NUMBER=CUSTOMER_PROBLEM.CUSTOMER_PROBLEM_NUMBER";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_EMPLOYEE EMPLOYEE ";
		sql_command += "     ON METERREADING.READ_BY=EMPLOYEE.EMPLOYEE_CODE ";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_WATER_METER METER";
		sql_command += "     ON METERREADING.comp_key=meter.comp_key";
		sql_command += "    ;";

		
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
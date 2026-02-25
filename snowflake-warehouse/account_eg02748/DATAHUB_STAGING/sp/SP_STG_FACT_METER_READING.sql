CREATE OR REPLACE PROCEDURE "SP_STG_FACT_METER_READING"()
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
// get timestamp for METERREADING table
//   get_datetime_sql_command = "select to_char(inc_etl_load_datetime,''YYYY-MM-DD HH:MM:SS'') from datahub_logging.etl_load_control_job ";
	
//	get_datetime_sql_command += "where stg_process_name = ''" + process_name + "'' AND table_name = ''IPS_ASSETMANAGEMENT_WATER_METERREADING'';";
//    var date_status_result = snowflake.execute({sqlText: get_datetime_sql_command}); 
 //   var METERREADING_inc_load_datetime =default_extraction_timestamp; 
//	if (date_status_result.getRowCount() > 0) 
//        {
//        date_status_result.next();                                                                               // expecting a maximum of 1 row
//        METERREADING_inc_load_datetime = date_status_result.getColumnValue(1);                                          // and only 1 return value
		//update the latest time included in extract for METERREADING table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_METERREADING ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_ASSETMANAGEMENT_WATER_METERREADING'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		

		
		
        
	if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Date updated for incremental extract for METERREADING table.'');";
            snowflake.execute({sqlText: debug_statement}); 
            }	
            
//  Step 5.

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG_FACT_METER_READING"});
         
    
//  Step 6.

//  Run insert statement
    
      var sql_command =  " INSERT INTO DATAHUB_STAGING.STG_FACT_METER_READING (  ";
		sql_command +=  "  COMP_KEY, ";
		sql_command +=  "  READ_DATE, ";
		sql_command +=  "  READING_ADDED_DATE, ";
		sql_command +=  "  READ_SOURCE, ";
		sql_command +=  "  CUSTOMER_PROBLEM_NUMBER, ";
		sql_command +=  "  READER_CODE, ";
		sql_command +=  "  READ_BY, ";
		sql_command +=  "  READ_REASON, ";
		sql_command +=  "  FIELD_NOTES, ";
		sql_command +=  "  READING_KEY, ";
		sql_command +=  "  BILLABLE_USAGE, ";
		sql_command +=  "  READING, ";
		sql_command +=  "  USAGE, ";
		sql_command +=  "  METER_READING_ESTIMATE_FLAG, ";
		sql_command +=  "  READY_FLAG, ";
		sql_command +=  "  FINAL_FLAG, ";
		sql_command +=  "  NO_READ_FLAG, ";
		sql_command +=  "  WORK_ORDER, ";
		sql_command +=  "  INSPECTION, ";
		sql_command +=  "  ADDED_BY, ";
		sql_command +=  "  LAST_MODIFIED_BY, ";
		sql_command +=  "  ETL_SOURCE_DATETIME, ";
		sql_command +=  "  ETL_RECORD_CREATED, ";
		sql_command +=  "  ETL_RECORD_UPDATED)";
		sql_command +=  "  SELECT ";
		sql_command +=  "  METERREADING.COMPKEY AS Comp_Key,";
		sql_command +=  "  METERREADING.READDTTM AS Read_Date,";
		sql_command +=  "  METERREADING.ADDDTTM AS Reading_Added_Date,";
		sql_command +=  "  METERREADING.READSRC AS Read_Source,";
		sql_command +=  "  METERREADING.CUSTPROB AS Customer_Problem_Number,";
		sql_command +=  "  METERREADING.READERCODE AS Reader_Code,";
		sql_command +=  "  METERREADING.READBY AS READ_BY,";
		sql_command +=  "  METERREADING.READREAS AS Read_Reason,";
		sql_command +=  "  METERREADING.FIELDNOTES AS Field_Notes,";
		sql_command +=  "      METERREADING.READINGKEY AS Reading_Key,";
		sql_command +=  "      METERREADING.BLUSAGE AS Billable_Usage,";
		sql_command +=  "      METERREADING.READING AS Reading,";
		sql_command +=  "      METERREADING.USAGE AS Usage,";
		sql_command +=  "      METERREADING.ESTMFLAG AS Meter_Reading_Estimate_Flag,";
		sql_command +=  "      METERREADING.READYFLAG AS Ready_Flag,";
		sql_command +=  "      METERREADING.FINALFLAG AS Final_Flag,";
		sql_command +=  "      METERREADING.NOREADFLAG AS No_Read_Flag,";
		sql_command +=  "  METERREADING.HISTKEY AS WORK_ORDER,";
		sql_command +=  "  METERREADING.INSPKEY AS INSPECTION,";
		sql_command +=  "  METERREADING.ADDBY AS ADDED_BY,";
		sql_command +=  "  METERREADING.MODBY AS LAST_MODIFIED_BY,";
		sql_command +=  "  METERREADING.ETL_LOAD_DATETIME AS ETL_SOURCE_DATETIME,";
		sql_command +=  "  current_timestamp() AS ETL_RECORD_CREATED,";
		sql_command +=  "  current_timestamp() AS ETL_RECORD_UPDATED";
		sql_command +=  "  FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_METERREADING METERREADING";
		sql_command +=  "    INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB METERREADING_CTRL";
		sql_command +=  "          ON METERREADING_CTRL.STG_PROCESS_NAME=''SP_STG_FACT_METER_READING'' AND METERREADING_CTRL.TABLE_NAME=''IPS_ASSETMANAGEMENT_WATER_METERREADING'' AND METERREADING_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";     
		sql_command +=  "   WHERE METERREADING.ETL_LOAD_DATETIME > METERREADING_CTRL.INC_ETL_LOAD_DATETIME ";
		sql_command +=  "      ; ";

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
        
	    log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''The insert job has failed. Quitting.'', " + process_execution_id.toString() + ",0,0);";
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
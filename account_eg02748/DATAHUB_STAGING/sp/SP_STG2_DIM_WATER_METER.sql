CREATE OR REPLACE PROCEDURE "SP_STG2_DIM_WATER_METER"()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG2_dim_water_meter"});
         
    
//  Step 5.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG2_DIM_WATER_METER (  ";
				
		sql_command += " ROUTE_SID, ";
		sql_command += " ROUTE_KEY, ";
		sql_command += " ADDRESS_KEY, ";
		sql_command += " COMP_KEY, ";
		sql_command += " METER_POSITION, ";
		sql_command += " ADDRESS_QUALIIFIER, ";
		sql_command += " AREA, ";
		sql_command += " AVERAGE_MONTHLY_USAGE, ";
		sql_command += " SERVICE_STATUS, ";
		sql_command += " UNIT_TYPE, ";
		sql_command += " METER_ADDED_DATE, ";
		sql_command += " METER_MODIFIED_DATE, ";
		sql_command += " METER_INSTALLED_DATE, ";
		sql_command += " METER_SIZE, ";
		sql_command += " SIZE_UNIT_OF_MEASURE, ";
		sql_command += " MANUFACTURER_KEY, ";
		sql_command += " METER_MODEL_NUMBER, ";
		sql_command += " METER_SERIAL_NUMBER, ";
		sql_command += " METER_SERVICE_TYPE, ";
		sql_command += " METER_ID, ";
		sql_command += " METER_DESCRIPTION, ";
		sql_command += " METER_MANUFACTURER_CODE, ";
		sql_command += " METER_OWNERSHIP, ";
		sql_command += " ETL_LOAD_DATETIME, ";
		sql_command += " ETL_RECORD_CREATED, ";
		sql_command += " ETL_RECORD_UPDATED)";
		sql_command += " SELECT ";
		sql_command += " case when WATERMETER.ROUTE_KEY is null or ROUTE.ROUTE_KEY is null then 0 else ROUTE_SID end as ROUTE_SID,";
		sql_command += " WATERMETER.ROUTE_KEY,";
		sql_command += " WATERMETER.ADDRESS_KEY, ";
		sql_command += " WATERMETER.COMP_KEY, ";
		sql_command += " WATERMETER.METER_POSITION, ";
		sql_command += " WATERMETER.ADDRESS_QUALIIFIER, ";
		sql_command += " WATERMETER.AREA, ";
		sql_command += " WATERMETER.AVERAGE_MONTHLY_USAGE, ";
		sql_command += " WATERMETER.SERVICE_STATUS, ";
		sql_command += " WATERMETER.UNIT_TYPE, ";
		sql_command += " WATERMETER.METER_ADDED_DATE, ";
		sql_command += " WATERMETER.METER_MODIFIED_DATE, ";
		sql_command += " WATERMETER.METER_INSTALLED_DATE, ";
		sql_command += " WATERMETER.METER_SIZE, ";
		sql_command += " WATERMETER.SIZE_UNIT_OF_MEASURE, ";
		sql_command += " WATERMETER.MANUFACTURER_KEY, ";
		sql_command += " WATERMETER.METER_MODEL_NUMBER, ";
		sql_command += " WATERMETER.METER_SERIAL_NUMBER, ";
		sql_command += " WATERMETER.METER_SERVICE_TYPE, ";
		sql_command += " WATERMETER.METER_ID, ";
		sql_command += " WATERMETER.METER_DESCRIPTION, ";
		sql_command += " WATERMETER.METER_MANUFACTURER_CODE, ";
		sql_command += " WATERMETER.METER_OWNERSHIP, ";
		sql_command += " WATERMETER.ETL_LOAD_DATETIME, ";
		sql_command += " WATERMETER.ETL_RECORD_CREATED, ";
		sql_command += " WATERMETER.ETL_RECORD_UPDATED";
		sql_command += " FROM DATAHUB_STAGING.STG_DIM_WATER_METER WATERMETER";
		sql_command += "   LEFT JOIN DATAHUB_ANALYTICS.DIM_ROUTE ROUTE";
		sql_command += "    ON WATERMETER.ROUTE_KEY=ROUTE.ROUTE_KEY ;";


		
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
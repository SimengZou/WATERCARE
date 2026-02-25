CREATE OR REPLACE PROCEDURE "SP_STG2_BRIDGE_METER_TO_ACCOUNT"()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG2_BRIDGE_METER_TO_ACCOUNT"});
         
    
//  Step 5.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG2_BRIDGE_METER_TO_ACCOUNT (  ";
		sql_command += "   WATER_METER_SID,";
		sql_command += "   ACCOUNT_SID,";
		sql_command += "   ACCOUNT_SERVICE_SID,";
		sql_command += "   WATER_METER_KEY,";
		sql_command += "   ACCOUNT_KEY,";
		sql_command += "   ACCOUNT_SERVICE_KEY,";
		sql_command += "   ACCOUNT_SERVICE_POSITION_KEY,";
		sql_command += "   SUBTRACTIVE_FLAG,";
		sql_command += "   CONSUMPTION_PERCENTAGE,";
		sql_command += "   CONSUMPTION_PERCENTAGE_FLAG,";
		sql_command += "   RECORD_END_DATE,";
		sql_command += "   ETL_SOURCE_DATETIME,";
		sql_command += "   ETL_RECORD_CREATED,";
		sql_command += "   ETL_RECORD_UPDATED,";
		sql_command += "   ETL_RECORD_DELETED,";
		sql_command += "   CURRENT_RECORD)";
		sql_command += "   SELECT DISTINCT";
		sql_command += "   CASE WHEN METER.COMP_KEY IS NULL THEN 0 ELSE METER.WATER_METER_SID END AS WATER_METER_SID,";
		sql_command += "   CASE WHEN ACCOUNT.ACCOUNT_KEY IS NULL  THEN 0 ELSE ACCOUNT.ACCOUNT_SID END AS ACCOUNT_SID,";
		sql_command += "   CASE WHEN ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY IS NULL  THEN 0 ELSE ACCOUNTSERVICE.ACCOUNT_SERVICE_SID END AS ACCOUNT_SERVICE_SID,";
		sql_command += "   METER.COMP_KEY AS WATER_METER_KEY,";
		sql_command += "   ACCOUNT.ACCOUNT_KEY AS ACCOUNT_KEY,";
		sql_command += "   ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY AS ACCOUNT_SERVICE_KEY,";
		sql_command += "   BRIDGE.ACCOUNT_SERVICE_POSITION_KEY AS ACCOUNT_SERVICE_POSITION_KEY,";
		sql_command += "   BRIDGE.SUBTRACTIVE_FLAG AS SUBTRACTIVE_FLAG,";
		sql_command += "   BRIDGE.CONSUMPTION_PERCENTAGE AS CONSUMPTION_PERCENTAGE,";
		sql_command += "   BRIDGE.CONSUMPTION_PERCENTAGE_FLAG AS CONSUMPTION_PERCENTAGE_FLAG,";
		sql_command += "   BRIDGE.RECORD_END_DATE,";
		sql_command += "   BRIDGE.ETL_SOURCE_DATETIME AS ETL_SOURCE_DATETIME,";
		sql_command += "   BRIDGE.ETL_RECORD_CREATED AS ETL_RECORD_CREATED,";
		sql_command += "   BRIDGE.ETL_RECORD_UPDATED AS ETL_RECORD_UPDATED, ";
		sql_command += "   BRIDGE.ETL_RECORD_DELETED AS ETL_RECORD_DELETED,";
		sql_command += "   coalesce(BRIDGE.CURRENT_RECORD,''Y'') AS CURRENT_RECORD";
		sql_command += "   FROM  DATAHUB_STAGING.STG_BRIDGE_METER_TO_ACCOUNT BRIDGE ";
		sql_command += "     FULL OUTER JOIN DATAHUB_ANALYTICS.DIM_WATER_METER METER";
		sql_command += "   ON BRIDGE.COMP_KEY =METER.COMP_KEY";
		sql_command += "     FULL OUTER JOIN DATAHUB_ANALYTICS.DIM_CUSTOMER_ACCOUNT ACCOUNT";
		sql_command += "   ON BRIDGE.ACCOUNT_KEY =ACCOUNT.ACCOUNT_KEY";
		sql_command += "     FULL OUTER JOIN DATAHUB_ANALYTICS.DIM_ACCOUNT_SERVICE ACCOUNTSERVICE";
		sql_command += "   ON BRIDGE.ACCOUNT_SERVICE_KEY =ACCOUNTSERVICE.ACCOUNT_SERVICE_KEY;";


		
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
CREATE OR REPLACE PROCEDURE "SP_STG_DIM_WATER_METER"()
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
// get timestamps for WATER METER table

		//update the latest time included in extract for COMPWMTR table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_ASSETMANAGEMENT_WATER_COMPWMTR'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
//update the latest time included in extract for MFG table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_RESOURCES_MFG) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_RESOURCES_MFG'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 

//update the latest time included in extract for RTADDR table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET_HISTORY.IPS_METERMANAGEMENT_WATER_RTADDR) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_METERMANAGEMENT_WATER_RTADDR'' AND schema_name=''DATAHUB_TARGET_HISTORY'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 			
				
        
	if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Date updated for incremental extract for MFG and COMPWMTR tables.'');";
            snowflake.execute({sqlText: debug_statement}); 
            }	
            
       
//  Step 5.

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG_DIM_WATER_METER"});
   
       
    
//  Step 6.

//  Run insert statement
    
        var sql_command = "INSERT INTO DATAHUB_STAGING.STG_DIM_WATER_METER (  ";
		sql_command += " ADDRESS_KEY, ";
		sql_command += " ROUTE_KEY, ";
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
		sql_command += " ETL_RECORD_UPDATED) ";
		sql_command += " ";
		sql_command += " SELECT COMPWMTR.ADDRKEY AS ADDRESS_KEY , ";
		sql_command += "   ROUTEADDR.ROUTEKEY AS Route_Key,";
		sql_command += "   COMPWMTR.COMPKEY AS COMP_KEY,";
		sql_command += "   COMPWMTR.POSITION AS METER_POSITION,";
		sql_command += "   COMPWMTR.ADDRQUAL AS ADDRESS_QUALIIFIER,";
		sql_command += "   COMPWMTR.AREA AS AREA,";
		sql_command += "   COMPWMTR.AVGMONUSG AS AVERAGE_MONTHLY_USAGE,";
		sql_command += "   COMPWMTR.SERVSTAT AS SERVICE_STATUS,";
		sql_command += "   COMPWMTR.UNITTYPE AS UNIT_TYPE,";
		sql_command += "   COMPWMTR.ADDDTTM AS METER_ADDED_DATE,";
		sql_command += "   COMPWMTR.MODDTTM AS METER_MODIFIED_DATE,";
		sql_command += "   COMPWMTR.INSTDATE AS METER_INSTALLED_DATE,";
		sql_command += "   COMPWMTR.METERSZ AS METER_SIZE,";
		sql_command += "   COMPWMTR.METERSZUOM AS SIZE_UNIT_OF_MEASURE,";
		sql_command += "   COMPWMTR.MFGKEY AS MANUFACTURER_KEY,";
		sql_command += "   COMPWMTR.MODELNO AS METER_MODEL_NUMBER,";
		sql_command += "   COMPWMTR.SERNO AS METER_SERIAL_NUMBER,";
		sql_command += "   COMPWMTR.SRVTYPE AS METER_SERVICE_TYPE,";
		sql_command += "   COMPWMTR.UNITID AS METER_ID,";
		sql_command += "   COMPWMTR.UNITDESC AS METER_DESCRIPTION,";
		sql_command += "   MFG.CODE AS METER_MANUFACTURER_CODE,";
		sql_command += "   COMPWMTR.OWN AS METER_OWNERSHIP,";
		sql_command += "   greatest(COMPWMTR.etl_load_datetime,coalesce(MFG.ETL_LOAD_DATETIME,COMPWMTR.ETL_LOAD_DATETIME),coalesce(routeaddr.rtaddr_ETL_LOAD_DATETIME,COMPWMTR.ETL_LOAD_DATETIME) ) AS  ETL_LOAD_DATETIME,";
		sql_command += "   current_timestamp() AS ETL_RECORD_CREATED,";
		sql_command += "   current_timestamp() AS ETL_RECORD_UPDATED";
		sql_command += "  FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR COMPWMTR";
		sql_command += "   LEFT JOIN ";
		sql_command += "   (SELECT ";
		sql_command += "          COMPWMTR2.COMPKEY,";
		sql_command += "          COMPWMTR2.ADDRKEY,";
		sql_command += "          COMPWMTR2.POSITION,";
		sql_command += "          COMPWMTR2.ETL_LOAD_DATETIME AS COMPWMTR_ETL_LOAD_DATETIME,";
		sql_command += "          RTADDR.ETL_LOAD_DATETIME AS RTADDR_ETL_LOAD_DATETIME,";
		sql_command += "          case when RTADDR.ETL_IS_DELETED then null else  RTADDR.ROUTEKEY END AS ROUTEKEY,";
		sql_command += "          row_number() OVER (PARTITION BY COMPWMTR2.COMPKEY ORDER BY RTADDR.ETL_IS_DELETED, RTADDR.adddttm DESC, RTADDR.ETL_LOAD_DATETIME DESC) AS latest_route";
		sql_command += "     FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR COMPWMTR2";
		sql_command += "   LEFT JOIN DATAHUB_TARGET_HISTORY.IPS_METERMANAGEMENT_WATER_RTADDR RTADDR";
		sql_command += "   ON COMPWMTR2.ADDRKEY = RTADDR.ADDRKEY AND COMPWMTR2.POSITION = RTADDR.POSITION";
		sql_command += "                      ) routeaddr";
		sql_command += "   ON COMPWMTR.COMPKEY= routeaddr.COMPKEY AND routeaddr.latest_route=1 ";
		sql_command += "   LEFT JOIN DATAHUB_TARGET.IPS_RESOURCES_MFG MFG";
		sql_command += "     ON COMPWMTR.MFGKEY=MFG.MFGKEY";
		sql_command += "   INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB COMPWMTR_CTRL ";
		sql_command += "     ON COMPWMTR_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_WATER_METER'' AND COMPWMTR_CTRL.TABLE_NAME=''IPS_ASSETMANAGEMENT_WATER_COMPWMTR'' AND COMPWMTR_CTRL.SCHEMA_NAME=''DATAHUB_TARGET'' ";
		sql_command += "   INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB MFG_CTRL ";
		sql_command += "     ON MFG_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_WATER_METER'' AND MFG_CTRL.TABLE_NAME=''IPS_RESOURCES_MFG'' AND MFG_CTRL.SCHEMA_NAME=''DATAHUB_TARGET'' ";
		sql_command += "  INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB routeaddr_CTRL";
		sql_command += "           ON routeaddr_CTRL.STG_PROCESS_NAME=''SP_STG_DIM_WATER_METER'' AND routeaddr_CTRL.TABLE_NAME=''IPS_METERMANAGEMENT_WATER_RTADDR'' AND routeaddr_CTRL.SCHEMA_NAME=''DATAHUB_TARGET_HISTORY''  ";
		sql_command += "   WHERE  COMPWMTR.ETL_LOAD_DATETIME> COMPWMTR_CTRL.INC_ETL_LOAD_DATETIME OR coalesce(MFG.ETL_LOAD_DATETIME,COMPWMTR.ETL_LOAD_DATETIME)> MFG_CTRL.INC_ETL_LOAD_DATETIME  ";
		sql_command += "    OR coalesce(routeaddr.rtaddr_ETL_LOAD_DATETIME,COMPWMTR.ETL_LOAD_DATETIME)>routeaddr_CTRL.INC_ETL_LOAD_DATETIME;";
  
  
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
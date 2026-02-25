CREATE OR REPLACE PROCEDURE "SP_STG_BRIDGE_METER_TO_ACCOUNT"()
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
// get timestamp for LINEITEM table
//   get_datetime_sql_command = "select to_char(inc_etl_load_datetime,''YYYY-MM-DD HH:MM:SS'') from datahub_logging.etl_load_control_job ";
	

//update the latest time included in extract for COMPWMTR table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_ASSETMANAGEMENT_WATER_COMPWMTR'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
//update the latest time included in extract for ASSLOC table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET_HISTORY.IPS_ASSETMANAGEMENT_ASSLOC) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_ASSETMANAGEMENT_ASSLOC'' AND schema_name=''DATAHUB_TARGET_HISTORY'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 


//update the latest time included in extract for ACCOUNTSERVICEPOSITION table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET_HISTORY.IPS_BILLING_ACCOUNTSERVICEPOSITION) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNTSERVICEPOSITION'' AND schema_name=''DATAHUB_TARGET_HISTORY'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 		
		
		
//update the latest time included in extract for ACCOUNTSERVICE table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET_HISTORY.IPS_BILLING_ACCOUNTSERVICE ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCOUNTSERVICE'' AND schema_name=''DATAHUB_TARGET_HISTORY'';";
	    snowflake.execute({sqlText: update_datetime_sql_command});
		
//update the latest time included in extract for SERVICEOPTIONS table
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_SERVICEOPTIONS ) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_SERVICEOPTIONS'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command});
		
		
        
	if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Date updated for incremental extract for BRIDGE METER ACCOUNT source tables.'');";
            snowflake.execute({sqlText: debug_statement}); 
            }	
            
//  Step 5.

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG_BRIDGE_METER_TO_ACCOUNT"});
         
    
//  Step 6.

//  Run insert statement


      var sql_command =  " INSERT INTO DATAHUB_STAGING.STG_BRIDGE_METER_TO_ACCOUNT (  ";	  
			sql_command += " COMP_KEY,";
			sql_command += " ACCOUNT_KEY,";
			sql_command += " ACCOUNT_SERVICE_KEY,";
			sql_command += " ACCOUNT_SERVICE_POSITION_KEY,";
			sql_command += " SUBTRACTIVE_FLAG,";
			sql_command += " CONSUMPTION_PERCENTAGE,";
			sql_command += " SERVICE_OPTION,";
			sql_command += " CONSUMPTION_PERCENTAGE_FLAG,";
			sql_command += " ETL_SOURCE_DATETIME,";
			sql_command += " ACCOUNTSERVICE_IS_DELETED,";
			sql_command += " ACCOUNTSERVICE_IS_EXPIRED,";
			sql_command += " ACCOUNTSERVICEPOSITION_IS_EXPIRED,";
			sql_command += " ASSLOC_IS_DELETED,";
			sql_command += " ASSLOC_IS_EXPIRED,";
			sql_command += " RECORD_END_DATE,";
			sql_command += " LATEST_RECORD,";
			sql_command += " ETL_RECORD_CREATED,";
			sql_command += " ETL_RECORD_UPDATED,";
			sql_command += " ETL_RECORD_DELETED,";
			sql_command += " CURRENT_RECORD)";
			sql_command += " SELECT ";
			sql_command += " COMP_KEY,";
			sql_command += " ACCOUNT_KEY,";
			sql_command += " ACCOUNT_SERVICE_KEY,";
			sql_command += " ACCOUNT_SERVICE_POSITION_KEY,";
			sql_command += " SUBTRACTIVE_FLAG,";
			sql_command += " CONSUMPTION_PERCENTAGE,";
			sql_command += " SERVICE_OPTION,";
			sql_command += " CONSUMPTION_PERCENTAGE_FLAG,";
			sql_command += " ETL_SOURCE_DATETIME,";
			sql_command += " ACCOUNTSERVICE_IS_DELETED,";
			sql_command += " ACCOUNTSERVICE_IS_EXPIRED,";
			sql_command += " ACCOUNTSERVICEPOSITION_IS_EXPIRED,";
			sql_command += " ASSLOC_IS_DELETED,";
			sql_command += " ASSLOC_IS_EXPIRED,";
			sql_command += " RECORD_END_DATE,";
			sql_command += " LATEST_RECORD,";
			sql_command += " ETL_RECORD_CREATED,";
			sql_command += " ETL_RECORD_UPDATED,";
			sql_command += " ETL_RECORD_DELETED,";
			sql_command += " CURRENT_RECORD";
			sql_command += " FROM ( ";
			sql_command += " SELECT DISTINCT COMPWMTR.compkey AS COMP_KEY,";
			sql_command += " ACCOUNTSERVICE.accountkey AS ACCOUNT_KEY, ";
			sql_command += " ACCOUNTSERVICE.accountservicekey AS ACCOUNT_SERVICE_KEY, ";
			sql_command += " ACCOUNTSERVICEPOSITION.ACCOUNTSERVICEPOSITIONKEY as ACCOUNT_SERVICE_POSITION_KEY,";
			sql_command += " ACCOUNTSERVICEPOSITION.subtractiveflag AS SUBTRACTIVE_FLAG,";
			sql_command += " ACCOUNTSERVICEPOSITION.CONSUMPTIONPERCENTAGE AS CONSUMPTION_PERCENTAGE,";
			sql_command += " SERVICEOPTIONS.CODE AS Service_Option,";
			sql_command += " case when trim(SERVICEOPTIONS.CODE) in (''Water'', ''LowFlow'',  ''Tanker'') then ''Y'' ELSE ''N'' END AS CONSUMPTION_PERCENTAGE_FLAG,";
			sql_command += " greatest(COMPWMTR.ETL_LOAD_DATETIME, COALESCE(ASSLOC.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME), ";
			sql_command += " COALESCE(ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME) , COALESCE(ACCOUNTSERVICE.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME), ";
			sql_command += " COALESCE(SERVICEOPTIONS.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME) ) AS ETL_SOURCE_DATETIME, ";
			sql_command += " COALESCE(ACCOUNTSERVICE.ETL_IS_DELETED,FALSE) AS ACCOUNTSERVICE_IS_DELETED,";
			sql_command += " CASE WHEN ACCOUNTSERVICE.STOPDTTM IS NOT NULL THEN TRUE ELSE FALSE END as ACCOUNTSERVICE_IS_EXPIRED,";
			sql_command += " CASE WHEN ACCOUNTSERVICEPOSITION.STOPDTTM IS NOT NULL THEN TRUE ELSE FALSE END AS ACCOUNTSERVICEPOSITION_IS_EXPIRED, ";
			sql_command += " coalesce(ASSLOC.ETL_IS_DELETED, FALSE) AS ASSLOC_IS_DELETED,";
			sql_command += " CASE WHEN ASSLOC.REMDT IS NOT NULL THEN TRUE ELSE FALSE END AS ASSLOC_IS_EXPIRED, ";
			sql_command += " least(coalesce(ASSLOC.REMDT, ACCOUNTSERVICEPOSITION.STOPDTTM),ACCOUNTSERVICEPOSITION.STOPDTTM)  AS RECORD_END_DATE, ";
			sql_command += " current_timestamp() AS ETL_RECORD_CREATED, ";
			sql_command += " current_timestamp() AS ETL_RECORD_UPDATED,";
			sql_command += " ACCOUNTSERVICEPOSITION.ADDDTTM  AS ETL_RECORD_DELETED,";
			sql_command += " CASE WHEN ACCOUNTSERVICEPOSITION.ETL_IS_DELETED OR ACCOUNTSERVICEPOSITION.STOPDTTM IS NOT NULL ";
			sql_command += " OR ASSLOC.REMDT IS NOT NULL OR coalesce(ASSLOC.ETL_IS_DELETED, FALSE) THEN ''N'' ELSE ''Y'' END AS CURRENT_RECORD,";
			sql_command += "  row_number() OVER (PARTITION BY ACCOUNTSERVICEPOSITIONKEY, COMPWMTR.compkey, ACCOUNTSERVICE.accountkey, ACCOUNTSERVICE.accountservicekey ORDER BY /*COMPWMTR.COMPKEY,*/ current_record desc) AS latest_record";
			sql_command += " FROM DATAHUB_TARGET_HISTORY.IPS_BILLING_ACCOUNTSERVICEPOSITION ACCOUNTSERVICEPOSITION";
			sql_command += " LEFT JOIN DATAHUB_TARGET_HISTORY.IPS_ASSETMANAGEMENT_ASSLOC  ASSLOC";
			sql_command += "   ON ASSLOC.ADDRKEY = ACCOUNTSERVICEPOSITION.ADDRKEY AND ASSLOC.POSITION = ACCOUNTSERVICEPOSITION.POSITION /* AND ASSLOC.REMDT IS NULL AND NOT ASSLOC.ETL_IS_DELETED */  ";
			sql_command += " LEFT JOIN DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR COMPWMTR";
			sql_command += "   ON COMPWMTR.COMPKEY = ASSLOC.COMPKEY ";
			sql_command += " LEFT JOIN DATAHUB_TARGET_HISTORY.IPS_BILLING_ACCOUNTSERVICE ACCOUNTSERVICE ";
			sql_command += "   ON ACCOUNTSERVICEPOSITION.ACCOUNTSERVICEKEY = ACCOUNTSERVICE.ACCOUNTSERVICEKEY";
			sql_command += " LEFT JOIN DATAHUB_TARGET.IPS_BILLING_SERVICEOPTIONS SERVICEOPTIONS";
			sql_command += "   ON ACCOUNTSERVICE.SERVICEOPTIONSKEY =SERVICEOPTIONS.SERVICEOPTIONSKEY ";
			sql_command += " LEFT JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB COMPWMTR_CTRL ";
			sql_command += "   ON COMPWMTR_CTRL.STG_PROCESS_NAME=''SP_STG_BRIDGE_METER_TO_ACCOUNT'' AND COMPWMTR_CTRL.TABLE_NAME=''IPS_ASSETMANAGEMENT_WATER_COMPWMTR'' AND COMPWMTR_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
			sql_command += " LEFT JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ASSLOC_CTRL ";
			sql_command += "   ON ASSLOC_CTRL.STG_PROCESS_NAME=''SP_STG_BRIDGE_METER_TO_ACCOUNT'' AND ASSLOC_CTRL.TABLE_NAME=''IPS_ASSETMANAGEMENT_ASSLOC'' AND ASSLOC_CTRL.SCHEMA_NAME=''DATAHUB_TARGET_HISTORY''";
			sql_command += " LEFT JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTSERVICEPOSITION_CTRL ";
			sql_command += "   ON ACCOUNTSERVICEPOSITION_CTRL.STG_PROCESS_NAME=''SP_STG_BRIDGE_METER_TO_ACCOUNT'' AND ACCOUNTSERVICEPOSITION_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNTSERVICEPOSITION'' AND ACCOUNTSERVICEPOSITION_CTRL.SCHEMA_NAME=''DATAHUB_TARGET_HISTORY''   ";
			sql_command += " LEFT JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCOUNTSERVICE_CTRL ";
			sql_command += "   ON ACCOUNTSERVICE_CTRL.STG_PROCESS_NAME=''SP_STG_BRIDGE_METER_TO_ACCOUNT'' AND ACCOUNTSERVICE_CTRL.TABLE_NAME=''IPS_BILLING_ACCOUNTSERVICE'' AND ACCOUNTSERVICE_CTRL.SCHEMA_NAME=''DATAHUB_TARGET_HISTORY''   ";
			sql_command += " LEFT JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB SERVICEOPTIONS_CTRL ";
			sql_command += "   ON SERVICEOPTIONS_CTRL.STG_PROCESS_NAME=''SP_STG_BRIDGE_METER_TO_ACCOUNT'' AND SERVICEOPTIONS_CTRL.TABLE_NAME=''IPS_BILLING_SERVICEOPTIONS'' AND SERVICEOPTIONS_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''   ";
			sql_command += " WHERE ";
			sql_command += " ( COALESCE(COMPWMTR.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME) >COMPWMTR_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  COALESCE(ASSLOC.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME) >ASSLOC_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME>ACCOUNTSERVICEPOSITION_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  COALESCE(ACCOUNTSERVICE.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME)>ACCOUNTSERVICE_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  COALESCE(SERVICEOPTIONS.ETL_LOAD_DATETIME,ACCOUNTSERVICEPOSITION.ETL_LOAD_DATETIME)>SERVICEOPTIONS_CTRL.INC_ETL_LOAD_DATETIME) )";
			sql_command += " WHERE latest_record=1;";

	  

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
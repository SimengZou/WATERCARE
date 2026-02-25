//  Name:       sp_stg32_ips_sm_billing_meter_old_routes
//
//  Purpose:    Load meter readings for Smart Meters from Data Lake to IPS - staging table 
//  stg32_ips_sm_billing_meter_old_routes loaded from staging table 
//  stg30_ips_sm_billing_meter and source target tables:
//   IPS_ASSETMANAGEMENT_WATER_COMPWMTR
//   IPS_METERMANAGEMENT_WATER_RTADDR 
//   IPS_METERMANAGEMENT_WATER_ROUTE 
//    by job sp_stg32_ips_sm_billing_meter_old_routes
//  Validates if the route assigned to meter is the latest one - if not, remove meter from the list
// Occasionally deleted routes appear, that should be ignored, there should be only 1 route assigned to a meter 
//
//  Description:
//
//  Stored procedure for IPS billing for smart meters from datalake
//

//  Documentation about consumption loads is stored here:
//  https://watercarestp.atlassian.net/wiki/spaces/BILLS/pages/2215378956/Meter+Readings+from+Data+Lake+requirements
//  https://watercarestp.atlassian.net/wiki/spaces/DATAHUB/pages/2246115369/Meter+Readings+from+Data+Lake+for+IPS
//
//
//  Steps:
//
//  1.  Get MAX ID of last run for this process and increment by 1 - used for logging
//
//  2.  Implement singleton pattern - ensure that the process is not already running
//
//  3.  Start new process execution
//
//  4.  Truncate target table
//
//  5.  Populate target table
//
//  6.  Commit transaction and complete process run
//


create or replace procedure datahub_staging.sp_stg32_ips_sm_billing_meter_old_routes()
    returns string
    language javascript
    execute as caller
    as
    $$
    
//  Variables
    
    var result = "";                                    // return status of this proc call
    const process_name = Object.keys(this)[0];          // name of currently executing process
    var debug_statement = "";                           // debug message statement
    var number_of_rows_inserted = 0;                             // track number of rows we have inserted
	var number_of_rows_updated = 0;                             // track number of rows we have updated
    
    
//  Step 1.

//  Select MAX process ID for this process
    
    var log_sql_command;
    log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_process ";
    log_sql_command += "where process_name = '" + process_name + "'";
    
    var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
    log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
    var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

    //  Increment MAX process ID to log a new run of this process
    
    if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
        {process_execution_id = 1;}
    else
        {process_execution_id = process_execution_id + 1;}
    
    
//  Step 2.

//  Get status of previous execution 
//      - for this we check if any previous execution is still running, where the status is "Running"

    var process_previous_status = "OK";
    log_sql_command = "select process_status from datahub_logging.etl_log_process ";
    log_sql_command += "where process_name = '" + process_name + "' and process_status = 'Running';";
    var log_status_result = snowflake.execute({sqlText: log_sql_command});                                      // execute select from log table
    if (log_status_result.getRowCount() > 0) 
        {
        log_status_result.next();                                                                               // expecting a maximum of 1 row
        process_previous_status = log_status_result.getColumnValue(1);                                          // and only 1 return value
        } 

        
        
    //  If the previous process execution is still running, then quit this run
    
    if(process_previous_status === "Running")       
        {
        //  Create a log entry to say we found a previous running process and we are quitting
        
        log_sql_command = "call datahub_logging.sp_etl_log_process('Insert','" + process_name + "','Failed','Previous process execution still Running. Quitting.', " + process_execution_id.toString() + ",0,0);";
        snowflake.execute({sqlText: log_sql_command});
        return "Failed";
        }
    
        

//  Step 3.

//  Start execution - log start

    log_sql_command = "call datahub_logging.sp_etl_log_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
    snowflake.execute({sqlText: log_sql_command});



            
//  Step 4.

//  Truncate target table 

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG32_IPS_SM_billing_meter_OLD_ROUTES;"});
   
       
    
//  Step 5.

//  Run insert statement  
    
        //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText: `INSERT INTO DATAHUB_STAGING.STG32_IPS_SM_BILLING_METER_OLD_ROUTES (UNITID, COMPKEY, ROUTEID, SERNO, UNITID_CLEAN) 
			SELECT 	UNITID, COMPKEY, ROUTEID, SERNO, UNITID_CLEAN
			FROM 
			(SELECT T.UNITID, T.COMPKEY, T.ROUTEID, r.routeid AS latest_routeid, T.SERNO, T.UNITID_CLEAN, rt.rtaddrkey, rt.adddttm,
			 ROW_NUMBER() OVER (PARTITION BY m.COMPKEY ORDER BY  rt.ADDDTTM DESC) as selection_order
			FROM DATAHUB_STAGING.STG30_IPS_SM_BILLING_METER t
			LEFT JOIN DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_COMPWMTR m ON T.compkey=m.compkey
					INNER JOIN  DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_RTADDR RT ON RT.ADDRKEY=m.ADDRKEY AND RT.POSITION=m.POSITION 
					INNER JOIN  DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_ROUTE R ON RT.ROUTEKEY = R.ROUTEKEY
				)
			WHERE routeid=latest_routeid AND selection_order=1; `} );
            var rs = sqlStmt.execute();
            var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
            var number_of_rows_updated =  sqlStmt.getNumRowsUpdated(); 
            
            
        
    
        //  Mark execution as successful
			
			result = "Success"; 


//  Step 6.

//  Completed execution - log end

			log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
			snowflake.execute({sqlText: log_sql_command});

			}

//  Error handling

    catch (err)  
        {
       
		var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
		//  Create a log entry to say INSERT STATEMENT failed
        
	    log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','The insert job has failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', " + process_execution_id.toString() + ",0,0);";
	    snowflake.execute({sqlText: log_sql_command});
		
		 
        return "Failed: " + err;   // Return error indicator.
        }


//    Return SP status to caller
  
    return result;
  
  
    $$
    ;
    
    
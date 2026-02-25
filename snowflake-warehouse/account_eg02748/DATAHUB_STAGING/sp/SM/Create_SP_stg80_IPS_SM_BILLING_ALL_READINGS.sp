//  Name:       sp_stg80_ips_sm_billing_all_readings
//
//  Purpose:    Load meter readings for Smart Meters from Data Lake to IPS - staging table 
//   stg80_ips_sm_billing_all_readings loaded from staging tables: 
//     stg50_ips_sm_billing_smart_read
//     stg70_ips_sm_billing_ips_read  
//  by job sp_stg80_ips_sm_billing_all_readings
//
//      Add all readings to one table  
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


create or replace procedure datahub_staging.sp_stg80_ips_sm_billing_all_readings()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG80_IPS_SM_billing_all_readings;"});
   
       
    
//  Step 5.

//  Run insert statement  
    
        //  Get number of rows inserted
        
        try
            {            
            var sqlStmt = snowflake.createStatement( {sqlText: ` INSERT INTO DATAHUB_STAGING.STG80_IPS_SM_BILLING_ALL_READINGS 
				(UNIT_ID, COMPKEY, ROUTE_ID, SM_READING, M_UNIT, READING_TYPE, FIELDNOTES, NOREADFLAG, IPS_READING, READING, IPS_READ_DATE, 
				SM_READING_DATE, READ_DATE, ACTUAL_READ_DATE, UNREAD_NOTE) 
				  SELECT 
				 sm.unitid AS unit_id, sm.compkey, 
				 sm.routeid AS route_id, sm.sm_reading, sm.m_unit, 
				 sm.READING_TYPE,
				 ips.fieldnotes,
				 ips.noreadflag,
				 ips.reading AS ips_reading,
				 CASE WHEN sm.sm_reading IS NULL THEN ips.reading ELSE sm.sm_reading END AS READING,
				 ips.ips_read_date,
				 sm_reading_date,
				 COALESCE( sm.read_date, ips.read_date) AS read_date,
				 COALESCE( sm.sm_reading_date, ips.ips_read_date) AS actual_read_date,
				 CASE WHEN sm_reading_date IS null  THEN 'NRSMART' ELSE NULL END AS UNREAD_NOTE 
				 FROM datahub_staging.STG50_IPS_SM_billing_smart_read sm
				 LEFT JOIN datahub_staging.STG70_IPS_SM_billing_IPS_READ ips
				   ON sm.compkey=ips.compkey
				  WHERE sm.COMPKEY NOT IN (SELECT compkey FROM DATAHUB_STAGING.STG55_IPS_SM_BILLING_4DAY_NOREADS) ;`} );
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
    
    
//  Name:       sp_stg40_ips_sm_billing_smart_read
//
//  Purpose:    Load meter readings for Smart Meters from Data Lake to IPS - staging table 
//   stg40_ips_sm_billing_smart_read loaded from staging tables: 
//  stg30_ips_sm_billing_meter 
//  STG10_IPS_SM_billing_readday
//  and target table:
//  SM_APSY_SMINF_CUR
//  by job sp_stg40_ips_sm_billing_smart_read
//  Get meter readings from the smart meter table 
//
//  Description:
//
//  Stored procedure for IPS billing for smart meters from datalake
//
//  Parameters: LAGDAYS FLOAT- number of days to allow for the lag - should be 4, but can be modified - for testing for example to 2
//  Extracts readings from the smart meter data
//
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


create or replace procedure datahub_staging.sp_stg40_ips_sm_billing_smart_read(LAGDAYS FLOAT)
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG40_IPS_SM_billing_smart_read;"});
   
       
    
//  Step 5.

//  Run insert statement  
    
        //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText: `INSERT INTO DATAHUB_STAGING.STG40_IPS_SM_BILLING_SMART_READ 
			(UNITID, COMPKEY, ROUTEID, SM_READING, M_UNIT, READING_TYPE, SM_READING_DATE, READ_DATE, CURRENT_READ_DATE)
			SELECT ips.UNITID, ips.COMPKEY, ips.ROUTEID, sm.M_VALUE AS SM_READING,sm.M_UNIT ,
			ips.reading_type,
			to_date(convert_timezone('UTC', 'Pacific/Auckland', sm.m_meter_time)) AS sm_READING_DATE,
			ips.read_date,
			ips.current_read_date
			FROM datahub_staging.STG39_IPS_SM_billing_all_meters ips 
			  LEFT JOIN TARGET_SM.SM_CURATED_MEASUREMENT sm 
				 ON ips.UNITID_CLEAN =sm.M_UNIT_ID 
				 AND
				   to_date(convert_timezone('UTC', 'Pacific/Auckland', sm.m_meter_time)) BETWEEN dateadd('day',-3,ips.read_date) AND dateadd('day',` + LAGDAYS.toString() +`,ips.read_date)
				 AND M_RESOURCE_TYPE IN ('Water_Flow_Readings_Total_Daily','Water_Flow_Readings_Total_Daily_absolute_billing')
                 AND M_BILLING = true ; `} );
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
    
    
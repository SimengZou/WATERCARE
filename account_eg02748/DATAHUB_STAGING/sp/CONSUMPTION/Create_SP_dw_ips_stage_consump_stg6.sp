//  Name:       sp_dw_ips_stage_consump_stg6
//
//  Purpose:    Load consumption data - staging table dw_ips_stage_consump_stg6 loaded from staging table dw_ips_stage_consump_stg5a by job sp_dw_ips_stage_consump_stg6
//
//  Description:
//
//  Stored procedure for consumption data load migrated from Old Redshift instance and Talend
//

//  Documentation about consumption loads is stored here:
//  https://watercarestp.atlassian.net/wiki/spaces/DATAHUB/pages/702415967/IPS+Consumption+Warehouse
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
//  4.  Trancate target table
//
//  5.  Populate target table
//
//  6.  Commit transaction and complete process run
//


create or replace procedure datahub_staging.sp_dw_ips_stage_consump_stg6()
    returns string
    language javascript
    execute as caller
    as
    $$
    
//  Variables
    
    var debug = "True";                                 // do we want debug messages?
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
    
    //  log debug message
    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Started process, new ID = " + process_execution_id.toString() + "');";
        snowflake.execute({sqlText: debug_statement}); 
        }
        
    
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

    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Running status = " + process_previous_status + "');";
        snowflake.execute({sqlText: debug_statement});
        }
        
        
    //  If the previous process execution is still running, then quit this run
    
    if(process_previous_status === "Running")       
        {
        //  Create a log entry to say we found a previous running process and we are quitting
        
        log_sql_command = "call datahub_logging.sp_etl_log_process('Insert','" + process_name + "','Failed','Previous process execution still Running. Quitting.', " + process_execution_id.toString() + ",0,0);";
        snowflake.execute({sqlText: log_sql_command});
        return "Failed";
        }
    
    //  log debug message
    if( debug === "True" ) 
        {
        debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','No Running process found');";
        snowflake.execute({sqlText: debug_statement}); 
        }
        

//  Step 3.

//  Start execution - log start

    log_sql_command = "call datahub_logging.sp_etl_log_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
    snowflake.execute({sqlText: log_sql_command});


    try
        {
        
        snowflake.execute ({sqlText: "begin transaction"});
        

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Begin Transaction');";
            snowflake.execute({sqlText: debug_statement}); 
            }


            
//  Step 4.

//  Truncate target table 

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.DW_IPS_STAGE_CONSUMP_STG6;"});
   
       
    
//  Step 5.

    
        //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText:  `INSERT INTO DATAHUB_STAGING.DW_IPS_STAGE_CONSUMP_STG6		
		 (account_key, account_service_key, address_key, start_datetime, stop_datetime, service_status, 
		 consumption_percentage, subtractive_flag, comp_key, comp_reading_key, rownum, prev_read_date, 
		 read_date, reading, usage, reading_days, reporting_usage, avg_daily_usage, cal_month, calendar_date, 
		 full_month_flag, start_dt, stop_dt) 
		 select a.account_key, a.account_service_key, a.address_key, a.start_datetime, 
		 a.stop_datetime, a.service_status, a.consumption_percentage, 
		 a.subtractive_flag, a.comp_key, a.comp_reading_key, a.rownum, a.prev_read_date, 
		 a.read_date, a.reading, a.usage, a.reading_days, a.reporting_usage, 
		 a.avg_daily_usage, a.cal_month, a.calendar_date, a.full_month_flag,
		 CASE WHEN CAST(dateadd(day, 1, prev_read_date) AS DATE) >= calendar_date 
		        THEN CAST(dateadd(day, 1, prev_read_date) AS DATE)
		      WHEN calendar_date >= CAST(dateadd(day, 1, Prev_read_date) AS DATE) 
		        THEN CAST(calendar_date AS DATE)
		      ELSE CAST(dateadd(day, 1, prev_read_date) AS DATE)
		      END AS start_dt,
		 CASE WHEN read_date <= cast(dateadd(day, -1, dateadd(mon, 1, calendar_date)) as date) 
		        THEN CAST(dateadd(day, 1,read_date) AS DATE)
		      WHEN cast(dateadd(day, -1, dateadd(mon, 1, calendar_date)) as date) <= read_date 
		        THEN cast(dateadd(day, 1, dateadd(day, -1, dateadd(mon, 1, calendar_date))) as DATE)
		        ELSE cast(dateadd(day, -1, dateadd(mon, 1, calendar_date)) as date)
		        END as stop_dt
		       from  ( SELECT distinct
		               datahub_staging.dw_ips_stage_consump_stg5a.*, cal_month, calendar_date, 
		     CASE WHEN (DATE_PART(yr, prev_read_date)*100)+DATE_PART(mon, prev_read_date)=cal_month THEN 'N'
		 WHEN (DATE_PART(yr, read_date)*100)+DATE_PART(mon, read_date)=cal_month THEN 'N'  ELSE 'Y'
		        END as full_month_flag
		   FROM  datahub_staging.dw_ips_stage_consump_stg5a
		     JOIN datahub_analytics.dw_ips_dim_date_calendar 
		       ON calendar_date BETWEEN date_trunc('month', prev_read_date) AND read_date
		     and cal_day_in_month = 1) a;
`} );
            var rs = sqlStmt.execute();
            var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
            var number_of_rows_updated =  sqlStmt.getNumRowsUpdated(); 
            
            }
         catch (err)
         {

            
            if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Failed to get number of rows inserted, err = " + err + "');";
            snowflake.execute({sqlText: debug_statement}); 
            }
            
          //  Create a log entry to say INSERT STATEMENT failed
        
	        log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','Failed to insert the rows. Quitting.', " + process_execution_id.toString() + ",0,0);";
	        snowflake.execute({sqlText: log_sql_command});
	        return "Failed";         
   
         }
    
    
        //  Mark execution as successful
        
        result = "Success"; 


//  Step 6.

//  Completed execution - log end

        log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        snowflake.execute ({sqlText: "commit"});


        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Commit Transaction');";
            snowflake.execute({sqlText: debug_statement}); 
            }
        }
        
    
//  Error handling

    catch (err)  
        {
        snowflake.execute ({sqlText: "rollback"});
		
		//  Create a log entry to say INSERT STATEMENT failed
        
	    log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','The insert job has failed. Rollback.', " + process_execution_id.toString() + ",0,0);";
	    snowflake.execute({sqlText: log_sql_command});
		
		if( debug === "True" ) 
			{
			debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Failed to get number of rows inserted, err = " + err + "');";
			snowflake.execute({sqlText: debug_statement}); 
			}
		 
        return "Failed: " + err;   // Return error indicator.
        }


//    Return SP status to caller
  
    return result;
  
  
    $$
    ;
    
    
//  Name:       sp_dw_ips_stage_consump_stg8a
//
//  Purpose:    Load consumption data - staging table dw_ips_stage_consump_stg8a loaded from staging table dw_ips_stage_consump_stg8 by job sp_dw_ips_stage_consump_stg8a
//  Calculating final avg_daily_usage, actual_days, actual_volume
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


create or replace procedure datahub_staging.sp_dw_ips_stage_consump_stg8a()
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

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.DW_IPS_STAGE_CONSUMP_STG8a;"});
   
       
    
//  Step 5.

//  Run insert statement  
				
    
        //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText: `	
	 INSERT INTO DATAHUB_STAGING.DW_IPS_STAGE_CONSUMP_STG8a
		 (ACCOUNT_KEY, ACCOUNT_SERVICE_KEY, ADDRESS_KEY, CONSUMPTION_PERCENTAGE, SUBTRACTIVE_FLAG,
		 COMP_KEY, MONTH, CAL_MONTH_NUM, CAL_YEAR, AVG_DAILY_USAGE, 
		 ACTUAL_DAYS, ACTUAL_VOLUME, FULL_MONTH_FLAG, EST_DAILY_AVG, EST_DAYS, EST_VOLUME, ESTIMATE_FLAG, ESTIMATE_METHOD, RATIO)
		 SELECT 
		   account_key, account_service_key, address_key, consumption_percentage, subtractive_flag,
		   comp_key, cal_month as month, cal_month_num, cal_year,
		   avg_daily_usage, actual_days, actual_volume, full_month_flag,
		   est_daily_avg, estimate_days as est_days, 
		   case when est_daily_avg is not null 
		     then (est_daily_avg*estimate_days) else null end est_volume,
		   estimate_flag, estimate_method, ratio
		 FROM (
		 select *,
		 Case when (full_month_flag is null or full_month_flag='N') 
		   Then cal_days_in_month-actual_days else null end estimate_days
		 from ( 
		   SELECT DISTINCT
		 stg7.account_key, stg7.account_service_key, 
		 stg7.address_key, stg7.consumption_percentage, stg7.subtractive_flag,
		 stg7.comp_key,  stg7.cal_month, 
		 right(cal_month, 2) as cal_month_num, 
		 case when right(cal_month, 2)='01' then 31  
		 when right(cal_month, 2)='02' then 
		   case when (left(cal_month, 4) % 100 <> 0) 
		    and (left(cal_month, 4) % 4 = 0) 
		    then 29 else 28 end
		 when right(cal_month, 2)='03' then 31 
		 when right(cal_month, 2)='04' then 30
		 when right(cal_month, 2)='05' then 31
		 when right(cal_month, 2)='06' then 30
		 when right(cal_month, 2)='07' then 31
		 when right(cal_month, 2)='08' then 31
		 when right(cal_month, 2)='09' then 30 
		 when right(cal_month, 2)='10' then 31 
		 when right(cal_month, 2)='11' then 30 
		 when right(cal_month, 2)='12' then 31 
		 else 0 end as cal_days_in_month,
		 left(cal_month, 4) as cal_year,    
		 stg7.avg_daily_usage, stg7.actual_days, 
		 stg7.actual_volume, stg7.full_month_flag,
		 case when stg7.full_month_flag='N' Then est_daily_avg else null end as est_daily_avg,
		 case when stg7.full_month_flag='Y' Then null 
		 when stg7.full_month_flag='N' Then 'partial' else 'Yes' end as estimate_flag,
		 case when (stg7.full_month_flag is null or stg7.full_month_flag='N') 
		   Then 'Prev Month Ratio' else null end as estimate_method,
		 case when stg7.full_month_flag='N' Then ratio else null end as ratio
		 FROM datahub_staging.dw_ips_stage_consump_stg7 stg7
		 left outer join datahub_staging.dw_ips_stage_consump_stg8 stg8 on stg8.comp_key = stg7.comp_key
		 ) T1
		 ) T2 ;`} );
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
    
    
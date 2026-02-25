//  Name:       sp_daily_consumption_load
//
//  Purpose:    Run all required stored procedures to load consumption tables
//
//  Description:
//
//  Stored procedure for IPS data loads to the analytics schema
//
//  This stored procedure loads data IPS tables to consumption data model/tables. It re-loads fully the consumption and 1 dimension table using stored procedures 
//  
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
//  4.  Run stored procedures
//
//  5.  Complete process run
//



create or replace procedure datahub_analytics.sp_daily_consumption_load()
    returns string
    language javascript
    execute as caller
    as
    $$
    
//  Variables
    
    var debug = "True";                                 // do we want debug messages?
    var result = "";                                    // return status of this proc call
    const process_name = Object.keys(this)[0];          // name of currently executing process
    const buffer_extract_hour = 1;                      // allow extra hour when extracting latest modifications from source data
    var debug_statement = "";                           // debug message statement
 
    
    
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
  
    
//  Step 4.

//  Run all required stored procedures



// /* Load Account Transaction data model */

//  define array of stored procedure calls 
        const call_sp = [];
			
		call_sp[0] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1(); ";
		call_sp[1] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1a(); ";
		call_sp[2] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1aa(); ";
		call_sp[3] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1b(); ";
		call_sp[4] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1bb(); ";
		call_sp[5] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1c(); ";
		call_sp[6] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1cc(); ";
		call_sp[7] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1d(); ";
		call_sp[8] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg1dd(); ";

		call_sp[9] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg2(); ";

		call_sp[10] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg3(); ";

		call_sp[11] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg4(); ";

		call_sp[12] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg4a(); ";

		call_sp[13] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg5(); ";

		call_sp[14] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg5a(); ";

		call_sp[15] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg6(); ";

		call_sp[16] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg6a(); ";

		call_sp[17] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg7(); ";

		call_sp[18] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg8(); ";

		call_sp[19] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg8a(); ";

		call_sp[20] = " CALL datahub_staging.sp_dw_ips_stage_consump_stg9(); ";

		call_sp[21] = " CALL datahub_analytics.sp_dw_ips_fact_consumption(); ";

		call_sp[22] = " CALL datahub_analytics.sp_dw_ips_dim_acct_comp_actsrv(); ";


	
	// Loop all stored procedures required for bill header full load - if one fails stop processing 
     var i=0;
	 let fLen  = call_sp.length;
	 
	 result = "Success"; 
	 
	 while (i<fLen && result === "Success") {
	     
		 
	
        try
            {
                
				var outputresult=snowflake.execute({sqlText: call_sp[i]});
			    var return_value = "";
			    while (outputresult.next())  {
			       return_value += outputresult.getColumnValue(1);
			       };
			      
				result=return_value;	
		
			  
			  }
	    catch (err)
			 {
			    log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','Failed to execute load sp. Quitting.', " + process_execution_id.toString() + ",0,0);";
                snowflake.execute({sqlText: log_sql_command});
	         
				if( debug === "True" ) 
				{
				debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Failed to run stored procedure, err = " + err + "');";
				snowflake.execute({sqlText: debug_statement}); 
				
				}
				i=fLen;
				result =  "Failed: " + err;   
				return result;
				// Return error indicator.
			 }
			 
			 i++;
		}	 
			//  Mark execution as successful if all have completed, fail if any sp fails
          
       

//  Step 5.

//  Completed execution - log end
     if (result === "Success") 
	   {
         log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', " + process_execution_id.toString() + ",0,0 );";
         snowflake.execute({sqlText: log_sql_command});
		 
		 //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Completed Account Transaction Model Load');";
            snowflake.execute({sqlText: debug_statement}); 
			}
       }
     else 	{
	     log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','Process execution failed.', " + process_execution_id.toString() + ",0,0 );";
         snowflake.execute({sqlText: log_sql_command});
		 
		  //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug('" + process_name + "','Loan Account Transaction Model Load  Failed');";
            snowflake.execute({sqlText: debug_statement}); 
			}
		 
       }	


 //    Return SP status to caller       
        
    return result;

  
    $$
    ;
    
    
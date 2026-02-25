//  Name:       sp_daily_ips_sm_billing_extract
//
//  Purpose:    Run all required stored procedures to generate daily ips billing extract for smart meters
//
//  Description:
//
//  Stored procedure for Smart meters billing extract
//
//  This stored procedure extracts data from smart meter tables and IPS in waterlake for the previous day or more - see offset value in first step
//  
//  Parameters:
//      OFFSET - Number indicating how many days should be offset from current date for the billing extract, usually for daily job 
//  value will be 1. If older extracts need to be generated number can be >1
//      LAGDAYS - number of days to allow for waiting for smart meters reading, before taking reading from IPS - should be 4, but can be modeified - fro testing for example to 2
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



create or replace procedure datahub_extract.sp_daily_ips_sm_billing_extract(OFFSET FLOAT,LAGDAYS FLOAT)
    returns string
    language javascript
    execute as caller
    as
    $$
    
//  Variables
    
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

//  Run all required stored procedures



// /* Load Account Transaction data model */

//  define array of stored procedure calls 
        const call_sp = [];
			
		call_sp[0] = " CALL datahub_staging.sp_stg10_ips_sm_billing_readday(" + OFFSET.toString() +"); ";
		call_sp[1] = " CALL datahub_staging.sp_stg20_ips_sm_billing_route(); ";
		call_sp[2] = " CALL datahub_staging.sp_stg30_ips_sm_billing_meter(); ";
		call_sp[3] = " CALL datahub_staging.sp_stg32_ips_sm_billing_meter_old_routes(); ";
		call_sp[4] = " CALL datahub_staging.sp_stg35_ips_sm_billing_out_of_cycle(" + OFFSET.toString() +"); ";
		call_sp[5] = " CALL datahub_staging.sp_stg37_ips_sm_billing_meter(); ";
		call_sp[6] = " CALL datahub_staging.sp_stg38_ips_sm_billing_all_meters(" + OFFSET.toString() +"); ";
		call_sp[7] = " CALL datahub_staging.sp_stg39_ips_sm_billing_all_meters(); ";
		call_sp[8] = " CALL datahub_staging.sp_stg40_ips_sm_billing_smart_read(" + LAGDAYS.toString() +"); ";
		call_sp[9] = " CALL datahub_staging.sp_stg50_ips_sm_billing_smart_read(); ";
		call_sp[10] = " CALL datahub_staging.sp_stg55_ips_sm_billing_4day_noreads(" + LAGDAYS.toString() +"); ";
		call_sp[11] = " CALL datahub_staging.sp_stg60_ips_sm_billing_readkey(" + LAGDAYS.toString() +"); ";
		call_sp[12] = " CALL datahub_staging.sp_stg70_ips_sm_billing_ips_read(); ";
		call_sp[13] = " CALL datahub_staging.sp_stg80_ips_sm_billing_all_readings(); ";
		call_sp[14] = " CALL datahub_staging.sp_stg90_ips_sm_billing_extract(); ";
		
		call_sp[15] = " CALL datahub_extract.sp_ips_sm_billing_extract_history(60); ";

		call_sp[16] = " CALL datahub_extract.sp_ips_sm_billing_generate_extract_s3(); ";
		
		call_sp[17] = " CALL datahub_extract.sp_ips_sm_billing_4day_noreads(); ";
		
		call_sp[18] = " CALL datahub_extract.sp_ips_sm_billing_4day_noreads_history(180); ";



	
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
			 
				var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
				
			    log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','The insert job has failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', " + process_execution_id.toString() + ",0,0);";
                snowflake.execute({sqlText: log_sql_command});
	         
			
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
		 
       }
     else 	{
	     log_sql_command = "call datahub_logging.sp_etl_log_process('UpdateEnd','" + process_name + "','Failed','Process execution failed.', " + process_execution_id.toString() + ",0,0 );";
         snowflake.execute({sqlText: log_sql_command});
		 
		  //  log debug message
		 
       }	


 //    Return SP status to caller       
        
    return result;

  
    $$
    ;
    
    
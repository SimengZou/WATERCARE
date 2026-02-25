CREATE OR REPLACE PROCEDURE "SP_DAILY_ACCOUNT_TRANSACTION_LOAD"()
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
  
    
//  Step 4.

//  Run all required stored procedures



// /* Load Account Transaction data model */

//  define array of stored procedure calls 
        const call_sp = [];
			
		call_sp[0] = " CALL DATAHUB_STAGING.SP_STG_DIM_ADJUSTMENT(); ";
		call_sp[1] = " CALL DATAHUB_ANALYTICS.SP_DIM_ADJUSTMENT(); ";

		call_sp[2] = " CALL DATAHUB_STAGING.SP_STG_DIM_PAYMENT(); ";
		call_sp[3] = " CALL DATAHUB_ANALYTICS.SP_DIM_PAYMENT(); ";
		
		call_sp[4] = " CALL DATAHUB_STAGING.SP_STG_DIM_REFUND(); ";
		call_sp[5] = " CALL DATAHUB_ANALYTICS.SP_DIM_REFUND(); ";
		
		call_sp[6] = " CALL DATAHUB_STAGING.SP_STG_DIM_TRANSACTION_TYPE(); ";
		call_sp[7] = " CALL DATAHUB_ANALYTICS.SP_DIM_TRANSACTION_TYPE(); ";
		
		call_sp[8] = " CALL DATAHUB_STAGING.SP_STG_DIM_TRANSACTION_COMMENTS(); ";
		call_sp[9] = " CALL DATAHUB_ANALYTICS.SP_DIM_TRANSACTION_COMMENTS(); ";


	//	/* get deltas */
		call_sp[10] = " CALL DATAHUB_STAGING.SP_STG_FACT_ACCOUNT_TRANSACTION(); ";

//  Initialise dimension records if needed
        call_sp[11] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_BILL(); ";
		call_sp[12] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_CUSTOMER_ACCOUNT(); ";
		call_sp[13] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_ACCOUNT_SERVICE(); ";
		call_sp[14] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_PAYMENT(); ";
		call_sp[15] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_ADJUSTMENT(); ";
		call_sp[16] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_REFUND(); ";
		call_sp[17] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_EMPLOYEE(); ";
		call_sp[18] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_TRANSACTION_COMMENTS(); ";
		call_sp[19] = " CALL DATAHUB_ANALYTICS.sp0AT_DIM_TRANSACTION_TYPE(); ";

       
//	/* extract SIDs from dimensions */
		call_sp[20] = " CALL DATAHUB_STAGING.SP_STG2_FACT_ACCOUNT_TRANSACTION(); ";

	//	/* merge new data to fact table */
		call_sp[21] = " CALL DATAHUB_ANALYTICS.SP_FACT_ACCOUNT_TRANSACTION(); ";

	
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
			    log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''Failed to execute load sp. Quitting.'', " + process_execution_id.toString() + ",0,0);";
                snowflake.execute({sqlText: log_sql_command});
	         
				if( debug === "True" ) 
				{
				debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Failed to run stored procedure, err = " + err + "'');";
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
         log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + ",0,0 );";
         snowflake.execute({sqlText: log_sql_command});
		 
		 //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Completed Account Transaction Model Load'');";
            snowflake.execute({sqlText: debug_statement}); 
			}
       }
     else 	{
	     log_sql_command = "call datahub_logging.sp_etl_log_process(''UpdateEnd'',''" + process_name + "'',''Failed'',''Process execution failed.'', " + process_execution_id.toString() + ",0,0 );";
         snowflake.execute({sqlText: log_sql_command});
		 
		  //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Loan Account Transaction Model Load  Failed'');";
            snowflake.execute({sqlText: debug_statement}); 
			}
		 
       }	


 //    Return SP status to caller       
        
    return result;

  
    ';
CREATE OR REPLACE PROCEDURE "SP_STG_FACT_ACCOUNT_TRANSACTION"()
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
	

		
//update the latest time included in extract for ACCTTRAN table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ACCTTRAN) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ACCTTRAN'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 		

//update the latest time included in extract for LINEITEM table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_LINEITEM) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_LINEITEM'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

//update the latest time included in extract for ADJUSTMENT table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_ADJUSTMENT) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_ADJUSTMENT'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 	

		
//update the latest time included in extract for BILL table	
		update_datetime_sql_command = "UPDATE datahub_logging.etl_load_control_job ";
		update_datetime_sql_command += "SET new_etl_load_datetime=(SELECT max(ETL_LOAD_DATETIME) FROM DATAHUB_TARGET.IPS_BILLING_BILL) ";
		update_datetime_sql_command += "WHERE stg_process_name = ''" + process_name + "'' AND table_name=''IPS_BILLING_BILL'' AND schema_name=''DATAHUB_TARGET'';";
	    snowflake.execute({sqlText: update_datetime_sql_command}); 
		
		
        
	if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_process_debug(''" + process_name + "'',''Date updated for incremental extract for ACCTRAN, LINEITEM, ADJUSTMENT and BILL tables.'');";
            snowflake.execute({sqlText: debug_statement}); 
            }	
            
//  Step 5.

//  Drop target table (if exists)

        
        snowflake.execute ({sqlText: "TRUNCATE TABLE DATAHUB_STAGING.STG_FACT_ACCOUNT_TRANSACTION"});
         
    
//  Step 6.

//  Run insert statement


 
      var sql_command =  "INSERT INTO DATAHUB_STAGING.STG_FACT_ACCOUNT_TRANSACTION (  "; 
			sql_command += " ACCOUNT_KEY, ";
			sql_command += " ACCOUNT_SERVICE_KEY, ";
			sql_command += " BILL_KEY, ";
			sql_command += " PAY_KEY, ";
			sql_command += " ADJUSTMENT_KEY, ";
			sql_command += " REFUND_KEY, ";
			sql_command += " TRANSACTION_COMMENTS_KEY, ";
			sql_command += " TRANSACTION_BY_EMPLOYEE_KEY, ";
			sql_command += " TRANSACTION_ADDED_BY_EMPLOYEE_KEY, ";
			sql_command += " TRANSACTION_MODIFIED_BY_EMPLOYEE_KEY, ";
			sql_command += " TRANSACTION_TYPE_CODE, ";
			sql_command += " TRANSACTION_DESIGNATOR_CODE, ";
			sql_command += " TRANSACTION_NUMBER, ";
			sql_command += " TRANSACTION_DATE, ";
			sql_command += " TRANSACTION_ADDED_DATE, ";
			sql_command += " TRANSACTION_MODIFIED_DATE, ";
			sql_command += " BILL_DATE,";
			sql_command += " ADJUSTMENT_STATUS_DATE, ";
			sql_command += " ADJUSTMENT_DATE, ";
			sql_command += " INTERNAL_FLAG, ";
			sql_command += " TRANSACTION_AMOUNT, ";
			sql_command += " NEEDS_JOURNAL, ";
			sql_command += " BILL_LINE_ITEM_KEY, ";
			sql_command += " TRANSACTION_SOURCE_CODE, ";
			sql_command += " TRANSACTION_SOURCE_TYPE, ";
			sql_command += " ETL_SOURCE_DATETIME, ";
			sql_command += " ETL_RECORD_CREATED, ";
			sql_command += " ETL_RECORD_UPDATED)";
			sql_command += " SELECT ";
			sql_command += " ACCTTRAN.ACCOUNTKEY AS Account_Key,";
			sql_command += " LINEITEM.ACCOUNTSERVICEKEY AS ACCOUNT_SERVICE_KEY,";
			sql_command += " LINEITEM.BILLKEY AS BILL_KEY,";
			sql_command += " ACCTTRAN.PAYKEY AS PAY_KEY, ";
			sql_command += " ACCTTRAN.ADJUSTMENTKEY AS ADJUSTMENT_KEY, ";
			sql_command += " ACCTTRAN.REFUNDKEY AS REFUND_KEY, ";
			sql_command += " ACCTTRAN.COMMENTSKEY AS TRANSACTION_COMMENTS_KEY, ";
			sql_command += " ACCTTRAN.TRANBY as Transaction_By_EMPLOYEE_KEY,";
			sql_command += " ACCTTRAN.ADDBY AS TRANSACTION_ADDED_BY_EMPLOYEE_KEY, ";
			sql_command += " ACCTTRAN.MODBY AS TRANSACTION_MODIFIED_BY_EMPLOYEE_KEY, ";
			sql_command += " ACCTTRAN.TRANSACTIONTYPE as Transaction_TYPE_CODE, ";
			sql_command += " ACCTTRAN.TRANSACTIONDESIGNATOR as Transaction_DESIGNATOR_CODE, ";
			sql_command += " ACCTTRAN.TRANNO as Transaction_Number,";
			sql_command += " ACCTTRAN.TRANDATE as Transaction_Date,";
			sql_command += " ACCTTRAN.ADDDTTM  AS Transaction_Added_Date, ";
			sql_command += " ACCTTRAN.MODDTTM AS Transaction_Modified_Date, ";
			sql_command += " BILL.BILLDATE AS BILL_DATE,";
			sql_command += " ADJUSTMENT.STATUSDATE AS ADJUSTMENT_STATUS_DATE, ";
			sql_command += " ADJUSTMENT.ADJUSTMENTDTTM AS ADJUSTMENT_DATE, ";
			sql_command += " ACCTTRAN.INTERNALFLAG AS Internal_Flag, ";
			sql_command += " ACCTTRAN.TRANAMT as Transaction_Amount,";
			sql_command += " ACCTTRAN.NEEDSJOURNAL AS NEEDS_JOURNAL,";
			sql_command += " ACCTTRAN.LINEITEMKEY AS Bill_Line_Item_Key,";
			sql_command += " ACCTTRAN.SOURCEKEY AS TRANSACTION_SOURCE_CODE, ";
			sql_command += "  ACCTTRAN.SOURCETYPE as Transaction_Source_Type,";
			sql_command += " GREATEST( ACCTTRAN.etl_load_datetime, COALESCE(LINEITEM.etl_load_datetime, ACCTTRAN.etl_load_datetime), ";
			sql_command += " COALESCE(ADJUSTMENT.etl_load_datetime, ACCTTRAN.etl_load_datetime), ";
			sql_command += " COALESCE(BILL.etl_load_datetime, ACCTTRAN.etl_load_datetime))  AS ETL_SOURCE_DATETIME, ";
			sql_command += " current_timestamp() AS ETL_RECORD_CREATED, ";
			sql_command += " current_timestamp() AS ETL_RECORD_UPDATED";
			sql_command += " FROM DATAHUB_TARGET.IPS_BILLING_ACCTTRAN ACCTTRAN";
			sql_command += " LEFT JOIN DATAHUB_TARGET.IPS_BILLING_LINEITEM LINEITEM ";
			sql_command += "   ON ACCTTRAN.LINEITEMKEY =LINEITEM.LINEITEMKEY ";
			sql_command += " LEFT JOIN DATAHUB_TARGET.IPS_BILLING_ADJUSTMENT ADJUSTMENT";
			sql_command += "   ON ACCTTRAN.ADJUSTMENTKEY =ADJUSTMENT.ADJUSTMENTKEY ";
			sql_command += " LEFT JOIN DATAHUB_TARGET.IPS_BILLING_BILL BILL";
			sql_command += " ON LINEITEM.BILLKEY =BILL.BILLKEY ";

			sql_command += " INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ACCTTRAN_CTRL";
			sql_command += " ON ACCTTRAN_CTRL.STG_PROCESS_NAME=''SP_STG_FACT_ACCOUNT_TRANSACTION'' AND ACCTTRAN_CTRL.TABLE_NAME=''IPS_BILLING_ACCTTRAN'' AND ACCTTRAN_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
			sql_command += " INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB LINEITEM_CTRL ";
			sql_command += " ON LINEITEM_CTRL.STG_PROCESS_NAME=''SP_STG_FACT_ACCOUNT_TRANSACTION'' AND LINEITEM_CTRL.TABLE_NAME=''IPS_BILLING_LINEITEM'' AND LINEITEM_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
			sql_command += " INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ADJUSTMENT_CTRL";
			sql_command += " ON ADJUSTMENT_CTRL.STG_PROCESS_NAME=''SP_STG_FACT_ACCOUNT_TRANSACTION'' AND ADJUSTMENT_CTRL.TABLE_NAME=''IPS_BILLING_ADJUSTMENT'' AND ADJUSTMENT_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
			sql_command += " INNER JOIN DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB BILL_CTRL";
			sql_command += " ON BILL_CTRL.STG_PROCESS_NAME=''SP_STG_FACT_ACCOUNT_TRANSACTION'' AND BILL_CTRL.TABLE_NAME=''IPS_BILLING_BILL'' AND BILL_CTRL.SCHEMA_NAME=''DATAHUB_TARGET''";
			sql_command += " WHERE  ACCTTRAN.ETL_LOAD_DATETIME>ACCTTRAN_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  LINEITEM.ETL_LOAD_DATETIME>LINEITEM_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  ADJUSTMENT.ETL_LOAD_DATETIME>ADJUSTMENT_CTRL.INC_ETL_LOAD_DATETIME";
			sql_command += " OR  BILL.ETL_LOAD_DATETIME>BILL_CTRL.INC_ETL_LOAD_DATETIME;";
    


	  

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
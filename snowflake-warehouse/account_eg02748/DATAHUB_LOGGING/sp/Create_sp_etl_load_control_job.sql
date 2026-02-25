//
//  Stored procedure to update the extract dates for the correct process
//  It sets the new date for the next extract and stores the historical date in history table

create or replace procedure datahub_logging.sp_etl_load_control_job(load_process_name VARCHAR)
    returns string
    language javascript
    execute as caller
    as
    $$
    
    //  Variables
    
    var result = "Success";
    
	
	try
            {
            var archive_sql_command;
// Archive current load date to history for the given load process			
            archive_sql_command = "insert into datahub_logging.etl_load_control_job_history (PROCESS_ID, STG_PROCESS_NAME, LOAD_PROCESS_NAME, SCHEMA_NAME, TABLE_NAME, NEW_ETL_LOAD_DATETIME, INC_ETL_LOAD_DATETIME, PREVIOUS_ETL_LOAD_DATETIME, UPDATED_DATETIME) ";
			archive_sql_command += " SELECT PROCESS_ID, STG_PROCESS_NAME, LOAD_PROCESS_NAME, SCHEMA_NAME, TABLE_NAME, NEW_ETL_LOAD_DATETIME, INC_ETL_LOAD_DATETIME, PREVIOUS_ETL_LOAD_DATETIME, current_timestamp() as UPDATED_DATETIME "
			archive_sql_command += " FROM DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB "
			archive_sql_command += " WHERE LOAD_PROCESS_NAME= '" + LOAD_PROCESS_NAME + "'";
			
//            log_sql_command += "values ('" + PROCESS_NAME + "'," + PROCESS_ID.toString() + ",'" + PROCESS_STATUS + "','" + LOG_MESSAGE + "')";    
            snowflake.execute({sqlText: archive_sql_command});
			
// update the new date for incremental load
			var update_sql_command;
            update_sql_command = "UPDATE DATAHUB_LOGGING.ETL_LOAD_CONTROL_JOB ";
			update_sql_command += " SET PREVIOUS_ETL_LOAD_DATETIME=INC_ETL_LOAD_DATETIME, "
			update_sql_command += " INC_ETL_LOAD_DATETIME =NEW_ETL_LOAD_DATETIME, NEW_ETL_LOAD_DATETIME =NULL, "
			update_sql_command += " UPDATED_DATETIME =current_timestamp() "
			update_sql_command += " WHERE NEW_ETL_LOAD_DATETIME IS NOT NULL AND LOAD_PROCESS_NAME= '" + LOAD_PROCESS_NAME + "'";
			 
            snowflake.execute({sqlText: update_sql_command});
			
			
            }
        catch (err)  
            {
            return "Failed: " + err;   // Return error indicator.
            }
    

 
 
    //    Return SP status to caller
  
    return result;
  
  
    $$
    ;
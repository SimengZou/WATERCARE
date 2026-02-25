//
//  Stored procedure to log process messages
//

create or replace procedure datahub_logging.sp_etl_log_ingestion_process(log_type VARCHAR, process_name VARCHAR, process_status VARCHAR, log_message VARCHAR, process_id FLOAT, insert_rows FLOAT, update_rows FLOAT)
    returns string
    language javascript
    execute as caller
    as
    $$
    
    //  Variables
    
    var result = "Success";
    
    
    //  Insert log message
    
    if( LOG_TYPE === "Insert")
        {
        try
            {
            var log_sql_command;
            log_sql_command = "insert into datahub_logging.etl_log_ingestion_process (process_name, process_execution_id, process_status, execution_message) ";
            log_sql_command += "values ('" + PROCESS_NAME + "'," + PROCESS_ID.toString() + ",'" + PROCESS_STATUS + "','" + LOG_MESSAGE + "')";    
            snowflake.execute({sqlText: log_sql_command});
            }
        catch (err)  
            {
            return "Failed: " + err;   // Return error indicator.
            }
        }
        
        
    //  Update log message
    
    if( LOG_TYPE === "Update")
        {
        try
            {
            var log_sql_command;
            log_sql_command = "update datahub_logging.etl_log_ingestion_process ";
            log_sql_command += "set process_status = '" + PROCESS_STATUS + "', ";
            log_sql_command += "execution_message = '" + LOG_MESSAGE + "' ";            
            log_sql_command += "where process_name = '" + PROCESS_NAME + "' and process_execution_id = " + PROCESS_ID.toString();    
            snowflake.execute({sqlText: log_sql_command});
            }
        catch (err)  
            {
            return "Failed: " + err;   // Return error indicator.
            }
        }
                
        
    //  Update log message and update end datetime
    
    if( LOG_TYPE === "UpdateEnd")
        {
        try
            {
            var log_sql_command;
            log_sql_command = "update datahub_logging.etl_log_ingestion_process ";
            log_sql_command += "set process_status = '" + PROCESS_STATUS + "', ";
            log_sql_command += "execution_message = '" + LOG_MESSAGE + "', ";            
            log_sql_command += "inserted_rows = " + INSERT_ROWS.toString() + ", ";   
            log_sql_command += "updated_rows = " + UPDATE_ROWS.toString() + ", ";          
            log_sql_command += "end_datetime = current_timestamp ";            
            log_sql_command += "where process_name = '" + PROCESS_NAME + "' and process_execution_id = " + PROCESS_ID.toString();    
            snowflake.execute({sqlText: log_sql_command});
            }
        catch (err)  
            {
            return "Failed: " + err;   // Return error indicator.
            }
        }
 
 
    //    Return SP status to caller
  
    return result;
  
  
    $$
    ;
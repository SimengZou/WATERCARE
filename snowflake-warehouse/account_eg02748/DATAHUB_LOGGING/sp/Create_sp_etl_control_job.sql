//
//  Stored procedure to log process messages
//

create or replace procedure datahub_sandbox.sp_etl_control_job(log_type VARCHAR, process_name VARCHAR, process_status VARCHAR, log_message VARCHAR, process_id FLOAT, num_rows FLOAT)
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
            log_sql_command = "insert into dev_wcc_datawarehouse.datahub_sandbox.log_process (process_name, process_execution_id, process_status, execution_message) ";
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
            log_sql_command = "update dev_wcc_datawarehouse.datahub_sandbox.log_process ";
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
            log_sql_command = "update dev_wcc_datawarehouse.datahub_sandbox.log_process ";
            log_sql_command += "set process_status = '" + PROCESS_STATUS + "', ";
            log_sql_command += "execution_message = '" + LOG_MESSAGE + "', ";            
            log_sql_command += "number_of_rows = " + NUM_ROWS.toString() + ", ";            
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
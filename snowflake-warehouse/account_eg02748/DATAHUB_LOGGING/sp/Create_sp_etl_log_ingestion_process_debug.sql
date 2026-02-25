//
//  Stored procedure to log debug messages
//

create or replace procedure datahub_logging.sp_etl_log_ingestion_process_debug(process_name VARCHAR, debug_message VARCHAR)
    returns string
    language javascript
    execute as caller
    as
    $$
    
    //  Variables
    
    var result = "Success";
    
    //  Insert debug message
    
    try
        {
        var log_sql_command;
        log_sql_command = "insert into datahub_logging.etl_log_ingestion_process_debug (process_name, debug_message) ";
        log_sql_command += "values ('" + PROCESS_NAME + "','" + DEBUG_MESSAGE + "')";    
        snowflake.execute({sqlText: log_sql_command});         // execute insert into log table
        }
    catch (err)  
        {
        return "Failed: " + err;   // Return error indicator.
        }
        
        
    //    Return SP status to caller
  
    return result;
  
  
    $$
    ;
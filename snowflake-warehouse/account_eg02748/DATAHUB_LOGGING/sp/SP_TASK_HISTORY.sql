CREATE OR REPLACE PROCEDURE DATAHUB_LOGGING.SP_TASK_HISTORY()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
    //  Variables
    var result = "";                                    // return status of this proc call
    const process_name = Object.keys(this)[0];          // name of currently executing process
    var number_of_rows_inserted = 0;                    // track number of rows we have inserted

    // Select MAX process ID for this process, increment it & call logging procedure     
	var log_sql_command;
	log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process ";
	log_sql_command += "where process_name = '" + process_name + "'";

	var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
	log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
	var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

	if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
		{process_execution_id = 1;}
	else
		{process_execution_id = process_execution_id + 1;}

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
    snowflake.execute({sqlText: log_sql_command});

    // Begin 
    snowflake.execute( {sqlText: "begin transaction;"} );

    try {
        // Get the maximum COMPLETED_TIME
        var stmt1 = snowflake.createStatement({
            sqlText: "SELECT COALESCE(MAX(COMPLETED_TIME), '1970-01-01 00:00:00') AS MAX_COMPLETED_TIME FROM DATAHUB_LOGGING.TASK_HISTORY"
        });

        var resultSet1 = stmt1.execute();
        resultSet1.next();
        var maxLastLoadTime = resultSet1.getColumnValue("MAX_COMPLETED_TIME");

        // Calculate the timestamp 3 hours ago
        var currentTimeMinusTwoHours = new Date();
        currentTimeMinusTwoHours.setHours(currentTimeMinusTwoHours.getHours() - 2);

        // Format the timestamp to 'YYYY-MM-DD HH:MI:SS'
        currentTimeMinusTwoHours = currentTimeMinusTwoHours.toISOString().slice(0, 19).replace('T', ' ');

        // Insert everything since the last load until 3 hours ago.
        var stmt2 = snowflake.createStatement({
            sqlText: `
                INSERT INTO DATAHUB_LOGGING.TASK_HISTORY ( 
                    NAME, QUERY_TEXT, CONDITION_TEXT, SCHEMA_NAME, TASK_SCHEMA_ID, DATABASE_NAME, TASK_DATABASE_ID,
	                SCHEDULED_TIME, COMPLETED_TIME, STATE, RETURN_VALUE, QUERY_ID, QUERY_START_TIME, ERROR_CODE,
                    ERROR_MESSAGE, GRAPH_VERSION, RUN_ID, ROOT_TASK_ID, SCHEDULED_FROM, INSTANCE_ID, ATTEMPT_NUMBER,
                    CONFIG
                )
                SELECT NAME, QUERY_TEXT, CONDITION_TEXT, SCHEMA_NAME, TASK_SCHEMA_ID, DATABASE_NAME, TASK_DATABASE_ID,
	                SCHEDULED_TIME, COMPLETED_TIME, STATE, RETURN_VALUE, QUERY_ID, QUERY_START_TIME, ERROR_CODE,
                    ERROR_MESSAGE, GRAPH_VERSION, RUN_ID, ROOT_TASK_ID, SCHEDULED_FROM, INSTANCE_ID, ATTEMPT_NUMBER,
                    CONFIG
                FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
                WHERE DATABASE_NAME = '${buildvar.env}_WCC_DATAWAREHOUSE'
                AND STATE IN ('SUCCEEDED','FAILED')
                AND SCHEMA_NAME NOT IN ('DATAHUB_EXTRACT', 'DATAHUB_ANALYTICS', 'DATAHUB_LOGGING', 'DATAHUB_STAGING' )
                AND COMPLETED_TIME > ? 
                AND COMPLETED_TIME <= ?
            `,
            binds: [maxLastLoadTime, currentTimeMinusTwoHours]
        });

        stmt2.execute();

        // Get number of rows inserted
        var number_of_rows_inserted = stmt2.getNumRowsInserted();

        // Commit         
        snowflake.execute ({sqlText: "commit;"});

        // Log success 
        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.'," + process_execution_id.toString() + ","+ number_of_rows_inserted.toString()+", 0);";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

    }   
    
    catch (err) {
        // Rollback
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');

        //  Create a log entry to say INSERT STATEMENT failed
        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : " + clean_error_msg  + "', " + process_execution_id.toString() + ",0,0);";
        snowflake.execute({sqlText: log_sql_command});
        throw "Failed: " + err.message;  
    }
    return result;
$$;
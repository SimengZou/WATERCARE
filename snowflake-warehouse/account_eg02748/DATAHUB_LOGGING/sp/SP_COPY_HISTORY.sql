CREATE OR REPLACE PROCEDURE DATAHUB_LOGGING.SP_COPY_HISTORY()
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
        // Get the maximum LAST_LOAD_TIME
        var stmt1 = snowflake.createStatement({
            sqlText: "SELECT COALESCE(MAX(LAST_LOAD_TIME), '1970-01-01 00:00:00') AS MAX_LAST_LOAD_TIME FROM DATAHUB_LOGGING.COPY_HISTORY"
        });

        var resultSet1 = stmt1.execute();
        resultSet1.next();
        var maxLastLoadTime = resultSet1.getColumnValue("MAX_LAST_LOAD_TIME");

        // Calculate the timestamp 3 hours ago
        var currentTimeMinusTwoHours = new Date();
        currentTimeMinusTwoHours.setHours(currentTimeMinusTwoHours.getHours() - 2);

        // Format the timestamp to 'YYYY-MM-DD HH:MI:SS'
        currentTimeMinusTwoHours = currentTimeMinusTwoHours.toISOString().slice(0, 19).replace('T', ' ');

        // Insert everything since the last load until 3 hours ago.
        var stmt2 = snowflake.createStatement({
            sqlText: `
                INSERT INTO DATAHUB_LOGGING.COPY_HISTORY (
                    FILE_NAME, STAGE_LOCATION, LAST_LOAD_TIME, ROW_COUNT, ROW_PARSED, FILE_SIZE, FIRST_ERROR_MESSAGE,
                    FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_CHARACTER_POS, FIRST_ERROR_COLUMN_NAME, ERROR_COUNT, ERROR_LIMIT,
                    STATUS, TABLE_ID, TABLE_NAME, TABLE_SCHEMA_ID, TABLE_SCHEMA_NAME, TABLE_CATALOG_ID, TABLE_CATALOG_NAME,
                    PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME, PIPE_RECEIVED_TIME, FIRST_COMMIT_TIME
                )
                SELECT FILE_NAME, STAGE_LOCATION, LAST_LOAD_TIME, ROW_COUNT, ROW_PARSED, FILE_SIZE, FIRST_ERROR_MESSAGE,
                    FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_CHARACTER_POS, FIRST_ERROR_COLUMN_NAME, ERROR_COUNT, ERROR_LIMIT,
                    STATUS, TABLE_ID, TABLE_NAME, TABLE_SCHEMA_ID, TABLE_SCHEMA_NAME, TABLE_CATALOG_ID, TABLE_CATALOG_NAME,
                    PIPE_CATALOG_NAME, PIPE_SCHEMA_NAME, PIPE_NAME, PIPE_RECEIVED_TIME, FIRST_COMMIT_TIME
                FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
                WHERE PIPE_CATALOG_NAME = '${buildvar.env}_WCC_DATAWAREHOUSE'
                AND LAST_LOAD_TIME > ? 
                AND LAST_LOAD_TIME <= ?
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
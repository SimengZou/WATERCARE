CREATE OR REPLACE PROCEDURE DATAHUB_INTEGRATION."SP_DEPM_LOAD_TARGET"(TARGET_TABLE VARCHAR, TIMESTAMP_COLUMN_NAME VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
//  Variables
	var debug = "True";                                     // do we want debug messages?
	var result = "";                                        // return status of this proc call
	const process_name = Object.keys(this)[0];              // name of currently executing process
	var debug_statement = "";                               // debug message statement
    var sql_command = "";                                   // holds the current SQL command
    var sql_stmt = "";                                      // holds the SQL statment created from the command
    const table_name = TARGET_TABLE;                        // the table name in target and target_history schmemas e.g. DEPM_DIM_ACCOUNT
    const timestamp_column_name = TIMESTAMP_COLUMN_NAME;    // the name of column that holds the timestamp e.g. Timestamp
    var source_date = ""                                    // the date of current data being processed from source
    var source_datetime = ""                                // the timestamp of current data being processed from source
    var max_source_date = ""                                // the maximum date in the source data
    var max_source_datetime = ""                            // the maximum timestamp in the source data
    var number_of_rows_inserted = 0                         // the number of new rows inserted into target or target history from source
                
//  Step 1.
//  Select MAX process ID for this process

	var log_sql_command;
	log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process " +
	                    "where process_name = '" + process_name + "'";

	var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
	log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
	var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

	//  Increment MAX process ID to log a new run of this process
	if (process_execution_id === undefined) {                                   // test if return value is NULL - first execution of this process
		process_execution_id = 1;
    }
	else {
        process_execution_id = process_execution_id + 1;
    }

	if( debug === "True" ) {
		debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Started process, new ID = " + process_execution_id.toString() + "');";
		snowflake.execute({sqlText: debug_statement}); 
	}

//  Step 2.
//  Start execution - log start

	log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
	snowflake.execute({sqlText: log_sql_command});

	snowflake.execute( {sqlText: "begin transaction;"} );

//  Step 3
//  Load data to Target and Target History	

	try	{
		if (debug === "True" ) {
			debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Begin Transaction');";
			snowflake.execute({sqlText: debug_statement}); 
		}

		// Get the maximum source date & timestamp.
		sql_command = 	"SELECT COALESCE(SUBSTR(MAX(" + timestamp_column_name + "),1,10),'1900-01-01')," + 
							" COALESCE(MAX(" + timestamp_column_name + "),'1900-01-01T00:00:00Z')" +
						" FROM DATAHUB_INTEGRATION.VW_STREAM_" + table_name;
		sql_stmt = snowflake.createStatement( {sqlText: sql_command} );
		var sql_results = sql_stmt.execute();
		sql_results.next() 			// only one row to retrieve
		max_source_date = sql_results.getColumnValueAsString(1);
		max_source_datetime = sql_results.getColumnValueAsString(2);

		// Get the maximum target date & timestamp.
		sql_command = "SELECT COALESCE(SUBSTR(MAX(" + timestamp_column_name + "),1,10),'1900-01-01')," + 
							" COALESCE(MAX(" + timestamp_column_name + "),'1900-01-01T00:00:00Z')" +
							" FROM TARGET_DEPM." + table_name;
		sql_stmt = snowflake.createStatement( {sqlText: sql_command} );
		sql_results = sql_stmt.execute();
		sql_results.next() 			// only one row to retrieve
		max_target_date = sql_results.getColumnValueAsString(1);
		max_target_datetime = sql_results.getColumnValueAsString(2);

		if (debug === "True") {
			debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + 
                        "',' max_source_date = " + max_source_date + 
						" max_source_datetime = " + max_source_datetime + 
						" max_target_date = " + max_target_date + 
						" max_target_datetime = " + max_target_datetime +
						" table_name = " + table_name + 
						" timestamp_column_name = " + timestamp_column_name+"');";
			snowflake.execute({sqlText: debug_statement}); 
		}

		// Check if there is a previous snapshot to be moved to Target history.
		if (max_source_date > max_target_date && max_target_date > "1900-01-01") {
			sql_command = "INSERT INTO TARGET_HISTORY_DEPM." + table_name + "_SNAPSHOT" +
							" (SELECT * FROM TARGET_DEPM." + table_name + 
							" WHERE SUBSTR(" + timestamp_column_name + ",1,10) < '" + max_source_date + "')";
			sql_results = snowflake.execute( {sqlText: sql_command} );
            number_of_rows_inserted += sql_results.getNumRowsAffected();
		}

		// Delete from target where timestamp is less than the maximum timestamp from the source.  
		// This caters for the scenario where the stored proc runs before all data has finished loading form source, as
		// it could be halfway through loading the current snapshot, so we dont want to move that to history or delete it.
		sql_command = "DELETE FROM TARGET_DEPM." + table_name +
						" WHERE " + timestamp_column_name + " < '" + max_source_datetime + "'";
		sql_results = snowflake.execute( {sqlText: sql_command} );

        // Loop through the stream to get data for previous dates (maximum timestamp for that day only).
        sql_command = "SELECT SUBSTR(" + timestamp_column_name + ",1,10) as Date," + 
							" MAX(" + timestamp_column_name + ") as MaxTimestamp" +
							" FROM DATAHUB_INTEGRATION.VW_STREAM_" + table_name +
                            " GROUP BY Date" + 
                            " ORDER BY Date";
		sql_stmt = snowflake.createStatement( {sqlText: sql_command} );
		var sql_loop_results = sql_stmt.execute();

        while (sql_loop_results.next())  {
            var source_date = sql_loop_results.getColumnValueAsString(1);
            var source_datetime = sql_loop_results.getColumnValueAsString(2);

			// Insert into target if it is the loop for the maximum source date AND the maxiumum source datetime is >=
			// the maximum datetime in target (could be resending old data or part of data already existing in target).
            if (source_date === max_source_date && max_source_datetime >= max_target_datetime) { 
                // Insert into target.
                sql_command = "INSERT INTO TARGET_DEPM." + table_name + 
                                " (SELECT * FROM DATAHUB_INTEGRATION.VW_STREAM_" + table_name + 
                                " WHERE " + timestamp_column_name + " = '" + max_source_datetime + "')";
                sql_results = snowflake.execute( {sqlText: sql_command} );
                number_of_rows_inserted += sql_results.getNumRowsAffected();
       		}
            else {
                // If the source data is earlier than the maximum source date, we want it to the data is to go straight
                // to target history (maximum timestamp for that day only) after first deleting from target history anything for that date.
                sql_command = "DELETE FROM TARGET_HISTORY_DEPM." + table_name + "_SNAPSHOT" +
                                " WHERE SUBSTR(" + timestamp_column_name + ",1,10) = '" + source_date + "'";
                sql_results = snowflake.execute( {sqlText: sql_command} );

                sql_command = "INSERT INTO TARGET_HISTORY_DEPM." + table_name +  "_SNAPSHOT" +
                                    " (SELECT * FROM DATAHUB_INTEGRATION.VW_STREAM_" + table_name + 
                                    "  WHERE " + timestamp_column_name + " = '" + source_datetime + "')";
                sql_results = snowflake.execute( {sqlText: sql_command} ); 
                number_of_rows_inserted += sql_results.getNumRowsAffected(); 
            }
        }

        snowflake.execute ({sqlText: "commit"});

//  Step 4.
//  End execution - log finish

        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name +    
        "','Success','Completed process execution.', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ ",0);";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

        //  log debug message
        if (debug === "True") {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Commit Transaction');";
            snowflake.execute({sqlText: debug_statement}); 
		}

    }

	catch (err) {
		snowflake.execute( {sqlText: "rollback;"} );
		throw "Failed: " + err + " Last SQL executed:" + sql_command;
    }

return result;

$$
;
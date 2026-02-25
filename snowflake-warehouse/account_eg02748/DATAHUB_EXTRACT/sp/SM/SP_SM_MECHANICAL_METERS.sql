CREATE OR REPLACE PROCEDURE DATAHUB_EXTRACT."SP_SM_MECHANICAL_METERS"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
//  Variables

	var debug = "True";                                 // do we want debug messages?
	var result = "";                                    // return status of this proc call
	const process_name = Object.keys(this)[0];          // name of currently executing process
	var debug_statement = "";                           // debug message statement
	var number_of_rows_inserted = 0;                             // track number of rows we have inserted
	var number_of_rows_updated = 0;                             // track number of rows we have updated
                
//  Step 1.
//  Select MAX process ID for this process
                  
	var log_sql_command;
	log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process ";
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
		debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Started process, new ID = " + process_execution_id.toString() + "'');";
		snowflake.execute({sqlText: debug_statement}); 
	}

//  Step 2.
//  Start execution - log start

	log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''Insert'',''" + process_name + "'',''Running'',''Started process execution.'', " + process_execution_id.toString() + ",0,0);";
	snowflake.execute({sqlText: log_sql_command});

	snowflake.execute( {sqlText: "begin transaction;"} );
	try
	{
		//  log debug message
		if( debug === "True" ) 
		{
			debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Begin Transaction'');";
			snowflake.execute({sqlText: debug_statement}); 
		}

//  Step 3.
//  Copy into the s3 bucket

		snowflake.execute( {sqlText: "ALTER SESSION SET TIMEZONE = ''Pacific/Auckland''"});

		snowflake.execute( {sqlText: "ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = ''YYYY-MM-DDTHH24:MI:SSZ''"});

		snowflake.execute( {sqlText: "ALTER SESSION SET TIMESTAMP_LTZ_OUTPUT_FORMAT = ''YYYY-MM-DDTHH24:MI:SSTZHTZM''"});

				//get a unique filename prefix using a timestamp
		const query = "SELECT TO_VARCHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD_HH24-MI-SS-FF'') AS file_name";
		const resultSet = snowflake.execute({ sqlText: query });

		if (resultSet.next()) {
			file_name = resultSet.getColumnValue("FILE_NAME");
		}


		sql_command =
			"copy into s3://wsl-${buildvar.env_lower}-deinf-smapp-mechanicalmeters-s3/" + file_name +
			" FROM (" +
			"	SELECT  OBJECT_CONSTRUCT(''accountId'', ACCOUNTID, ''readingTime'', READINGTIME, ''waterUsage'', WATERUSAGE, ''billingPeriodFromDate'',BILLINGPERIODFROMDATE,''billingPeriodToDate'',BILLINGPERIODTODATE,''isEstimate'',ISESTIMATE)" +
			"	FROM (" +
			"		SELECT ACCOUNTID, READINGTIME, WATERUSAGE, BILLINGPERIODFROMDATE, BILLINGPERIODTODATE, ISESTIMATE" +
			"		FROM DATAHUB_EXTRACT.VW_STREAM_SM_MECHANICAL_METERS" +
			"		)" +
			"	)" +
			" storage_integration = ${buildvar.env}_WSL_SMAPP_MECHANICALMETERS_S3" +
			" FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)" +
			" OVERWRITE = TRUE" +
			" encryption=(type=''AWS_SSE_S3'')" +
			" MAX_FILE_SIZE=9999"

		snowflake.execute( {sqlText: sql_command});

		snowflake.execute ({sqlText: "commit"});

//  Step 4.
//  End execution - log finish


        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Commit Transaction'');";
            snowflake.execute({sqlText: debug_statement}); 
            }

        }

        catch (err)  {
            snowflake.execute( {sqlText: "rollback;"} );
            throw "Failed: " + err;   // Return a success/error indicator.
            }


  return result;
  ';
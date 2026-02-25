CREATE OR REPLACE PROCEDURE DATAHUB_EXTRACT.SP_SM_CURATED_MEASUREMENT_LEAK()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
//  Variables
	var debug = "True";                                 // do we want debug messages?
	var result = "";                                    // return status of this proc call
	const process_name = Object.keys(this)[0];          // name of currently executing process
	var debug_statement = "";                           // debug message statement
	var number_of_rows_inserted = 0;                    // track number of rows we have inserted
	var number_of_rows_updated = 0;                     // track number of rows we have updated
	var file_name  = "";								// file name prefix for S3 files

	snowflake.execute( {sqlText: "begin transaction;"} );
	
	try
	{
		//get a unique filename prefix using a timestamp
		const query = "SELECT TO_VARCHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD_HH24-MI-SS-FF'') AS file_name";
		const resultSet = snowflake.execute({ sqlText: query });

		if (resultSet.next()) {
			file_name = resultSet.getColumnValue("FILE_NAME");
		}

		sql_command =
	 		"copy into s3://wsl-${buildvar.env_lower}-deinf-smapp-leakdetection-s3/" + file_name +
			" FROM (" +
			"	SELECT  OBJECT_CONSTRUCT(''accountNumber'', IPS_ACCOUNTNUMBER, ''meterId'', M_UNIT_ID, ''lastReadingDateBelowThreshold'', lastReadingDateBelowThreshold,''lastReadingDate'',lastReadingDate)" +
			" 	FROM (" +
			"		select IPS_ACCOUNTNUMBER" +
			" 			,M_UNIT_ID" +
			" 			,lastReadingDateBelowThreshold" +
			" 			,lastReadingDate" +
			" 		from DATAHUB_EXTRACT.VW_SM_STREAM_CURATED_MEASUREMENT_LEAK" +
			" 		)" +
			" 	)" +
			" storage_integration = ${buildvar.env}_WSL_SMAPP_LEAKDETECTION_S3" +
			" FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)" +
			" OVERWRITE = TRUE" +
			" encryption=(type=''AWS_SSE_S3'')" +
			" MAX_FILE_SIZE=9999"

		snowflake.execute( {sqlText: sql_command});      

        snowflake.execute ({sqlText: "commit"});

        result = "Success"; 
	}       

	catch (err)  {
		snowflake.execute( {sqlText: "rollback;"} );
		throw "Failed: " + err.message;   // Return a success/error indicator.
	}
	return result;
';
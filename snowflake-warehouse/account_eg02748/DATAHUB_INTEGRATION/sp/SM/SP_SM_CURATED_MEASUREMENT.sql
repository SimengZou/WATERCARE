CREATE OR REPLACE PROCEDURE DATAHUB_INTEGRATION.SP_SM_CURATED_MEASUREMENT()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

//  Variables

var result = "";                                    // return status of this proc call
const process_name = Object.keys(this)[0];          // name of currently executing process
var number_of_rows_inserted = 0;                             // track number of rows we have inserted
var number_of_rows_updated = 0;                             // track number of rows we have updated

//  Step 1 Select MAX process ID for this process
                  
	var log_sql_command;
	log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process ";
	log_sql_command += "where process_name = '" + process_name + "'";

	var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
	log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
	var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

	//  Increment MAX process ID to log a new run of this process

	if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
		{process_execution_id = 1;}
	else
		{process_execution_id = process_execution_id + 1;}

//  Step 2 Start execution - log start

log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
snowflake.execute({sqlText: log_sql_command});

snowflake.execute( {sqlText: "begin transaction;"} );
try
    {
        var sql_command = ` 
        merge into  TARGET_SM.SM_CURATED_MEASUREMENT as target using 
            (SELECT M_PLATFORM_ID,
                    M_PLATFORM_TIME,
                    M_PLATFORM_NAME,
                    M_UNIT_ID,
                    M_MANUFACTURER_NAME,
                    M_PRODUCT_CLASS,
                    M_RESOURCE_TYPE,
                    M_RESOURCE_DESCRIPTION,
                    M_METER_TIME,
                    M_BILLING,
                    M_UNIT,
                    M_VALUE,
                	M_ETL_JOB_ID1,
                	M_ETL_JOB_ID2,
                	M_CUSTOM_1,
                	M_CUSTOM_2,
                	ETL_LANDING_LOAD_DATETIME,
                	ETL_LOAD_DATETIME,
                	ETL_LOAD_METADATAFILENAME
            FROM DATAHUB_INTEGRATION.VW_STREAM_SM_CURATED_MEASUREMENT
            WHERE ROWNUMBER = 1) as src 
         on
            ((target.M_UNIT_ID=src.M_UNIT_ID) OR 
                (target.M_UNIT_ID IS NULL AND src.M_UNIT_ID IS NULL)) AND
            target.M_RESOURCE_TYPE=src.M_RESOURCE_TYPE AND
            target.M_METER_TIME=src.M_METER_TIME AND
            target.M_VALUE=src.M_VALUE
        when matched and (TARGET.M_PLATFORM_NAME <> SRC.M_PLATFORM_NAME or
                TARGET.M_MANUFACTURER_NAME <> SRC.M_MANUFACTURER_NAME or
                TARGET.M_PRODUCT_CLASS <> SRC.M_PRODUCT_CLASS or (target.M_PRODUCT_CLASS is null and SRC.M_PRODUCT_CLASS is NOT null ) or (target.M_PRODUCT_CLASS is NOT null and SRC.M_PRODUCT_CLASS is null ) or
                TARGET.M_RESOURCE_DESCRIPTION <> SRC.M_RESOURCE_DESCRIPTION or
                TARGET.M_BILLING <> SRC.M_BILLING or
                TARGET.M_UNIT <> SRC.M_UNIT or (target.M_UNIT is null and SRC.M_UNIT is NOT null ) or (target.M_UNIT is NOT null and SRC.M_UNIT is null ) or
                TARGET.M_CUSTOM_1 <> SRC.M_CUSTOM_1 or (target.M_CUSTOM_1 is null and SRC.M_CUSTOM_1 is NOT null ) or (target.M_CUSTOM_1 is NOT null and SRC.M_CUSTOM_1 is null ) or
                TARGET.M_CUSTOM_2 <> SRC.M_CUSTOM_2 or (target.M_CUSTOM_2 is null and SRC.M_CUSTOM_2 is NOT null ) or (target.M_CUSTOM_2 is NOT null and SRC.M_CUSTOM_2 is null ))
        then update set
            target.M_PLATFORM_ID = src.M_PLATFORM_ID,
            target.M_PLATFORM_TIME = src.M_PLATFORM_TIME,
            target.M_PLATFORM_NAME = src.M_PLATFORM_NAME,
            target.M_UNIT_ID = src.M_UNIT_ID,
            target.M_MANUFACTURER_NAME = src.M_MANUFACTURER_NAME,
            target.M_PRODUCT_CLASS = src.M_PRODUCT_CLASS,
            target.M_RESOURCE_TYPE = src.M_RESOURCE_TYPE,
            target.M_RESOURCE_DESCRIPTION = src.M_RESOURCE_DESCRIPTION,
            target.M_METER_TIME = src.M_METER_TIME,
            target.M_BILLING = src.M_BILLING,
            target.M_UNIT = src.M_UNIT,
            target.M_VALUE = src.M_VALUE,
            target.M_ETL_JOB_ID1 = src.M_ETL_JOB_ID1,
            target.M_ETL_JOB_ID2 = src.M_ETL_JOB_ID2,
            target.M_CUSTOM_1 = src.M_CUSTOM_1,
            target.M_CUSTOM_2 = src.M_CUSTOM_2,
            target.ETL_LOAD_DATETIME = src.ETL_LOAD_DATETIME,
            target.ETL_LOAD_METADATAFILENAME = src.ETL_LOAD_METADATAFILENAME
        when not matched then insert ( 
                M_PLATFORM_ID,
                M_PLATFORM_TIME,
                M_PLATFORM_NAME,
                M_UNIT_ID,
                M_MANUFACTURER_NAME,
                M_PRODUCT_CLASS,
                M_RESOURCE_TYPE,
                M_RESOURCE_DESCRIPTION,
                M_METER_TIME,
                M_BILLING,
                M_UNIT,
                M_VALUE,
                M_ETL_JOB_ID1,
                M_ETL_JOB_ID2,
                M_CUSTOM_1,
                M_CUSTOM_2,
                ETL_LOAD_DATETIME,
                ETL_LOAD_METADATAFILENAME) 
        values (src.M_PLATFORM_ID,
                src.M_PLATFORM_TIME,
                src.M_PLATFORM_NAME,
                src.M_UNIT_ID,
                src.M_MANUFACTURER_NAME,
                src.M_PRODUCT_CLASS,
                src.M_RESOURCE_TYPE,
                src.M_RESOURCE_DESCRIPTION,
                src.M_METER_TIME,
                src.M_BILLING,
                src.M_UNIT,
                src.M_VALUE,
                src.M_ETL_JOB_ID1,
                src.M_ETL_JOB_ID2,
                src.M_CUSTOM_1,
                src.M_CUSTOM_2,
                src.ETL_LOAD_DATETIME,
                src.ETL_LOAD_METADATAFILENAME);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
                    
//  Get number of rows inserted
    var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
    var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();

    snowflake.execute ({sqlText: "commit"});

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
    snowflake.execute({sqlText: log_sql_command});

    result = "Success"; 

    }       

    catch (err)  {
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
    //  Create a log entry to say INSERT STATEMENT failed
    
    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', " + process_execution_id.toString() + ",0,0);";
    snowflake.execute({sqlText: log_sql_command});
        ;
        throw "Failed: " + err.message;   // Return a success/error indicator.
        }
    return result;
$$;
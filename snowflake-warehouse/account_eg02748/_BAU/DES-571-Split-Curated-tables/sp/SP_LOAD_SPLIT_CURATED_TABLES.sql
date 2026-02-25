
create or replace procedure DATAHUB_INTEGRATION.SM_LOAD_SPLIT_TARGET_TABLES()
    returns varchar not null
    language javascript
    as
    $$
  
    //  Variables
                  
          
    var result = "";                                    // return status of this proc call
    const process_name = Object.keys(this)[0];          // name of currently executing process
    var number_of_rows_inserted = 0;                    // track number of rows we have inserted

    //  Begin stransaction              
              
    snowflake.execute( {sqlText: "begin transaction;"} );
    
    try {
                          
                                                   
        // Insert into Alarms/Events curated table
                     
        var sql_command = ` 
            INSERT INTO TARGET_SM.SM_APSY_ALARMSEVENTS_CUR
            SELECT
                AE_PLATFORM_ID,
                TO_TIMESTAMP(AE_PLATFORM_TIME,6) as AE_PLATFORM_TIME,
                AE_PLATFORM_NAME,
                AE_UNIT_ID,
                AE_MANUFACTURER_NAME,
                AE_PRODUCT_CLASS,
                AE_ID,
                AE_DESCRIPTION,
                AE_CODE,
                TO_TIMESTAMP(AE_METER_TIME,6) as AE_METER_TIME,
                TO_VARIANT(AE_VALUE) as AE_VALUE,
                AE_ETL_JOB_ID1,
                AE_ETL_JOB_ID2,
                AE_CUSTOM_1,
                AE_CUSTOM_2,
                ETL_LOAD_DATETIME,
                ETL_LOAD_METADATAFILENAME
            FROM 
                DATAHUB_TARGET.SM_APSY_SMINF_CUR
            WHERE
                AE_PLATFORM_ID is not NULL;`;    

        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();


        // Insert into Measurement curated table
                     
        var sql_command = ` 
            INSERT INTO TARGET_SM.SM_APSY_MEASUREMENT_CUR
            SELECT 
                M_PLATFORM_ID,
                TO_TIMESTAMP(M_PLATFORM_TIME,6) as M_PLATFORM_TIME,
                M_PLATFORM_NAME,
                M_UNIT_ID,
                M_MANUFACTURER_NAME,
                M_PRODUCT_CLASS,
                M_RESOURCE_TYPE,
                M_RESOURCE_DESCRIPTION,
                TO_TIMESTAMP(M_METER_TIME,6) as M_METER_TIME,
                M_BILLING,
                M_UNIT,
                M_VALUE,
                M_ETL_JOB_ID1,
                M_ETL_JOB_ID2,
                M_CUSTOM_1,
                M_CUSTOM_2,
                ETL_LOAD_DATETIME,
                ETL_LOAD_METADATAFILENAME
            FROM 
                DATAHUB_TARGET.SM_APSY_SMINF_CUR
            WHERE
                M_PLATFORM_ID IS NOT NULL;`;    
                
        //  Get number of rows inserted
            
        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();
        var number_of_rows_inserted = sqlStmt.getNumRowsInserted();            

        // Commit transaction
         
        snowflake.execute ({sqlText: "commit"});

        result = "Success"; 
        }       

    catch (err)  
        { 
        debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','" + err + "');";
        snowflake.execute({sqlText: debug_statement});
        snowflake.execute( {sqlText: "rollback;"} );
        return "Failed: " + err;   // Return a success/error indicator.
        }


  return result;
  $$;

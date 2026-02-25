create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5FAILURES()
    returns varchar not null
                language javascript
                as
                $$

//  Variables

var result = "";                                    // return status of this proc call
const process_name = Object.keys(this)[0];          // name of currently executing process
var number_of_rows_inserted = 0;                             // track number of rows we have inserted
var number_of_rows_updated = 0;                             // track number of rows we have updated


//  Step 1.

//  Start execution - log start

log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', 0,0,0);";
snowflake.execute({sqlText: log_sql_command});

snowflake.execute( {sqlText: "begin transaction;"} );
try
    {
        var sql_command = `
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5FAILURES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5FAILURES as target using (SELECT * FROM (SELECT 
            strm.FAL_CODE, 
            strm.FAL_CREATED, 
            strm.FAL_DESC, 
            strm.FAL_ENABLEWORKORDERS, 
            strm.FAL_GEN, 
            strm.FAL_GROUP, 
            strm.FAL_LASTSAVED, 
            strm.FAL_NOTUSED, 
            strm.FAL_PARTFAILURE, 
            strm.FAL_UPDATECOUNT, 
            strm.FAL_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FAL_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FAILURES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FAL_CODE=src.FAL_CODE) OR (target.FAL_CODE IS NULL AND src.FAL_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.FAL_CODE=src.FAL_CODE, 
                    target.FAL_CREATED=src.FAL_CREATED, 
                    target.FAL_DESC=src.FAL_DESC, 
                    target.FAL_ENABLEWORKORDERS=src.FAL_ENABLEWORKORDERS, 
                    target.FAL_GEN=src.FAL_GEN, 
                    target.FAL_GROUP=src.FAL_GROUP, 
                    target.FAL_LASTSAVED=src.FAL_LASTSAVED, 
                    target.FAL_NOTUSED=src.FAL_NOTUSED, 
                    target.FAL_PARTFAILURE=src.FAL_PARTFAILURE, 
                    target.FAL_UPDATECOUNT=src.FAL_UPDATECOUNT, 
                    target.FAL_UPDATED=src.FAL_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    FAL_CODE, 
                    FAL_CREATED, 
                    FAL_DESC, 
                    FAL_ENABLEWORKORDERS, 
                    FAL_GEN, 
                    FAL_GROUP, 
                    FAL_LASTSAVED, 
                    FAL_NOTUSED, 
                    FAL_PARTFAILURE, 
                    FAL_UPDATECOUNT, 
                    FAL_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.FAL_CODE, 
                    src.FAL_CREATED, 
                    src.FAL_DESC, 
                    src.FAL_ENABLEWORKORDERS, 
                    src.FAL_GEN, 
                    src.FAL_GROUP, 
                    src.FAL_LASTSAVED, 
                    src.FAL_NOTUSED, 
                    src.FAL_PARTFAILURE, 
                    src.FAL_UPDATECOUNT, 
                    src.FAL_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5FAILURES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.FAL_CODE, 
            strm.FAL_CREATED, 
            strm.FAL_DESC, 
            strm.FAL_ENABLEWORKORDERS, 
            strm.FAL_GEN, 
            strm.FAL_GROUP, 
            strm.FAL_LASTSAVED, 
            strm.FAL_NOTUSED, 
            strm.FAL_PARTFAILURE, 
            strm.FAL_UPDATECOUNT, 
            strm.FAL_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FAL_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FAILURES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FAL_CODE=src.FAL_CODE) OR (target.FAL_CODE IS NULL AND src.FAL_CODE IS NULL)) 
                when matched then update set
                    target.FAL_CODE=src.FAL_CODE, 
                    target.FAL_CREATED=src.FAL_CREATED, 
                    target.FAL_DESC=src.FAL_DESC, 
                    target.FAL_ENABLEWORKORDERS=src.FAL_ENABLEWORKORDERS, 
                    target.FAL_GEN=src.FAL_GEN, 
                    target.FAL_GROUP=src.FAL_GROUP, 
                    target.FAL_LASTSAVED=src.FAL_LASTSAVED, 
                    target.FAL_NOTUSED=src.FAL_NOTUSED, 
                    target.FAL_PARTFAILURE=src.FAL_PARTFAILURE, 
                    target.FAL_UPDATECOUNT=src.FAL_UPDATECOUNT, 
                    target.FAL_UPDATED=src.FAL_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( FAL_CODE, FAL_CREATED, FAL_DESC, FAL_ENABLEWORKORDERS, FAL_GEN, FAL_GROUP, FAL_LASTSAVED, FAL_NOTUSED, FAL_PARTFAILURE, FAL_UPDATECOUNT, FAL_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.FAL_CODE, 
                    src.FAL_CREATED, 
                    src.FAL_DESC, 
                    src.FAL_ENABLEWORKORDERS, 
                    src.FAL_GEN, 
                    src.FAL_GROUP, 
                    src.FAL_LASTSAVED, 
                    src.FAL_NOTUSED, 
                    src.FAL_PARTFAILURE, 
                    src.FAL_UPDATECOUNT, 
                    src.FAL_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename,
                    case when ETL_DELETED=TRUE then TRUE else FALSE end,
                    ETL_DELETED);
    `
} );

                    
//  Get number of rows inserted
        
        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();
        var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
        var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();
                    

        
    snowflake.execute ({sqlText: "commit"});

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', 0 ," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
    snowflake.execute({sqlText: log_sql_command});

    result = "Success"; 

    }       

    catch (err)  {
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
    //  Create a log entry to say INSERT STATEMENT failed
    
    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', 0 ,0,0);";
    snowflake.execute({sqlText: log_sql_command});
        ;
        throw "Failed: " + err.message;   // Return a success/error indicator.
        }
    return result;
    $$;
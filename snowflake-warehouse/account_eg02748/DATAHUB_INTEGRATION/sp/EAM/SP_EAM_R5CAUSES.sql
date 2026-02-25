create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CAUSES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CAUSES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CAUSES as target using (SELECT * FROM (SELECT 
            strm.CAU_CODE, 
            strm.CAU_CREATED, 
            strm.CAU_DESC, 
            strm.CAU_ENABLEWORKORDERS, 
            strm.CAU_GEN, 
            strm.CAU_GROUP, 
            strm.CAU_LASTSAVED, 
            strm.CAU_NOTUSED, 
            strm.CAU_PARTFAILURE, 
            strm.CAU_UPDATECOUNT, 
            strm.CAU_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CAU_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CAUSES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CAU_CODE=src.CAU_CODE) OR (target.CAU_CODE IS NULL AND src.CAU_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CAU_CODE=src.CAU_CODE, 
                    target.CAU_CREATED=src.CAU_CREATED, 
                    target.CAU_DESC=src.CAU_DESC, 
                    target.CAU_ENABLEWORKORDERS=src.CAU_ENABLEWORKORDERS, 
                    target.CAU_GEN=src.CAU_GEN, 
                    target.CAU_GROUP=src.CAU_GROUP, 
                    target.CAU_LASTSAVED=src.CAU_LASTSAVED, 
                    target.CAU_NOTUSED=src.CAU_NOTUSED, 
                    target.CAU_PARTFAILURE=src.CAU_PARTFAILURE, 
                    target.CAU_UPDATECOUNT=src.CAU_UPDATECOUNT, 
                    target.CAU_UPDATED=src.CAU_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CAU_CODE, 
                    CAU_CREATED, 
                    CAU_DESC, 
                    CAU_ENABLEWORKORDERS, 
                    CAU_GEN, 
                    CAU_GROUP, 
                    CAU_LASTSAVED, 
                    CAU_NOTUSED, 
                    CAU_PARTFAILURE, 
                    CAU_UPDATECOUNT, 
                    CAU_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CAU_CODE, 
                    src.CAU_CREATED, 
                    src.CAU_DESC, 
                    src.CAU_ENABLEWORKORDERS, 
                    src.CAU_GEN, 
                    src.CAU_GROUP, 
                    src.CAU_LASTSAVED, 
                    src.CAU_NOTUSED, 
                    src.CAU_PARTFAILURE, 
                    src.CAU_UPDATECOUNT, 
                    src.CAU_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CAUSES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CAU_CODE, 
            strm.CAU_CREATED, 
            strm.CAU_DESC, 
            strm.CAU_ENABLEWORKORDERS, 
            strm.CAU_GEN, 
            strm.CAU_GROUP, 
            strm.CAU_LASTSAVED, 
            strm.CAU_NOTUSED, 
            strm.CAU_PARTFAILURE, 
            strm.CAU_UPDATECOUNT, 
            strm.CAU_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CAU_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CAUSES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CAU_CODE=src.CAU_CODE) OR (target.CAU_CODE IS NULL AND src.CAU_CODE IS NULL)) 
                when matched then update set
                    target.CAU_CODE=src.CAU_CODE, 
                    target.CAU_CREATED=src.CAU_CREATED, 
                    target.CAU_DESC=src.CAU_DESC, 
                    target.CAU_ENABLEWORKORDERS=src.CAU_ENABLEWORKORDERS, 
                    target.CAU_GEN=src.CAU_GEN, 
                    target.CAU_GROUP=src.CAU_GROUP, 
                    target.CAU_LASTSAVED=src.CAU_LASTSAVED, 
                    target.CAU_NOTUSED=src.CAU_NOTUSED, 
                    target.CAU_PARTFAILURE=src.CAU_PARTFAILURE, 
                    target.CAU_UPDATECOUNT=src.CAU_UPDATECOUNT, 
                    target.CAU_UPDATED=src.CAU_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CAU_CODE, CAU_CREATED, CAU_DESC, CAU_ENABLEWORKORDERS, CAU_GEN, CAU_GROUP, CAU_LASTSAVED, CAU_NOTUSED, CAU_PARTFAILURE, CAU_UPDATECOUNT, CAU_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CAU_CODE, 
                    src.CAU_CREATED, 
                    src.CAU_DESC, 
                    src.CAU_ENABLEWORKORDERS, 
                    src.CAU_GEN, 
                    src.CAU_GROUP, 
                    src.CAU_LASTSAVED, 
                    src.CAU_NOTUSED, 
                    src.CAU_PARTFAILURE, 
                    src.CAU_UPDATECOUNT, 
                    src.CAU_UPDATED, 
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
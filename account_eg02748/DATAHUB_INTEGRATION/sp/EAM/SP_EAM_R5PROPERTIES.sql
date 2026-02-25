create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PROPERTIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PROPERTIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PROPERTIES as target using (SELECT * FROM (SELECT 
            strm.PRO_CODE, 
            strm.PRO_CREATED, 
            strm.PRO_INCLUDEINGRIDS, 
            strm.PRO_LASTSAVED, 
            strm.PRO_MAX, 
            strm.PRO_MIN, 
            strm.PRO_RENTITY, 
            strm.PRO_TEXT, 
            strm.PRO_TYPE, 
            strm.PRO_UPDATECOUNT, 
            strm.PRO_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRO_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRO_CODE=src.PRO_CODE) OR (target.PRO_CODE IS NULL AND src.PRO_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PRO_CODE=src.PRO_CODE, 
                    target.PRO_CREATED=src.PRO_CREATED, 
                    target.PRO_INCLUDEINGRIDS=src.PRO_INCLUDEINGRIDS, 
                    target.PRO_LASTSAVED=src.PRO_LASTSAVED, 
                    target.PRO_MAX=src.PRO_MAX, 
                    target.PRO_MIN=src.PRO_MIN, 
                    target.PRO_RENTITY=src.PRO_RENTITY, 
                    target.PRO_TEXT=src.PRO_TEXT, 
                    target.PRO_TYPE=src.PRO_TYPE, 
                    target.PRO_UPDATECOUNT=src.PRO_UPDATECOUNT, 
                    target.PRO_UPDATED=src.PRO_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PRO_CODE, 
                    PRO_CREATED, 
                    PRO_INCLUDEINGRIDS, 
                    PRO_LASTSAVED, 
                    PRO_MAX, 
                    PRO_MIN, 
                    PRO_RENTITY, 
                    PRO_TEXT, 
                    PRO_TYPE, 
                    PRO_UPDATECOUNT, 
                    PRO_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PRO_CODE, 
                    src.PRO_CREATED, 
                    src.PRO_INCLUDEINGRIDS, 
                    src.PRO_LASTSAVED, 
                    src.PRO_MAX, 
                    src.PRO_MIN, 
                    src.PRO_RENTITY, 
                    src.PRO_TEXT, 
                    src.PRO_TYPE, 
                    src.PRO_UPDATECOUNT, 
                    src.PRO_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PROPERTIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PRO_CODE, 
            strm.PRO_CREATED, 
            strm.PRO_INCLUDEINGRIDS, 
            strm.PRO_LASTSAVED, 
            strm.PRO_MAX, 
            strm.PRO_MIN, 
            strm.PRO_RENTITY, 
            strm.PRO_TEXT, 
            strm.PRO_TYPE, 
            strm.PRO_UPDATECOUNT, 
            strm.PRO_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRO_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRO_CODE=src.PRO_CODE) OR (target.PRO_CODE IS NULL AND src.PRO_CODE IS NULL)) 
                when matched then update set
                    target.PRO_CODE=src.PRO_CODE, 
                    target.PRO_CREATED=src.PRO_CREATED, 
                    target.PRO_INCLUDEINGRIDS=src.PRO_INCLUDEINGRIDS, 
                    target.PRO_LASTSAVED=src.PRO_LASTSAVED, 
                    target.PRO_MAX=src.PRO_MAX, 
                    target.PRO_MIN=src.PRO_MIN, 
                    target.PRO_RENTITY=src.PRO_RENTITY, 
                    target.PRO_TEXT=src.PRO_TEXT, 
                    target.PRO_TYPE=src.PRO_TYPE, 
                    target.PRO_UPDATECOUNT=src.PRO_UPDATECOUNT, 
                    target.PRO_UPDATED=src.PRO_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PRO_CODE, PRO_CREATED, PRO_INCLUDEINGRIDS, PRO_LASTSAVED, PRO_MAX, PRO_MIN, PRO_RENTITY, PRO_TEXT, PRO_TYPE, PRO_UPDATECOUNT, PRO_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PRO_CODE, 
                    src.PRO_CREATED, 
                    src.PRO_INCLUDEINGRIDS, 
                    src.PRO_LASTSAVED, 
                    src.PRO_MAX, 
                    src.PRO_MIN, 
                    src.PRO_RENTITY, 
                    src.PRO_TEXT, 
                    src.PRO_TYPE, 
                    src.PRO_UPDATECOUNT, 
                    src.PRO_UPDATED, 
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
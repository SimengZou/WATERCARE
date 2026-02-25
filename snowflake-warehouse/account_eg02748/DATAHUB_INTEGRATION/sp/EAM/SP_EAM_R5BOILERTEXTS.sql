create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5BOILERTEXTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5BOILERTEXTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5BOILERTEXTS as target using (SELECT * FROM (SELECT 
            strm.BOT_ADLEN, 
            strm.BOT_CHANGED, 
            strm.BOT_CLONED, 
            strm.BOT_CREATED, 
            strm.BOT_DEST, 
            strm.BOT_DISPLAY, 
            strm.BOT_FLD1, 
            strm.BOT_FLD2, 
            strm.BOT_FUNCTION, 
            strm.BOT_LANG, 
            strm.BOT_LASTSAVED, 
            strm.BOT_LENGTH, 
            strm.BOT_LVCR, 
            strm.BOT_NUMBER, 
            strm.BOT_PAGE, 
            strm.BOT_POOL, 
            strm.BOT_PRTP, 
            strm.BOT_TEXT, 
            strm.BOT_UPDATECOUNT, 
            strm.BOT_UPDATED, 
            strm.BOT_XPOS, 
            strm.BOT_YPOS, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BOT_FUNCTION,
            strm.BOT_NUMBER,
            strm.BOT_LANG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5BOILERTEXTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BOT_FUNCTION=src.BOT_FUNCTION) OR (target.BOT_FUNCTION IS NULL AND src.BOT_FUNCTION IS NULL)) AND
            ((target.BOT_NUMBER=src.BOT_NUMBER) OR (target.BOT_NUMBER IS NULL AND src.BOT_NUMBER IS NULL)) AND
            ((target.BOT_LANG=src.BOT_LANG) OR (target.BOT_LANG IS NULL AND src.BOT_LANG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.BOT_ADLEN=src.BOT_ADLEN, 
                    target.BOT_CHANGED=src.BOT_CHANGED, 
                    target.BOT_CLONED=src.BOT_CLONED, 
                    target.BOT_CREATED=src.BOT_CREATED, 
                    target.BOT_DEST=src.BOT_DEST, 
                    target.BOT_DISPLAY=src.BOT_DISPLAY, 
                    target.BOT_FLD1=src.BOT_FLD1, 
                    target.BOT_FLD2=src.BOT_FLD2, 
                    target.BOT_FUNCTION=src.BOT_FUNCTION, 
                    target.BOT_LANG=src.BOT_LANG, 
                    target.BOT_LASTSAVED=src.BOT_LASTSAVED, 
                    target.BOT_LENGTH=src.BOT_LENGTH, 
                    target.BOT_LVCR=src.BOT_LVCR, 
                    target.BOT_NUMBER=src.BOT_NUMBER, 
                    target.BOT_PAGE=src.BOT_PAGE, 
                    target.BOT_POOL=src.BOT_POOL, 
                    target.BOT_PRTP=src.BOT_PRTP, 
                    target.BOT_TEXT=src.BOT_TEXT, 
                    target.BOT_UPDATECOUNT=src.BOT_UPDATECOUNT, 
                    target.BOT_UPDATED=src.BOT_UPDATED, 
                    target.BOT_XPOS=src.BOT_XPOS, 
                    target.BOT_YPOS=src.BOT_YPOS, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    BOT_ADLEN, 
                    BOT_CHANGED, 
                    BOT_CLONED, 
                    BOT_CREATED, 
                    BOT_DEST, 
                    BOT_DISPLAY, 
                    BOT_FLD1, 
                    BOT_FLD2, 
                    BOT_FUNCTION, 
                    BOT_LANG, 
                    BOT_LASTSAVED, 
                    BOT_LENGTH, 
                    BOT_LVCR, 
                    BOT_NUMBER, 
                    BOT_PAGE, 
                    BOT_POOL, 
                    BOT_PRTP, 
                    BOT_TEXT, 
                    BOT_UPDATECOUNT, 
                    BOT_UPDATED, 
                    BOT_XPOS, 
                    BOT_YPOS, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.BOT_ADLEN, 
                    src.BOT_CHANGED, 
                    src.BOT_CLONED, 
                    src.BOT_CREATED, 
                    src.BOT_DEST, 
                    src.BOT_DISPLAY, 
                    src.BOT_FLD1, 
                    src.BOT_FLD2, 
                    src.BOT_FUNCTION, 
                    src.BOT_LANG, 
                    src.BOT_LASTSAVED, 
                    src.BOT_LENGTH, 
                    src.BOT_LVCR, 
                    src.BOT_NUMBER, 
                    src.BOT_PAGE, 
                    src.BOT_POOL, 
                    src.BOT_PRTP, 
                    src.BOT_TEXT, 
                    src.BOT_UPDATECOUNT, 
                    src.BOT_UPDATED, 
                    src.BOT_XPOS, 
                    src.BOT_YPOS, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5BOILERTEXTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.BOT_ADLEN, 
            strm.BOT_CHANGED, 
            strm.BOT_CLONED, 
            strm.BOT_CREATED, 
            strm.BOT_DEST, 
            strm.BOT_DISPLAY, 
            strm.BOT_FLD1, 
            strm.BOT_FLD2, 
            strm.BOT_FUNCTION, 
            strm.BOT_LANG, 
            strm.BOT_LASTSAVED, 
            strm.BOT_LENGTH, 
            strm.BOT_LVCR, 
            strm.BOT_NUMBER, 
            strm.BOT_PAGE, 
            strm.BOT_POOL, 
            strm.BOT_PRTP, 
            strm.BOT_TEXT, 
            strm.BOT_UPDATECOUNT, 
            strm.BOT_UPDATED, 
            strm.BOT_XPOS, 
            strm.BOT_YPOS, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BOT_FUNCTION,
            strm.BOT_NUMBER,
            strm.BOT_LANG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5BOILERTEXTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BOT_FUNCTION=src.BOT_FUNCTION) OR (target.BOT_FUNCTION IS NULL AND src.BOT_FUNCTION IS NULL)) AND
            ((target.BOT_NUMBER=src.BOT_NUMBER) OR (target.BOT_NUMBER IS NULL AND src.BOT_NUMBER IS NULL)) AND
            ((target.BOT_LANG=src.BOT_LANG) OR (target.BOT_LANG IS NULL AND src.BOT_LANG IS NULL)) 
                when matched then update set
                    target.BOT_ADLEN=src.BOT_ADLEN, 
                    target.BOT_CHANGED=src.BOT_CHANGED, 
                    target.BOT_CLONED=src.BOT_CLONED, 
                    target.BOT_CREATED=src.BOT_CREATED, 
                    target.BOT_DEST=src.BOT_DEST, 
                    target.BOT_DISPLAY=src.BOT_DISPLAY, 
                    target.BOT_FLD1=src.BOT_FLD1, 
                    target.BOT_FLD2=src.BOT_FLD2, 
                    target.BOT_FUNCTION=src.BOT_FUNCTION, 
                    target.BOT_LANG=src.BOT_LANG, 
                    target.BOT_LASTSAVED=src.BOT_LASTSAVED, 
                    target.BOT_LENGTH=src.BOT_LENGTH, 
                    target.BOT_LVCR=src.BOT_LVCR, 
                    target.BOT_NUMBER=src.BOT_NUMBER, 
                    target.BOT_PAGE=src.BOT_PAGE, 
                    target.BOT_POOL=src.BOT_POOL, 
                    target.BOT_PRTP=src.BOT_PRTP, 
                    target.BOT_TEXT=src.BOT_TEXT, 
                    target.BOT_UPDATECOUNT=src.BOT_UPDATECOUNT, 
                    target.BOT_UPDATED=src.BOT_UPDATED, 
                    target.BOT_XPOS=src.BOT_XPOS, 
                    target.BOT_YPOS=src.BOT_YPOS, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( BOT_ADLEN, BOT_CHANGED, BOT_CLONED, BOT_CREATED, BOT_DEST, BOT_DISPLAY, BOT_FLD1, BOT_FLD2, BOT_FUNCTION, BOT_LANG, BOT_LASTSAVED, BOT_LENGTH, BOT_LVCR, BOT_NUMBER, BOT_PAGE, BOT_POOL, BOT_PRTP, BOT_TEXT, BOT_UPDATECOUNT, BOT_UPDATED, BOT_XPOS, BOT_YPOS, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.BOT_ADLEN, 
                    src.BOT_CHANGED, 
                    src.BOT_CLONED, 
                    src.BOT_CREATED, 
                    src.BOT_DEST, 
                    src.BOT_DISPLAY, 
                    src.BOT_FLD1, 
                    src.BOT_FLD2, 
                    src.BOT_FUNCTION, 
                    src.BOT_LANG, 
                    src.BOT_LASTSAVED, 
                    src.BOT_LENGTH, 
                    src.BOT_LVCR, 
                    src.BOT_NUMBER, 
                    src.BOT_PAGE, 
                    src.BOT_POOL, 
                    src.BOT_PRTP, 
                    src.BOT_TEXT, 
                    src.BOT_UPDATECOUNT, 
                    src.BOT_UPDATED, 
                    src.BOT_XPOS, 
                    src.BOT_YPOS, 
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
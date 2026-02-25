create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ADDETAILS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ADDETAILS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ADDETAILS as target using (SELECT * FROM (SELECT 
            strm.ADD_CODE, 
            strm.ADD_CREATED, 
            strm.ADD_ENTITY, 
            strm.ADD_LANG, 
            strm.ADD_LASTSAVED, 
            strm.ADD_LINE, 
            strm.ADD_PRINT, 
            strm.ADD_RENTITY, 
            strm.ADD_RTYPE, 
            strm.ADD_TEXT, 
            strm.ADD_TYPE, 
            strm.ADD_UPDATECOUNT, 
            strm.ADD_UPDATED, 
            strm.ADD_UPDUSER, 
            strm.ADD_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADD_CODE,
            strm.ADD_RENTITY,
            strm.ADD_TYPE,
            strm.ADD_LANG,
            strm.ADD_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDETAILS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADD_CODE=src.ADD_CODE) OR (target.ADD_CODE IS NULL AND src.ADD_CODE IS NULL)) AND
            ((target.ADD_RENTITY=src.ADD_RENTITY) OR (target.ADD_RENTITY IS NULL AND src.ADD_RENTITY IS NULL)) AND
            ((target.ADD_TYPE=src.ADD_TYPE) OR (target.ADD_TYPE IS NULL AND src.ADD_TYPE IS NULL)) AND
            ((target.ADD_LANG=src.ADD_LANG) OR (target.ADD_LANG IS NULL AND src.ADD_LANG IS NULL)) AND
            ((target.ADD_LINE=src.ADD_LINE) OR (target.ADD_LINE IS NULL AND src.ADD_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADD_CODE=src.ADD_CODE, 
                    target.ADD_CREATED=src.ADD_CREATED, 
                    target.ADD_ENTITY=src.ADD_ENTITY, 
                    target.ADD_LANG=src.ADD_LANG, 
                    target.ADD_LASTSAVED=src.ADD_LASTSAVED, 
                    target.ADD_LINE=src.ADD_LINE, 
                    target.ADD_PRINT=src.ADD_PRINT, 
                    target.ADD_RENTITY=src.ADD_RENTITY, 
                    target.ADD_RTYPE=src.ADD_RTYPE, 
                    target.ADD_TEXT=src.ADD_TEXT, 
                    target.ADD_TYPE=src.ADD_TYPE, 
                    target.ADD_UPDATECOUNT=src.ADD_UPDATECOUNT, 
                    target.ADD_UPDATED=src.ADD_UPDATED, 
                    target.ADD_UPDUSER=src.ADD_UPDUSER, 
                    target.ADD_USER=src.ADD_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADD_CODE, 
                    ADD_CREATED, 
                    ADD_ENTITY, 
                    ADD_LANG, 
                    ADD_LASTSAVED, 
                    ADD_LINE, 
                    ADD_PRINT, 
                    ADD_RENTITY, 
                    ADD_RTYPE, 
                    ADD_TEXT, 
                    ADD_TYPE, 
                    ADD_UPDATECOUNT, 
                    ADD_UPDATED, 
                    ADD_UPDUSER, 
                    ADD_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADD_CODE, 
                    src.ADD_CREATED, 
                    src.ADD_ENTITY, 
                    src.ADD_LANG, 
                    src.ADD_LASTSAVED, 
                    src.ADD_LINE, 
                    src.ADD_PRINT, 
                    src.ADD_RENTITY, 
                    src.ADD_RTYPE, 
                    src.ADD_TEXT, 
                    src.ADD_TYPE, 
                    src.ADD_UPDATECOUNT, 
                    src.ADD_UPDATED, 
                    src.ADD_UPDUSER, 
                    src.ADD_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ADDETAILS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ADD_CODE, 
            strm.ADD_CREATED, 
            strm.ADD_ENTITY, 
            strm.ADD_LANG, 
            strm.ADD_LASTSAVED, 
            strm.ADD_LINE, 
            strm.ADD_PRINT, 
            strm.ADD_RENTITY, 
            strm.ADD_RTYPE, 
            strm.ADD_TEXT, 
            strm.ADD_TYPE, 
            strm.ADD_UPDATECOUNT, 
            strm.ADD_UPDATED, 
            strm.ADD_UPDUSER, 
            strm.ADD_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADD_CODE,
            strm.ADD_RENTITY,
            strm.ADD_TYPE,
            strm.ADD_LANG,
            strm.ADD_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDETAILS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADD_CODE=src.ADD_CODE) OR (target.ADD_CODE IS NULL AND src.ADD_CODE IS NULL)) AND
            ((target.ADD_RENTITY=src.ADD_RENTITY) OR (target.ADD_RENTITY IS NULL AND src.ADD_RENTITY IS NULL)) AND
            ((target.ADD_TYPE=src.ADD_TYPE) OR (target.ADD_TYPE IS NULL AND src.ADD_TYPE IS NULL)) AND
            ((target.ADD_LANG=src.ADD_LANG) OR (target.ADD_LANG IS NULL AND src.ADD_LANG IS NULL)) AND
            ((target.ADD_LINE=src.ADD_LINE) OR (target.ADD_LINE IS NULL AND src.ADD_LINE IS NULL)) 
                when matched then update set
                    target.ADD_CODE=src.ADD_CODE, 
                    target.ADD_CREATED=src.ADD_CREATED, 
                    target.ADD_ENTITY=src.ADD_ENTITY, 
                    target.ADD_LANG=src.ADD_LANG, 
                    target.ADD_LASTSAVED=src.ADD_LASTSAVED, 
                    target.ADD_LINE=src.ADD_LINE, 
                    target.ADD_PRINT=src.ADD_PRINT, 
                    target.ADD_RENTITY=src.ADD_RENTITY, 
                    target.ADD_RTYPE=src.ADD_RTYPE, 
                    target.ADD_TEXT=src.ADD_TEXT, 
                    target.ADD_TYPE=src.ADD_TYPE, 
                    target.ADD_UPDATECOUNT=src.ADD_UPDATECOUNT, 
                    target.ADD_UPDATED=src.ADD_UPDATED, 
                    target.ADD_UPDUSER=src.ADD_UPDUSER, 
                    target.ADD_USER=src.ADD_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADD_CODE, ADD_CREATED, ADD_ENTITY, ADD_LANG, ADD_LASTSAVED, ADD_LINE, ADD_PRINT, ADD_RENTITY, ADD_RTYPE, ADD_TEXT, ADD_TYPE, ADD_UPDATECOUNT, ADD_UPDATED, ADD_UPDUSER, ADD_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADD_CODE, 
                    src.ADD_CREATED, 
                    src.ADD_ENTITY, 
                    src.ADD_LANG, 
                    src.ADD_LASTSAVED, 
                    src.ADD_LINE, 
                    src.ADD_PRINT, 
                    src.ADD_RENTITY, 
                    src.ADD_RTYPE, 
                    src.ADD_TEXT, 
                    src.ADD_TYPE, 
                    src.ADD_UPDATECOUNT, 
                    src.ADD_UPDATED, 
                    src.ADD_UPDUSER, 
                    src.ADD_USER, 
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
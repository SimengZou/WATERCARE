create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MAILTEMPLATE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MAILTEMPLATE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MAILTEMPLATE as target using (SELECT * FROM (SELECT 
            strm.MAT_CODE, 
            strm.MAT_DESC, 
            strm.MAT_EMAIL, 
            strm.MAT_FROMEMAIL, 
            strm.MAT_LASTSAVED, 
            strm.MAT_MAIL, 
            strm.MAT_NOTEBOOKEMAILS, 
            strm.MAT_PUSHNOTIFICATION, 
            strm.MAT_REPORT, 
            strm.MAT_SUBJECT, 
            strm.MAT_TEXT, 
            strm.MAT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAT_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILTEMPLATE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAT_CODE=src.MAT_CODE) OR (target.MAT_CODE IS NULL AND src.MAT_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MAT_CODE=src.MAT_CODE, 
                    target.MAT_DESC=src.MAT_DESC, 
                    target.MAT_EMAIL=src.MAT_EMAIL, 
                    target.MAT_FROMEMAIL=src.MAT_FROMEMAIL, 
                    target.MAT_LASTSAVED=src.MAT_LASTSAVED, 
                    target.MAT_MAIL=src.MAT_MAIL, 
                    target.MAT_NOTEBOOKEMAILS=src.MAT_NOTEBOOKEMAILS, 
                    target.MAT_PUSHNOTIFICATION=src.MAT_PUSHNOTIFICATION, 
                    target.MAT_REPORT=src.MAT_REPORT, 
                    target.MAT_SUBJECT=src.MAT_SUBJECT, 
                    target.MAT_TEXT=src.MAT_TEXT, 
                    target.MAT_UPDATECOUNT=src.MAT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MAT_CODE, 
                    MAT_DESC, 
                    MAT_EMAIL, 
                    MAT_FROMEMAIL, 
                    MAT_LASTSAVED, 
                    MAT_MAIL, 
                    MAT_NOTEBOOKEMAILS, 
                    MAT_PUSHNOTIFICATION, 
                    MAT_REPORT, 
                    MAT_SUBJECT, 
                    MAT_TEXT, 
                    MAT_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MAT_CODE, 
                    src.MAT_DESC, 
                    src.MAT_EMAIL, 
                    src.MAT_FROMEMAIL, 
                    src.MAT_LASTSAVED, 
                    src.MAT_MAIL, 
                    src.MAT_NOTEBOOKEMAILS, 
                    src.MAT_PUSHNOTIFICATION, 
                    src.MAT_REPORT, 
                    src.MAT_SUBJECT, 
                    src.MAT_TEXT, 
                    src.MAT_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MAILTEMPLATE_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MAT_CODE, 
            strm.MAT_DESC, 
            strm.MAT_EMAIL, 
            strm.MAT_FROMEMAIL, 
            strm.MAT_LASTSAVED, 
            strm.MAT_MAIL, 
            strm.MAT_NOTEBOOKEMAILS, 
            strm.MAT_PUSHNOTIFICATION, 
            strm.MAT_REPORT, 
            strm.MAT_SUBJECT, 
            strm.MAT_TEXT, 
            strm.MAT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAT_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILTEMPLATE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAT_CODE=src.MAT_CODE) OR (target.MAT_CODE IS NULL AND src.MAT_CODE IS NULL)) 
                when matched then update set
                    target.MAT_CODE=src.MAT_CODE, 
                    target.MAT_DESC=src.MAT_DESC, 
                    target.MAT_EMAIL=src.MAT_EMAIL, 
                    target.MAT_FROMEMAIL=src.MAT_FROMEMAIL, 
                    target.MAT_LASTSAVED=src.MAT_LASTSAVED, 
                    target.MAT_MAIL=src.MAT_MAIL, 
                    target.MAT_NOTEBOOKEMAILS=src.MAT_NOTEBOOKEMAILS, 
                    target.MAT_PUSHNOTIFICATION=src.MAT_PUSHNOTIFICATION, 
                    target.MAT_REPORT=src.MAT_REPORT, 
                    target.MAT_SUBJECT=src.MAT_SUBJECT, 
                    target.MAT_TEXT=src.MAT_TEXT, 
                    target.MAT_UPDATECOUNT=src.MAT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MAT_CODE, MAT_DESC, MAT_EMAIL, MAT_FROMEMAIL, MAT_LASTSAVED, MAT_MAIL, MAT_NOTEBOOKEMAILS, MAT_PUSHNOTIFICATION, MAT_REPORT, MAT_SUBJECT, MAT_TEXT, MAT_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MAT_CODE, 
                    src.MAT_DESC, 
                    src.MAT_EMAIL, 
                    src.MAT_FROMEMAIL, 
                    src.MAT_LASTSAVED, 
                    src.MAT_MAIL, 
                    src.MAT_NOTEBOOKEMAILS, 
                    src.MAT_PUSHNOTIFICATION, 
                    src.MAT_REPORT, 
                    src.MAT_SUBJECT, 
                    src.MAT_TEXT, 
                    src.MAT_UPDATECOUNT, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5QUERIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5QUERIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5QUERIES as target using (SELECT * FROM (SELECT 
            strm.QUE_ASSET, 
            strm.QUE_CHART, 
            strm.QUE_CODE, 
            strm.QUE_DESC, 
            strm.QUE_EQUIPMENTRANKING, 
            strm.QUE_INBOX, 
            strm.QUE_KPI, 
            strm.QUE_LASTSAVED, 
            strm.QUE_LOOKUP, 
            strm.QUE_NORMAL, 
            strm.QUE_NOTE, 
            strm.QUE_TEXT, 
            strm.QUE_UPDATECOUNT, 
            strm.QUE_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.QUE_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5QUERIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.QUE_CODE=src.QUE_CODE) OR (target.QUE_CODE IS NULL AND src.QUE_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.QUE_ASSET=src.QUE_ASSET, 
                    target.QUE_CHART=src.QUE_CHART, 
                    target.QUE_CODE=src.QUE_CODE, 
                    target.QUE_DESC=src.QUE_DESC, 
                    target.QUE_EQUIPMENTRANKING=src.QUE_EQUIPMENTRANKING, 
                    target.QUE_INBOX=src.QUE_INBOX, 
                    target.QUE_KPI=src.QUE_KPI, 
                    target.QUE_LASTSAVED=src.QUE_LASTSAVED, 
                    target.QUE_LOOKUP=src.QUE_LOOKUP, 
                    target.QUE_NORMAL=src.QUE_NORMAL, 
                    target.QUE_NOTE=src.QUE_NOTE, 
                    target.QUE_TEXT=src.QUE_TEXT, 
                    target.QUE_UPDATECOUNT=src.QUE_UPDATECOUNT, 
                    target.QUE_UPDATED=src.QUE_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    QUE_ASSET, 
                    QUE_CHART, 
                    QUE_CODE, 
                    QUE_DESC, 
                    QUE_EQUIPMENTRANKING, 
                    QUE_INBOX, 
                    QUE_KPI, 
                    QUE_LASTSAVED, 
                    QUE_LOOKUP, 
                    QUE_NORMAL, 
                    QUE_NOTE, 
                    QUE_TEXT, 
                    QUE_UPDATECOUNT, 
                    QUE_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.QUE_ASSET, 
                    src.QUE_CHART, 
                    src.QUE_CODE, 
                    src.QUE_DESC, 
                    src.QUE_EQUIPMENTRANKING, 
                    src.QUE_INBOX, 
                    src.QUE_KPI, 
                    src.QUE_LASTSAVED, 
                    src.QUE_LOOKUP, 
                    src.QUE_NORMAL, 
                    src.QUE_NOTE, 
                    src.QUE_TEXT, 
                    src.QUE_UPDATECOUNT, 
                    src.QUE_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5QUERIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.QUE_ASSET, 
            strm.QUE_CHART, 
            strm.QUE_CODE, 
            strm.QUE_DESC, 
            strm.QUE_EQUIPMENTRANKING, 
            strm.QUE_INBOX, 
            strm.QUE_KPI, 
            strm.QUE_LASTSAVED, 
            strm.QUE_LOOKUP, 
            strm.QUE_NORMAL, 
            strm.QUE_NOTE, 
            strm.QUE_TEXT, 
            strm.QUE_UPDATECOUNT, 
            strm.QUE_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.QUE_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5QUERIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.QUE_CODE=src.QUE_CODE) OR (target.QUE_CODE IS NULL AND src.QUE_CODE IS NULL)) 
                when matched then update set
                    target.QUE_ASSET=src.QUE_ASSET, 
                    target.QUE_CHART=src.QUE_CHART, 
                    target.QUE_CODE=src.QUE_CODE, 
                    target.QUE_DESC=src.QUE_DESC, 
                    target.QUE_EQUIPMENTRANKING=src.QUE_EQUIPMENTRANKING, 
                    target.QUE_INBOX=src.QUE_INBOX, 
                    target.QUE_KPI=src.QUE_KPI, 
                    target.QUE_LASTSAVED=src.QUE_LASTSAVED, 
                    target.QUE_LOOKUP=src.QUE_LOOKUP, 
                    target.QUE_NORMAL=src.QUE_NORMAL, 
                    target.QUE_NOTE=src.QUE_NOTE, 
                    target.QUE_TEXT=src.QUE_TEXT, 
                    target.QUE_UPDATECOUNT=src.QUE_UPDATECOUNT, 
                    target.QUE_UPDATED=src.QUE_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( QUE_ASSET, QUE_CHART, QUE_CODE, QUE_DESC, QUE_EQUIPMENTRANKING, QUE_INBOX, QUE_KPI, QUE_LASTSAVED, QUE_LOOKUP, QUE_NORMAL, QUE_NOTE, QUE_TEXT, QUE_UPDATECOUNT, QUE_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.QUE_ASSET, 
                    src.QUE_CHART, 
                    src.QUE_CODE, 
                    src.QUE_DESC, 
                    src.QUE_EQUIPMENTRANKING, 
                    src.QUE_INBOX, 
                    src.QUE_KPI, 
                    src.QUE_LASTSAVED, 
                    src.QUE_LOOKUP, 
                    src.QUE_NORMAL, 
                    src.QUE_NOTE, 
                    src.QUE_TEXT, 
                    src.QUE_UPDATECOUNT, 
                    src.QUE_UPDATED, 
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
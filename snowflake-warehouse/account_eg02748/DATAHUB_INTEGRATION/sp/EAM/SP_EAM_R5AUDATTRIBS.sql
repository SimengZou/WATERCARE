create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5AUDATTRIBS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5AUDATTRIBS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5AUDATTRIBS as target using (SELECT * FROM (SELECT 
            strm.AAT_CODE, 
            strm.AAT_COLUMN, 
            strm.AAT_COMMENT, 
            strm.AAT_DELETE, 
            strm.AAT_ENTEREDBY, 
            strm.AAT_INSERT, 
            strm.AAT_LASTSAVED, 
            strm.AAT_TABLE, 
            strm.AAT_TEXT, 
            strm.AAT_UPDATE, 
            strm.AAT_UPDATECOUNT, 
            strm.AAT_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AAT_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUDATTRIBS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AAT_CODE=src.AAT_CODE) OR (target.AAT_CODE IS NULL AND src.AAT_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.AAT_CODE=src.AAT_CODE, 
                    target.AAT_COLUMN=src.AAT_COLUMN, 
                    target.AAT_COMMENT=src.AAT_COMMENT, 
                    target.AAT_DELETE=src.AAT_DELETE, 
                    target.AAT_ENTEREDBY=src.AAT_ENTEREDBY, 
                    target.AAT_INSERT=src.AAT_INSERT, 
                    target.AAT_LASTSAVED=src.AAT_LASTSAVED, 
                    target.AAT_TABLE=src.AAT_TABLE, 
                    target.AAT_TEXT=src.AAT_TEXT, 
                    target.AAT_UPDATE=src.AAT_UPDATE, 
                    target.AAT_UPDATECOUNT=src.AAT_UPDATECOUNT, 
                    target.AAT_UPDATED=src.AAT_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    AAT_CODE, 
                    AAT_COLUMN, 
                    AAT_COMMENT, 
                    AAT_DELETE, 
                    AAT_ENTEREDBY, 
                    AAT_INSERT, 
                    AAT_LASTSAVED, 
                    AAT_TABLE, 
                    AAT_TEXT, 
                    AAT_UPDATE, 
                    AAT_UPDATECOUNT, 
                    AAT_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.AAT_CODE, 
                    src.AAT_COLUMN, 
                    src.AAT_COMMENT, 
                    src.AAT_DELETE, 
                    src.AAT_ENTEREDBY, 
                    src.AAT_INSERT, 
                    src.AAT_LASTSAVED, 
                    src.AAT_TABLE, 
                    src.AAT_TEXT, 
                    src.AAT_UPDATE, 
                    src.AAT_UPDATECOUNT, 
                    src.AAT_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5AUDATTRIBS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.AAT_CODE, 
            strm.AAT_COLUMN, 
            strm.AAT_COMMENT, 
            strm.AAT_DELETE, 
            strm.AAT_ENTEREDBY, 
            strm.AAT_INSERT, 
            strm.AAT_LASTSAVED, 
            strm.AAT_TABLE, 
            strm.AAT_TEXT, 
            strm.AAT_UPDATE, 
            strm.AAT_UPDATECOUNT, 
            strm.AAT_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AAT_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUDATTRIBS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AAT_CODE=src.AAT_CODE) OR (target.AAT_CODE IS NULL AND src.AAT_CODE IS NULL)) 
                when matched then update set
                    target.AAT_CODE=src.AAT_CODE, 
                    target.AAT_COLUMN=src.AAT_COLUMN, 
                    target.AAT_COMMENT=src.AAT_COMMENT, 
                    target.AAT_DELETE=src.AAT_DELETE, 
                    target.AAT_ENTEREDBY=src.AAT_ENTEREDBY, 
                    target.AAT_INSERT=src.AAT_INSERT, 
                    target.AAT_LASTSAVED=src.AAT_LASTSAVED, 
                    target.AAT_TABLE=src.AAT_TABLE, 
                    target.AAT_TEXT=src.AAT_TEXT, 
                    target.AAT_UPDATE=src.AAT_UPDATE, 
                    target.AAT_UPDATECOUNT=src.AAT_UPDATECOUNT, 
                    target.AAT_UPDATED=src.AAT_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( AAT_CODE, AAT_COLUMN, AAT_COMMENT, AAT_DELETE, AAT_ENTEREDBY, AAT_INSERT, AAT_LASTSAVED, AAT_TABLE, AAT_TEXT, AAT_UPDATE, AAT_UPDATECOUNT, AAT_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.AAT_CODE, 
                    src.AAT_COLUMN, 
                    src.AAT_COMMENT, 
                    src.AAT_DELETE, 
                    src.AAT_ENTEREDBY, 
                    src.AAT_INSERT, 
                    src.AAT_LASTSAVED, 
                    src.AAT_TABLE, 
                    src.AAT_TEXT, 
                    src.AAT_UPDATE, 
                    src.AAT_UPDATECOUNT, 
                    src.AAT_UPDATED, 
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
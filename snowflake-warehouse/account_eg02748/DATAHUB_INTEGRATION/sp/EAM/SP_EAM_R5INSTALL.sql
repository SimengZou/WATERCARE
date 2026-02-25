create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5INSTALL()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5INSTALL_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5INSTALL as target using (SELECT * FROM (SELECT 
            strm.INS_CODE, 
            strm.INS_COMMENT, 
            strm.INS_DESC, 
            strm.INS_EDCOMMENT, 
            strm.INS_EXTENDED, 
            strm.INS_FIXED, 
            strm.INS_LASTSAVED, 
            strm.INS_MODULE, 
            strm.INS_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.INS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5INSTALL as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.INS_CODE=src.INS_CODE) OR (target.INS_CODE IS NULL AND src.INS_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.INS_CODE=src.INS_CODE, 
                    target.INS_COMMENT=src.INS_COMMENT, 
                    target.INS_DESC=src.INS_DESC, 
                    target.INS_EDCOMMENT=src.INS_EDCOMMENT, 
                    target.INS_EXTENDED=src.INS_EXTENDED, 
                    target.INS_FIXED=src.INS_FIXED, 
                    target.INS_LASTSAVED=src.INS_LASTSAVED, 
                    target.INS_MODULE=src.INS_MODULE, 
                    target.INS_UPDATECOUNT=src.INS_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    INS_CODE, 
                    INS_COMMENT, 
                    INS_DESC, 
                    INS_EDCOMMENT, 
                    INS_EXTENDED, 
                    INS_FIXED, 
                    INS_LASTSAVED, 
                    INS_MODULE, 
                    INS_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.INS_CODE, 
                    src.INS_COMMENT, 
                    src.INS_DESC, 
                    src.INS_EDCOMMENT, 
                    src.INS_EXTENDED, 
                    src.INS_FIXED, 
                    src.INS_LASTSAVED, 
                    src.INS_MODULE, 
                    src.INS_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5INSTALL_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.INS_CODE, 
            strm.INS_COMMENT, 
            strm.INS_DESC, 
            strm.INS_EDCOMMENT, 
            strm.INS_EXTENDED, 
            strm.INS_FIXED, 
            strm.INS_LASTSAVED, 
            strm.INS_MODULE, 
            strm.INS_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.INS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5INSTALL as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.INS_CODE=src.INS_CODE) OR (target.INS_CODE IS NULL AND src.INS_CODE IS NULL)) 
                when matched then update set
                    target.INS_CODE=src.INS_CODE, 
                    target.INS_COMMENT=src.INS_COMMENT, 
                    target.INS_DESC=src.INS_DESC, 
                    target.INS_EDCOMMENT=src.INS_EDCOMMENT, 
                    target.INS_EXTENDED=src.INS_EXTENDED, 
                    target.INS_FIXED=src.INS_FIXED, 
                    target.INS_LASTSAVED=src.INS_LASTSAVED, 
                    target.INS_MODULE=src.INS_MODULE, 
                    target.INS_UPDATECOUNT=src.INS_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( INS_CODE, INS_COMMENT, INS_DESC, INS_EDCOMMENT, INS_EXTENDED, INS_FIXED, INS_LASTSAVED, INS_MODULE, INS_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.INS_CODE, 
                    src.INS_COMMENT, 
                    src.INS_DESC, 
                    src.INS_EDCOMMENT, 
                    src.INS_EXTENDED, 
                    src.INS_FIXED, 
                    src.INS_LASTSAVED, 
                    src.INS_MODULE, 
                    src.INS_UPDATECOUNT, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5WSRSPHIST()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5WSRSPHIST_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5WSRSPHIST as target using (SELECT * FROM (SELECT 
            strm.WSR_ADDRESS, 
            strm.WSR_DATAAREA, 
            strm.WSR_LASTSAVED, 
            strm.WSR_MESSAGE, 
            strm.WSR_PROCESS, 
            strm.WSR_RSTATUS, 
            strm.WSR_STATUS, 
            strm.WSR_STATUS_MESSAGE, 
            strm.WSR_TIME, 
            strm.WSR_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WSR_MESSAGE,
            strm.WSR_PROCESS ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSRSPHIST as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WSR_MESSAGE=src.WSR_MESSAGE) OR (target.WSR_MESSAGE IS NULL AND src.WSR_MESSAGE IS NULL)) AND
            ((target.WSR_PROCESS=src.WSR_PROCESS) OR (target.WSR_PROCESS IS NULL AND src.WSR_PROCESS IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.WSR_ADDRESS=src.WSR_ADDRESS, 
                    target.WSR_DATAAREA=src.WSR_DATAAREA, 
                    target.WSR_LASTSAVED=src.WSR_LASTSAVED, 
                    target.WSR_MESSAGE=src.WSR_MESSAGE, 
                    target.WSR_PROCESS=src.WSR_PROCESS, 
                    target.WSR_RSTATUS=src.WSR_RSTATUS, 
                    target.WSR_STATUS=src.WSR_STATUS, 
                    target.WSR_STATUS_MESSAGE=src.WSR_STATUS_MESSAGE, 
                    target.WSR_TIME=src.WSR_TIME, 
                    target.WSR_UPDATECOUNT=src.WSR_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    WSR_ADDRESS, 
                    WSR_DATAAREA, 
                    WSR_LASTSAVED, 
                    WSR_MESSAGE, 
                    WSR_PROCESS, 
                    WSR_RSTATUS, 
                    WSR_STATUS, 
                    WSR_STATUS_MESSAGE, 
                    WSR_TIME, 
                    WSR_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.WSR_ADDRESS, 
                    src.WSR_DATAAREA, 
                    src.WSR_LASTSAVED, 
                    src.WSR_MESSAGE, 
                    src.WSR_PROCESS, 
                    src.WSR_RSTATUS, 
                    src.WSR_STATUS, 
                    src.WSR_STATUS_MESSAGE, 
                    src.WSR_TIME, 
                    src.WSR_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5WSRSPHIST_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.WSR_ADDRESS, 
            strm.WSR_DATAAREA, 
            strm.WSR_LASTSAVED, 
            strm.WSR_MESSAGE, 
            strm.WSR_PROCESS, 
            strm.WSR_RSTATUS, 
            strm.WSR_STATUS, 
            strm.WSR_STATUS_MESSAGE, 
            strm.WSR_TIME, 
            strm.WSR_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WSR_MESSAGE,
            strm.WSR_PROCESS ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSRSPHIST as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WSR_MESSAGE=src.WSR_MESSAGE) OR (target.WSR_MESSAGE IS NULL AND src.WSR_MESSAGE IS NULL)) AND
            ((target.WSR_PROCESS=src.WSR_PROCESS) OR (target.WSR_PROCESS IS NULL AND src.WSR_PROCESS IS NULL)) 
                when matched then update set
                    target.WSR_ADDRESS=src.WSR_ADDRESS, 
                    target.WSR_DATAAREA=src.WSR_DATAAREA, 
                    target.WSR_LASTSAVED=src.WSR_LASTSAVED, 
                    target.WSR_MESSAGE=src.WSR_MESSAGE, 
                    target.WSR_PROCESS=src.WSR_PROCESS, 
                    target.WSR_RSTATUS=src.WSR_RSTATUS, 
                    target.WSR_STATUS=src.WSR_STATUS, 
                    target.WSR_STATUS_MESSAGE=src.WSR_STATUS_MESSAGE, 
                    target.WSR_TIME=src.WSR_TIME, 
                    target.WSR_UPDATECOUNT=src.WSR_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( WSR_ADDRESS, WSR_DATAAREA, WSR_LASTSAVED, WSR_MESSAGE, WSR_PROCESS, WSR_RSTATUS, WSR_STATUS, WSR_STATUS_MESSAGE, WSR_TIME, WSR_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.WSR_ADDRESS, 
                    src.WSR_DATAAREA, 
                    src.WSR_LASTSAVED, 
                    src.WSR_MESSAGE, 
                    src.WSR_PROCESS, 
                    src.WSR_RSTATUS, 
                    src.WSR_STATUS, 
                    src.WSR_STATUS_MESSAGE, 
                    src.WSR_TIME, 
                    src.WSR_UPDATECOUNT, 
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
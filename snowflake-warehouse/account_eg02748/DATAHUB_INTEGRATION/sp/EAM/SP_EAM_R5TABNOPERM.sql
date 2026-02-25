create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5TABNOPERM()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5TABNOPERM_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5TABNOPERM as target using (SELECT * FROM (SELECT 
            strm.TPN_FUNCTION, 
            strm.TPN_LASTSAVED, 
            strm.TPN_NODELETE, 
            strm.TPN_NOINSERT, 
            strm.TPN_NOSELECT, 
            strm.TPN_NOUPDATE, 
            strm.TPN_TAB, 
            strm.TPN_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TPN_FUNCTION,
            strm.TPN_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TABNOPERM as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TPN_FUNCTION=src.TPN_FUNCTION) OR (target.TPN_FUNCTION IS NULL AND src.TPN_FUNCTION IS NULL)) AND
            ((target.TPN_TAB=src.TPN_TAB) OR (target.TPN_TAB IS NULL AND src.TPN_TAB IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.TPN_FUNCTION=src.TPN_FUNCTION, 
                    target.TPN_LASTSAVED=src.TPN_LASTSAVED, 
                    target.TPN_NODELETE=src.TPN_NODELETE, 
                    target.TPN_NOINSERT=src.TPN_NOINSERT, 
                    target.TPN_NOSELECT=src.TPN_NOSELECT, 
                    target.TPN_NOUPDATE=src.TPN_NOUPDATE, 
                    target.TPN_TAB=src.TPN_TAB, 
                    target.TPN_UPDATECOUNT=src.TPN_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    TPN_FUNCTION, 
                    TPN_LASTSAVED, 
                    TPN_NODELETE, 
                    TPN_NOINSERT, 
                    TPN_NOSELECT, 
                    TPN_NOUPDATE, 
                    TPN_TAB, 
                    TPN_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.TPN_FUNCTION, 
                    src.TPN_LASTSAVED, 
                    src.TPN_NODELETE, 
                    src.TPN_NOINSERT, 
                    src.TPN_NOSELECT, 
                    src.TPN_NOUPDATE, 
                    src.TPN_TAB, 
                    src.TPN_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5TABNOPERM_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.TPN_FUNCTION, 
            strm.TPN_LASTSAVED, 
            strm.TPN_NODELETE, 
            strm.TPN_NOINSERT, 
            strm.TPN_NOSELECT, 
            strm.TPN_NOUPDATE, 
            strm.TPN_TAB, 
            strm.TPN_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TPN_FUNCTION,
            strm.TPN_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TABNOPERM as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TPN_FUNCTION=src.TPN_FUNCTION) OR (target.TPN_FUNCTION IS NULL AND src.TPN_FUNCTION IS NULL)) AND
            ((target.TPN_TAB=src.TPN_TAB) OR (target.TPN_TAB IS NULL AND src.TPN_TAB IS NULL)) 
                when matched then update set
                    target.TPN_FUNCTION=src.TPN_FUNCTION, 
                    target.TPN_LASTSAVED=src.TPN_LASTSAVED, 
                    target.TPN_NODELETE=src.TPN_NODELETE, 
                    target.TPN_NOINSERT=src.TPN_NOINSERT, 
                    target.TPN_NOSELECT=src.TPN_NOSELECT, 
                    target.TPN_NOUPDATE=src.TPN_NOUPDATE, 
                    target.TPN_TAB=src.TPN_TAB, 
                    target.TPN_UPDATECOUNT=src.TPN_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( TPN_FUNCTION, TPN_LASTSAVED, TPN_NODELETE, TPN_NOINSERT, TPN_NOSELECT, TPN_NOUPDATE, TPN_TAB, TPN_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.TPN_FUNCTION, 
                    src.TPN_LASTSAVED, 
                    src.TPN_NODELETE, 
                    src.TPN_NOINSERT, 
                    src.TPN_NOSELECT, 
                    src.TPN_NOUPDATE, 
                    src.TPN_TAB, 
                    src.TPN_UPDATECOUNT, 
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
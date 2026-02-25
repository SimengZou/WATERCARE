create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPOPTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPOPTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPOPTIONS as target using (SELECT * FROM (SELECT 
            strm.ROP_FUNCTION, 
            strm.ROP_LASTSAVED, 
            strm.ROP_MEKEY, 
            strm.ROP_PARAMETER, 
            strm.ROP_SEQNO, 
            strm.ROP_UPDATECOUNT, 
            strm.ROP_UPDATED, 
            strm.ROP_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROP_FUNCTION,
            strm.ROP_PARAMETER,
            strm.ROP_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPOPTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROP_FUNCTION=src.ROP_FUNCTION) OR (target.ROP_FUNCTION IS NULL AND src.ROP_FUNCTION IS NULL)) AND
            ((target.ROP_PARAMETER=src.ROP_PARAMETER) OR (target.ROP_PARAMETER IS NULL AND src.ROP_PARAMETER IS NULL)) AND
            ((target.ROP_SEQNO=src.ROP_SEQNO) OR (target.ROP_SEQNO IS NULL AND src.ROP_SEQNO IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ROP_FUNCTION=src.ROP_FUNCTION, 
                    target.ROP_LASTSAVED=src.ROP_LASTSAVED, 
                    target.ROP_MEKEY=src.ROP_MEKEY, 
                    target.ROP_PARAMETER=src.ROP_PARAMETER, 
                    target.ROP_SEQNO=src.ROP_SEQNO, 
                    target.ROP_UPDATECOUNT=src.ROP_UPDATECOUNT, 
                    target.ROP_UPDATED=src.ROP_UPDATED, 
                    target.ROP_VALUE=src.ROP_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ROP_FUNCTION, 
                    ROP_LASTSAVED, 
                    ROP_MEKEY, 
                    ROP_PARAMETER, 
                    ROP_SEQNO, 
                    ROP_UPDATECOUNT, 
                    ROP_UPDATED, 
                    ROP_VALUE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ROP_FUNCTION, 
                    src.ROP_LASTSAVED, 
                    src.ROP_MEKEY, 
                    src.ROP_PARAMETER, 
                    src.ROP_SEQNO, 
                    src.ROP_UPDATECOUNT, 
                    src.ROP_UPDATED, 
                    src.ROP_VALUE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPOPTIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ROP_FUNCTION, 
            strm.ROP_LASTSAVED, 
            strm.ROP_MEKEY, 
            strm.ROP_PARAMETER, 
            strm.ROP_SEQNO, 
            strm.ROP_UPDATECOUNT, 
            strm.ROP_UPDATED, 
            strm.ROP_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROP_FUNCTION,
            strm.ROP_PARAMETER,
            strm.ROP_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPOPTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROP_FUNCTION=src.ROP_FUNCTION) OR (target.ROP_FUNCTION IS NULL AND src.ROP_FUNCTION IS NULL)) AND
            ((target.ROP_PARAMETER=src.ROP_PARAMETER) OR (target.ROP_PARAMETER IS NULL AND src.ROP_PARAMETER IS NULL)) AND
            ((target.ROP_SEQNO=src.ROP_SEQNO) OR (target.ROP_SEQNO IS NULL AND src.ROP_SEQNO IS NULL)) 
                when matched then update set
                    target.ROP_FUNCTION=src.ROP_FUNCTION, 
                    target.ROP_LASTSAVED=src.ROP_LASTSAVED, 
                    target.ROP_MEKEY=src.ROP_MEKEY, 
                    target.ROP_PARAMETER=src.ROP_PARAMETER, 
                    target.ROP_SEQNO=src.ROP_SEQNO, 
                    target.ROP_UPDATECOUNT=src.ROP_UPDATECOUNT, 
                    target.ROP_UPDATED=src.ROP_UPDATED, 
                    target.ROP_VALUE=src.ROP_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ROP_FUNCTION, ROP_LASTSAVED, ROP_MEKEY, ROP_PARAMETER, ROP_SEQNO, ROP_UPDATECOUNT, ROP_UPDATED, ROP_VALUE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ROP_FUNCTION, 
                    src.ROP_LASTSAVED, 
                    src.ROP_MEKEY, 
                    src.ROP_PARAMETER, 
                    src.ROP_SEQNO, 
                    src.ROP_UPDATECOUNT, 
                    src.ROP_UPDATED, 
                    src.ROP_VALUE, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5FILTERTABCOLUMNS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5FILTERTABCOLUMNS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5FILTERTABCOLUMNS as target using (SELECT * FROM (SELECT 
            strm.FTC_CLOB, 
            strm.FTC_COLUMN, 
            strm.FTC_DATATYPE, 
            strm.FTC_LASTSAVED, 
            strm.FTC_TABLE, 
            strm.FTC_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FTC_TABLE,
            strm.FTC_COLUMN ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FILTERTABCOLUMNS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FTC_TABLE=src.FTC_TABLE) OR (target.FTC_TABLE IS NULL AND src.FTC_TABLE IS NULL)) AND
            ((target.FTC_COLUMN=src.FTC_COLUMN) OR (target.FTC_COLUMN IS NULL AND src.FTC_COLUMN IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.FTC_CLOB=src.FTC_CLOB, 
                    target.FTC_COLUMN=src.FTC_COLUMN, 
                    target.FTC_DATATYPE=src.FTC_DATATYPE, 
                    target.FTC_LASTSAVED=src.FTC_LASTSAVED, 
                    target.FTC_TABLE=src.FTC_TABLE, 
                    target.FTC_UPDATECOUNT=src.FTC_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    FTC_CLOB, 
                    FTC_COLUMN, 
                    FTC_DATATYPE, 
                    FTC_LASTSAVED, 
                    FTC_TABLE, 
                    FTC_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.FTC_CLOB, 
                    src.FTC_COLUMN, 
                    src.FTC_DATATYPE, 
                    src.FTC_LASTSAVED, 
                    src.FTC_TABLE, 
                    src.FTC_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5FILTERTABCOLUMNS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.FTC_CLOB, 
            strm.FTC_COLUMN, 
            strm.FTC_DATATYPE, 
            strm.FTC_LASTSAVED, 
            strm.FTC_TABLE, 
            strm.FTC_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FTC_TABLE,
            strm.FTC_COLUMN ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FILTERTABCOLUMNS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FTC_TABLE=src.FTC_TABLE) OR (target.FTC_TABLE IS NULL AND src.FTC_TABLE IS NULL)) AND
            ((target.FTC_COLUMN=src.FTC_COLUMN) OR (target.FTC_COLUMN IS NULL AND src.FTC_COLUMN IS NULL)) 
                when matched then update set
                    target.FTC_CLOB=src.FTC_CLOB, 
                    target.FTC_COLUMN=src.FTC_COLUMN, 
                    target.FTC_DATATYPE=src.FTC_DATATYPE, 
                    target.FTC_LASTSAVED=src.FTC_LASTSAVED, 
                    target.FTC_TABLE=src.FTC_TABLE, 
                    target.FTC_UPDATECOUNT=src.FTC_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( FTC_CLOB, FTC_COLUMN, FTC_DATATYPE, FTC_LASTSAVED, FTC_TABLE, FTC_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.FTC_CLOB, 
                    src.FTC_COLUMN, 
                    src.FTC_DATATYPE, 
                    src.FTC_LASTSAVED, 
                    src.FTC_TABLE, 
                    src.FTC_UPDATECOUNT, 
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
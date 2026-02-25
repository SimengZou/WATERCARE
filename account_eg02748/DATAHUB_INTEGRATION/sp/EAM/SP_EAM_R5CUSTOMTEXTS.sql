create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CUSTOMTEXTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CUSTOMTEXTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CUSTOMTEXTS as target using (SELECT * FROM (SELECT 
            strm.CTT_DATE, 
            strm.CTT_LANG, 
            strm.CTT_LASTSAVED, 
            strm.CTT_LENGTH, 
            strm.CTT_ORIGTEXT, 
            strm.CTT_POOL, 
            strm.CTT_TEXT, 
            strm.CTT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CTT_POOL,
            strm.CTT_LANG,
            strm.CTT_LENGTH ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CUSTOMTEXTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CTT_POOL=src.CTT_POOL) OR (target.CTT_POOL IS NULL AND src.CTT_POOL IS NULL)) AND
            ((target.CTT_LANG=src.CTT_LANG) OR (target.CTT_LANG IS NULL AND src.CTT_LANG IS NULL)) AND
            ((target.CTT_LENGTH=src.CTT_LENGTH) OR (target.CTT_LENGTH IS NULL AND src.CTT_LENGTH IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CTT_DATE=src.CTT_DATE, 
                    target.CTT_LANG=src.CTT_LANG, 
                    target.CTT_LASTSAVED=src.CTT_LASTSAVED, 
                    target.CTT_LENGTH=src.CTT_LENGTH, 
                    target.CTT_ORIGTEXT=src.CTT_ORIGTEXT, 
                    target.CTT_POOL=src.CTT_POOL, 
                    target.CTT_TEXT=src.CTT_TEXT, 
                    target.CTT_UPDATECOUNT=src.CTT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CTT_DATE, 
                    CTT_LANG, 
                    CTT_LASTSAVED, 
                    CTT_LENGTH, 
                    CTT_ORIGTEXT, 
                    CTT_POOL, 
                    CTT_TEXT, 
                    CTT_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CTT_DATE, 
                    src.CTT_LANG, 
                    src.CTT_LASTSAVED, 
                    src.CTT_LENGTH, 
                    src.CTT_ORIGTEXT, 
                    src.CTT_POOL, 
                    src.CTT_TEXT, 
                    src.CTT_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CUSTOMTEXTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CTT_DATE, 
            strm.CTT_LANG, 
            strm.CTT_LASTSAVED, 
            strm.CTT_LENGTH, 
            strm.CTT_ORIGTEXT, 
            strm.CTT_POOL, 
            strm.CTT_TEXT, 
            strm.CTT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CTT_POOL,
            strm.CTT_LANG,
            strm.CTT_LENGTH ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CUSTOMTEXTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CTT_POOL=src.CTT_POOL) OR (target.CTT_POOL IS NULL AND src.CTT_POOL IS NULL)) AND
            ((target.CTT_LANG=src.CTT_LANG) OR (target.CTT_LANG IS NULL AND src.CTT_LANG IS NULL)) AND
            ((target.CTT_LENGTH=src.CTT_LENGTH) OR (target.CTT_LENGTH IS NULL AND src.CTT_LENGTH IS NULL)) 
                when matched then update set
                    target.CTT_DATE=src.CTT_DATE, 
                    target.CTT_LANG=src.CTT_LANG, 
                    target.CTT_LASTSAVED=src.CTT_LASTSAVED, 
                    target.CTT_LENGTH=src.CTT_LENGTH, 
                    target.CTT_ORIGTEXT=src.CTT_ORIGTEXT, 
                    target.CTT_POOL=src.CTT_POOL, 
                    target.CTT_TEXT=src.CTT_TEXT, 
                    target.CTT_UPDATECOUNT=src.CTT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CTT_DATE, CTT_LANG, CTT_LASTSAVED, CTT_LENGTH, CTT_ORIGTEXT, CTT_POOL, CTT_TEXT, CTT_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CTT_DATE, 
                    src.CTT_LANG, 
                    src.CTT_LASTSAVED, 
                    src.CTT_LENGTH, 
                    src.CTT_ORIGTEXT, 
                    src.CTT_POOL, 
                    src.CTT_TEXT, 
                    src.CTT_UPDATECOUNT, 
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
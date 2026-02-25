create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ACDPARAMS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ACDPARAMS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ACDPARAMS as target using (SELECT * FROM (SELECT 
            strm.ADP_DATATYPE, 
            strm.ADP_DEFAULT, 
            strm.ADP_DESCSQL, 
            strm.ADP_HINT, 
            strm.ADP_LASTSAVED, 
            strm.ADP_LENGTH, 
            strm.ADP_LOVSQL, 
            strm.ADP_PROMPT, 
            strm.ADP_PROMPTX, 
            strm.ADP_PROMPTY, 
            strm.ADP_QUERY, 
            strm.ADP_REQUIRED, 
            strm.ADP_SEGMENT, 
            strm.ADP_SEGMENTX, 
            strm.ADP_SEGMENTY, 
            strm.ADP_SEQ, 
            strm.ADP_UPDATECOUNT, 
            strm.ADP_VALUELOOKUP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADP_SEGMENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACDPARAMS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADP_SEGMENT=src.ADP_SEGMENT) OR (target.ADP_SEGMENT IS NULL AND src.ADP_SEGMENT IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADP_DATATYPE=src.ADP_DATATYPE, 
                    target.ADP_DEFAULT=src.ADP_DEFAULT, 
                    target.ADP_DESCSQL=src.ADP_DESCSQL, 
                    target.ADP_HINT=src.ADP_HINT, 
                    target.ADP_LASTSAVED=src.ADP_LASTSAVED, 
                    target.ADP_LENGTH=src.ADP_LENGTH, 
                    target.ADP_LOVSQL=src.ADP_LOVSQL, 
                    target.ADP_PROMPT=src.ADP_PROMPT, 
                    target.ADP_PROMPTX=src.ADP_PROMPTX, 
                    target.ADP_PROMPTY=src.ADP_PROMPTY, 
                    target.ADP_QUERY=src.ADP_QUERY, 
                    target.ADP_REQUIRED=src.ADP_REQUIRED, 
                    target.ADP_SEGMENT=src.ADP_SEGMENT, 
                    target.ADP_SEGMENTX=src.ADP_SEGMENTX, 
                    target.ADP_SEGMENTY=src.ADP_SEGMENTY, 
                    target.ADP_SEQ=src.ADP_SEQ, 
                    target.ADP_UPDATECOUNT=src.ADP_UPDATECOUNT, 
                    target.ADP_VALUELOOKUP=src.ADP_VALUELOOKUP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADP_DATATYPE, 
                    ADP_DEFAULT, 
                    ADP_DESCSQL, 
                    ADP_HINT, 
                    ADP_LASTSAVED, 
                    ADP_LENGTH, 
                    ADP_LOVSQL, 
                    ADP_PROMPT, 
                    ADP_PROMPTX, 
                    ADP_PROMPTY, 
                    ADP_QUERY, 
                    ADP_REQUIRED, 
                    ADP_SEGMENT, 
                    ADP_SEGMENTX, 
                    ADP_SEGMENTY, 
                    ADP_SEQ, 
                    ADP_UPDATECOUNT, 
                    ADP_VALUELOOKUP, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADP_DATATYPE, 
                    src.ADP_DEFAULT, 
                    src.ADP_DESCSQL, 
                    src.ADP_HINT, 
                    src.ADP_LASTSAVED, 
                    src.ADP_LENGTH, 
                    src.ADP_LOVSQL, 
                    src.ADP_PROMPT, 
                    src.ADP_PROMPTX, 
                    src.ADP_PROMPTY, 
                    src.ADP_QUERY, 
                    src.ADP_REQUIRED, 
                    src.ADP_SEGMENT, 
                    src.ADP_SEGMENTX, 
                    src.ADP_SEGMENTY, 
                    src.ADP_SEQ, 
                    src.ADP_UPDATECOUNT, 
                    src.ADP_VALUELOOKUP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ACDPARAMS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ADP_DATATYPE, 
            strm.ADP_DEFAULT, 
            strm.ADP_DESCSQL, 
            strm.ADP_HINT, 
            strm.ADP_LASTSAVED, 
            strm.ADP_LENGTH, 
            strm.ADP_LOVSQL, 
            strm.ADP_PROMPT, 
            strm.ADP_PROMPTX, 
            strm.ADP_PROMPTY, 
            strm.ADP_QUERY, 
            strm.ADP_REQUIRED, 
            strm.ADP_SEGMENT, 
            strm.ADP_SEGMENTX, 
            strm.ADP_SEGMENTY, 
            strm.ADP_SEQ, 
            strm.ADP_UPDATECOUNT, 
            strm.ADP_VALUELOOKUP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADP_SEGMENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACDPARAMS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADP_SEGMENT=src.ADP_SEGMENT) OR (target.ADP_SEGMENT IS NULL AND src.ADP_SEGMENT IS NULL)) 
                when matched then update set
                    target.ADP_DATATYPE=src.ADP_DATATYPE, 
                    target.ADP_DEFAULT=src.ADP_DEFAULT, 
                    target.ADP_DESCSQL=src.ADP_DESCSQL, 
                    target.ADP_HINT=src.ADP_HINT, 
                    target.ADP_LASTSAVED=src.ADP_LASTSAVED, 
                    target.ADP_LENGTH=src.ADP_LENGTH, 
                    target.ADP_LOVSQL=src.ADP_LOVSQL, 
                    target.ADP_PROMPT=src.ADP_PROMPT, 
                    target.ADP_PROMPTX=src.ADP_PROMPTX, 
                    target.ADP_PROMPTY=src.ADP_PROMPTY, 
                    target.ADP_QUERY=src.ADP_QUERY, 
                    target.ADP_REQUIRED=src.ADP_REQUIRED, 
                    target.ADP_SEGMENT=src.ADP_SEGMENT, 
                    target.ADP_SEGMENTX=src.ADP_SEGMENTX, 
                    target.ADP_SEGMENTY=src.ADP_SEGMENTY, 
                    target.ADP_SEQ=src.ADP_SEQ, 
                    target.ADP_UPDATECOUNT=src.ADP_UPDATECOUNT, 
                    target.ADP_VALUELOOKUP=src.ADP_VALUELOOKUP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADP_DATATYPE, ADP_DEFAULT, ADP_DESCSQL, ADP_HINT, ADP_LASTSAVED, ADP_LENGTH, ADP_LOVSQL, ADP_PROMPT, ADP_PROMPTX, ADP_PROMPTY, ADP_QUERY, ADP_REQUIRED, ADP_SEGMENT, ADP_SEGMENTX, ADP_SEGMENTY, ADP_SEQ, ADP_UPDATECOUNT, ADP_VALUELOOKUP, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADP_DATATYPE, 
                    src.ADP_DEFAULT, 
                    src.ADP_DESCSQL, 
                    src.ADP_HINT, 
                    src.ADP_LASTSAVED, 
                    src.ADP_LENGTH, 
                    src.ADP_LOVSQL, 
                    src.ADP_PROMPT, 
                    src.ADP_PROMPTX, 
                    src.ADP_PROMPTY, 
                    src.ADP_QUERY, 
                    src.ADP_REQUIRED, 
                    src.ADP_SEGMENT, 
                    src.ADP_SEGMENTX, 
                    src.ADP_SEGMENTY, 
                    src.ADP_SEQ, 
                    src.ADP_UPDATECOUNT, 
                    src.ADP_VALUELOOKUP, 
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
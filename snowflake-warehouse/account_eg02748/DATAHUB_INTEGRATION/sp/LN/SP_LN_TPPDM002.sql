create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPDM002()
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
                            INSERT INTO LANDING_ERROR.LN_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPDM002_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPDM002 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.deleted, 
            strm.loco, 
            strm.maps, 
            strm.maps_kw, 
            strm.mpss, 
            strm.mpss_kw, 
            strm.mpvw, 
            strm.mpvw_kw, 
            strm.sequencenumber, 
            strm.sses, 
            strm.sses_kw, 
            strm.ssvw, 
            strm.ssvw_kw, 
            strm.timestamp, 
            strm.tprf, 
            strm.tprf_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.loco,
            strm.maps ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM002 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.maps=src.maps) OR (target.maps IS NULL AND src.maps IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.loco=src.loco, 
                    target.maps=src.maps, 
                    target.maps_kw=src.maps_kw, 
                    target.mpss=src.mpss, 
                    target.mpss_kw=src.mpss_kw, 
                    target.mpvw=src.mpvw, 
                    target.mpvw_kw=src.mpvw_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sses=src.sses, 
                    target.sses_kw=src.sses_kw, 
                    target.ssvw=src.ssvw, 
                    target.ssvw_kw=src.ssvw_kw, 
                    target.timestamp=src.timestamp, 
                    target.tprf=src.tprf, 
                    target.tprf_kw=src.tprf_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    deleted, 
                    loco, 
                    maps, 
                    maps_kw, 
                    mpss, 
                    mpss_kw, 
                    mpvw, 
                    mpvw_kw, 
                    sequencenumber, 
                    sses, 
                    sses_kw, 
                    ssvw, 
                    ssvw_kw, 
                    timestamp, 
                    tprf, 
                    tprf_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.deleted, 
                    src.loco, 
                    src.maps, 
                    src.maps_kw, 
                    src.mpss, 
                    src.mpss_kw, 
                    src.mpvw, 
                    src.mpvw_kw, 
                    src.sequencenumber, 
                    src.sses, 
                    src.sses_kw, 
                    src.ssvw, 
                    src.ssvw_kw, 
                    src.timestamp, 
                    src.tprf, 
                    src.tprf_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPDM002_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.deleted, 
            strm.loco, 
            strm.maps, 
            strm.maps_kw, 
            strm.mpss, 
            strm.mpss_kw, 
            strm.mpvw, 
            strm.mpvw_kw, 
            strm.sequencenumber, 
            strm.sses, 
            strm.sses_kw, 
            strm.ssvw, 
            strm.ssvw_kw, 
            strm.timestamp, 
            strm.tprf, 
            strm.tprf_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.loco,
            strm.maps ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM002 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.maps=src.maps) OR (target.maps IS NULL AND src.maps IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.loco=src.loco, 
                    target.maps=src.maps, 
                    target.maps_kw=src.maps_kw, 
                    target.mpss=src.mpss, 
                    target.mpss_kw=src.mpss_kw, 
                    target.mpvw=src.mpvw, 
                    target.mpvw_kw=src.mpvw_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sses=src.sses, 
                    target.sses_kw=src.sses_kw, 
                    target.ssvw=src.ssvw, 
                    target.ssvw_kw=src.ssvw_kw, 
                    target.timestamp=src.timestamp, 
                    target.tprf=src.tprf, 
                    target.tprf_kw=src.tprf_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, deleted, loco, maps, maps_kw, mpss, mpss_kw, mpvw, mpvw_kw, sequencenumber, sses, sses_kw, ssvw, ssvw_kw, timestamp, tprf, tprf_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.deleted, 
                    src.loco, 
                    src.maps, 
                    src.maps_kw, 
                    src.mpss, 
                    src.mpss_kw, 
                    src.mpvw, 
                    src.mpvw_kw, 
                    src.sequencenumber, 
                    src.sses, 
                    src.sses_kw, 
                    src.ssvw, 
                    src.ssvw_kw, 
                    src.timestamp, 
                    src.tprf, 
                    src.tprf_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
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
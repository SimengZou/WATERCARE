create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM120()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM120_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM120 as target using (SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.anbr_aext_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.deleted, 
            strm.dkey, 
            strm.dqty, 
            strm.lkey, 
            strm.memo, 
            strm.sequencenumber, 
            strm.srce, 
            strm.srce_kw, 
            strm.timestamp, 
            strm.trsc, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.dkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM120 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.dkey=src.dkey) OR (target.dkey IS NULL AND src.dkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.anbr_aext_ref_compnr=src.anbr_aext_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dkey=src.dkey, 
                    target.dqty=src.dqty, 
                    target.lkey=src.lkey, 
                    target.memo=src.memo, 
                    target.sequencenumber=src.sequencenumber, 
                    target.srce=src.srce, 
                    target.srce_kw=src.srce_kw, 
                    target.timestamp=src.timestamp, 
                    target.trsc=src.trsc, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aext, 
                    anbr, 
                    anbr_aext_ref_compnr, 
                    comp, 
                    compnr, 
                    deleted, 
                    dkey, 
                    dqty, 
                    lkey, 
                    memo, 
                    sequencenumber, 
                    srce, 
                    srce_kw, 
                    timestamp, 
                    trsc, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.anbr_aext_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.deleted, 
                    src.dkey, 
                    src.dqty, 
                    src.lkey, 
                    src.memo, 
                    src.sequencenumber, 
                    src.srce, 
                    src.srce_kw, 
                    src.timestamp, 
                    src.trsc, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM120_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.anbr_aext_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.deleted, 
            strm.dkey, 
            strm.dqty, 
            strm.lkey, 
            strm.memo, 
            strm.sequencenumber, 
            strm.srce, 
            strm.srce_kw, 
            strm.timestamp, 
            strm.trsc, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.dkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM120 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.dkey=src.dkey) OR (target.dkey IS NULL AND src.dkey IS NULL)) 
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.anbr_aext_ref_compnr=src.anbr_aext_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dkey=src.dkey, 
                    target.dqty=src.dqty, 
                    target.lkey=src.lkey, 
                    target.memo=src.memo, 
                    target.sequencenumber=src.sequencenumber, 
                    target.srce=src.srce, 
                    target.srce_kw=src.srce_kw, 
                    target.timestamp=src.timestamp, 
                    target.trsc=src.trsc, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aext, anbr, anbr_aext_ref_compnr, comp, compnr, deleted, dkey, dqty, lkey, memo, sequencenumber, srce, srce_kw, timestamp, trsc, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.anbr_aext_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.deleted, 
                    src.dkey, 
                    src.dqty, 
                    src.lkey, 
                    src.memo, 
                    src.sequencenumber, 
                    src.srce, 
                    src.srce_kw, 
                    src.timestamp, 
                    src.trsc, 
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
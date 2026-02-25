create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM111()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM111_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM111 as target using (SELECT * FROM (SELECT 
            strm.cact, 
            strm.cact_ref_compnr, 
            strm.camr, 
            strm.camr_ref_compnr, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_homc, 
            strm.camt_refc, 
            strm.camt_rpc1, 
            strm.camt_rpc2, 
            strm.caro, 
            strm.caro_ref_compnr, 
            strm.cfln, 
            strm.compnr, 
            strm.cseq, 
            strm.deleted, 
            strm.fplv, 
            strm.fplv_kw, 
            strm.pris, 
            strm.pris_dwhc, 
            strm.pris_homc, 
            strm.pris_refc, 
            strm.pris_rpc1, 
            strm.pris_rpc2, 
            strm.sequencenumber, 
            strm.term, 
            strm.term_cfln_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.term,
            strm.cfln,
            strm.cseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM111 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.term=src.term) OR (target.term IS NULL AND src.term IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.cseq=src.cseq) OR (target.cseq IS NULL AND src.cseq IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cact=src.cact, 
                    target.cact_ref_compnr=src.cact_ref_compnr, 
                    target.camr=src.camr, 
                    target.camr_ref_compnr=src.camr_ref_compnr, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_homc=src.camt_homc, 
                    target.camt_refc=src.camt_refc, 
                    target.camt_rpc1=src.camt_rpc1, 
                    target.camt_rpc2=src.camt_rpc2, 
                    target.caro=src.caro, 
                    target.caro_ref_compnr=src.caro_ref_compnr, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.cseq=src.cseq, 
                    target.deleted=src.deleted, 
                    target.fplv=src.fplv, 
                    target.fplv_kw=src.fplv_kw, 
                    target.pris=src.pris, 
                    target.pris_dwhc=src.pris_dwhc, 
                    target.pris_homc=src.pris_homc, 
                    target.pris_refc=src.pris_refc, 
                    target.pris_rpc1=src.pris_rpc1, 
                    target.pris_rpc2=src.pris_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.term=src.term, 
                    target.term_cfln_ref_compnr=src.term_cfln_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cact, 
                    cact_ref_compnr, 
                    camr, 
                    camr_ref_compnr, 
                    camt_cntc, 
                    camt_dwhc, 
                    camt_homc, 
                    camt_refc, 
                    camt_rpc1, 
                    camt_rpc2, 
                    caro, 
                    caro_ref_compnr, 
                    cfln, 
                    compnr, 
                    cseq, 
                    deleted, 
                    fplv, 
                    fplv_kw, 
                    pris, 
                    pris_dwhc, 
                    pris_homc, 
                    pris_refc, 
                    pris_rpc1, 
                    pris_rpc2, 
                    sequencenumber, 
                    term, 
                    term_cfln_ref_compnr, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cact, 
                    src.cact_ref_compnr, 
                    src.camr, 
                    src.camr_ref_compnr, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_homc, 
                    src.camt_refc, 
                    src.camt_rpc1, 
                    src.camt_rpc2, 
                    src.caro, 
                    src.caro_ref_compnr, 
                    src.cfln, 
                    src.compnr, 
                    src.cseq, 
                    src.deleted, 
                    src.fplv, 
                    src.fplv_kw, 
                    src.pris, 
                    src.pris_dwhc, 
                    src.pris_homc, 
                    src.pris_refc, 
                    src.pris_rpc1, 
                    src.pris_rpc2, 
                    src.sequencenumber, 
                    src.term, 
                    src.term_cfln_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM111_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cact, 
            strm.cact_ref_compnr, 
            strm.camr, 
            strm.camr_ref_compnr, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_homc, 
            strm.camt_refc, 
            strm.camt_rpc1, 
            strm.camt_rpc2, 
            strm.caro, 
            strm.caro_ref_compnr, 
            strm.cfln, 
            strm.compnr, 
            strm.cseq, 
            strm.deleted, 
            strm.fplv, 
            strm.fplv_kw, 
            strm.pris, 
            strm.pris_dwhc, 
            strm.pris_homc, 
            strm.pris_refc, 
            strm.pris_rpc1, 
            strm.pris_rpc2, 
            strm.sequencenumber, 
            strm.term, 
            strm.term_cfln_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.term,
            strm.cfln,
            strm.cseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM111 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.term=src.term) OR (target.term IS NULL AND src.term IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.cseq=src.cseq) OR (target.cseq IS NULL AND src.cseq IS NULL)) 
                when matched then update set
                    target.cact=src.cact, 
                    target.cact_ref_compnr=src.cact_ref_compnr, 
                    target.camr=src.camr, 
                    target.camr_ref_compnr=src.camr_ref_compnr, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_homc=src.camt_homc, 
                    target.camt_refc=src.camt_refc, 
                    target.camt_rpc1=src.camt_rpc1, 
                    target.camt_rpc2=src.camt_rpc2, 
                    target.caro=src.caro, 
                    target.caro_ref_compnr=src.caro_ref_compnr, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.cseq=src.cseq, 
                    target.deleted=src.deleted, 
                    target.fplv=src.fplv, 
                    target.fplv_kw=src.fplv_kw, 
                    target.pris=src.pris, 
                    target.pris_dwhc=src.pris_dwhc, 
                    target.pris_homc=src.pris_homc, 
                    target.pris_refc=src.pris_refc, 
                    target.pris_rpc1=src.pris_rpc1, 
                    target.pris_rpc2=src.pris_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.term=src.term, 
                    target.term_cfln_ref_compnr=src.term_cfln_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cact, cact_ref_compnr, camr, camr_ref_compnr, camt_cntc, camt_dwhc, camt_homc, camt_refc, camt_rpc1, camt_rpc2, caro, caro_ref_compnr, cfln, compnr, cseq, deleted, fplv, fplv_kw, pris, pris_dwhc, pris_homc, pris_refc, pris_rpc1, pris_rpc2, sequencenumber, term, term_cfln_ref_compnr, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cact, 
                    src.cact_ref_compnr, 
                    src.camr, 
                    src.camr_ref_compnr, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_homc, 
                    src.camt_refc, 
                    src.camt_rpc1, 
                    src.camt_rpc2, 
                    src.caro, 
                    src.caro_ref_compnr, 
                    src.cfln, 
                    src.compnr, 
                    src.cseq, 
                    src.deleted, 
                    src.fplv, 
                    src.fplv_kw, 
                    src.pris, 
                    src.pris_dwhc, 
                    src.pris_homc, 
                    src.pris_refc, 
                    src.pris_rpc1, 
                    src.pris_rpc2, 
                    src.sequencenumber, 
                    src.term, 
                    src.term_cfln_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
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
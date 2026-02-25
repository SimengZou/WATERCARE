create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM139()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM139_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM139 as target using (SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amnt_dwhc, 
            strm.amnt_homc, 
            strm.amnt_refc, 
            strm.amnt_rpc1, 
            strm.amnt_rpc2, 
            strm.avtm, 
            strm.avtm_butm, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_refc, 
            strm.cctp, 
            strm.cctp_ref_compnr, 
            strm.cfln, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.crmt, 
            strm.cseq, 
            strm.cstm, 
            strm.cstm_butm, 
            strm.deleted, 
            strm.nrbt, 
            strm.proc, 
            strm.proc_kw, 
            strm.sequencenumber, 
            strm.sptm, 
            strm.sptm_butm, 
            strm.term, 
            strm.term_cfln_cctp_cotp_nrbt_ref_compnr, 
            strm.term_cfln_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.utpc, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.term,
            strm.cfln,
            strm.cctp,
            strm.cotp,
            strm.nrbt,
            strm.cseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM139 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.term=src.term) OR (target.term IS NULL AND src.term IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.cctp=src.cctp) OR (target.cctp IS NULL AND src.cctp IS NULL)) AND
            ((target.cotp=src.cotp) OR (target.cotp IS NULL AND src.cotp IS NULL)) AND
            ((target.nrbt=src.nrbt) OR (target.nrbt IS NULL AND src.nrbt IS NULL)) AND
            ((target.cseq=src.cseq) OR (target.cseq IS NULL AND src.cseq IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amnt_dwhc=src.amnt_dwhc, 
                    target.amnt_homc=src.amnt_homc, 
                    target.amnt_refc=src.amnt_refc, 
                    target.amnt_rpc1=src.amnt_rpc1, 
                    target.amnt_rpc2=src.amnt_rpc2, 
                    target.avtm=src.avtm, 
                    target.avtm_butm=src.avtm_butm, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_refc=src.camt_refc, 
                    target.cctp=src.cctp, 
                    target.cctp_ref_compnr=src.cctp_ref_compnr, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.crmt=src.crmt, 
                    target.cseq=src.cseq, 
                    target.cstm=src.cstm, 
                    target.cstm_butm=src.cstm_butm, 
                    target.deleted=src.deleted, 
                    target.nrbt=src.nrbt, 
                    target.proc=src.proc, 
                    target.proc_kw=src.proc_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sptm=src.sptm, 
                    target.sptm_butm=src.sptm_butm, 
                    target.term=src.term, 
                    target.term_cfln_cctp_cotp_nrbt_ref_compnr=src.term_cfln_cctp_cotp_nrbt_ref_compnr, 
                    target.term_cfln_ref_compnr=src.term_cfln_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.utpc=src.utpc, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amnt, 
                    amnt_dwhc, 
                    amnt_homc, 
                    amnt_refc, 
                    amnt_rpc1, 
                    amnt_rpc2, 
                    avtm, 
                    avtm_butm, 
                    camt_1, 
                    camt_2, 
                    camt_3, 
                    camt_cntc, 
                    camt_dwhc, 
                    camt_refc, 
                    cctp, 
                    cctp_ref_compnr, 
                    cfln, 
                    compnr, 
                    cotp, 
                    cotp_kw, 
                    crmt, 
                    cseq, 
                    cstm, 
                    cstm_butm, 
                    deleted, 
                    nrbt, 
                    proc, 
                    proc_kw, 
                    sequencenumber, 
                    sptm, 
                    sptm_butm, 
                    term, 
                    term_cfln_cctp_cotp_nrbt_ref_compnr, 
                    term_cfln_ref_compnr, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    utpc, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amnt, 
                    src.amnt_dwhc, 
                    src.amnt_homc, 
                    src.amnt_refc, 
                    src.amnt_rpc1, 
                    src.amnt_rpc2, 
                    src.avtm, 
                    src.avtm_butm, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_refc, 
                    src.cctp, 
                    src.cctp_ref_compnr, 
                    src.cfln, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.crmt, 
                    src.cseq, 
                    src.cstm, 
                    src.cstm_butm, 
                    src.deleted, 
                    src.nrbt, 
                    src.proc, 
                    src.proc_kw, 
                    src.sequencenumber, 
                    src.sptm, 
                    src.sptm_butm, 
                    src.term, 
                    src.term_cfln_cctp_cotp_nrbt_ref_compnr, 
                    src.term_cfln_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.utpc,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM139_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amnt_dwhc, 
            strm.amnt_homc, 
            strm.amnt_refc, 
            strm.amnt_rpc1, 
            strm.amnt_rpc2, 
            strm.avtm, 
            strm.avtm_butm, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_refc, 
            strm.cctp, 
            strm.cctp_ref_compnr, 
            strm.cfln, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.crmt, 
            strm.cseq, 
            strm.cstm, 
            strm.cstm_butm, 
            strm.deleted, 
            strm.nrbt, 
            strm.proc, 
            strm.proc_kw, 
            strm.sequencenumber, 
            strm.sptm, 
            strm.sptm_butm, 
            strm.term, 
            strm.term_cfln_cctp_cotp_nrbt_ref_compnr, 
            strm.term_cfln_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.utpc, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.term,
            strm.cfln,
            strm.cctp,
            strm.cotp,
            strm.nrbt,
            strm.cseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM139 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.term=src.term) OR (target.term IS NULL AND src.term IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.cctp=src.cctp) OR (target.cctp IS NULL AND src.cctp IS NULL)) AND
            ((target.cotp=src.cotp) OR (target.cotp IS NULL AND src.cotp IS NULL)) AND
            ((target.nrbt=src.nrbt) OR (target.nrbt IS NULL AND src.nrbt IS NULL)) AND
            ((target.cseq=src.cseq) OR (target.cseq IS NULL AND src.cseq IS NULL)) 
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amnt_dwhc=src.amnt_dwhc, 
                    target.amnt_homc=src.amnt_homc, 
                    target.amnt_refc=src.amnt_refc, 
                    target.amnt_rpc1=src.amnt_rpc1, 
                    target.amnt_rpc2=src.amnt_rpc2, 
                    target.avtm=src.avtm, 
                    target.avtm_butm=src.avtm_butm, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_refc=src.camt_refc, 
                    target.cctp=src.cctp, 
                    target.cctp_ref_compnr=src.cctp_ref_compnr, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.crmt=src.crmt, 
                    target.cseq=src.cseq, 
                    target.cstm=src.cstm, 
                    target.cstm_butm=src.cstm_butm, 
                    target.deleted=src.deleted, 
                    target.nrbt=src.nrbt, 
                    target.proc=src.proc, 
                    target.proc_kw=src.proc_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sptm=src.sptm, 
                    target.sptm_butm=src.sptm_butm, 
                    target.term=src.term, 
                    target.term_cfln_cctp_cotp_nrbt_ref_compnr=src.term_cfln_cctp_cotp_nrbt_ref_compnr, 
                    target.term_cfln_ref_compnr=src.term_cfln_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.utpc=src.utpc, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amnt, amnt_dwhc, amnt_homc, amnt_refc, amnt_rpc1, amnt_rpc2, avtm, avtm_butm, camt_1, camt_2, camt_3, camt_cntc, camt_dwhc, camt_refc, cctp, cctp_ref_compnr, cfln, compnr, cotp, cotp_kw, crmt, cseq, cstm, cstm_butm, deleted, nrbt, proc, proc_kw, sequencenumber, sptm, sptm_butm, term, term_cfln_cctp_cotp_nrbt_ref_compnr, term_cfln_ref_compnr, timestamp, txta, txta_ref_compnr, username, utpc, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amnt, 
                    src.amnt_dwhc, 
                    src.amnt_homc, 
                    src.amnt_refc, 
                    src.amnt_rpc1, 
                    src.amnt_rpc2, 
                    src.avtm, 
                    src.avtm_butm, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_refc, 
                    src.cctp, 
                    src.cctp_ref_compnr, 
                    src.cfln, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.crmt, 
                    src.cseq, 
                    src.cstm, 
                    src.cstm_butm, 
                    src.deleted, 
                    src.nrbt, 
                    src.proc, 
                    src.proc_kw, 
                    src.sequencenumber, 
                    src.sptm, 
                    src.sptm_butm, 
                    src.term, 
                    src.term_cfln_cctp_cotp_nrbt_ref_compnr, 
                    src.term_cfln_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.utpc,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM501()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM501_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM501 as target using (SELECT * FROM (SELECT 
            strm.cctp, 
            strm.cctp_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.clst, 
            strm.clst_ref_compnr, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.cstp, 
            strm.cstp_ref_compnr, 
            strm.cvln, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.item_sern_ref_compnr, 
            strm.litm, 
            strm.litm_lser_ref_compnr, 
            strm.litm_ref_compnr, 
            strm.lser, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.orno, 
            strm.ortp, 
            strm.ortp_kw, 
            strm.pono, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.sern, 
            strm.spco_1, 
            strm.spco_2, 
            strm.spco_3, 
            strm.spco_dwhc, 
            strm.spco_refc, 
            strm.spco_trnc, 
            strm.spsa, 
            strm.spsa_dwhc, 
            strm.spsa_homc, 
            strm.spsa_refc, 
            strm.spsa_rpc1, 
            strm.spsa_rpc2, 
            strm.tcst, 
            strm.tcst_kw, 
            strm.timestamp, 
            strm.trtm, 
            strm.username, 
            strm.wrtp, 
            strm.wrtp_kw, 
            strm.wrty, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.wrty,
            strm.item,
            strm.sern,
            strm.trtm,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM501 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.wrty=src.wrty) OR (target.wrty IS NULL AND src.wrty IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.trtm=src.trtm) OR (target.trtm IS NULL AND src.trtm IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cctp=src.cctp, 
                    target.cctp_ref_compnr=src.cctp_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.clst=src.clst, 
                    target.clst_ref_compnr=src.clst_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.cstp=src.cstp, 
                    target.cstp_ref_compnr=src.cstp_ref_compnr, 
                    target.cvln=src.cvln, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.item_sern_ref_compnr=src.item_sern_ref_compnr, 
                    target.litm=src.litm, 
                    target.litm_lser_ref_compnr=src.litm_lser_ref_compnr, 
                    target.litm_ref_compnr=src.litm_ref_compnr, 
                    target.lser=src.lser, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.orno=src.orno, 
                    target.ortp=src.ortp, 
                    target.ortp_kw=src.ortp_kw, 
                    target.pono=src.pono, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.spco_1=src.spco_1, 
                    target.spco_2=src.spco_2, 
                    target.spco_3=src.spco_3, 
                    target.spco_dwhc=src.spco_dwhc, 
                    target.spco_refc=src.spco_refc, 
                    target.spco_trnc=src.spco_trnc, 
                    target.spsa=src.spsa, 
                    target.spsa_dwhc=src.spsa_dwhc, 
                    target.spsa_homc=src.spsa_homc, 
                    target.spsa_refc=src.spsa_refc, 
                    target.spsa_rpc1=src.spsa_rpc1, 
                    target.spsa_rpc2=src.spsa_rpc2, 
                    target.tcst=src.tcst, 
                    target.tcst_kw=src.tcst_kw, 
                    target.timestamp=src.timestamp, 
                    target.trtm=src.trtm, 
                    target.username=src.username, 
                    target.wrtp=src.wrtp, 
                    target.wrtp_kw=src.wrtp_kw, 
                    target.wrty=src.wrty, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cctp, 
                    cctp_ref_compnr, 
                    ccur, 
                    ccur_ref_compnr, 
                    clst, 
                    clst_ref_compnr, 
                    compnr, 
                    cotp, 
                    cotp_kw, 
                    cstp, 
                    cstp_ref_compnr, 
                    cvln, 
                    cwoc, 
                    cwoc_ref_compnr, 
                    deleted, 
                    item, 
                    item_ref_compnr, 
                    item_sern_ref_compnr, 
                    litm, 
                    litm_lser_ref_compnr, 
                    litm_ref_compnr, 
                    lser, 
                    ofbp, 
                    ofbp_ref_compnr, 
                    orno, 
                    ortp, 
                    ortp_kw, 
                    pono, 
                    seqn, 
                    sequencenumber, 
                    sern, 
                    spco_1, 
                    spco_2, 
                    spco_3, 
                    spco_dwhc, 
                    spco_refc, 
                    spco_trnc, 
                    spsa, 
                    spsa_dwhc, 
                    spsa_homc, 
                    spsa_refc, 
                    spsa_rpc1, 
                    spsa_rpc2, 
                    tcst, 
                    tcst_kw, 
                    timestamp, 
                    trtm, 
                    username, 
                    wrtp, 
                    wrtp_kw, 
                    wrty, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cctp, 
                    src.cctp_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.clst, 
                    src.clst_ref_compnr, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.cstp, 
                    src.cstp_ref_compnr, 
                    src.cvln, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.item_sern_ref_compnr, 
                    src.litm, 
                    src.litm_lser_ref_compnr, 
                    src.litm_ref_compnr, 
                    src.lser, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.orno, 
                    src.ortp, 
                    src.ortp_kw, 
                    src.pono, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.sern, 
                    src.spco_1, 
                    src.spco_2, 
                    src.spco_3, 
                    src.spco_dwhc, 
                    src.spco_refc, 
                    src.spco_trnc, 
                    src.spsa, 
                    src.spsa_dwhc, 
                    src.spsa_homc, 
                    src.spsa_refc, 
                    src.spsa_rpc1, 
                    src.spsa_rpc2, 
                    src.tcst, 
                    src.tcst_kw, 
                    src.timestamp, 
                    src.trtm, 
                    src.username, 
                    src.wrtp, 
                    src.wrtp_kw, 
                    src.wrty,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM501_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cctp, 
            strm.cctp_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.clst, 
            strm.clst_ref_compnr, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.cstp, 
            strm.cstp_ref_compnr, 
            strm.cvln, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.item_sern_ref_compnr, 
            strm.litm, 
            strm.litm_lser_ref_compnr, 
            strm.litm_ref_compnr, 
            strm.lser, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.orno, 
            strm.ortp, 
            strm.ortp_kw, 
            strm.pono, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.sern, 
            strm.spco_1, 
            strm.spco_2, 
            strm.spco_3, 
            strm.spco_dwhc, 
            strm.spco_refc, 
            strm.spco_trnc, 
            strm.spsa, 
            strm.spsa_dwhc, 
            strm.spsa_homc, 
            strm.spsa_refc, 
            strm.spsa_rpc1, 
            strm.spsa_rpc2, 
            strm.tcst, 
            strm.tcst_kw, 
            strm.timestamp, 
            strm.trtm, 
            strm.username, 
            strm.wrtp, 
            strm.wrtp_kw, 
            strm.wrty, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.wrty,
            strm.item,
            strm.sern,
            strm.trtm,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM501 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.wrty=src.wrty) OR (target.wrty IS NULL AND src.wrty IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.trtm=src.trtm) OR (target.trtm IS NULL AND src.trtm IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.cctp=src.cctp, 
                    target.cctp_ref_compnr=src.cctp_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.clst=src.clst, 
                    target.clst_ref_compnr=src.clst_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.cstp=src.cstp, 
                    target.cstp_ref_compnr=src.cstp_ref_compnr, 
                    target.cvln=src.cvln, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.item_sern_ref_compnr=src.item_sern_ref_compnr, 
                    target.litm=src.litm, 
                    target.litm_lser_ref_compnr=src.litm_lser_ref_compnr, 
                    target.litm_ref_compnr=src.litm_ref_compnr, 
                    target.lser=src.lser, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.orno=src.orno, 
                    target.ortp=src.ortp, 
                    target.ortp_kw=src.ortp_kw, 
                    target.pono=src.pono, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.spco_1=src.spco_1, 
                    target.spco_2=src.spco_2, 
                    target.spco_3=src.spco_3, 
                    target.spco_dwhc=src.spco_dwhc, 
                    target.spco_refc=src.spco_refc, 
                    target.spco_trnc=src.spco_trnc, 
                    target.spsa=src.spsa, 
                    target.spsa_dwhc=src.spsa_dwhc, 
                    target.spsa_homc=src.spsa_homc, 
                    target.spsa_refc=src.spsa_refc, 
                    target.spsa_rpc1=src.spsa_rpc1, 
                    target.spsa_rpc2=src.spsa_rpc2, 
                    target.tcst=src.tcst, 
                    target.tcst_kw=src.tcst_kw, 
                    target.timestamp=src.timestamp, 
                    target.trtm=src.trtm, 
                    target.username=src.username, 
                    target.wrtp=src.wrtp, 
                    target.wrtp_kw=src.wrtp_kw, 
                    target.wrty=src.wrty, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cctp, cctp_ref_compnr, ccur, ccur_ref_compnr, clst, clst_ref_compnr, compnr, cotp, cotp_kw, cstp, cstp_ref_compnr, cvln, cwoc, cwoc_ref_compnr, deleted, item, item_ref_compnr, item_sern_ref_compnr, litm, litm_lser_ref_compnr, litm_ref_compnr, lser, ofbp, ofbp_ref_compnr, orno, ortp, ortp_kw, pono, seqn, sequencenumber, sern, spco_1, spco_2, spco_3, spco_dwhc, spco_refc, spco_trnc, spsa, spsa_dwhc, spsa_homc, spsa_refc, spsa_rpc1, spsa_rpc2, tcst, tcst_kw, timestamp, trtm, username, wrtp, wrtp_kw, wrty, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cctp, 
                    src.cctp_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.clst, 
                    src.clst_ref_compnr, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.cstp, 
                    src.cstp_ref_compnr, 
                    src.cvln, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.item_sern_ref_compnr, 
                    src.litm, 
                    src.litm_lser_ref_compnr, 
                    src.litm_ref_compnr, 
                    src.lser, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.orno, 
                    src.ortp, 
                    src.ortp_kw, 
                    src.pono, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.sern, 
                    src.spco_1, 
                    src.spco_2, 
                    src.spco_3, 
                    src.spco_dwhc, 
                    src.spco_refc, 
                    src.spco_trnc, 
                    src.spsa, 
                    src.spsa_dwhc, 
                    src.spsa_homc, 
                    src.spsa_refc, 
                    src.spsa_rpc1, 
                    src.spsa_rpc2, 
                    src.tcst, 
                    src.tcst_kw, 
                    src.timestamp, 
                    src.trtm, 
                    src.username, 
                    src.wrtp, 
                    src.wrtp_kw, 
                    src.wrty,     
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
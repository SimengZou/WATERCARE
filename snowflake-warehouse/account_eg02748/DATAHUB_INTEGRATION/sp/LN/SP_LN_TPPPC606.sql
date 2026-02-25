create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPPC606()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPPC606_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPPC606 as target using (SELECT * FROM (SELECT 
            strm.amoc, 
            strm.amoc_cntc, 
            strm.amoc_dwhc, 
            strm.amoc_homc, 
            strm.amoc_prjc, 
            strm.amoc_refc, 
            strm.amoc_rpc1, 
            strm.amoc_rpc2, 
            strm.appr, 
            strm.appr_kw, 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_fcmp, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.deleted, 
            strm.eatc_cntc, 
            strm.eatc_dwhc, 
            strm.eatc_homc, 
            strm.eatc_prjc, 
            strm.eatc_refc, 
            strm.eatc_rpc1, 
            strm.eatc_rpc2, 
            strm.eatc_trnc, 
            strm.etoc_cntc, 
            strm.etoc_dwhc, 
            strm.etoc_homc, 
            strm.etoc_prjc, 
            strm.etoc_refc, 
            strm.etoc_rpc1, 
            strm.etoc_rpc2, 
            strm.etoc_trnc, 
            strm.ovhd, 
            strm.ovhd_ref_compnr, 
            strm.rdat, 
            strm.rgdt, 
            strm.rtcc_1, 
            strm.rtcc_2, 
            strm.rtcc_3, 
            strm.rtfc_1, 
            strm.rtfc_2, 
            strm.rtfc_3, 
            strm.sequencenumber, 
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
            strm.cprj,
            strm.rgdt,
            strm.ovhd,
            strm.cspa,
            strm.cact ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC606 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.rgdt=src.rgdt) OR (target.rgdt IS NULL AND src.rgdt IS NULL)) AND
            ((target.ovhd=src.ovhd) OR (target.ovhd IS NULL AND src.ovhd IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amoc=src.amoc, 
                    target.amoc_cntc=src.amoc_cntc, 
                    target.amoc_dwhc=src.amoc_dwhc, 
                    target.amoc_homc=src.amoc_homc, 
                    target.amoc_prjc=src.amoc_prjc, 
                    target.amoc_refc=src.amoc_refc, 
                    target.amoc_rpc1=src.amoc_rpc1, 
                    target.amoc_rpc2=src.amoc_rpc2, 
                    target.appr=src.appr, 
                    target.appr_kw=src.appr_kw, 
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_fcmp=src.cprj_fcmp, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.deleted=src.deleted, 
                    target.eatc_cntc=src.eatc_cntc, 
                    target.eatc_dwhc=src.eatc_dwhc, 
                    target.eatc_homc=src.eatc_homc, 
                    target.eatc_prjc=src.eatc_prjc, 
                    target.eatc_refc=src.eatc_refc, 
                    target.eatc_rpc1=src.eatc_rpc1, 
                    target.eatc_rpc2=src.eatc_rpc2, 
                    target.eatc_trnc=src.eatc_trnc, 
                    target.etoc_cntc=src.etoc_cntc, 
                    target.etoc_dwhc=src.etoc_dwhc, 
                    target.etoc_homc=src.etoc_homc, 
                    target.etoc_prjc=src.etoc_prjc, 
                    target.etoc_refc=src.etoc_refc, 
                    target.etoc_rpc1=src.etoc_rpc1, 
                    target.etoc_rpc2=src.etoc_rpc2, 
                    target.etoc_trnc=src.etoc_trnc, 
                    target.ovhd=src.ovhd, 
                    target.ovhd_ref_compnr=src.ovhd_ref_compnr, 
                    target.rdat=src.rdat, 
                    target.rgdt=src.rgdt, 
                    target.rtcc_1=src.rtcc_1, 
                    target.rtcc_2=src.rtcc_2, 
                    target.rtcc_3=src.rtcc_3, 
                    target.rtfc_1=src.rtfc_1, 
                    target.rtfc_2=src.rtfc_2, 
                    target.rtfc_3=src.rtfc_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amoc, 
                    amoc_cntc, 
                    amoc_dwhc, 
                    amoc_homc, 
                    amoc_prjc, 
                    amoc_refc, 
                    amoc_rpc1, 
                    amoc_rpc2, 
                    appr, 
                    appr_kw, 
                    cact, 
                    ccco, 
                    ccco_ref_compnr, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cspa_ref_compnr, 
                    cprj_fcmp, 
                    cprj_ref_compnr, 
                    cspa, 
                    deleted, 
                    eatc_cntc, 
                    eatc_dwhc, 
                    eatc_homc, 
                    eatc_prjc, 
                    eatc_refc, 
                    eatc_rpc1, 
                    eatc_rpc2, 
                    eatc_trnc, 
                    etoc_cntc, 
                    etoc_dwhc, 
                    etoc_homc, 
                    etoc_prjc, 
                    etoc_refc, 
                    etoc_rpc1, 
                    etoc_rpc2, 
                    etoc_trnc, 
                    ovhd, 
                    ovhd_ref_compnr, 
                    rdat, 
                    rgdt, 
                    rtcc_1, 
                    rtcc_2, 
                    rtcc_3, 
                    rtfc_1, 
                    rtfc_2, 
                    rtfc_3, 
                    sequencenumber, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amoc, 
                    src.amoc_cntc, 
                    src.amoc_dwhc, 
                    src.amoc_homc, 
                    src.amoc_prjc, 
                    src.amoc_refc, 
                    src.amoc_rpc1, 
                    src.amoc_rpc2, 
                    src.appr, 
                    src.appr_kw, 
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_fcmp, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.deleted, 
                    src.eatc_cntc, 
                    src.eatc_dwhc, 
                    src.eatc_homc, 
                    src.eatc_prjc, 
                    src.eatc_refc, 
                    src.eatc_rpc1, 
                    src.eatc_rpc2, 
                    src.eatc_trnc, 
                    src.etoc_cntc, 
                    src.etoc_dwhc, 
                    src.etoc_homc, 
                    src.etoc_prjc, 
                    src.etoc_refc, 
                    src.etoc_rpc1, 
                    src.etoc_rpc2, 
                    src.etoc_trnc, 
                    src.ovhd, 
                    src.ovhd_ref_compnr, 
                    src.rdat, 
                    src.rgdt, 
                    src.rtcc_1, 
                    src.rtcc_2, 
                    src.rtcc_3, 
                    src.rtfc_1, 
                    src.rtfc_2, 
                    src.rtfc_3, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPPC606_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amoc, 
            strm.amoc_cntc, 
            strm.amoc_dwhc, 
            strm.amoc_homc, 
            strm.amoc_prjc, 
            strm.amoc_refc, 
            strm.amoc_rpc1, 
            strm.amoc_rpc2, 
            strm.appr, 
            strm.appr_kw, 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_fcmp, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.deleted, 
            strm.eatc_cntc, 
            strm.eatc_dwhc, 
            strm.eatc_homc, 
            strm.eatc_prjc, 
            strm.eatc_refc, 
            strm.eatc_rpc1, 
            strm.eatc_rpc2, 
            strm.eatc_trnc, 
            strm.etoc_cntc, 
            strm.etoc_dwhc, 
            strm.etoc_homc, 
            strm.etoc_prjc, 
            strm.etoc_refc, 
            strm.etoc_rpc1, 
            strm.etoc_rpc2, 
            strm.etoc_trnc, 
            strm.ovhd, 
            strm.ovhd_ref_compnr, 
            strm.rdat, 
            strm.rgdt, 
            strm.rtcc_1, 
            strm.rtcc_2, 
            strm.rtcc_3, 
            strm.rtfc_1, 
            strm.rtfc_2, 
            strm.rtfc_3, 
            strm.sequencenumber, 
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
            strm.cprj,
            strm.rgdt,
            strm.ovhd,
            strm.cspa,
            strm.cact ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC606 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.rgdt=src.rgdt) OR (target.rgdt IS NULL AND src.rgdt IS NULL)) AND
            ((target.ovhd=src.ovhd) OR (target.ovhd IS NULL AND src.ovhd IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) 
                when matched then update set
                    target.amoc=src.amoc, 
                    target.amoc_cntc=src.amoc_cntc, 
                    target.amoc_dwhc=src.amoc_dwhc, 
                    target.amoc_homc=src.amoc_homc, 
                    target.amoc_prjc=src.amoc_prjc, 
                    target.amoc_refc=src.amoc_refc, 
                    target.amoc_rpc1=src.amoc_rpc1, 
                    target.amoc_rpc2=src.amoc_rpc2, 
                    target.appr=src.appr, 
                    target.appr_kw=src.appr_kw, 
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_fcmp=src.cprj_fcmp, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.deleted=src.deleted, 
                    target.eatc_cntc=src.eatc_cntc, 
                    target.eatc_dwhc=src.eatc_dwhc, 
                    target.eatc_homc=src.eatc_homc, 
                    target.eatc_prjc=src.eatc_prjc, 
                    target.eatc_refc=src.eatc_refc, 
                    target.eatc_rpc1=src.eatc_rpc1, 
                    target.eatc_rpc2=src.eatc_rpc2, 
                    target.eatc_trnc=src.eatc_trnc, 
                    target.etoc_cntc=src.etoc_cntc, 
                    target.etoc_dwhc=src.etoc_dwhc, 
                    target.etoc_homc=src.etoc_homc, 
                    target.etoc_prjc=src.etoc_prjc, 
                    target.etoc_refc=src.etoc_refc, 
                    target.etoc_rpc1=src.etoc_rpc1, 
                    target.etoc_rpc2=src.etoc_rpc2, 
                    target.etoc_trnc=src.etoc_trnc, 
                    target.ovhd=src.ovhd, 
                    target.ovhd_ref_compnr=src.ovhd_ref_compnr, 
                    target.rdat=src.rdat, 
                    target.rgdt=src.rgdt, 
                    target.rtcc_1=src.rtcc_1, 
                    target.rtcc_2=src.rtcc_2, 
                    target.rtcc_3=src.rtcc_3, 
                    target.rtfc_1=src.rtfc_1, 
                    target.rtfc_2=src.rtfc_2, 
                    target.rtfc_3=src.rtfc_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amoc, amoc_cntc, amoc_dwhc, amoc_homc, amoc_prjc, amoc_refc, amoc_rpc1, amoc_rpc2, appr, appr_kw, cact, ccco, ccco_ref_compnr, ccur, ccur_ref_compnr, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cspa_ref_compnr, cprj_fcmp, cprj_ref_compnr, cspa, deleted, eatc_cntc, eatc_dwhc, eatc_homc, eatc_prjc, eatc_refc, eatc_rpc1, eatc_rpc2, eatc_trnc, etoc_cntc, etoc_dwhc, etoc_homc, etoc_prjc, etoc_refc, etoc_rpc1, etoc_rpc2, etoc_trnc, ovhd, ovhd_ref_compnr, rdat, rgdt, rtcc_1, rtcc_2, rtcc_3, rtfc_1, rtfc_2, rtfc_3, sequencenumber, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amoc, 
                    src.amoc_cntc, 
                    src.amoc_dwhc, 
                    src.amoc_homc, 
                    src.amoc_prjc, 
                    src.amoc_refc, 
                    src.amoc_rpc1, 
                    src.amoc_rpc2, 
                    src.appr, 
                    src.appr_kw, 
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_fcmp, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.deleted, 
                    src.eatc_cntc, 
                    src.eatc_dwhc, 
                    src.eatc_homc, 
                    src.eatc_prjc, 
                    src.eatc_refc, 
                    src.eatc_rpc1, 
                    src.eatc_rpc2, 
                    src.eatc_trnc, 
                    src.etoc_cntc, 
                    src.etoc_dwhc, 
                    src.etoc_homc, 
                    src.etoc_prjc, 
                    src.etoc_refc, 
                    src.etoc_rpc1, 
                    src.etoc_rpc2, 
                    src.etoc_trnc, 
                    src.ovhd, 
                    src.ovhd_ref_compnr, 
                    src.rdat, 
                    src.rgdt, 
                    src.rtcc_1, 
                    src.rtcc_2, 
                    src.rtcc_3, 
                    src.rtfc_1, 
                    src.rtfc_2, 
                    src.rtfc_3, 
                    src.sequencenumber, 
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
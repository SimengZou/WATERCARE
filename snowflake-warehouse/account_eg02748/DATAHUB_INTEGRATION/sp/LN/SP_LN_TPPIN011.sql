create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPIN011()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPIN011_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPIN011 as target using (SELECT * FROM (SELECT 
            strm.adva, 
            strm.amnt, 
            strm.amnt_cntc, 
            strm.amnt_dwhc, 
            strm.amnt_homc, 
            strm.amnt_prjc, 
            strm.amnt_refc, 
            strm.amnt_rpc1, 
            strm.amnt_rpc2, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cnln, 
            strm.cnpr, 
            strm.cnpr_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.cono_cnln_ref_compnr, 
            strm.cono_cnln_sern_cprj_ofbp_ref_compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.dlvr, 
            strm.fcmp, 
            strm.idln, 
            strm.idoc, 
            strm.iseq, 
            strm.ityp, 
            strm.nins, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.pamt, 
            strm.prdt, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.schd, 
            strm.sequencenumber, 
            strm.serl, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.cnln,
            strm.sern,
            strm.cprj,
            strm.ofbp,
            strm.serl ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPIN011 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.cnln=src.cnln) OR (target.cnln IS NULL AND src.cnln IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ofbp=src.ofbp) OR (target.ofbp IS NULL AND src.ofbp IS NULL)) AND
            ((target.serl=src.serl) OR (target.serl IS NULL AND src.serl IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.adva=src.adva, 
                    target.amnt=src.amnt, 
                    target.amnt_cntc=src.amnt_cntc, 
                    target.amnt_dwhc=src.amnt_dwhc, 
                    target.amnt_homc=src.amnt_homc, 
                    target.amnt_prjc=src.amnt_prjc, 
                    target.amnt_refc=src.amnt_refc, 
                    target.amnt_rpc1=src.amnt_rpc1, 
                    target.amnt_rpc2=src.amnt_rpc2, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cnln=src.cnln, 
                    target.cnpr=src.cnpr, 
                    target.cnpr_ref_compnr=src.cnpr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.cono_cnln_ref_compnr=src.cono_cnln_ref_compnr, 
                    target.cono_cnln_sern_cprj_ofbp_ref_compnr=src.cono_cnln_sern_cprj_ofbp_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlvr=src.dlvr, 
                    target.fcmp=src.fcmp, 
                    target.idln=src.idln, 
                    target.idoc=src.idoc, 
                    target.iseq=src.iseq, 
                    target.ityp=src.ityp, 
                    target.nins=src.nins, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.pamt=src.pamt, 
                    target.prdt=src.prdt, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.schd=src.schd, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    adva, 
                    amnt, 
                    amnt_cntc, 
                    amnt_dwhc, 
                    amnt_homc, 
                    amnt_prjc, 
                    amnt_refc, 
                    amnt_rpc1, 
                    amnt_rpc2, 
                    ccur, 
                    ccur_ref_compnr, 
                    cnln, 
                    cnpr, 
                    cnpr_ref_compnr, 
                    compnr, 
                    cono, 
                    cono_cnln_ref_compnr, 
                    cono_cnln_sern_cprj_ofbp_ref_compnr, 
                    cprj, 
                    cprj_ref_compnr, 
                    deleted, 
                    dlvr, 
                    fcmp, 
                    idln, 
                    idoc, 
                    iseq, 
                    ityp, 
                    nins, 
                    ofbp, 
                    ofbp_ref_compnr, 
                    pamt, 
                    prdt, 
                    rate_1, 
                    rate_2, 
                    rate_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rdat, 
                    rtyp, 
                    rtyp_ref_compnr, 
                    schd, 
                    sequencenumber, 
                    serl, 
                    sern, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.adva, 
                    src.amnt, 
                    src.amnt_cntc, 
                    src.amnt_dwhc, 
                    src.amnt_homc, 
                    src.amnt_prjc, 
                    src.amnt_refc, 
                    src.amnt_rpc1, 
                    src.amnt_rpc2, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cnln, 
                    src.cnpr, 
                    src.cnpr_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.cono_cnln_ref_compnr, 
                    src.cono_cnln_sern_cprj_ofbp_ref_compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.dlvr, 
                    src.fcmp, 
                    src.idln, 
                    src.idoc, 
                    src.iseq, 
                    src.ityp, 
                    src.nins, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.pamt, 
                    src.prdt, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.schd, 
                    src.sequencenumber, 
                    src.serl, 
                    src.sern, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPIN011_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.adva, 
            strm.amnt, 
            strm.amnt_cntc, 
            strm.amnt_dwhc, 
            strm.amnt_homc, 
            strm.amnt_prjc, 
            strm.amnt_refc, 
            strm.amnt_rpc1, 
            strm.amnt_rpc2, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cnln, 
            strm.cnpr, 
            strm.cnpr_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.cono_cnln_ref_compnr, 
            strm.cono_cnln_sern_cprj_ofbp_ref_compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.dlvr, 
            strm.fcmp, 
            strm.idln, 
            strm.idoc, 
            strm.iseq, 
            strm.ityp, 
            strm.nins, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.pamt, 
            strm.prdt, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.schd, 
            strm.sequencenumber, 
            strm.serl, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.cnln,
            strm.sern,
            strm.cprj,
            strm.ofbp,
            strm.serl ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPIN011 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.cnln=src.cnln) OR (target.cnln IS NULL AND src.cnln IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ofbp=src.ofbp) OR (target.ofbp IS NULL AND src.ofbp IS NULL)) AND
            ((target.serl=src.serl) OR (target.serl IS NULL AND src.serl IS NULL)) 
                when matched then update set
                    target.adva=src.adva, 
                    target.amnt=src.amnt, 
                    target.amnt_cntc=src.amnt_cntc, 
                    target.amnt_dwhc=src.amnt_dwhc, 
                    target.amnt_homc=src.amnt_homc, 
                    target.amnt_prjc=src.amnt_prjc, 
                    target.amnt_refc=src.amnt_refc, 
                    target.amnt_rpc1=src.amnt_rpc1, 
                    target.amnt_rpc2=src.amnt_rpc2, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cnln=src.cnln, 
                    target.cnpr=src.cnpr, 
                    target.cnpr_ref_compnr=src.cnpr_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.cono_cnln_ref_compnr=src.cono_cnln_ref_compnr, 
                    target.cono_cnln_sern_cprj_ofbp_ref_compnr=src.cono_cnln_sern_cprj_ofbp_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlvr=src.dlvr, 
                    target.fcmp=src.fcmp, 
                    target.idln=src.idln, 
                    target.idoc=src.idoc, 
                    target.iseq=src.iseq, 
                    target.ityp=src.ityp, 
                    target.nins=src.nins, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.pamt=src.pamt, 
                    target.prdt=src.prdt, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.schd=src.schd, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( adva, amnt, amnt_cntc, amnt_dwhc, amnt_homc, amnt_prjc, amnt_refc, amnt_rpc1, amnt_rpc2, ccur, ccur_ref_compnr, cnln, cnpr, cnpr_ref_compnr, compnr, cono, cono_cnln_ref_compnr, cono_cnln_sern_cprj_ofbp_ref_compnr, cprj, cprj_ref_compnr, deleted, dlvr, fcmp, idln, idoc, iseq, ityp, nins, ofbp, ofbp_ref_compnr, pamt, prdt, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rdat, rtyp, rtyp_ref_compnr, schd, sequencenumber, serl, sern, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.adva, 
                    src.amnt, 
                    src.amnt_cntc, 
                    src.amnt_dwhc, 
                    src.amnt_homc, 
                    src.amnt_prjc, 
                    src.amnt_refc, 
                    src.amnt_rpc1, 
                    src.amnt_rpc2, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cnln, 
                    src.cnpr, 
                    src.cnpr_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.cono_cnln_ref_compnr, 
                    src.cono_cnln_sern_cprj_ofbp_ref_compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.dlvr, 
                    src.fcmp, 
                    src.idln, 
                    src.idoc, 
                    src.iseq, 
                    src.ityp, 
                    src.nins, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.pamt, 
                    src.prdt, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.schd, 
                    src.sequencenumber, 
                    src.serl, 
                    src.sern, 
                    src.timestamp, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFACP251()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFACP251_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFACP251 as target using (SELECT * FROM (SELECT 
            strm.amth_1, 
            strm.amth_2, 
            strm.amth_3, 
            strm.aprp, 
            strm.apry, 
            strm.buex, 
            strm.buex_kw, 
            strm.ccur, 
            strm.compnr, 
            strm.data, 
            strm.dbmo, 
            strm.dbmo_kw, 
            strm.deleted, 
            strm.finl, 
            strm.finl_kw, 
            strm.iamt, 
            strm.icom, 
            strm.idoc, 
            strm.ifbp, 
            strm.ifbp_ref_compnr, 
            strm.imrf, 
            strm.iqan, 
            strm.ityp, 
            strm.loco, 
            strm.orno, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pdfd, 
            strm.pdfd_kw, 
            strm.pdif, 
            strm.pono, 
            strm.pric, 
            strm.pric_kw, 
            strm.ratd, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcno, 
            strm.rseq, 
            strm.rtyp, 
            strm.sequencenumber, 
            strm.shpm, 
            strm.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.icom,
            strm.ityp,
            strm.idoc,
            strm.loco,
            strm.shpm,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb,
            strm.rcno,
            strm.rseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP251 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.icom=src.icom) OR (target.icom IS NULL AND src.icom IS NULL)) AND
            ((target.ityp=src.ityp) OR (target.ityp IS NULL AND src.ityp IS NULL)) AND
            ((target.idoc=src.idoc) OR (target.idoc IS NULL AND src.idoc IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) AND
            ((target.rseq=src.rseq) OR (target.rseq IS NULL AND src.rseq IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amth_1=src.amth_1, 
                    target.amth_2=src.amth_2, 
                    target.amth_3=src.amth_3, 
                    target.aprp=src.aprp, 
                    target.apry=src.apry, 
                    target.buex=src.buex, 
                    target.buex_kw=src.buex_kw, 
                    target.ccur=src.ccur, 
                    target.compnr=src.compnr, 
                    target.data=src.data, 
                    target.dbmo=src.dbmo, 
                    target.dbmo_kw=src.dbmo_kw, 
                    target.deleted=src.deleted, 
                    target.finl=src.finl, 
                    target.finl_kw=src.finl_kw, 
                    target.iamt=src.iamt, 
                    target.icom=src.icom, 
                    target.idoc=src.idoc, 
                    target.ifbp=src.ifbp, 
                    target.ifbp_ref_compnr=src.ifbp_ref_compnr, 
                    target.imrf=src.imrf, 
                    target.iqan=src.iqan, 
                    target.ityp=src.ityp, 
                    target.loco=src.loco, 
                    target.orno=src.orno, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pdfd=src.pdfd, 
                    target.pdfd_kw=src.pdfd_kw, 
                    target.pdif=src.pdif, 
                    target.pono=src.pono, 
                    target.pric=src.pric, 
                    target.pric_kw=src.pric_kw, 
                    target.ratd=src.ratd, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcno=src.rcno, 
                    target.rseq=src.rseq, 
                    target.rtyp=src.rtyp, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shpm=src.shpm, 
                    target.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr=src.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amth_1, 
                    amth_2, 
                    amth_3, 
                    aprp, 
                    apry, 
                    buex, 
                    buex_kw, 
                    ccur, 
                    compnr, 
                    data, 
                    dbmo, 
                    dbmo_kw, 
                    deleted, 
                    finl, 
                    finl_kw, 
                    iamt, 
                    icom, 
                    idoc, 
                    ifbp, 
                    ifbp_ref_compnr, 
                    imrf, 
                    iqan, 
                    ityp, 
                    loco, 
                    orno, 
                    otyp, 
                    otyp_kw, 
                    pdfd, 
                    pdfd_kw, 
                    pdif, 
                    pono, 
                    pric, 
                    pric_kw, 
                    ratd, 
                    rate_1, 
                    rate_2, 
                    rate_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rcno, 
                    rseq, 
                    rtyp, 
                    sequencenumber, 
                    shpm, 
                    shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                    sqnb, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amth_1, 
                    src.amth_2, 
                    src.amth_3, 
                    src.aprp, 
                    src.apry, 
                    src.buex, 
                    src.buex_kw, 
                    src.ccur, 
                    src.compnr, 
                    src.data, 
                    src.dbmo, 
                    src.dbmo_kw, 
                    src.deleted, 
                    src.finl, 
                    src.finl_kw, 
                    src.iamt, 
                    src.icom, 
                    src.idoc, 
                    src.ifbp, 
                    src.ifbp_ref_compnr, 
                    src.imrf, 
                    src.iqan, 
                    src.ityp, 
                    src.loco, 
                    src.orno, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pdfd, 
                    src.pdfd_kw, 
                    src.pdif, 
                    src.pono, 
                    src.pric, 
                    src.pric_kw, 
                    src.ratd, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcno, 
                    src.rseq, 
                    src.rtyp, 
                    src.sequencenumber, 
                    src.shpm, 
                    src.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                    src.sqnb, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFACP251_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amth_1, 
            strm.amth_2, 
            strm.amth_3, 
            strm.aprp, 
            strm.apry, 
            strm.buex, 
            strm.buex_kw, 
            strm.ccur, 
            strm.compnr, 
            strm.data, 
            strm.dbmo, 
            strm.dbmo_kw, 
            strm.deleted, 
            strm.finl, 
            strm.finl_kw, 
            strm.iamt, 
            strm.icom, 
            strm.idoc, 
            strm.ifbp, 
            strm.ifbp_ref_compnr, 
            strm.imrf, 
            strm.iqan, 
            strm.ityp, 
            strm.loco, 
            strm.orno, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pdfd, 
            strm.pdfd_kw, 
            strm.pdif, 
            strm.pono, 
            strm.pric, 
            strm.pric_kw, 
            strm.ratd, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcno, 
            strm.rseq, 
            strm.rtyp, 
            strm.sequencenumber, 
            strm.shpm, 
            strm.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.icom,
            strm.ityp,
            strm.idoc,
            strm.loco,
            strm.shpm,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb,
            strm.rcno,
            strm.rseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP251 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.icom=src.icom) OR (target.icom IS NULL AND src.icom IS NULL)) AND
            ((target.ityp=src.ityp) OR (target.ityp IS NULL AND src.ityp IS NULL)) AND
            ((target.idoc=src.idoc) OR (target.idoc IS NULL AND src.idoc IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.rcno=src.rcno) OR (target.rcno IS NULL AND src.rcno IS NULL)) AND
            ((target.rseq=src.rseq) OR (target.rseq IS NULL AND src.rseq IS NULL)) 
                when matched then update set
                    target.amth_1=src.amth_1, 
                    target.amth_2=src.amth_2, 
                    target.amth_3=src.amth_3, 
                    target.aprp=src.aprp, 
                    target.apry=src.apry, 
                    target.buex=src.buex, 
                    target.buex_kw=src.buex_kw, 
                    target.ccur=src.ccur, 
                    target.compnr=src.compnr, 
                    target.data=src.data, 
                    target.dbmo=src.dbmo, 
                    target.dbmo_kw=src.dbmo_kw, 
                    target.deleted=src.deleted, 
                    target.finl=src.finl, 
                    target.finl_kw=src.finl_kw, 
                    target.iamt=src.iamt, 
                    target.icom=src.icom, 
                    target.idoc=src.idoc, 
                    target.ifbp=src.ifbp, 
                    target.ifbp_ref_compnr=src.ifbp_ref_compnr, 
                    target.imrf=src.imrf, 
                    target.iqan=src.iqan, 
                    target.ityp=src.ityp, 
                    target.loco=src.loco, 
                    target.orno=src.orno, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pdfd=src.pdfd, 
                    target.pdfd_kw=src.pdfd_kw, 
                    target.pdif=src.pdif, 
                    target.pono=src.pono, 
                    target.pric=src.pric, 
                    target.pric_kw=src.pric_kw, 
                    target.ratd=src.ratd, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcno=src.rcno, 
                    target.rseq=src.rseq, 
                    target.rtyp=src.rtyp, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shpm=src.shpm, 
                    target.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr=src.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amth_1, amth_2, amth_3, aprp, apry, buex, buex_kw, ccur, compnr, data, dbmo, dbmo_kw, deleted, finl, finl_kw, iamt, icom, idoc, ifbp, ifbp_ref_compnr, imrf, iqan, ityp, loco, orno, otyp, otyp_kw, pdfd, pdfd_kw, pdif, pono, pric, pric_kw, ratd, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rcno, rseq, rtyp, sequencenumber, shpm, shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, sqnb, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amth_1, 
                    src.amth_2, 
                    src.amth_3, 
                    src.aprp, 
                    src.apry, 
                    src.buex, 
                    src.buex_kw, 
                    src.ccur, 
                    src.compnr, 
                    src.data, 
                    src.dbmo, 
                    src.dbmo_kw, 
                    src.deleted, 
                    src.finl, 
                    src.finl_kw, 
                    src.iamt, 
                    src.icom, 
                    src.idoc, 
                    src.ifbp, 
                    src.ifbp_ref_compnr, 
                    src.imrf, 
                    src.iqan, 
                    src.ityp, 
                    src.loco, 
                    src.orno, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pdfd, 
                    src.pdfd_kw, 
                    src.pdif, 
                    src.pono, 
                    src.pric, 
                    src.pric_kw, 
                    src.ratd, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcno, 
                    src.rseq, 
                    src.rtyp, 
                    src.sequencenumber, 
                    src.shpm, 
                    src.shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr, 
                    src.sqnb, 
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
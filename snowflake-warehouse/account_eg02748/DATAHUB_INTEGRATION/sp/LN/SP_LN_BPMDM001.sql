create or replace procedure DATAHUB_INTEGRATION.SP_LN_BPMDM001()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_BPMDM001_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_BPMDM001 as target using (SELECT * FROM (SELECT 
            strm.bano, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccty, 
            strm.ccty_ref_compnr, 
            strm.cedt, 
            strm.cist, 
            strm.compnr, 
            strm.ctrg, 
            strm.daob, 
            strm.deleted, 
            strm.edte, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.emtp, 
            strm.emtp_kw, 
            strm.finr, 
            strm.hwem, 
            strm.jobt, 
            strm.mail, 
            strm.msad, 
            strm.msty, 
            strm.msty_kw, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.prtn, 
            strm.ptbp, 
            strm.ptbp_ref_compnr, 
            strm.rgno, 
            strm.sdte, 
            strm.sequencenumber, 
            strm.sexe, 
            strm.sexe_kw, 
            strm.supv, 
            strm.supv_ref_compnr, 
            strm.tctr, 
            strm.tefw, 
            strm.telm, 
            strm.telw, 
            strm.timestamp, 
            strm.tlw1, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.ucrm, 
            strm.ucrm_kw, 
            strm.username, 
            strm.wtsc, 
            strm.wtsc_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.emno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_BPMDM001 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.emno=src.emno) OR (target.emno IS NULL AND src.emno IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bano=src.bano, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccty=src.ccty, 
                    target.ccty_ref_compnr=src.ccty_ref_compnr, 
                    target.cedt=src.cedt, 
                    target.cist=src.cist, 
                    target.compnr=src.compnr, 
                    target.ctrg=src.ctrg, 
                    target.daob=src.daob, 
                    target.deleted=src.deleted, 
                    target.edte=src.edte, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.emtp=src.emtp, 
                    target.emtp_kw=src.emtp_kw, 
                    target.finr=src.finr, 
                    target.hwem=src.hwem, 
                    target.jobt=src.jobt, 
                    target.mail=src.mail, 
                    target.msad=src.msad, 
                    target.msty=src.msty, 
                    target.msty_kw=src.msty_kw, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.prtn=src.prtn, 
                    target.ptbp=src.ptbp, 
                    target.ptbp_ref_compnr=src.ptbp_ref_compnr, 
                    target.rgno=src.rgno, 
                    target.sdte=src.sdte, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sexe=src.sexe, 
                    target.sexe_kw=src.sexe_kw, 
                    target.supv=src.supv, 
                    target.supv_ref_compnr=src.supv_ref_compnr, 
                    target.tctr=src.tctr, 
                    target.tefw=src.tefw, 
                    target.telm=src.telm, 
                    target.telw=src.telw, 
                    target.timestamp=src.timestamp, 
                    target.tlw1=src.tlw1, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.ucrm=src.ucrm, 
                    target.ucrm_kw=src.ucrm_kw, 
                    target.username=src.username, 
                    target.wtsc=src.wtsc, 
                    target.wtsc_ref_compnr=src.wtsc_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bano, 
                    cadr, 
                    cadr_ref_compnr, 
                    ccty, 
                    ccty_ref_compnr, 
                    cedt, 
                    cist, 
                    compnr, 
                    ctrg, 
                    daob, 
                    deleted, 
                    edte, 
                    emno, 
                    emno_ref_compnr, 
                    emtp, 
                    emtp_kw, 
                    finr, 
                    hwem, 
                    jobt, 
                    mail, 
                    msad, 
                    msty, 
                    msty_kw, 
                    otbp, 
                    otbp_ref_compnr, 
                    prtn, 
                    ptbp, 
                    ptbp_ref_compnr, 
                    rgno, 
                    sdte, 
                    sequencenumber, 
                    sexe, 
                    sexe_kw, 
                    supv, 
                    supv_ref_compnr, 
                    tctr, 
                    tefw, 
                    telm, 
                    telw, 
                    timestamp, 
                    tlw1, 
                    txta, 
                    txta_ref_compnr, 
                    ucrm, 
                    ucrm_kw, 
                    username, 
                    wtsc, 
                    wtsc_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bano, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccty, 
                    src.ccty_ref_compnr, 
                    src.cedt, 
                    src.cist, 
                    src.compnr, 
                    src.ctrg, 
                    src.daob, 
                    src.deleted, 
                    src.edte, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.emtp, 
                    src.emtp_kw, 
                    src.finr, 
                    src.hwem, 
                    src.jobt, 
                    src.mail, 
                    src.msad, 
                    src.msty, 
                    src.msty_kw, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.prtn, 
                    src.ptbp, 
                    src.ptbp_ref_compnr, 
                    src.rgno, 
                    src.sdte, 
                    src.sequencenumber, 
                    src.sexe, 
                    src.sexe_kw, 
                    src.supv, 
                    src.supv_ref_compnr, 
                    src.tctr, 
                    src.tefw, 
                    src.telm, 
                    src.telw, 
                    src.timestamp, 
                    src.tlw1, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.ucrm, 
                    src.ucrm_kw, 
                    src.username, 
                    src.wtsc, 
                    src.wtsc_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_BPMDM001_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bano, 
            strm.cadr, 
            strm.cadr_ref_compnr, 
            strm.ccty, 
            strm.ccty_ref_compnr, 
            strm.cedt, 
            strm.cist, 
            strm.compnr, 
            strm.ctrg, 
            strm.daob, 
            strm.deleted, 
            strm.edte, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.emtp, 
            strm.emtp_kw, 
            strm.finr, 
            strm.hwem, 
            strm.jobt, 
            strm.mail, 
            strm.msad, 
            strm.msty, 
            strm.msty_kw, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.prtn, 
            strm.ptbp, 
            strm.ptbp_ref_compnr, 
            strm.rgno, 
            strm.sdte, 
            strm.sequencenumber, 
            strm.sexe, 
            strm.sexe_kw, 
            strm.supv, 
            strm.supv_ref_compnr, 
            strm.tctr, 
            strm.tefw, 
            strm.telm, 
            strm.telw, 
            strm.timestamp, 
            strm.tlw1, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.ucrm, 
            strm.ucrm_kw, 
            strm.username, 
            strm.wtsc, 
            strm.wtsc_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.emno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_BPMDM001 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.emno=src.emno) OR (target.emno IS NULL AND src.emno IS NULL)) 
                when matched then update set
                    target.bano=src.bano, 
                    target.cadr=src.cadr, 
                    target.cadr_ref_compnr=src.cadr_ref_compnr, 
                    target.ccty=src.ccty, 
                    target.ccty_ref_compnr=src.ccty_ref_compnr, 
                    target.cedt=src.cedt, 
                    target.cist=src.cist, 
                    target.compnr=src.compnr, 
                    target.ctrg=src.ctrg, 
                    target.daob=src.daob, 
                    target.deleted=src.deleted, 
                    target.edte=src.edte, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.emtp=src.emtp, 
                    target.emtp_kw=src.emtp_kw, 
                    target.finr=src.finr, 
                    target.hwem=src.hwem, 
                    target.jobt=src.jobt, 
                    target.mail=src.mail, 
                    target.msad=src.msad, 
                    target.msty=src.msty, 
                    target.msty_kw=src.msty_kw, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.prtn=src.prtn, 
                    target.ptbp=src.ptbp, 
                    target.ptbp_ref_compnr=src.ptbp_ref_compnr, 
                    target.rgno=src.rgno, 
                    target.sdte=src.sdte, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sexe=src.sexe, 
                    target.sexe_kw=src.sexe_kw, 
                    target.supv=src.supv, 
                    target.supv_ref_compnr=src.supv_ref_compnr, 
                    target.tctr=src.tctr, 
                    target.tefw=src.tefw, 
                    target.telm=src.telm, 
                    target.telw=src.telw, 
                    target.timestamp=src.timestamp, 
                    target.tlw1=src.tlw1, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.ucrm=src.ucrm, 
                    target.ucrm_kw=src.ucrm_kw, 
                    target.username=src.username, 
                    target.wtsc=src.wtsc, 
                    target.wtsc_ref_compnr=src.wtsc_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bano, cadr, cadr_ref_compnr, ccty, ccty_ref_compnr, cedt, cist, compnr, ctrg, daob, deleted, edte, emno, emno_ref_compnr, emtp, emtp_kw, finr, hwem, jobt, mail, msad, msty, msty_kw, otbp, otbp_ref_compnr, prtn, ptbp, ptbp_ref_compnr, rgno, sdte, sequencenumber, sexe, sexe_kw, supv, supv_ref_compnr, tctr, tefw, telm, telw, timestamp, tlw1, txta, txta_ref_compnr, ucrm, ucrm_kw, username, wtsc, wtsc_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bano, 
                    src.cadr, 
                    src.cadr_ref_compnr, 
                    src.ccty, 
                    src.ccty_ref_compnr, 
                    src.cedt, 
                    src.cist, 
                    src.compnr, 
                    src.ctrg, 
                    src.daob, 
                    src.deleted, 
                    src.edte, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.emtp, 
                    src.emtp_kw, 
                    src.finr, 
                    src.hwem, 
                    src.jobt, 
                    src.mail, 
                    src.msad, 
                    src.msty, 
                    src.msty_kw, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.prtn, 
                    src.ptbp, 
                    src.ptbp_ref_compnr, 
                    src.rgno, 
                    src.sdte, 
                    src.sequencenumber, 
                    src.sexe, 
                    src.sexe_kw, 
                    src.supv, 
                    src.supv_ref_compnr, 
                    src.tctr, 
                    src.tefw, 
                    src.telm, 
                    src.telw, 
                    src.timestamp, 
                    src.tlw1, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.ucrm, 
                    src.ucrm_kw, 
                    src.username, 
                    src.wtsc, 
                    src.wtsc_ref_compnr,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFST120()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFST120_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFST120 as target using (SELECT * FROM (SELECT 
            strm.acca, 
            strm.accn, 
            strm.accs, 
            strm.accs_kw, 
            strm.acct, 
            strm.acct_kw, 
            strm.acrd, 
            strm.atxt, 
            strm.atxt_ref_compnr, 
            strm.cfsa, 
            strm.cfsa_kw, 
            strm.cgla, 
            strm.compnr, 
            strm.dbcr, 
            strm.dbcr_kw, 
            strm.dcgl, 
            strm.dcgl_kw, 
            strm.dcsw, 
            strm.dcsw_kw, 
            strm.deleted, 
            strm.desc, 
            strm.facc, 
            strm.fstm, 
            strm.fstm_acca_ref_compnr, 
            strm.fstm_acrd_ref_compnr, 
            strm.fstm_cgla_ref_compnr, 
            strm.fstm_facc_ref_compnr, 
            strm.fstm_pacc_ref_compnr, 
            strm.fstm_ref_compnr, 
            strm.muaa, 
            strm.muaa_kw, 
            strm.pacc, 
            strm.prta, 
            strm.prta_kw, 
            strm.pseq, 
            strm.rhis, 
            strm.rhis_kw, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.sequencenumber, 
            strm.sign, 
            strm.sign_kw, 
            strm.subl, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm,
            strm.accn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST120 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) AND
            ((target.accn=src.accn) OR (target.accn IS NULL AND src.accn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acca=src.acca, 
                    target.accn=src.accn, 
                    target.accs=src.accs, 
                    target.accs_kw=src.accs_kw, 
                    target.acct=src.acct, 
                    target.acct_kw=src.acct_kw, 
                    target.acrd=src.acrd, 
                    target.atxt=src.atxt, 
                    target.atxt_ref_compnr=src.atxt_ref_compnr, 
                    target.cfsa=src.cfsa, 
                    target.cfsa_kw=src.cfsa_kw, 
                    target.cgla=src.cgla, 
                    target.compnr=src.compnr, 
                    target.dbcr=src.dbcr, 
                    target.dbcr_kw=src.dbcr_kw, 
                    target.dcgl=src.dcgl, 
                    target.dcgl_kw=src.dcgl_kw, 
                    target.dcsw=src.dcsw, 
                    target.dcsw_kw=src.dcsw_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.facc=src.facc, 
                    target.fstm=src.fstm, 
                    target.fstm_acca_ref_compnr=src.fstm_acca_ref_compnr, 
                    target.fstm_acrd_ref_compnr=src.fstm_acrd_ref_compnr, 
                    target.fstm_cgla_ref_compnr=src.fstm_cgla_ref_compnr, 
                    target.fstm_facc_ref_compnr=src.fstm_facc_ref_compnr, 
                    target.fstm_pacc_ref_compnr=src.fstm_pacc_ref_compnr, 
                    target.fstm_ref_compnr=src.fstm_ref_compnr, 
                    target.muaa=src.muaa, 
                    target.muaa_kw=src.muaa_kw, 
                    target.pacc=src.pacc, 
                    target.prta=src.prta, 
                    target.prta_kw=src.prta_kw, 
                    target.pseq=src.pseq, 
                    target.rhis=src.rhis, 
                    target.rhis_kw=src.rhis_kw, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sign=src.sign, 
                    target.sign_kw=src.sign_kw, 
                    target.subl=src.subl, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acca, 
                    accn, 
                    accs, 
                    accs_kw, 
                    acct, 
                    acct_kw, 
                    acrd, 
                    atxt, 
                    atxt_ref_compnr, 
                    cfsa, 
                    cfsa_kw, 
                    cgla, 
                    compnr, 
                    dbcr, 
                    dbcr_kw, 
                    dcgl, 
                    dcgl_kw, 
                    dcsw, 
                    dcsw_kw, 
                    deleted, 
                    desc, 
                    facc, 
                    fstm, 
                    fstm_acca_ref_compnr, 
                    fstm_acrd_ref_compnr, 
                    fstm_cgla_ref_compnr, 
                    fstm_facc_ref_compnr, 
                    fstm_pacc_ref_compnr, 
                    fstm_ref_compnr, 
                    muaa, 
                    muaa_kw, 
                    pacc, 
                    prta, 
                    prta_kw, 
                    pseq, 
                    rhis, 
                    rhis_kw, 
                    rtyp, 
                    rtyp_ref_compnr, 
                    sequencenumber, 
                    sign, 
                    sign_kw, 
                    subl, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acca, 
                    src.accn, 
                    src.accs, 
                    src.accs_kw, 
                    src.acct, 
                    src.acct_kw, 
                    src.acrd, 
                    src.atxt, 
                    src.atxt_ref_compnr, 
                    src.cfsa, 
                    src.cfsa_kw, 
                    src.cgla, 
                    src.compnr, 
                    src.dbcr, 
                    src.dbcr_kw, 
                    src.dcgl, 
                    src.dcgl_kw, 
                    src.dcsw, 
                    src.dcsw_kw, 
                    src.deleted, 
                    src.desc, 
                    src.facc, 
                    src.fstm, 
                    src.fstm_acca_ref_compnr, 
                    src.fstm_acrd_ref_compnr, 
                    src.fstm_cgla_ref_compnr, 
                    src.fstm_facc_ref_compnr, 
                    src.fstm_pacc_ref_compnr, 
                    src.fstm_ref_compnr, 
                    src.muaa, 
                    src.muaa_kw, 
                    src.pacc, 
                    src.prta, 
                    src.prta_kw, 
                    src.pseq, 
                    src.rhis, 
                    src.rhis_kw, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.sequencenumber, 
                    src.sign, 
                    src.sign_kw, 
                    src.subl, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFST120_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acca, 
            strm.accn, 
            strm.accs, 
            strm.accs_kw, 
            strm.acct, 
            strm.acct_kw, 
            strm.acrd, 
            strm.atxt, 
            strm.atxt_ref_compnr, 
            strm.cfsa, 
            strm.cfsa_kw, 
            strm.cgla, 
            strm.compnr, 
            strm.dbcr, 
            strm.dbcr_kw, 
            strm.dcgl, 
            strm.dcgl_kw, 
            strm.dcsw, 
            strm.dcsw_kw, 
            strm.deleted, 
            strm.desc, 
            strm.facc, 
            strm.fstm, 
            strm.fstm_acca_ref_compnr, 
            strm.fstm_acrd_ref_compnr, 
            strm.fstm_cgla_ref_compnr, 
            strm.fstm_facc_ref_compnr, 
            strm.fstm_pacc_ref_compnr, 
            strm.fstm_ref_compnr, 
            strm.muaa, 
            strm.muaa_kw, 
            strm.pacc, 
            strm.prta, 
            strm.prta_kw, 
            strm.pseq, 
            strm.rhis, 
            strm.rhis_kw, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.sequencenumber, 
            strm.sign, 
            strm.sign_kw, 
            strm.subl, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm,
            strm.accn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST120 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) AND
            ((target.accn=src.accn) OR (target.accn IS NULL AND src.accn IS NULL)) 
                when matched then update set
                    target.acca=src.acca, 
                    target.accn=src.accn, 
                    target.accs=src.accs, 
                    target.accs_kw=src.accs_kw, 
                    target.acct=src.acct, 
                    target.acct_kw=src.acct_kw, 
                    target.acrd=src.acrd, 
                    target.atxt=src.atxt, 
                    target.atxt_ref_compnr=src.atxt_ref_compnr, 
                    target.cfsa=src.cfsa, 
                    target.cfsa_kw=src.cfsa_kw, 
                    target.cgla=src.cgla, 
                    target.compnr=src.compnr, 
                    target.dbcr=src.dbcr, 
                    target.dbcr_kw=src.dbcr_kw, 
                    target.dcgl=src.dcgl, 
                    target.dcgl_kw=src.dcgl_kw, 
                    target.dcsw=src.dcsw, 
                    target.dcsw_kw=src.dcsw_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.facc=src.facc, 
                    target.fstm=src.fstm, 
                    target.fstm_acca_ref_compnr=src.fstm_acca_ref_compnr, 
                    target.fstm_acrd_ref_compnr=src.fstm_acrd_ref_compnr, 
                    target.fstm_cgla_ref_compnr=src.fstm_cgla_ref_compnr, 
                    target.fstm_facc_ref_compnr=src.fstm_facc_ref_compnr, 
                    target.fstm_pacc_ref_compnr=src.fstm_pacc_ref_compnr, 
                    target.fstm_ref_compnr=src.fstm_ref_compnr, 
                    target.muaa=src.muaa, 
                    target.muaa_kw=src.muaa_kw, 
                    target.pacc=src.pacc, 
                    target.prta=src.prta, 
                    target.prta_kw=src.prta_kw, 
                    target.pseq=src.pseq, 
                    target.rhis=src.rhis, 
                    target.rhis_kw=src.rhis_kw, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sign=src.sign, 
                    target.sign_kw=src.sign_kw, 
                    target.subl=src.subl, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acca, accn, accs, accs_kw, acct, acct_kw, acrd, atxt, atxt_ref_compnr, cfsa, cfsa_kw, cgla, compnr, dbcr, dbcr_kw, dcgl, dcgl_kw, dcsw, dcsw_kw, deleted, desc, facc, fstm, fstm_acca_ref_compnr, fstm_acrd_ref_compnr, fstm_cgla_ref_compnr, fstm_facc_ref_compnr, fstm_pacc_ref_compnr, fstm_ref_compnr, muaa, muaa_kw, pacc, prta, prta_kw, pseq, rhis, rhis_kw, rtyp, rtyp_ref_compnr, sequencenumber, sign, sign_kw, subl, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acca, 
                    src.accn, 
                    src.accs, 
                    src.accs_kw, 
                    src.acct, 
                    src.acct_kw, 
                    src.acrd, 
                    src.atxt, 
                    src.atxt_ref_compnr, 
                    src.cfsa, 
                    src.cfsa_kw, 
                    src.cgla, 
                    src.compnr, 
                    src.dbcr, 
                    src.dbcr_kw, 
                    src.dcgl, 
                    src.dcgl_kw, 
                    src.dcsw, 
                    src.dcsw_kw, 
                    src.deleted, 
                    src.desc, 
                    src.facc, 
                    src.fstm, 
                    src.fstm_acca_ref_compnr, 
                    src.fstm_acrd_ref_compnr, 
                    src.fstm_cgla_ref_compnr, 
                    src.fstm_facc_ref_compnr, 
                    src.fstm_pacc_ref_compnr, 
                    src.fstm_ref_compnr, 
                    src.muaa, 
                    src.muaa_kw, 
                    src.pacc, 
                    src.prta, 
                    src.prta_kw, 
                    src.pseq, 
                    src.rhis, 
                    src.rhis_kw, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.sequencenumber, 
                    src.sign, 
                    src.sign_kw, 
                    src.subl, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM200()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM200_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM200 as target using (SELECT * FROM (SELECT 
            strm.aexm, 
            strm.aexm_ref_compnr, 
            strm.agrp, 
            strm.agrp_ref_compnr, 
            strm.amth, 
            strm.amth_ref_compnr, 
            strm.anbm, 
            strm.anbm_ref_compnr, 
            strm.aslf, 
            strm.bslv, 
            strm.bslv_kw, 
            strm.cmth, 
            strm.cmth_ref_compnr, 
            strm.compnr, 
            strm.ctgy, 
            strm.cthc, 
            strm.cthr, 
            strm.deleted, 
            strm.desc, 
            strm.dscr, 
            strm.dscr_kw, 
            strm.fadr, 
            strm.fmth, 
            strm.fmth_ref_compnr, 
            strm.itcd, 
            strm.itcd_kw, 
            strm.life, 
            strm.lmth, 
            strm.lmth_ref_compnr, 
            strm.mmth, 
            strm.mmth_ref_compnr, 
            strm.mskc, 
            strm.mskc_ref_compnr, 
            strm.omth, 
            strm.omth_ref_compnr, 
            strm.prop, 
            strm.prop_ref_compnr, 
            strm.sctg, 
            strm.sequencenumber, 
            strm.smth, 
            strm.smth_ref_compnr, 
            strm.timestamp, 
            strm.tmth, 
            strm.tmth_ref_compnr, 
            strm.umth, 
            strm.umth_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ctgy ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM200 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ctgy=src.ctgy) OR (target.ctgy IS NULL AND src.ctgy IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aexm=src.aexm, 
                    target.aexm_ref_compnr=src.aexm_ref_compnr, 
                    target.agrp=src.agrp, 
                    target.agrp_ref_compnr=src.agrp_ref_compnr, 
                    target.amth=src.amth, 
                    target.amth_ref_compnr=src.amth_ref_compnr, 
                    target.anbm=src.anbm, 
                    target.anbm_ref_compnr=src.anbm_ref_compnr, 
                    target.aslf=src.aslf, 
                    target.bslv=src.bslv, 
                    target.bslv_kw=src.bslv_kw, 
                    target.cmth=src.cmth, 
                    target.cmth_ref_compnr=src.cmth_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.ctgy=src.ctgy, 
                    target.cthc=src.cthc, 
                    target.cthr=src.cthr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.dscr=src.dscr, 
                    target.dscr_kw=src.dscr_kw, 
                    target.fadr=src.fadr, 
                    target.fmth=src.fmth, 
                    target.fmth_ref_compnr=src.fmth_ref_compnr, 
                    target.itcd=src.itcd, 
                    target.itcd_kw=src.itcd_kw, 
                    target.life=src.life, 
                    target.lmth=src.lmth, 
                    target.lmth_ref_compnr=src.lmth_ref_compnr, 
                    target.mmth=src.mmth, 
                    target.mmth_ref_compnr=src.mmth_ref_compnr, 
                    target.mskc=src.mskc, 
                    target.mskc_ref_compnr=src.mskc_ref_compnr, 
                    target.omth=src.omth, 
                    target.omth_ref_compnr=src.omth_ref_compnr, 
                    target.prop=src.prop, 
                    target.prop_ref_compnr=src.prop_ref_compnr, 
                    target.sctg=src.sctg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.smth=src.smth, 
                    target.smth_ref_compnr=src.smth_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.tmth=src.tmth, 
                    target.tmth_ref_compnr=src.tmth_ref_compnr, 
                    target.umth=src.umth, 
                    target.umth_ref_compnr=src.umth_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aexm, 
                    aexm_ref_compnr, 
                    agrp, 
                    agrp_ref_compnr, 
                    amth, 
                    amth_ref_compnr, 
                    anbm, 
                    anbm_ref_compnr, 
                    aslf, 
                    bslv, 
                    bslv_kw, 
                    cmth, 
                    cmth_ref_compnr, 
                    compnr, 
                    ctgy, 
                    cthc, 
                    cthr, 
                    deleted, 
                    desc, 
                    dscr, 
                    dscr_kw, 
                    fadr, 
                    fmth, 
                    fmth_ref_compnr, 
                    itcd, 
                    itcd_kw, 
                    life, 
                    lmth, 
                    lmth_ref_compnr, 
                    mmth, 
                    mmth_ref_compnr, 
                    mskc, 
                    mskc_ref_compnr, 
                    omth, 
                    omth_ref_compnr, 
                    prop, 
                    prop_ref_compnr, 
                    sctg, 
                    sequencenumber, 
                    smth, 
                    smth_ref_compnr, 
                    timestamp, 
                    tmth, 
                    tmth_ref_compnr, 
                    umth, 
                    umth_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aexm, 
                    src.aexm_ref_compnr, 
                    src.agrp, 
                    src.agrp_ref_compnr, 
                    src.amth, 
                    src.amth_ref_compnr, 
                    src.anbm, 
                    src.anbm_ref_compnr, 
                    src.aslf, 
                    src.bslv, 
                    src.bslv_kw, 
                    src.cmth, 
                    src.cmth_ref_compnr, 
                    src.compnr, 
                    src.ctgy, 
                    src.cthc, 
                    src.cthr, 
                    src.deleted, 
                    src.desc, 
                    src.dscr, 
                    src.dscr_kw, 
                    src.fadr, 
                    src.fmth, 
                    src.fmth_ref_compnr, 
                    src.itcd, 
                    src.itcd_kw, 
                    src.life, 
                    src.lmth, 
                    src.lmth_ref_compnr, 
                    src.mmth, 
                    src.mmth_ref_compnr, 
                    src.mskc, 
                    src.mskc_ref_compnr, 
                    src.omth, 
                    src.omth_ref_compnr, 
                    src.prop, 
                    src.prop_ref_compnr, 
                    src.sctg, 
                    src.sequencenumber, 
                    src.smth, 
                    src.smth_ref_compnr, 
                    src.timestamp, 
                    src.tmth, 
                    src.tmth_ref_compnr, 
                    src.umth, 
                    src.umth_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM200_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aexm, 
            strm.aexm_ref_compnr, 
            strm.agrp, 
            strm.agrp_ref_compnr, 
            strm.amth, 
            strm.amth_ref_compnr, 
            strm.anbm, 
            strm.anbm_ref_compnr, 
            strm.aslf, 
            strm.bslv, 
            strm.bslv_kw, 
            strm.cmth, 
            strm.cmth_ref_compnr, 
            strm.compnr, 
            strm.ctgy, 
            strm.cthc, 
            strm.cthr, 
            strm.deleted, 
            strm.desc, 
            strm.dscr, 
            strm.dscr_kw, 
            strm.fadr, 
            strm.fmth, 
            strm.fmth_ref_compnr, 
            strm.itcd, 
            strm.itcd_kw, 
            strm.life, 
            strm.lmth, 
            strm.lmth_ref_compnr, 
            strm.mmth, 
            strm.mmth_ref_compnr, 
            strm.mskc, 
            strm.mskc_ref_compnr, 
            strm.omth, 
            strm.omth_ref_compnr, 
            strm.prop, 
            strm.prop_ref_compnr, 
            strm.sctg, 
            strm.sequencenumber, 
            strm.smth, 
            strm.smth_ref_compnr, 
            strm.timestamp, 
            strm.tmth, 
            strm.tmth_ref_compnr, 
            strm.umth, 
            strm.umth_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ctgy ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM200 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ctgy=src.ctgy) OR (target.ctgy IS NULL AND src.ctgy IS NULL)) 
                when matched then update set
                    target.aexm=src.aexm, 
                    target.aexm_ref_compnr=src.aexm_ref_compnr, 
                    target.agrp=src.agrp, 
                    target.agrp_ref_compnr=src.agrp_ref_compnr, 
                    target.amth=src.amth, 
                    target.amth_ref_compnr=src.amth_ref_compnr, 
                    target.anbm=src.anbm, 
                    target.anbm_ref_compnr=src.anbm_ref_compnr, 
                    target.aslf=src.aslf, 
                    target.bslv=src.bslv, 
                    target.bslv_kw=src.bslv_kw, 
                    target.cmth=src.cmth, 
                    target.cmth_ref_compnr=src.cmth_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.ctgy=src.ctgy, 
                    target.cthc=src.cthc, 
                    target.cthr=src.cthr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.dscr=src.dscr, 
                    target.dscr_kw=src.dscr_kw, 
                    target.fadr=src.fadr, 
                    target.fmth=src.fmth, 
                    target.fmth_ref_compnr=src.fmth_ref_compnr, 
                    target.itcd=src.itcd, 
                    target.itcd_kw=src.itcd_kw, 
                    target.life=src.life, 
                    target.lmth=src.lmth, 
                    target.lmth_ref_compnr=src.lmth_ref_compnr, 
                    target.mmth=src.mmth, 
                    target.mmth_ref_compnr=src.mmth_ref_compnr, 
                    target.mskc=src.mskc, 
                    target.mskc_ref_compnr=src.mskc_ref_compnr, 
                    target.omth=src.omth, 
                    target.omth_ref_compnr=src.omth_ref_compnr, 
                    target.prop=src.prop, 
                    target.prop_ref_compnr=src.prop_ref_compnr, 
                    target.sctg=src.sctg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.smth=src.smth, 
                    target.smth_ref_compnr=src.smth_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.tmth=src.tmth, 
                    target.tmth_ref_compnr=src.tmth_ref_compnr, 
                    target.umth=src.umth, 
                    target.umth_ref_compnr=src.umth_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aexm, aexm_ref_compnr, agrp, agrp_ref_compnr, amth, amth_ref_compnr, anbm, anbm_ref_compnr, aslf, bslv, bslv_kw, cmth, cmth_ref_compnr, compnr, ctgy, cthc, cthr, deleted, desc, dscr, dscr_kw, fadr, fmth, fmth_ref_compnr, itcd, itcd_kw, life, lmth, lmth_ref_compnr, mmth, mmth_ref_compnr, mskc, mskc_ref_compnr, omth, omth_ref_compnr, prop, prop_ref_compnr, sctg, sequencenumber, smth, smth_ref_compnr, timestamp, tmth, tmth_ref_compnr, umth, umth_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aexm, 
                    src.aexm_ref_compnr, 
                    src.agrp, 
                    src.agrp_ref_compnr, 
                    src.amth, 
                    src.amth_ref_compnr, 
                    src.anbm, 
                    src.anbm_ref_compnr, 
                    src.aslf, 
                    src.bslv, 
                    src.bslv_kw, 
                    src.cmth, 
                    src.cmth_ref_compnr, 
                    src.compnr, 
                    src.ctgy, 
                    src.cthc, 
                    src.cthr, 
                    src.deleted, 
                    src.desc, 
                    src.dscr, 
                    src.dscr_kw, 
                    src.fadr, 
                    src.fmth, 
                    src.fmth_ref_compnr, 
                    src.itcd, 
                    src.itcd_kw, 
                    src.life, 
                    src.lmth, 
                    src.lmth_ref_compnr, 
                    src.mmth, 
                    src.mmth_ref_compnr, 
                    src.mskc, 
                    src.mskc_ref_compnr, 
                    src.omth, 
                    src.omth_ref_compnr, 
                    src.prop, 
                    src.prop_ref_compnr, 
                    src.sctg, 
                    src.sequencenumber, 
                    src.smth, 
                    src.smth_ref_compnr, 
                    src.timestamp, 
                    src.tmth, 
                    src.tmth_ref_compnr, 
                    src.umth, 
                    src.umth_ref_compnr, 
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
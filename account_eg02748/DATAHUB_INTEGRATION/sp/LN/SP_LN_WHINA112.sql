create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINA112()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINA112_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINA112 as target using (SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.bfbp, 
            strm.clot, 
            strm.compnr, 
            strm.cons, 
            strm.cons_kw, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.dlin, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.itid, 
            strm.itse, 
            strm.koor, 
            strm.koor_kw, 
            strm.lgdt, 
            strm.ocmp, 
            strm.ocmp_ref_compnr, 
            strm.orno, 
            strm.ownr, 
            strm.owns, 
            strm.owns_kw, 
            strm.phtr, 
            strm.phtr_kw, 
            strm.pono, 
            strm.qaiu, 
            strm.qhnd, 
            strm.qskt, 
            strm.qstk, 
            strm.reje, 
            strm.reje_kw, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.serl, 
            strm.srnb, 
            strm.tagn, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.vaco, 
            strm.vaco_kw, 
            strm.wvgr, 
            strm.wvgr_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item,
            strm.cwar,
            strm.trdt,
            strm.seqn,
            strm.inwp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA112 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.inwp=src.inwp) OR (target.inwp IS NULL AND src.inwp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.bfbp=src.bfbp, 
                    target.clot=src.clot, 
                    target.compnr=src.compnr, 
                    target.cons=src.cons, 
                    target.cons_kw=src.cons_kw, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlin=src.dlin, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.itid=src.itid, 
                    target.itse=src.itse, 
                    target.koor=src.koor, 
                    target.koor_kw=src.koor_kw, 
                    target.lgdt=src.lgdt, 
                    target.ocmp=src.ocmp, 
                    target.ocmp_ref_compnr=src.ocmp_ref_compnr, 
                    target.orno=src.orno, 
                    target.ownr=src.ownr, 
                    target.owns=src.owns, 
                    target.owns_kw=src.owns_kw, 
                    target.phtr=src.phtr, 
                    target.phtr_kw=src.phtr_kw, 
                    target.pono=src.pono, 
                    target.qaiu=src.qaiu, 
                    target.qhnd=src.qhnd, 
                    target.qskt=src.qskt, 
                    target.qstk=src.qstk, 
                    target.reje=src.reje, 
                    target.reje_kw=src.reje_kw, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.srnb=src.srnb, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.vaco=src.vaco, 
                    target.vaco_kw=src.vaco_kw, 
                    target.wvgr=src.wvgr, 
                    target.wvgr_ref_compnr=src.wvgr_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    atse, 
                    atse_ref_compnr, 
                    bfbp, 
                    clot, 
                    compnr, 
                    cons, 
                    cons_kw, 
                    cprj, 
                    cprj_ref_compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    dlin, 
                    inwp, 
                    inwp_kw, 
                    item, 
                    item_ref_compnr, 
                    itid, 
                    itse, 
                    koor, 
                    koor_kw, 
                    lgdt, 
                    ocmp, 
                    ocmp_ref_compnr, 
                    orno, 
                    ownr, 
                    owns, 
                    owns_kw, 
                    phtr, 
                    phtr_kw, 
                    pono, 
                    qaiu, 
                    qhnd, 
                    qskt, 
                    qstk, 
                    reje, 
                    reje_kw, 
                    seqn, 
                    sequencenumber, 
                    serl, 
                    srnb, 
                    tagn, 
                    timestamp, 
                    trdt, 
                    username, 
                    vaco, 
                    vaco_kw, 
                    wvgr, 
                    wvgr_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.bfbp, 
                    src.clot, 
                    src.compnr, 
                    src.cons, 
                    src.cons_kw, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.dlin, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.itid, 
                    src.itse, 
                    src.koor, 
                    src.koor_kw, 
                    src.lgdt, 
                    src.ocmp, 
                    src.ocmp_ref_compnr, 
                    src.orno, 
                    src.ownr, 
                    src.owns, 
                    src.owns_kw, 
                    src.phtr, 
                    src.phtr_kw, 
                    src.pono, 
                    src.qaiu, 
                    src.qhnd, 
                    src.qskt, 
                    src.qstk, 
                    src.reje, 
                    src.reje_kw, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.serl, 
                    src.srnb, 
                    src.tagn, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.vaco, 
                    src.vaco_kw, 
                    src.wvgr, 
                    src.wvgr_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINA112_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.bfbp, 
            strm.clot, 
            strm.compnr, 
            strm.cons, 
            strm.cons_kw, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.dlin, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.itid, 
            strm.itse, 
            strm.koor, 
            strm.koor_kw, 
            strm.lgdt, 
            strm.ocmp, 
            strm.ocmp_ref_compnr, 
            strm.orno, 
            strm.ownr, 
            strm.owns, 
            strm.owns_kw, 
            strm.phtr, 
            strm.phtr_kw, 
            strm.pono, 
            strm.qaiu, 
            strm.qhnd, 
            strm.qskt, 
            strm.qstk, 
            strm.reje, 
            strm.reje_kw, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.serl, 
            strm.srnb, 
            strm.tagn, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.vaco, 
            strm.vaco_kw, 
            strm.wvgr, 
            strm.wvgr_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item,
            strm.cwar,
            strm.trdt,
            strm.seqn,
            strm.inwp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA112 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.inwp=src.inwp) OR (target.inwp IS NULL AND src.inwp IS NULL)) 
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.bfbp=src.bfbp, 
                    target.clot=src.clot, 
                    target.compnr=src.compnr, 
                    target.cons=src.cons, 
                    target.cons_kw=src.cons_kw, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlin=src.dlin, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.itid=src.itid, 
                    target.itse=src.itse, 
                    target.koor=src.koor, 
                    target.koor_kw=src.koor_kw, 
                    target.lgdt=src.lgdt, 
                    target.ocmp=src.ocmp, 
                    target.ocmp_ref_compnr=src.ocmp_ref_compnr, 
                    target.orno=src.orno, 
                    target.ownr=src.ownr, 
                    target.owns=src.owns, 
                    target.owns_kw=src.owns_kw, 
                    target.phtr=src.phtr, 
                    target.phtr_kw=src.phtr_kw, 
                    target.pono=src.pono, 
                    target.qaiu=src.qaiu, 
                    target.qhnd=src.qhnd, 
                    target.qskt=src.qskt, 
                    target.qstk=src.qstk, 
                    target.reje=src.reje, 
                    target.reje_kw=src.reje_kw, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.srnb=src.srnb, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.vaco=src.vaco, 
                    target.vaco_kw=src.vaco_kw, 
                    target.wvgr=src.wvgr, 
                    target.wvgr_ref_compnr=src.wvgr_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( atse, atse_ref_compnr, bfbp, clot, compnr, cons, cons_kw, cprj, cprj_ref_compnr, cwar, cwar_ref_compnr, deleted, dlin, inwp, inwp_kw, item, item_ref_compnr, itid, itse, koor, koor_kw, lgdt, ocmp, ocmp_ref_compnr, orno, ownr, owns, owns_kw, phtr, phtr_kw, pono, qaiu, qhnd, qskt, qstk, reje, reje_kw, seqn, sequencenumber, serl, srnb, tagn, timestamp, trdt, username, vaco, vaco_kw, wvgr, wvgr_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.bfbp, 
                    src.clot, 
                    src.compnr, 
                    src.cons, 
                    src.cons_kw, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.dlin, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.itid, 
                    src.itse, 
                    src.koor, 
                    src.koor_kw, 
                    src.lgdt, 
                    src.ocmp, 
                    src.ocmp_ref_compnr, 
                    src.orno, 
                    src.ownr, 
                    src.owns, 
                    src.owns_kw, 
                    src.phtr, 
                    src.phtr_kw, 
                    src.pono, 
                    src.qaiu, 
                    src.qhnd, 
                    src.qskt, 
                    src.qstk, 
                    src.reje, 
                    src.reje_kw, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.serl, 
                    src.srnb, 
                    src.tagn, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.vaco, 
                    src.vaco_kw, 
                    src.wvgr, 
                    src.wvgr_ref_compnr,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINA116()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINA116_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINA116 as target using (SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.cbyi, 
            strm.cbyi_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cuso, 
            strm.cuso_kw, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.depc, 
            strm.depc_ref_compnr, 
            strm.dlin, 
            strm.iror, 
            strm.iror_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.ivsq, 
            strm.koor, 
            strm.koor_kw, 
            strm.lgdt, 
            strm.orno, 
            strm.owns, 
            strm.owns_kw, 
            strm.pono, 
            strm.prdt, 
            strm.proc, 
            strm.proc_kw, 
            strm.pyps, 
            strm.rcln, 
            strm.rcno, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.srnb, 
            strm.stkq, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.wdep, 
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
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA116 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.cbyi=src.cbyi, 
                    target.cbyi_kw=src.cbyi_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cuso=src.cuso, 
                    target.cuso_kw=src.cuso_kw, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.depc=src.depc, 
                    target.depc_ref_compnr=src.depc_ref_compnr, 
                    target.dlin=src.dlin, 
                    target.iror=src.iror, 
                    target.iror_kw=src.iror_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ivsq=src.ivsq, 
                    target.koor=src.koor, 
                    target.koor_kw=src.koor_kw, 
                    target.lgdt=src.lgdt, 
                    target.orno=src.orno, 
                    target.owns=src.owns, 
                    target.owns_kw=src.owns_kw, 
                    target.pono=src.pono, 
                    target.prdt=src.prdt, 
                    target.proc=src.proc, 
                    target.proc_kw=src.proc_kw, 
                    target.pyps=src.pyps, 
                    target.rcln=src.rcln, 
                    target.rcno=src.rcno, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.srnb=src.srnb, 
                    target.stkq=src.stkq, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.wdep=src.wdep, 
                    target.wvgr=src.wvgr, 
                    target.wvgr_ref_compnr=src.wvgr_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    atse, 
                    atse_ref_compnr, 
                    cbyi, 
                    cbyi_kw, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cprj, 
                    cprj_ref_compnr, 
                    cuso, 
                    cuso_kw, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    depc, 
                    depc_ref_compnr, 
                    dlin, 
                    iror, 
                    iror_kw, 
                    item, 
                    item_ref_compnr, 
                    ivsq, 
                    koor, 
                    koor_kw, 
                    lgdt, 
                    orno, 
                    owns, 
                    owns_kw, 
                    pono, 
                    prdt, 
                    proc, 
                    proc_kw, 
                    pyps, 
                    rcln, 
                    rcno, 
                    seqn, 
                    sequencenumber, 
                    srnb, 
                    stkq, 
                    timestamp, 
                    trdt, 
                    username, 
                    wdep, 
                    wvgr, 
                    wvgr_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.cbyi, 
                    src.cbyi_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cuso, 
                    src.cuso_kw, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.depc, 
                    src.depc_ref_compnr, 
                    src.dlin, 
                    src.iror, 
                    src.iror_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.ivsq, 
                    src.koor, 
                    src.koor_kw, 
                    src.lgdt, 
                    src.orno, 
                    src.owns, 
                    src.owns_kw, 
                    src.pono, 
                    src.prdt, 
                    src.proc, 
                    src.proc_kw, 
                    src.pyps, 
                    src.rcln, 
                    src.rcno, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.srnb, 
                    src.stkq, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.wdep, 
                    src.wvgr, 
                    src.wvgr_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINA116_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.cbyi, 
            strm.cbyi_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cuso, 
            strm.cuso_kw, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.depc, 
            strm.depc_ref_compnr, 
            strm.dlin, 
            strm.iror, 
            strm.iror_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.ivsq, 
            strm.koor, 
            strm.koor_kw, 
            strm.lgdt, 
            strm.orno, 
            strm.owns, 
            strm.owns_kw, 
            strm.pono, 
            strm.prdt, 
            strm.proc, 
            strm.proc_kw, 
            strm.pyps, 
            strm.rcln, 
            strm.rcno, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.srnb, 
            strm.stkq, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.wdep, 
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
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA116 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.cbyi=src.cbyi, 
                    target.cbyi_kw=src.cbyi_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cuso=src.cuso, 
                    target.cuso_kw=src.cuso_kw, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.depc=src.depc, 
                    target.depc_ref_compnr=src.depc_ref_compnr, 
                    target.dlin=src.dlin, 
                    target.iror=src.iror, 
                    target.iror_kw=src.iror_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ivsq=src.ivsq, 
                    target.koor=src.koor, 
                    target.koor_kw=src.koor_kw, 
                    target.lgdt=src.lgdt, 
                    target.orno=src.orno, 
                    target.owns=src.owns, 
                    target.owns_kw=src.owns_kw, 
                    target.pono=src.pono, 
                    target.prdt=src.prdt, 
                    target.proc=src.proc, 
                    target.proc_kw=src.proc_kw, 
                    target.pyps=src.pyps, 
                    target.rcln=src.rcln, 
                    target.rcno=src.rcno, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.srnb=src.srnb, 
                    target.stkq=src.stkq, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.wdep=src.wdep, 
                    target.wvgr=src.wvgr, 
                    target.wvgr_ref_compnr=src.wvgr_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( atse, atse_ref_compnr, cbyi, cbyi_kw, ccur, ccur_ref_compnr, compnr, cprj, cprj_ref_compnr, cuso, cuso_kw, cwar, cwar_ref_compnr, deleted, depc, depc_ref_compnr, dlin, iror, iror_kw, item, item_ref_compnr, ivsq, koor, koor_kw, lgdt, orno, owns, owns_kw, pono, prdt, proc, proc_kw, pyps, rcln, rcno, seqn, sequencenumber, srnb, stkq, timestamp, trdt, username, wdep, wvgr, wvgr_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.cbyi, 
                    src.cbyi_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cuso, 
                    src.cuso_kw, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.depc, 
                    src.depc_ref_compnr, 
                    src.dlin, 
                    src.iror, 
                    src.iror_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.ivsq, 
                    src.koor, 
                    src.koor_kw, 
                    src.lgdt, 
                    src.orno, 
                    src.owns, 
                    src.owns_kw, 
                    src.pono, 
                    src.prdt, 
                    src.proc, 
                    src.proc_kw, 
                    src.pyps, 
                    src.rcln, 
                    src.rcno, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.srnb, 
                    src.stkq, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.wdep, 
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
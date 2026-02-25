create or replace procedure DATAHUB_INTEGRATION.SP_LN_TIPCF500()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TIPCF500_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TIPCF500 as target using (SELECT * FROM (SELECT 
            strm.acfs, 
            strm.acfs_kw, 
            strm.acfv, 
            strm.acfv_kw, 
            strm.altn, 
            strm.alws, 
            strm.ccty, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpva, 
            strm.cuid, 
            strm.cuni, 
            strm.cuno, 
            strm.cuno_ref_compnr, 
            strm.cwar, 
            strm.deleted, 
            strm.dsca, 
            strm.enho, 
            strm.enho_kw, 
            strm.imag, 
            strm.irfo, 
            strm.irft, 
            strm.irft_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lnst, 
            strm.lnst_kw, 
            strm.olid, 
            strm.pcfd, 
            strm.prds, 
            strm.qana, 
            strm.refo, 
            strm.refp, 
            strm.reft, 
            strm.reft_kw, 
            strm.sequencenumber, 
            strm.site, 
            strm.slpr, 
            strm.sost, 
            strm.sost_kw, 
            strm.spcf, 
            strm.spcf_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vali, 
            strm.vali_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpva ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIPCF500 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpva=src.cpva) OR (target.cpva IS NULL AND src.cpva IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acfs=src.acfs, 
                    target.acfs_kw=src.acfs_kw, 
                    target.acfv=src.acfv, 
                    target.acfv_kw=src.acfv_kw, 
                    target.altn=src.altn, 
                    target.alws=src.alws, 
                    target.ccty=src.ccty, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpva=src.cpva, 
                    target.cuid=src.cuid, 
                    target.cuni=src.cuni, 
                    target.cuno=src.cuno, 
                    target.cuno_ref_compnr=src.cuno_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.enho=src.enho, 
                    target.enho_kw=src.enho_kw, 
                    target.imag=src.imag, 
                    target.irfo=src.irfo, 
                    target.irft=src.irft, 
                    target.irft_kw=src.irft_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lnst=src.lnst, 
                    target.lnst_kw=src.lnst_kw, 
                    target.olid=src.olid, 
                    target.pcfd=src.pcfd, 
                    target.prds=src.prds, 
                    target.qana=src.qana, 
                    target.refo=src.refo, 
                    target.refp=src.refp, 
                    target.reft=src.reft, 
                    target.reft_kw=src.reft_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.site=src.site, 
                    target.slpr=src.slpr, 
                    target.sost=src.sost, 
                    target.sost_kw=src.sost_kw, 
                    target.spcf=src.spcf, 
                    target.spcf_kw=src.spcf_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vali=src.vali, 
                    target.vali_kw=src.vali_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acfs, 
                    acfs_kw, 
                    acfv, 
                    acfv_kw, 
                    altn, 
                    alws, 
                    ccty, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cpva, 
                    cuid, 
                    cuni, 
                    cuno, 
                    cuno_ref_compnr, 
                    cwar, 
                    deleted, 
                    dsca, 
                    enho, 
                    enho_kw, 
                    imag, 
                    irfo, 
                    irft, 
                    irft_kw, 
                    item, 
                    item_ref_compnr, 
                    lnst, 
                    lnst_kw, 
                    olid, 
                    pcfd, 
                    prds, 
                    qana, 
                    refo, 
                    refp, 
                    reft, 
                    reft_kw, 
                    sequencenumber, 
                    site, 
                    slpr, 
                    sost, 
                    sost_kw, 
                    spcf, 
                    spcf_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    vali, 
                    vali_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acfs, 
                    src.acfs_kw, 
                    src.acfv, 
                    src.acfv_kw, 
                    src.altn, 
                    src.alws, 
                    src.ccty, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpva, 
                    src.cuid, 
                    src.cuni, 
                    src.cuno, 
                    src.cuno_ref_compnr, 
                    src.cwar, 
                    src.deleted, 
                    src.dsca, 
                    src.enho, 
                    src.enho_kw, 
                    src.imag, 
                    src.irfo, 
                    src.irft, 
                    src.irft_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lnst, 
                    src.lnst_kw, 
                    src.olid, 
                    src.pcfd, 
                    src.prds, 
                    src.qana, 
                    src.refo, 
                    src.refp, 
                    src.reft, 
                    src.reft_kw, 
                    src.sequencenumber, 
                    src.site, 
                    src.slpr, 
                    src.sost, 
                    src.sost_kw, 
                    src.spcf, 
                    src.spcf_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.vali, 
                    src.vali_kw,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TIPCF500_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acfs, 
            strm.acfs_kw, 
            strm.acfv, 
            strm.acfv_kw, 
            strm.altn, 
            strm.alws, 
            strm.ccty, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpva, 
            strm.cuid, 
            strm.cuni, 
            strm.cuno, 
            strm.cuno_ref_compnr, 
            strm.cwar, 
            strm.deleted, 
            strm.dsca, 
            strm.enho, 
            strm.enho_kw, 
            strm.imag, 
            strm.irfo, 
            strm.irft, 
            strm.irft_kw, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lnst, 
            strm.lnst_kw, 
            strm.olid, 
            strm.pcfd, 
            strm.prds, 
            strm.qana, 
            strm.refo, 
            strm.refp, 
            strm.reft, 
            strm.reft_kw, 
            strm.sequencenumber, 
            strm.site, 
            strm.slpr, 
            strm.sost, 
            strm.sost_kw, 
            strm.spcf, 
            strm.spcf_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vali, 
            strm.vali_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpva ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIPCF500 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpva=src.cpva) OR (target.cpva IS NULL AND src.cpva IS NULL)) 
                when matched then update set
                    target.acfs=src.acfs, 
                    target.acfs_kw=src.acfs_kw, 
                    target.acfv=src.acfv, 
                    target.acfv_kw=src.acfv_kw, 
                    target.altn=src.altn, 
                    target.alws=src.alws, 
                    target.ccty=src.ccty, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpva=src.cpva, 
                    target.cuid=src.cuid, 
                    target.cuni=src.cuni, 
                    target.cuno=src.cuno, 
                    target.cuno_ref_compnr=src.cuno_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.enho=src.enho, 
                    target.enho_kw=src.enho_kw, 
                    target.imag=src.imag, 
                    target.irfo=src.irfo, 
                    target.irft=src.irft, 
                    target.irft_kw=src.irft_kw, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lnst=src.lnst, 
                    target.lnst_kw=src.lnst_kw, 
                    target.olid=src.olid, 
                    target.pcfd=src.pcfd, 
                    target.prds=src.prds, 
                    target.qana=src.qana, 
                    target.refo=src.refo, 
                    target.refp=src.refp, 
                    target.reft=src.reft, 
                    target.reft_kw=src.reft_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.site=src.site, 
                    target.slpr=src.slpr, 
                    target.sost=src.sost, 
                    target.sost_kw=src.sost_kw, 
                    target.spcf=src.spcf, 
                    target.spcf_kw=src.spcf_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vali=src.vali, 
                    target.vali_kw=src.vali_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acfs, acfs_kw, acfv, acfv_kw, altn, alws, ccty, ccur, ccur_ref_compnr, compnr, cpva, cuid, cuni, cuno, cuno_ref_compnr, cwar, deleted, dsca, enho, enho_kw, imag, irfo, irft, irft_kw, item, item_ref_compnr, lnst, lnst_kw, olid, pcfd, prds, qana, refo, refp, reft, reft_kw, sequencenumber, site, slpr, sost, sost_kw, spcf, spcf_kw, timestamp, txta, txta_ref_compnr, username, vali, vali_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acfs, 
                    src.acfs_kw, 
                    src.acfv, 
                    src.acfv_kw, 
                    src.altn, 
                    src.alws, 
                    src.ccty, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpva, 
                    src.cuid, 
                    src.cuni, 
                    src.cuno, 
                    src.cuno_ref_compnr, 
                    src.cwar, 
                    src.deleted, 
                    src.dsca, 
                    src.enho, 
                    src.enho_kw, 
                    src.imag, 
                    src.irfo, 
                    src.irft, 
                    src.irft_kw, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lnst, 
                    src.lnst_kw, 
                    src.olid, 
                    src.pcfd, 
                    src.prds, 
                    src.qana, 
                    src.refo, 
                    src.refp, 
                    src.reft, 
                    src.reft_kw, 
                    src.sequencenumber, 
                    src.site, 
                    src.slpr, 
                    src.sost, 
                    src.sost_kw, 
                    src.spcf, 
                    src.spcf_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.vali, 
                    src.vali_kw,     
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
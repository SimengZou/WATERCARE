create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINH600()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINH600_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINH600 as target using (SELECT * FROM (SELECT 
            strm.boml, 
            strm.cdor, 
            strm.cdos, 
            strm.cdos_kw, 
            strm.cdpd, 
            strm.cdpd_ref_compnr, 
            strm.cdty, 
            strm.cdty_kw, 
            strm.cdun, 
            strm.cdun_ref_compnr, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.cwar_stlo_ref_compnr, 
            strm.deleted, 
            strm.dlin, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lcdt, 
            strm.oorg, 
            strm.oorg_kw, 
            strm.orno, 
            strm.oset, 
            strm.pddt, 
            strm.pono, 
            strm.qact, 
            strm.qadv, 
            strm.qapp, 
            strm.qpla, 
            strm.qreq, 
            strm.qrun, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.stlo, 
            strm.sypr, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.uspr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cdor ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH600 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cdor=src.cdor) OR (target.cdor IS NULL AND src.cdor IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.boml=src.boml, 
                    target.cdor=src.cdor, 
                    target.cdos=src.cdos, 
                    target.cdos_kw=src.cdos_kw, 
                    target.cdpd=src.cdpd, 
                    target.cdpd_ref_compnr=src.cdpd_ref_compnr, 
                    target.cdty=src.cdty, 
                    target.cdty_kw=src.cdty_kw, 
                    target.cdun=src.cdun, 
                    target.cdun_ref_compnr=src.cdun_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.cwar_stlo_ref_compnr=src.cwar_stlo_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlin=src.dlin, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lcdt=src.lcdt, 
                    target.oorg=src.oorg, 
                    target.oorg_kw=src.oorg_kw, 
                    target.orno=src.orno, 
                    target.oset=src.oset, 
                    target.pddt=src.pddt, 
                    target.pono=src.pono, 
                    target.qact=src.qact, 
                    target.qadv=src.qadv, 
                    target.qapp=src.qapp, 
                    target.qpla=src.qpla, 
                    target.qreq=src.qreq, 
                    target.qrun=src.qrun, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stlo=src.stlo, 
                    target.sypr=src.sypr, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.uspr=src.uspr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    boml, 
                    cdor, 
                    cdos, 
                    cdos_kw, 
                    cdpd, 
                    cdpd_ref_compnr, 
                    cdty, 
                    cdty_kw, 
                    cdun, 
                    cdun_ref_compnr, 
                    compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    cwar_stlo_ref_compnr, 
                    deleted, 
                    dlin, 
                    item, 
                    item_ref_compnr, 
                    lcdt, 
                    oorg, 
                    oorg_kw, 
                    orno, 
                    oset, 
                    pddt, 
                    pono, 
                    qact, 
                    qadv, 
                    qapp, 
                    qpla, 
                    qreq, 
                    qrun, 
                    seqn, 
                    sequencenumber, 
                    stlo, 
                    sypr, 
                    timestamp, 
                    user, 
                    username, 
                    uspr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.boml, 
                    src.cdor, 
                    src.cdos, 
                    src.cdos_kw, 
                    src.cdpd, 
                    src.cdpd_ref_compnr, 
                    src.cdty, 
                    src.cdty_kw, 
                    src.cdun, 
                    src.cdun_ref_compnr, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.cwar_stlo_ref_compnr, 
                    src.deleted, 
                    src.dlin, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lcdt, 
                    src.oorg, 
                    src.oorg_kw, 
                    src.orno, 
                    src.oset, 
                    src.pddt, 
                    src.pono, 
                    src.qact, 
                    src.qadv, 
                    src.qapp, 
                    src.qpla, 
                    src.qreq, 
                    src.qrun, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.stlo, 
                    src.sypr, 
                    src.timestamp, 
                    src.user, 
                    src.username, 
                    src.uspr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINH600_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.boml, 
            strm.cdor, 
            strm.cdos, 
            strm.cdos_kw, 
            strm.cdpd, 
            strm.cdpd_ref_compnr, 
            strm.cdty, 
            strm.cdty_kw, 
            strm.cdun, 
            strm.cdun_ref_compnr, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.cwar_stlo_ref_compnr, 
            strm.deleted, 
            strm.dlin, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lcdt, 
            strm.oorg, 
            strm.oorg_kw, 
            strm.orno, 
            strm.oset, 
            strm.pddt, 
            strm.pono, 
            strm.qact, 
            strm.qadv, 
            strm.qapp, 
            strm.qpla, 
            strm.qreq, 
            strm.qrun, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.stlo, 
            strm.sypr, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.uspr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cdor ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH600 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cdor=src.cdor) OR (target.cdor IS NULL AND src.cdor IS NULL)) 
                when matched then update set
                    target.boml=src.boml, 
                    target.cdor=src.cdor, 
                    target.cdos=src.cdos, 
                    target.cdos_kw=src.cdos_kw, 
                    target.cdpd=src.cdpd, 
                    target.cdpd_ref_compnr=src.cdpd_ref_compnr, 
                    target.cdty=src.cdty, 
                    target.cdty_kw=src.cdty_kw, 
                    target.cdun=src.cdun, 
                    target.cdun_ref_compnr=src.cdun_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.cwar_stlo_ref_compnr=src.cwar_stlo_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dlin=src.dlin, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lcdt=src.lcdt, 
                    target.oorg=src.oorg, 
                    target.oorg_kw=src.oorg_kw, 
                    target.orno=src.orno, 
                    target.oset=src.oset, 
                    target.pddt=src.pddt, 
                    target.pono=src.pono, 
                    target.qact=src.qact, 
                    target.qadv=src.qadv, 
                    target.qapp=src.qapp, 
                    target.qpla=src.qpla, 
                    target.qreq=src.qreq, 
                    target.qrun=src.qrun, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stlo=src.stlo, 
                    target.sypr=src.sypr, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.uspr=src.uspr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( boml, cdor, cdos, cdos_kw, cdpd, cdpd_ref_compnr, cdty, cdty_kw, cdun, cdun_ref_compnr, compnr, cwar, cwar_ref_compnr, cwar_stlo_ref_compnr, deleted, dlin, item, item_ref_compnr, lcdt, oorg, oorg_kw, orno, oset, pddt, pono, qact, qadv, qapp, qpla, qreq, qrun, seqn, sequencenumber, stlo, sypr, timestamp, user, username, uspr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.boml, 
                    src.cdor, 
                    src.cdos, 
                    src.cdos_kw, 
                    src.cdpd, 
                    src.cdpd_ref_compnr, 
                    src.cdty, 
                    src.cdty_kw, 
                    src.cdun, 
                    src.cdun_ref_compnr, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.cwar_stlo_ref_compnr, 
                    src.deleted, 
                    src.dlin, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lcdt, 
                    src.oorg, 
                    src.oorg_kw, 
                    src.orno, 
                    src.oset, 
                    src.pddt, 
                    src.pono, 
                    src.qact, 
                    src.qadv, 
                    src.qapp, 
                    src.qpla, 
                    src.qreq, 
                    src.qrun, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.stlo, 
                    src.sypr, 
                    src.timestamp, 
                    src.user, 
                    src.username, 
                    src.uspr,     
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
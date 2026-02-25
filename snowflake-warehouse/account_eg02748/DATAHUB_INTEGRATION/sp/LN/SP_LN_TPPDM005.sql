create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPDM005()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPDM005_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPDM005 as target using (SELECT * FROM (SELECT 
            strm.blbl, 
            strm.blbl_kw, 
            strm.ccfu, 
            strm.ccfu_kw, 
            strm.ccit, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.copt, 
            strm.copt_kw, 
            strm.cpmc_1, 
            strm.cpmc_2, 
            strm.cpmc_3, 
            strm.cppp, 
            strm.cppp_kw, 
            strm.cprp, 
            strm.cuti, 
            strm.cuti_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.ltcp, 
            strm.osys, 
            strm.osys_kw, 
            strm.pgwh, 
            strm.pgwh_kw, 
            strm.prre, 
            strm.prre_kw, 
            strm.seak, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.usyn, 
            strm.usyn_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM005 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.ccfu=src.ccfu, 
                    target.ccfu_kw=src.ccfu_kw, 
                    target.ccit=src.ccit, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.copt=src.copt, 
                    target.copt_kw=src.copt_kw, 
                    target.cpmc_1=src.cpmc_1, 
                    target.cpmc_2=src.cpmc_2, 
                    target.cpmc_3=src.cpmc_3, 
                    target.cppp=src.cppp, 
                    target.cppp_kw=src.cppp_kw, 
                    target.cprp=src.cprp, 
                    target.cuti=src.cuti, 
                    target.cuti_ref_compnr=src.cuti_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ltcp=src.ltcp, 
                    target.osys=src.osys, 
                    target.osys_kw=src.osys_kw, 
                    target.pgwh=src.pgwh, 
                    target.pgwh_kw=src.pgwh_kw, 
                    target.prre=src.prre, 
                    target.prre_kw=src.prre_kw, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.usyn=src.usyn, 
                    target.usyn_kw=src.usyn_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    blbl, 
                    blbl_kw, 
                    ccfu, 
                    ccfu_kw, 
                    ccit, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    copt, 
                    copt_kw, 
                    cpmc_1, 
                    cpmc_2, 
                    cpmc_3, 
                    cppp, 
                    cppp_kw, 
                    cprp, 
                    cuti, 
                    cuti_ref_compnr, 
                    deleted, 
                    item, 
                    item_ref_compnr, 
                    ltcp, 
                    osys, 
                    osys_kw, 
                    pgwh, 
                    pgwh_kw, 
                    prre, 
                    prre_kw, 
                    seak, 
                    sequencenumber, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    usyn, 
                    usyn_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.blbl, 
                    src.blbl_kw, 
                    src.ccfu, 
                    src.ccfu_kw, 
                    src.ccit, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.copt, 
                    src.copt_kw, 
                    src.cpmc_1, 
                    src.cpmc_2, 
                    src.cpmc_3, 
                    src.cppp, 
                    src.cppp_kw, 
                    src.cprp, 
                    src.cuti, 
                    src.cuti_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.ltcp, 
                    src.osys, 
                    src.osys_kw, 
                    src.pgwh, 
                    src.pgwh_kw, 
                    src.prre, 
                    src.prre_kw, 
                    src.seak, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.usyn, 
                    src.usyn_kw,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPDM005_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.blbl, 
            strm.blbl_kw, 
            strm.ccfu, 
            strm.ccfu_kw, 
            strm.ccit, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.copt, 
            strm.copt_kw, 
            strm.cpmc_1, 
            strm.cpmc_2, 
            strm.cpmc_3, 
            strm.cppp, 
            strm.cppp_kw, 
            strm.cprp, 
            strm.cuti, 
            strm.cuti_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.ltcp, 
            strm.osys, 
            strm.osys_kw, 
            strm.pgwh, 
            strm.pgwh_kw, 
            strm.prre, 
            strm.prre_kw, 
            strm.seak, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.usyn, 
            strm.usyn_kw, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM005 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) 
                when matched then update set
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.ccfu=src.ccfu, 
                    target.ccfu_kw=src.ccfu_kw, 
                    target.ccit=src.ccit, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.copt=src.copt, 
                    target.copt_kw=src.copt_kw, 
                    target.cpmc_1=src.cpmc_1, 
                    target.cpmc_2=src.cpmc_2, 
                    target.cpmc_3=src.cpmc_3, 
                    target.cppp=src.cppp, 
                    target.cppp_kw=src.cppp_kw, 
                    target.cprp=src.cprp, 
                    target.cuti=src.cuti, 
                    target.cuti_ref_compnr=src.cuti_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ltcp=src.ltcp, 
                    target.osys=src.osys, 
                    target.osys_kw=src.osys_kw, 
                    target.pgwh=src.pgwh, 
                    target.pgwh_kw=src.pgwh_kw, 
                    target.prre=src.prre, 
                    target.prre_kw=src.prre_kw, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.usyn=src.usyn, 
                    target.usyn_kw=src.usyn_kw, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( blbl, blbl_kw, ccfu, ccfu_kw, ccit, ccur, ccur_ref_compnr, compnr, copt, copt_kw, cpmc_1, cpmc_2, cpmc_3, cppp, cppp_kw, cprp, cuti, cuti_ref_compnr, deleted, item, item_ref_compnr, ltcp, osys, osys_kw, pgwh, pgwh_kw, prre, prre_kw, seak, sequencenumber, timestamp, txta, txta_ref_compnr, username, usyn, usyn_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.blbl, 
                    src.blbl_kw, 
                    src.ccfu, 
                    src.ccfu_kw, 
                    src.ccit, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.copt, 
                    src.copt_kw, 
                    src.cpmc_1, 
                    src.cpmc_2, 
                    src.cpmc_3, 
                    src.cppp, 
                    src.cppp_kw, 
                    src.cprp, 
                    src.cuti, 
                    src.cuti_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.ltcp, 
                    src.osys, 
                    src.osys_kw, 
                    src.pgwh, 
                    src.pgwh_kw, 
                    src.prre, 
                    src.prre_kw, 
                    src.seak, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.usyn, 
                    src.usyn_kw,     
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
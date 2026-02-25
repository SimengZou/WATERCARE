create or replace procedure DATAHUB_INTEGRATION.SP_LN_TIROU101()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TIROU101_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TIROU101 as target using (SELECT * FROM (SELECT 
            strm.bwca, 
            strm.bwca_ref_compnr, 
            strm.bwce, 
            strm.bwce_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.lcdr, 
            strm.lmdt, 
            strm.mitm, 
            strm.mitm_ref_compnr, 
            strm.mrau, 
            strm.mrau_kw, 
            strm.opro, 
            strm.ract, 
            strm.rcal, 
            strm.runi, 
            strm.ruoq, 
            strm.sequencenumber, 
            strm.stcf, 
            strm.stcf_kw, 
            strm.stor, 
            strm.stor_kw, 
            strm.strc, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.unef, 
            strm.unef_kw, 
            strm.username, 
            strm.wkbt, 
            strm.wkbt_kw, 
            strm.woar, 
            strm.woar_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.mitm,
            strm.opro ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIROU101 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.mitm=src.mitm) OR (target.mitm IS NULL AND src.mitm IS NULL)) AND
            ((target.opro=src.opro) OR (target.opro IS NULL AND src.opro IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bwca=src.bwca, 
                    target.bwca_ref_compnr=src.bwca_ref_compnr, 
                    target.bwce=src.bwce, 
                    target.bwce_ref_compnr=src.bwce_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.lcdr=src.lcdr, 
                    target.lmdt=src.lmdt, 
                    target.mitm=src.mitm, 
                    target.mitm_ref_compnr=src.mitm_ref_compnr, 
                    target.mrau=src.mrau, 
                    target.mrau_kw=src.mrau_kw, 
                    target.opro=src.opro, 
                    target.ract=src.ract, 
                    target.rcal=src.rcal, 
                    target.runi=src.runi, 
                    target.ruoq=src.ruoq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stcf=src.stcf, 
                    target.stcf_kw=src.stcf_kw, 
                    target.stor=src.stor, 
                    target.stor_kw=src.stor_kw, 
                    target.strc=src.strc, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.unef=src.unef, 
                    target.unef_kw=src.unef_kw, 
                    target.username=src.username, 
                    target.wkbt=src.wkbt, 
                    target.wkbt_kw=src.wkbt_kw, 
                    target.woar=src.woar, 
                    target.woar_ref_compnr=src.woar_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bwca, 
                    bwca_ref_compnr, 
                    bwce, 
                    bwce_ref_compnr, 
                    compnr, 
                    deleted, 
                    dsca, 
                    lcdr, 
                    lmdt, 
                    mitm, 
                    mitm_ref_compnr, 
                    mrau, 
                    mrau_kw, 
                    opro, 
                    ract, 
                    rcal, 
                    runi, 
                    ruoq, 
                    sequencenumber, 
                    stcf, 
                    stcf_kw, 
                    stor, 
                    stor_kw, 
                    strc, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    unef, 
                    unef_kw, 
                    username, 
                    wkbt, 
                    wkbt_kw, 
                    woar, 
                    woar_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bwca, 
                    src.bwca_ref_compnr, 
                    src.bwce, 
                    src.bwce_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.lcdr, 
                    src.lmdt, 
                    src.mitm, 
                    src.mitm_ref_compnr, 
                    src.mrau, 
                    src.mrau_kw, 
                    src.opro, 
                    src.ract, 
                    src.rcal, 
                    src.runi, 
                    src.ruoq, 
                    src.sequencenumber, 
                    src.stcf, 
                    src.stcf_kw, 
                    src.stor, 
                    src.stor_kw, 
                    src.strc, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.unef, 
                    src.unef_kw, 
                    src.username, 
                    src.wkbt, 
                    src.wkbt_kw, 
                    src.woar, 
                    src.woar_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TIROU101_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bwca, 
            strm.bwca_ref_compnr, 
            strm.bwce, 
            strm.bwce_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.lcdr, 
            strm.lmdt, 
            strm.mitm, 
            strm.mitm_ref_compnr, 
            strm.mrau, 
            strm.mrau_kw, 
            strm.opro, 
            strm.ract, 
            strm.rcal, 
            strm.runi, 
            strm.ruoq, 
            strm.sequencenumber, 
            strm.stcf, 
            strm.stcf_kw, 
            strm.stor, 
            strm.stor_kw, 
            strm.strc, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.unef, 
            strm.unef_kw, 
            strm.username, 
            strm.wkbt, 
            strm.wkbt_kw, 
            strm.woar, 
            strm.woar_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.mitm,
            strm.opro ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIROU101 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.mitm=src.mitm) OR (target.mitm IS NULL AND src.mitm IS NULL)) AND
            ((target.opro=src.opro) OR (target.opro IS NULL AND src.opro IS NULL)) 
                when matched then update set
                    target.bwca=src.bwca, 
                    target.bwca_ref_compnr=src.bwca_ref_compnr, 
                    target.bwce=src.bwce, 
                    target.bwce_ref_compnr=src.bwce_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.lcdr=src.lcdr, 
                    target.lmdt=src.lmdt, 
                    target.mitm=src.mitm, 
                    target.mitm_ref_compnr=src.mitm_ref_compnr, 
                    target.mrau=src.mrau, 
                    target.mrau_kw=src.mrau_kw, 
                    target.opro=src.opro, 
                    target.ract=src.ract, 
                    target.rcal=src.rcal, 
                    target.runi=src.runi, 
                    target.ruoq=src.ruoq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stcf=src.stcf, 
                    target.stcf_kw=src.stcf_kw, 
                    target.stor=src.stor, 
                    target.stor_kw=src.stor_kw, 
                    target.strc=src.strc, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.unef=src.unef, 
                    target.unef_kw=src.unef_kw, 
                    target.username=src.username, 
                    target.wkbt=src.wkbt, 
                    target.wkbt_kw=src.wkbt_kw, 
                    target.woar=src.woar, 
                    target.woar_ref_compnr=src.woar_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bwca, bwca_ref_compnr, bwce, bwce_ref_compnr, compnr, deleted, dsca, lcdr, lmdt, mitm, mitm_ref_compnr, mrau, mrau_kw, opro, ract, rcal, runi, ruoq, sequencenumber, stcf, stcf_kw, stor, stor_kw, strc, timestamp, txta, txta_ref_compnr, unef, unef_kw, username, wkbt, wkbt_kw, woar, woar_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bwca, 
                    src.bwca_ref_compnr, 
                    src.bwce, 
                    src.bwce_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.lcdr, 
                    src.lmdt, 
                    src.mitm, 
                    src.mitm_ref_compnr, 
                    src.mrau, 
                    src.mrau_kw, 
                    src.opro, 
                    src.ract, 
                    src.rcal, 
                    src.runi, 
                    src.ruoq, 
                    src.sequencenumber, 
                    src.stcf, 
                    src.stcf_kw, 
                    src.stor, 
                    src.stor_kw, 
                    src.strc, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.unef, 
                    src.unef_kw, 
                    src.username, 
                    src.wkbt, 
                    src.wkbt_kw, 
                    src.woar, 
                    src.woar_ref_compnr,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM320()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM320_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM320 as target using (SELECT * FROM (SELECT 
            strm.actn, 
            strm.actn_kw, 
            strm.amdy, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_refc, 
            strm.cchn, 
            strm.chdt, 
            strm.cind, 
            strm.cind_ref_compnr, 
            strm.cody_1, 
            strm.cody_2, 
            strm.cody_3, 
            strm.compnr, 
            strm.crtm, 
            strm.csco, 
            strm.csco_ref_compnr, 
            strm.csmt, 
            strm.deleted, 
            strm.desc, 
            strm.efdt, 
            strm.erfa, 
            strm.exdt, 
            strm.icpn, 
            strm.inpc, 
            strm.nrpe, 
            strm.ochn, 
            strm.peru, 
            strm.peru_kw, 
            strm.rsmt, 
            strm.rsmt_dwhc, 
            strm.rsmt_homc, 
            strm.rsmt_refc, 
            strm.rsmt_rpc1, 
            strm.rsmt_rpc2, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.term, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM320 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.actn=src.actn, 
                    target.actn_kw=src.actn_kw, 
                    target.amdy=src.amdy, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_refc=src.camt_refc, 
                    target.cchn=src.cchn, 
                    target.chdt=src.chdt, 
                    target.cind=src.cind, 
                    target.cind_ref_compnr=src.cind_ref_compnr, 
                    target.cody_1=src.cody_1, 
                    target.cody_2=src.cody_2, 
                    target.cody_3=src.cody_3, 
                    target.compnr=src.compnr, 
                    target.crtm=src.crtm, 
                    target.csco=src.csco, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.csmt=src.csmt, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.efdt=src.efdt, 
                    target.erfa=src.erfa, 
                    target.exdt=src.exdt, 
                    target.icpn=src.icpn, 
                    target.inpc=src.inpc, 
                    target.nrpe=src.nrpe, 
                    target.ochn=src.ochn, 
                    target.peru=src.peru, 
                    target.peru_kw=src.peru_kw, 
                    target.rsmt=src.rsmt, 
                    target.rsmt_dwhc=src.rsmt_dwhc, 
                    target.rsmt_homc=src.rsmt_homc, 
                    target.rsmt_refc=src.rsmt_refc, 
                    target.rsmt_rpc1=src.rsmt_rpc1, 
                    target.rsmt_rpc2=src.rsmt_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.term=src.term, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    actn, 
                    actn_kw, 
                    amdy, 
                    camt_1, 
                    camt_2, 
                    camt_3, 
                    camt_cntc, 
                    camt_dwhc, 
                    camt_refc, 
                    cchn, 
                    chdt, 
                    cind, 
                    cind_ref_compnr, 
                    cody_1, 
                    cody_2, 
                    cody_3, 
                    compnr, 
                    crtm, 
                    csco, 
                    csco_ref_compnr, 
                    csmt, 
                    deleted, 
                    desc, 
                    efdt, 
                    erfa, 
                    exdt, 
                    icpn, 
                    inpc, 
                    nrpe, 
                    ochn, 
                    peru, 
                    peru_kw, 
                    rsmt, 
                    rsmt_dwhc, 
                    rsmt_homc, 
                    rsmt_refc, 
                    rsmt_rpc1, 
                    rsmt_rpc2, 
                    sequencenumber, 
                    stat, 
                    stat_kw, 
                    term, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.actn, 
                    src.actn_kw, 
                    src.amdy, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_refc, 
                    src.cchn, 
                    src.chdt, 
                    src.cind, 
                    src.cind_ref_compnr, 
                    src.cody_1, 
                    src.cody_2, 
                    src.cody_3, 
                    src.compnr, 
                    src.crtm, 
                    src.csco, 
                    src.csco_ref_compnr, 
                    src.csmt, 
                    src.deleted, 
                    src.desc, 
                    src.efdt, 
                    src.erfa, 
                    src.exdt, 
                    src.icpn, 
                    src.inpc, 
                    src.nrpe, 
                    src.ochn, 
                    src.peru, 
                    src.peru_kw, 
                    src.rsmt, 
                    src.rsmt_dwhc, 
                    src.rsmt_homc, 
                    src.rsmt_refc, 
                    src.rsmt_rpc1, 
                    src.rsmt_rpc2, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.term, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM320_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.actn, 
            strm.actn_kw, 
            strm.amdy, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.camt_cntc, 
            strm.camt_dwhc, 
            strm.camt_refc, 
            strm.cchn, 
            strm.chdt, 
            strm.cind, 
            strm.cind_ref_compnr, 
            strm.cody_1, 
            strm.cody_2, 
            strm.cody_3, 
            strm.compnr, 
            strm.crtm, 
            strm.csco, 
            strm.csco_ref_compnr, 
            strm.csmt, 
            strm.deleted, 
            strm.desc, 
            strm.efdt, 
            strm.erfa, 
            strm.exdt, 
            strm.icpn, 
            strm.inpc, 
            strm.nrpe, 
            strm.ochn, 
            strm.peru, 
            strm.peru_kw, 
            strm.rsmt, 
            strm.rsmt_dwhc, 
            strm.rsmt_homc, 
            strm.rsmt_refc, 
            strm.rsmt_rpc1, 
            strm.rsmt_rpc2, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.term, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM320 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) 
                when matched then update set
                    target.actn=src.actn, 
                    target.actn_kw=src.actn_kw, 
                    target.amdy=src.amdy, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.camt_cntc=src.camt_cntc, 
                    target.camt_dwhc=src.camt_dwhc, 
                    target.camt_refc=src.camt_refc, 
                    target.cchn=src.cchn, 
                    target.chdt=src.chdt, 
                    target.cind=src.cind, 
                    target.cind_ref_compnr=src.cind_ref_compnr, 
                    target.cody_1=src.cody_1, 
                    target.cody_2=src.cody_2, 
                    target.cody_3=src.cody_3, 
                    target.compnr=src.compnr, 
                    target.crtm=src.crtm, 
                    target.csco=src.csco, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.csmt=src.csmt, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.efdt=src.efdt, 
                    target.erfa=src.erfa, 
                    target.exdt=src.exdt, 
                    target.icpn=src.icpn, 
                    target.inpc=src.inpc, 
                    target.nrpe=src.nrpe, 
                    target.ochn=src.ochn, 
                    target.peru=src.peru, 
                    target.peru_kw=src.peru_kw, 
                    target.rsmt=src.rsmt, 
                    target.rsmt_dwhc=src.rsmt_dwhc, 
                    target.rsmt_homc=src.rsmt_homc, 
                    target.rsmt_refc=src.rsmt_refc, 
                    target.rsmt_rpc1=src.rsmt_rpc1, 
                    target.rsmt_rpc2=src.rsmt_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.term=src.term, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( actn, actn_kw, amdy, camt_1, camt_2, camt_3, camt_cntc, camt_dwhc, camt_refc, cchn, chdt, cind, cind_ref_compnr, cody_1, cody_2, cody_3, compnr, crtm, csco, csco_ref_compnr, csmt, deleted, desc, efdt, erfa, exdt, icpn, inpc, nrpe, ochn, peru, peru_kw, rsmt, rsmt_dwhc, rsmt_homc, rsmt_refc, rsmt_rpc1, rsmt_rpc2, sequencenumber, stat, stat_kw, term, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.actn, 
                    src.actn_kw, 
                    src.amdy, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.camt_cntc, 
                    src.camt_dwhc, 
                    src.camt_refc, 
                    src.cchn, 
                    src.chdt, 
                    src.cind, 
                    src.cind_ref_compnr, 
                    src.cody_1, 
                    src.cody_2, 
                    src.cody_3, 
                    src.compnr, 
                    src.crtm, 
                    src.csco, 
                    src.csco_ref_compnr, 
                    src.csmt, 
                    src.deleted, 
                    src.desc, 
                    src.efdt, 
                    src.erfa, 
                    src.exdt, 
                    src.icpn, 
                    src.inpc, 
                    src.nrpe, 
                    src.ochn, 
                    src.peru, 
                    src.peru_kw, 
                    src.rsmt, 
                    src.rsmt_dwhc, 
                    src.rsmt_homc, 
                    src.rsmt_refc, 
                    src.rsmt_rpc1, 
                    src.rsmt_rpc2, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.term, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
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
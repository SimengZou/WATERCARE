create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPDM110()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPDM110_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPDM110 as target using (SELECT * FROM (SELECT 
            strm.afrt, 
            strm.afrt_kw, 
            strm.blbl, 
            strm.blbl_kw, 
            strm.cact, 
            strm.capt, 
            strm.capt_kw, 
            strm.compnr, 
            strm.cspt, 
            strm.cspt_kw, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.cuti, 
            strm.cuti_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.eoty, 
            strm.eoty_kw, 
            strm.freq, 
            strm.lvps, 
            strm.lvps_kw, 
            strm.mevl, 
            strm.mevl_kw, 
            strm.plmc, 
            strm.plmc_ref_compnr, 
            strm.plmp, 
            strm.prin, 
            strm.prin_kw, 
            strm.prms, 
            strm.prnd, 
            strm.prsp, 
            strm.prst, 
            strm.rent, 
            strm.rent_kw, 
            strm.rfac, 
            strm.rout, 
            strm.seak, 
            strm.sequencenumber, 
            strm.setl, 
            strm.setl_kw, 
            strm.tact, 
            strm.tact_kw, 
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
            strm.cact ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM110 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.afrt=src.afrt, 
                    target.afrt_kw=src.afrt_kw, 
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.cact=src.cact, 
                    target.capt=src.capt, 
                    target.capt_kw=src.capt_kw, 
                    target.compnr=src.compnr, 
                    target.cspt=src.cspt, 
                    target.cspt_kw=src.cspt_kw, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.cuti=src.cuti, 
                    target.cuti_ref_compnr=src.cuti_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.eoty=src.eoty, 
                    target.eoty_kw=src.eoty_kw, 
                    target.freq=src.freq, 
                    target.lvps=src.lvps, 
                    target.lvps_kw=src.lvps_kw, 
                    target.mevl=src.mevl, 
                    target.mevl_kw=src.mevl_kw, 
                    target.plmc=src.plmc, 
                    target.plmc_ref_compnr=src.plmc_ref_compnr, 
                    target.plmp=src.plmp, 
                    target.prin=src.prin, 
                    target.prin_kw=src.prin_kw, 
                    target.prms=src.prms, 
                    target.prnd=src.prnd, 
                    target.prsp=src.prsp, 
                    target.prst=src.prst, 
                    target.rent=src.rent, 
                    target.rent_kw=src.rent_kw, 
                    target.rfac=src.rfac, 
                    target.rout=src.rout, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.setl=src.setl, 
                    target.setl_kw=src.setl_kw, 
                    target.tact=src.tact, 
                    target.tact_kw=src.tact_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    afrt, 
                    afrt_kw, 
                    blbl, 
                    blbl_kw, 
                    cact, 
                    capt, 
                    capt_kw, 
                    compnr, 
                    cspt, 
                    cspt_kw, 
                    cuni, 
                    cuni_ref_compnr, 
                    cuti, 
                    cuti_ref_compnr, 
                    deleted, 
                    desc, 
                    eoty, 
                    eoty_kw, 
                    freq, 
                    lvps, 
                    lvps_kw, 
                    mevl, 
                    mevl_kw, 
                    plmc, 
                    plmc_ref_compnr, 
                    plmp, 
                    prin, 
                    prin_kw, 
                    prms, 
                    prnd, 
                    prsp, 
                    prst, 
                    rent, 
                    rent_kw, 
                    rfac, 
                    rout, 
                    seak, 
                    sequencenumber, 
                    setl, 
                    setl_kw, 
                    tact, 
                    tact_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.afrt, 
                    src.afrt_kw, 
                    src.blbl, 
                    src.blbl_kw, 
                    src.cact, 
                    src.capt, 
                    src.capt_kw, 
                    src.compnr, 
                    src.cspt, 
                    src.cspt_kw, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.cuti, 
                    src.cuti_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.eoty, 
                    src.eoty_kw, 
                    src.freq, 
                    src.lvps, 
                    src.lvps_kw, 
                    src.mevl, 
                    src.mevl_kw, 
                    src.plmc, 
                    src.plmc_ref_compnr, 
                    src.plmp, 
                    src.prin, 
                    src.prin_kw, 
                    src.prms, 
                    src.prnd, 
                    src.prsp, 
                    src.prst, 
                    src.rent, 
                    src.rent_kw, 
                    src.rfac, 
                    src.rout, 
                    src.seak, 
                    src.sequencenumber, 
                    src.setl, 
                    src.setl_kw, 
                    src.tact, 
                    src.tact_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPDM110_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.afrt, 
            strm.afrt_kw, 
            strm.blbl, 
            strm.blbl_kw, 
            strm.cact, 
            strm.capt, 
            strm.capt_kw, 
            strm.compnr, 
            strm.cspt, 
            strm.cspt_kw, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.cuti, 
            strm.cuti_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.eoty, 
            strm.eoty_kw, 
            strm.freq, 
            strm.lvps, 
            strm.lvps_kw, 
            strm.mevl, 
            strm.mevl_kw, 
            strm.plmc, 
            strm.plmc_ref_compnr, 
            strm.plmp, 
            strm.prin, 
            strm.prin_kw, 
            strm.prms, 
            strm.prnd, 
            strm.prsp, 
            strm.prst, 
            strm.rent, 
            strm.rent_kw, 
            strm.rfac, 
            strm.rout, 
            strm.seak, 
            strm.sequencenumber, 
            strm.setl, 
            strm.setl_kw, 
            strm.tact, 
            strm.tact_kw, 
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
            strm.cact ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM110 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) 
                when matched then update set
                    target.afrt=src.afrt, 
                    target.afrt_kw=src.afrt_kw, 
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.cact=src.cact, 
                    target.capt=src.capt, 
                    target.capt_kw=src.capt_kw, 
                    target.compnr=src.compnr, 
                    target.cspt=src.cspt, 
                    target.cspt_kw=src.cspt_kw, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.cuti=src.cuti, 
                    target.cuti_ref_compnr=src.cuti_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.eoty=src.eoty, 
                    target.eoty_kw=src.eoty_kw, 
                    target.freq=src.freq, 
                    target.lvps=src.lvps, 
                    target.lvps_kw=src.lvps_kw, 
                    target.mevl=src.mevl, 
                    target.mevl_kw=src.mevl_kw, 
                    target.plmc=src.plmc, 
                    target.plmc_ref_compnr=src.plmc_ref_compnr, 
                    target.plmp=src.plmp, 
                    target.prin=src.prin, 
                    target.prin_kw=src.prin_kw, 
                    target.prms=src.prms, 
                    target.prnd=src.prnd, 
                    target.prsp=src.prsp, 
                    target.prst=src.prst, 
                    target.rent=src.rent, 
                    target.rent_kw=src.rent_kw, 
                    target.rfac=src.rfac, 
                    target.rout=src.rout, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.setl=src.setl, 
                    target.setl_kw=src.setl_kw, 
                    target.tact=src.tact, 
                    target.tact_kw=src.tact_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( afrt, afrt_kw, blbl, blbl_kw, cact, capt, capt_kw, compnr, cspt, cspt_kw, cuni, cuni_ref_compnr, cuti, cuti_ref_compnr, deleted, desc, eoty, eoty_kw, freq, lvps, lvps_kw, mevl, mevl_kw, plmc, plmc_ref_compnr, plmp, prin, prin_kw, prms, prnd, prsp, prst, rent, rent_kw, rfac, rout, seak, sequencenumber, setl, setl_kw, tact, tact_kw, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.afrt, 
                    src.afrt_kw, 
                    src.blbl, 
                    src.blbl_kw, 
                    src.cact, 
                    src.capt, 
                    src.capt_kw, 
                    src.compnr, 
                    src.cspt, 
                    src.cspt_kw, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.cuti, 
                    src.cuti_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.eoty, 
                    src.eoty_kw, 
                    src.freq, 
                    src.lvps, 
                    src.lvps_kw, 
                    src.mevl, 
                    src.mevl_kw, 
                    src.plmc, 
                    src.plmc_ref_compnr, 
                    src.plmp, 
                    src.prin, 
                    src.prin_kw, 
                    src.prms, 
                    src.prnd, 
                    src.prsp, 
                    src.prst, 
                    src.rent, 
                    src.rent_kw, 
                    src.rfac, 
                    src.rout, 
                    src.seak, 
                    src.sequencenumber, 
                    src.setl, 
                    src.setl_kw, 
                    src.tact, 
                    src.tact_kw, 
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
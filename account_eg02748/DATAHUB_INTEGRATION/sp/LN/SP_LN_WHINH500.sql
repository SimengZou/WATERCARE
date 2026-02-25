create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINH500()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINH500_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINH500 as target using (SELECT * FROM (SELECT 
            strm.ccst, 
            strm.ccst_kw, 
            strm.cdf_rdte, 
            strm.cdf_rfin, 
            strm.cdf_rfin_kw, 
            strm.cdf_rusr, 
            strm.cntn, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.odat, 
            strm.orno, 
            strm.prcc, 
            strm.prcc_kw, 
            strm.prnt, 
            strm.prnt_kw, 
            strm.rcyn, 
            strm.rcyn_kw, 
            strm.redt, 
            strm.refe, 
            strm.sequencenumber, 
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
            strm.orno,
            strm.cntn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH500 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.cntn=src.cntn) OR (target.cntn IS NULL AND src.cntn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ccst=src.ccst, 
                    target.ccst_kw=src.ccst_kw, 
                    target.cdf_rdte=src.cdf_rdte, 
                    target.cdf_rfin=src.cdf_rfin, 
                    target.cdf_rfin_kw=src.cdf_rfin_kw, 
                    target.cdf_rusr=src.cdf_rusr, 
                    target.cntn=src.cntn, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.odat=src.odat, 
                    target.orno=src.orno, 
                    target.prcc=src.prcc, 
                    target.prcc_kw=src.prcc_kw, 
                    target.prnt=src.prnt, 
                    target.prnt_kw=src.prnt_kw, 
                    target.rcyn=src.rcyn, 
                    target.rcyn_kw=src.rcyn_kw, 
                    target.redt=src.redt, 
                    target.refe=src.refe, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ccst, 
                    ccst_kw, 
                    cdf_rdte, 
                    cdf_rfin, 
                    cdf_rfin_kw, 
                    cdf_rusr, 
                    cntn, 
                    compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    emno, 
                    emno_ref_compnr, 
                    odat, 
                    orno, 
                    prcc, 
                    prcc_kw, 
                    prnt, 
                    prnt_kw, 
                    rcyn, 
                    rcyn_kw, 
                    redt, 
                    refe, 
                    sequencenumber, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ccst, 
                    src.ccst_kw, 
                    src.cdf_rdte, 
                    src.cdf_rfin, 
                    src.cdf_rfin_kw, 
                    src.cdf_rusr, 
                    src.cntn, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.odat, 
                    src.orno, 
                    src.prcc, 
                    src.prcc_kw, 
                    src.prnt, 
                    src.prnt_kw, 
                    src.rcyn, 
                    src.rcyn_kw, 
                    src.redt, 
                    src.refe, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINH500_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ccst, 
            strm.ccst_kw, 
            strm.cdf_rdte, 
            strm.cdf_rfin, 
            strm.cdf_rfin_kw, 
            strm.cdf_rusr, 
            strm.cntn, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.odat, 
            strm.orno, 
            strm.prcc, 
            strm.prcc_kw, 
            strm.prnt, 
            strm.prnt_kw, 
            strm.rcyn, 
            strm.rcyn_kw, 
            strm.redt, 
            strm.refe, 
            strm.sequencenumber, 
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
            strm.orno,
            strm.cntn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH500 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.cntn=src.cntn) OR (target.cntn IS NULL AND src.cntn IS NULL)) 
                when matched then update set
                    target.ccst=src.ccst, 
                    target.ccst_kw=src.ccst_kw, 
                    target.cdf_rdte=src.cdf_rdte, 
                    target.cdf_rfin=src.cdf_rfin, 
                    target.cdf_rfin_kw=src.cdf_rfin_kw, 
                    target.cdf_rusr=src.cdf_rusr, 
                    target.cntn=src.cntn, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.odat=src.odat, 
                    target.orno=src.orno, 
                    target.prcc=src.prcc, 
                    target.prcc_kw=src.prcc_kw, 
                    target.prnt=src.prnt, 
                    target.prnt_kw=src.prnt_kw, 
                    target.rcyn=src.rcyn, 
                    target.rcyn_kw=src.rcyn_kw, 
                    target.redt=src.redt, 
                    target.refe=src.refe, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ccst, ccst_kw, cdf_rdte, cdf_rfin, cdf_rfin_kw, cdf_rusr, cntn, compnr, cwar, cwar_ref_compnr, deleted, emno, emno_ref_compnr, odat, orno, prcc, prcc_kw, prnt, prnt_kw, rcyn, rcyn_kw, redt, refe, sequencenumber, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ccst, 
                    src.ccst_kw, 
                    src.cdf_rdte, 
                    src.cdf_rfin, 
                    src.cdf_rfin_kw, 
                    src.cdf_rusr, 
                    src.cntn, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.odat, 
                    src.orno, 
                    src.prcc, 
                    src.prcc_kw, 
                    src.prnt, 
                    src.prnt_kw, 
                    src.rcyn, 
                    src.rcyn_kw, 
                    src.redt, 
                    src.refe, 
                    src.sequencenumber, 
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
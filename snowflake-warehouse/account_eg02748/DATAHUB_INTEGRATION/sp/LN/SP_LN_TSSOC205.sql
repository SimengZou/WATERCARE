create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSSOC205()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSSOC205_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSSOC205 as target using (SELECT * FROM (SELECT 
            strm.acdu, 
            strm.acln, 
            strm.actm, 
            strm.actp, 
            strm.actp_kw, 
            strm.aftm, 
            strm.astm, 
            strm.atft, 
            strm.atst, 
            strm.cmbb_orno_acln_ref_compnr, 
            strm.cmbc_orno_acln_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dltm, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.esdt, 
            strm.jctm, 
            strm.line, 
            strm.ltct, 
            strm.orgn, 
            strm.orgn_kw, 
            strm.orno, 
            strm.pftm, 
            strm.pgcn, 
            strm.pgdt, 
            strm.pged, 
            strm.pged_kw, 
            strm.pgit, 
            strm.pgrd, 
            strm.pgrd_kw, 
            strm.pstm, 
            strm.ptft, 
            strm.ptst, 
            strm.rejr, 
            strm.rejr_ref_compnr, 
            strm.rjtm, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.trdi, 
            strm.trdi_buln, 
            strm.trdu, 
            strm.trdu_butm, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.uecp, 
            strm.uecp_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orgn,
            strm.orno,
            strm.line ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSSOC205 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orgn=src.orgn) OR (target.orgn IS NULL AND src.orgn IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.line=src.line) OR (target.line IS NULL AND src.line IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acdu=src.acdu, 
                    target.acln=src.acln, 
                    target.actm=src.actm, 
                    target.actp=src.actp, 
                    target.actp_kw=src.actp_kw, 
                    target.aftm=src.aftm, 
                    target.astm=src.astm, 
                    target.atft=src.atft, 
                    target.atst=src.atst, 
                    target.cmbb_orno_acln_ref_compnr=src.cmbb_orno_acln_ref_compnr, 
                    target.cmbc_orno_acln_ref_compnr=src.cmbc_orno_acln_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dltm=src.dltm, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.esdt=src.esdt, 
                    target.jctm=src.jctm, 
                    target.line=src.line, 
                    target.ltct=src.ltct, 
                    target.orgn=src.orgn, 
                    target.orgn_kw=src.orgn_kw, 
                    target.orno=src.orno, 
                    target.pftm=src.pftm, 
                    target.pgcn=src.pgcn, 
                    target.pgdt=src.pgdt, 
                    target.pged=src.pged, 
                    target.pged_kw=src.pged_kw, 
                    target.pgit=src.pgit, 
                    target.pgrd=src.pgrd, 
                    target.pgrd_kw=src.pgrd_kw, 
                    target.pstm=src.pstm, 
                    target.ptft=src.ptft, 
                    target.ptst=src.ptst, 
                    target.rejr=src.rejr, 
                    target.rejr_ref_compnr=src.rejr_ref_compnr, 
                    target.rjtm=src.rjtm, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.trdi=src.trdi, 
                    target.trdi_buln=src.trdi_buln, 
                    target.trdu=src.trdu, 
                    target.trdu_butm=src.trdu_butm, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.uecp=src.uecp, 
                    target.uecp_kw=src.uecp_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acdu, 
                    acln, 
                    actm, 
                    actp, 
                    actp_kw, 
                    aftm, 
                    astm, 
                    atft, 
                    atst, 
                    cmbb_orno_acln_ref_compnr, 
                    cmbc_orno_acln_ref_compnr, 
                    compnr, 
                    deleted, 
                    dltm, 
                    emno, 
                    emno_ref_compnr, 
                    esdt, 
                    jctm, 
                    line, 
                    ltct, 
                    orgn, 
                    orgn_kw, 
                    orno, 
                    pftm, 
                    pgcn, 
                    pgdt, 
                    pged, 
                    pged_kw, 
                    pgit, 
                    pgrd, 
                    pgrd_kw, 
                    pstm, 
                    ptft, 
                    ptst, 
                    rejr, 
                    rejr_ref_compnr, 
                    rjtm, 
                    sequencenumber, 
                    stat, 
                    stat_kw, 
                    timestamp, 
                    trdi, 
                    trdi_buln, 
                    trdu, 
                    trdu_butm, 
                    txta, 
                    txta_ref_compnr, 
                    uecp, 
                    uecp_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acdu, 
                    src.acln, 
                    src.actm, 
                    src.actp, 
                    src.actp_kw, 
                    src.aftm, 
                    src.astm, 
                    src.atft, 
                    src.atst, 
                    src.cmbb_orno_acln_ref_compnr, 
                    src.cmbc_orno_acln_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dltm, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.esdt, 
                    src.jctm, 
                    src.line, 
                    src.ltct, 
                    src.orgn, 
                    src.orgn_kw, 
                    src.orno, 
                    src.pftm, 
                    src.pgcn, 
                    src.pgdt, 
                    src.pged, 
                    src.pged_kw, 
                    src.pgit, 
                    src.pgrd, 
                    src.pgrd_kw, 
                    src.pstm, 
                    src.ptft, 
                    src.ptst, 
                    src.rejr, 
                    src.rejr_ref_compnr, 
                    src.rjtm, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.trdi, 
                    src.trdi_buln, 
                    src.trdu, 
                    src.trdu_butm, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.uecp, 
                    src.uecp_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSSOC205_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acdu, 
            strm.acln, 
            strm.actm, 
            strm.actp, 
            strm.actp_kw, 
            strm.aftm, 
            strm.astm, 
            strm.atft, 
            strm.atst, 
            strm.cmbb_orno_acln_ref_compnr, 
            strm.cmbc_orno_acln_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dltm, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.esdt, 
            strm.jctm, 
            strm.line, 
            strm.ltct, 
            strm.orgn, 
            strm.orgn_kw, 
            strm.orno, 
            strm.pftm, 
            strm.pgcn, 
            strm.pgdt, 
            strm.pged, 
            strm.pged_kw, 
            strm.pgit, 
            strm.pgrd, 
            strm.pgrd_kw, 
            strm.pstm, 
            strm.ptft, 
            strm.ptst, 
            strm.rejr, 
            strm.rejr_ref_compnr, 
            strm.rjtm, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.trdi, 
            strm.trdi_buln, 
            strm.trdu, 
            strm.trdu_butm, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.uecp, 
            strm.uecp_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orgn,
            strm.orno,
            strm.line ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSSOC205 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orgn=src.orgn) OR (target.orgn IS NULL AND src.orgn IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.line=src.line) OR (target.line IS NULL AND src.line IS NULL)) 
                when matched then update set
                    target.acdu=src.acdu, 
                    target.acln=src.acln, 
                    target.actm=src.actm, 
                    target.actp=src.actp, 
                    target.actp_kw=src.actp_kw, 
                    target.aftm=src.aftm, 
                    target.astm=src.astm, 
                    target.atft=src.atft, 
                    target.atst=src.atst, 
                    target.cmbb_orno_acln_ref_compnr=src.cmbb_orno_acln_ref_compnr, 
                    target.cmbc_orno_acln_ref_compnr=src.cmbc_orno_acln_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dltm=src.dltm, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.esdt=src.esdt, 
                    target.jctm=src.jctm, 
                    target.line=src.line, 
                    target.ltct=src.ltct, 
                    target.orgn=src.orgn, 
                    target.orgn_kw=src.orgn_kw, 
                    target.orno=src.orno, 
                    target.pftm=src.pftm, 
                    target.pgcn=src.pgcn, 
                    target.pgdt=src.pgdt, 
                    target.pged=src.pged, 
                    target.pged_kw=src.pged_kw, 
                    target.pgit=src.pgit, 
                    target.pgrd=src.pgrd, 
                    target.pgrd_kw=src.pgrd_kw, 
                    target.pstm=src.pstm, 
                    target.ptft=src.ptft, 
                    target.ptst=src.ptst, 
                    target.rejr=src.rejr, 
                    target.rejr_ref_compnr=src.rejr_ref_compnr, 
                    target.rjtm=src.rjtm, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.trdi=src.trdi, 
                    target.trdi_buln=src.trdi_buln, 
                    target.trdu=src.trdu, 
                    target.trdu_butm=src.trdu_butm, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.uecp=src.uecp, 
                    target.uecp_kw=src.uecp_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acdu, acln, actm, actp, actp_kw, aftm, astm, atft, atst, cmbb_orno_acln_ref_compnr, cmbc_orno_acln_ref_compnr, compnr, deleted, dltm, emno, emno_ref_compnr, esdt, jctm, line, ltct, orgn, orgn_kw, orno, pftm, pgcn, pgdt, pged, pged_kw, pgit, pgrd, pgrd_kw, pstm, ptft, ptst, rejr, rejr_ref_compnr, rjtm, sequencenumber, stat, stat_kw, timestamp, trdi, trdi_buln, trdu, trdu_butm, txta, txta_ref_compnr, uecp, uecp_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acdu, 
                    src.acln, 
                    src.actm, 
                    src.actp, 
                    src.actp_kw, 
                    src.aftm, 
                    src.astm, 
                    src.atft, 
                    src.atst, 
                    src.cmbb_orno_acln_ref_compnr, 
                    src.cmbc_orno_acln_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dltm, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.esdt, 
                    src.jctm, 
                    src.line, 
                    src.ltct, 
                    src.orgn, 
                    src.orgn_kw, 
                    src.orno, 
                    src.pftm, 
                    src.pgcn, 
                    src.pgdt, 
                    src.pged, 
                    src.pged_kw, 
                    src.pgit, 
                    src.pgrd, 
                    src.pgrd_kw, 
                    src.pstm, 
                    src.ptft, 
                    src.ptst, 
                    src.rejr, 
                    src.rejr_ref_compnr, 
                    src.rjtm, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.trdi, 
                    src.trdi_buln, 
                    src.trdu, 
                    src.trdu_butm, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.uecp, 
                    src.uecp_kw, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPDM040()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPDM040_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPDM040 as target using (SELECT * FROM (SELECT 
            strm.acta, 
            strm.acta_kw, 
            strm.actp, 
            strm.actp_kw, 
            strm.blbl, 
            strm.blbl_kw, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.cccp, 
            strm.cccp_ref_compnr, 
            strm.ccfu, 
            strm.ccfu_kw, 
            strm.ccic, 
            strm.ccip, 
            strm.ccip_ref_compnr, 
            strm.ccsp, 
            strm.ccsp_ref_compnr, 
            strm.cico, 
            strm.compnr, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.cvat, 
            strm.cvat_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.ltcp, 
            strm.ltip, 
            strm.ltsp, 
            strm.pcmc_1, 
            strm.pcmc_2, 
            strm.pcmc_3, 
            strm.pimc_1, 
            strm.pimc_2, 
            strm.pimc_3, 
            strm.pric, 
            strm.prii, 
            strm.pris, 
            strm.prre, 
            strm.prre_kw, 
            strm.psmc_1, 
            strm.psmc_2, 
            strm.psmc_3, 
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
            strm.cico ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM040 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cico=src.cico) OR (target.cico IS NULL AND src.cico IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acta=src.acta, 
                    target.acta_kw=src.acta_kw, 
                    target.actp=src.actp, 
                    target.actp_kw=src.actp_kw, 
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.cccp=src.cccp, 
                    target.cccp_ref_compnr=src.cccp_ref_compnr, 
                    target.ccfu=src.ccfu, 
                    target.ccfu_kw=src.ccfu_kw, 
                    target.ccic=src.ccic, 
                    target.ccip=src.ccip, 
                    target.ccip_ref_compnr=src.ccip_ref_compnr, 
                    target.ccsp=src.ccsp, 
                    target.ccsp_ref_compnr=src.ccsp_ref_compnr, 
                    target.cico=src.cico, 
                    target.compnr=src.compnr, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.cvat=src.cvat, 
                    target.cvat_ref_compnr=src.cvat_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.ltcp=src.ltcp, 
                    target.ltip=src.ltip, 
                    target.ltsp=src.ltsp, 
                    target.pcmc_1=src.pcmc_1, 
                    target.pcmc_2=src.pcmc_2, 
                    target.pcmc_3=src.pcmc_3, 
                    target.pimc_1=src.pimc_1, 
                    target.pimc_2=src.pimc_2, 
                    target.pimc_3=src.pimc_3, 
                    target.pric=src.pric, 
                    target.prii=src.prii, 
                    target.pris=src.pris, 
                    target.prre=src.prre, 
                    target.prre_kw=src.prre_kw, 
                    target.psmc_1=src.psmc_1, 
                    target.psmc_2=src.psmc_2, 
                    target.psmc_3=src.psmc_3, 
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
                    acta, 
                    acta_kw, 
                    actp, 
                    actp_kw, 
                    blbl, 
                    blbl_kw, 
                    ccco, 
                    ccco_ref_compnr, 
                    cccp, 
                    cccp_ref_compnr, 
                    ccfu, 
                    ccfu_kw, 
                    ccic, 
                    ccip, 
                    ccip_ref_compnr, 
                    ccsp, 
                    ccsp_ref_compnr, 
                    cico, 
                    compnr, 
                    cuni, 
                    cuni_ref_compnr, 
                    cvat, 
                    cvat_ref_compnr, 
                    deleted, 
                    desc, 
                    ltcp, 
                    ltip, 
                    ltsp, 
                    pcmc_1, 
                    pcmc_2, 
                    pcmc_3, 
                    pimc_1, 
                    pimc_2, 
                    pimc_3, 
                    pric, 
                    prii, 
                    pris, 
                    prre, 
                    prre_kw, 
                    psmc_1, 
                    psmc_2, 
                    psmc_3, 
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
                    src.acta, 
                    src.acta_kw, 
                    src.actp, 
                    src.actp_kw, 
                    src.blbl, 
                    src.blbl_kw, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.cccp, 
                    src.cccp_ref_compnr, 
                    src.ccfu, 
                    src.ccfu_kw, 
                    src.ccic, 
                    src.ccip, 
                    src.ccip_ref_compnr, 
                    src.ccsp, 
                    src.ccsp_ref_compnr, 
                    src.cico, 
                    src.compnr, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.cvat, 
                    src.cvat_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.ltcp, 
                    src.ltip, 
                    src.ltsp, 
                    src.pcmc_1, 
                    src.pcmc_2, 
                    src.pcmc_3, 
                    src.pimc_1, 
                    src.pimc_2, 
                    src.pimc_3, 
                    src.pric, 
                    src.prii, 
                    src.pris, 
                    src.prre, 
                    src.prre_kw, 
                    src.psmc_1, 
                    src.psmc_2, 
                    src.psmc_3, 
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

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPDM040_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acta, 
            strm.acta_kw, 
            strm.actp, 
            strm.actp_kw, 
            strm.blbl, 
            strm.blbl_kw, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.cccp, 
            strm.cccp_ref_compnr, 
            strm.ccfu, 
            strm.ccfu_kw, 
            strm.ccic, 
            strm.ccip, 
            strm.ccip_ref_compnr, 
            strm.ccsp, 
            strm.ccsp_ref_compnr, 
            strm.cico, 
            strm.compnr, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.cvat, 
            strm.cvat_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.ltcp, 
            strm.ltip, 
            strm.ltsp, 
            strm.pcmc_1, 
            strm.pcmc_2, 
            strm.pcmc_3, 
            strm.pimc_1, 
            strm.pimc_2, 
            strm.pimc_3, 
            strm.pric, 
            strm.prii, 
            strm.pris, 
            strm.prre, 
            strm.prre_kw, 
            strm.psmc_1, 
            strm.psmc_2, 
            strm.psmc_3, 
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
            strm.cico ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPDM040 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cico=src.cico) OR (target.cico IS NULL AND src.cico IS NULL)) 
                when matched then update set
                    target.acta=src.acta, 
                    target.acta_kw=src.acta_kw, 
                    target.actp=src.actp, 
                    target.actp_kw=src.actp_kw, 
                    target.blbl=src.blbl, 
                    target.blbl_kw=src.blbl_kw, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.cccp=src.cccp, 
                    target.cccp_ref_compnr=src.cccp_ref_compnr, 
                    target.ccfu=src.ccfu, 
                    target.ccfu_kw=src.ccfu_kw, 
                    target.ccic=src.ccic, 
                    target.ccip=src.ccip, 
                    target.ccip_ref_compnr=src.ccip_ref_compnr, 
                    target.ccsp=src.ccsp, 
                    target.ccsp_ref_compnr=src.ccsp_ref_compnr, 
                    target.cico=src.cico, 
                    target.compnr=src.compnr, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.cvat=src.cvat, 
                    target.cvat_ref_compnr=src.cvat_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.ltcp=src.ltcp, 
                    target.ltip=src.ltip, 
                    target.ltsp=src.ltsp, 
                    target.pcmc_1=src.pcmc_1, 
                    target.pcmc_2=src.pcmc_2, 
                    target.pcmc_3=src.pcmc_3, 
                    target.pimc_1=src.pimc_1, 
                    target.pimc_2=src.pimc_2, 
                    target.pimc_3=src.pimc_3, 
                    target.pric=src.pric, 
                    target.prii=src.prii, 
                    target.pris=src.pris, 
                    target.prre=src.prre, 
                    target.prre_kw=src.prre_kw, 
                    target.psmc_1=src.psmc_1, 
                    target.psmc_2=src.psmc_2, 
                    target.psmc_3=src.psmc_3, 
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
        when not matched   then insert ( acta, acta_kw, actp, actp_kw, blbl, blbl_kw, ccco, ccco_ref_compnr, cccp, cccp_ref_compnr, ccfu, ccfu_kw, ccic, ccip, ccip_ref_compnr, ccsp, ccsp_ref_compnr, cico, compnr, cuni, cuni_ref_compnr, cvat, cvat_ref_compnr, deleted, desc, ltcp, ltip, ltsp, pcmc_1, pcmc_2, pcmc_3, pimc_1, pimc_2, pimc_3, pric, prii, pris, prre, prre_kw, psmc_1, psmc_2, psmc_3, seak, sequencenumber, timestamp, txta, txta_ref_compnr, username, usyn, usyn_kw, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acta, 
                    src.acta_kw, 
                    src.actp, 
                    src.actp_kw, 
                    src.blbl, 
                    src.blbl_kw, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.cccp, 
                    src.cccp_ref_compnr, 
                    src.ccfu, 
                    src.ccfu_kw, 
                    src.ccic, 
                    src.ccip, 
                    src.ccip_ref_compnr, 
                    src.ccsp, 
                    src.ccsp_ref_compnr, 
                    src.cico, 
                    src.compnr, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.cvat, 
                    src.cvat_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.ltcp, 
                    src.ltip, 
                    src.ltsp, 
                    src.pcmc_1, 
                    src.pcmc_2, 
                    src.pcmc_3, 
                    src.pimc_1, 
                    src.pimc_2, 
                    src.pimc_3, 
                    src.pric, 
                    src.prii, 
                    src.pris, 
                    src.prre, 
                    src.prre_kw, 
                    src.psmc_1, 
                    src.psmc_2, 
                    src.psmc_3, 
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
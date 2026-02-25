create or replace procedure DATAHUB_INTEGRATION.SP_LN_TDPUR094()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TDPUR094_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TDPUR094 as target using (SELECT * FROM (SELECT 
            strm.cbor, 
            strm.cbor_kw, 
            strm.cfnm, 
            strm.cfnm_kw, 
            strm.cnsp, 
            strm.cnsp_kw, 
            strm.cnsr, 
            strm.cnsr_kw, 
            strm.compnr, 
            strm.coun, 
            strm.coun_kw, 
            strm.deleted, 
            strm.drct, 
            strm.drct_kw, 
            strm.dsca, 
            strm.efdt, 
            strm.etpc, 
            strm.etpc_kw, 
            strm.exdt, 
            strm.ngpc, 
            strm.ngpc_ref_compnr, 
            strm.ngpc_sepc_ref_compnr, 
            strm.ngpo, 
            strm.ngpo_ref_compnr, 
            strm.ngpo_sepo_ref_compnr, 
            strm.ngpq, 
            strm.ngpq_ref_compnr, 
            strm.ngpq_sepq_ref_compnr, 
            strm.pmsk, 
            strm.potp, 
            strm.proc, 
            strm.reto, 
            strm.reto_kw, 
            strm.sepc, 
            strm.sepo, 
            strm.sepq, 
            strm.sequencenumber, 
            strm.slcp, 
            strm.slcp_kw, 
            strm.subc, 
            strm.subc_kw, 
            strm.sund, 
            strm.sund_kw, 
            strm.timestamp, 
            strm.username, 
            strm.wrhp, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.potp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR094 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.potp=src.potp) OR (target.potp IS NULL AND src.potp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cbor=src.cbor, 
                    target.cbor_kw=src.cbor_kw, 
                    target.cfnm=src.cfnm, 
                    target.cfnm_kw=src.cfnm_kw, 
                    target.cnsp=src.cnsp, 
                    target.cnsp_kw=src.cnsp_kw, 
                    target.cnsr=src.cnsr, 
                    target.cnsr_kw=src.cnsr_kw, 
                    target.compnr=src.compnr, 
                    target.coun=src.coun, 
                    target.coun_kw=src.coun_kw, 
                    target.deleted=src.deleted, 
                    target.drct=src.drct, 
                    target.drct_kw=src.drct_kw, 
                    target.dsca=src.dsca, 
                    target.efdt=src.efdt, 
                    target.etpc=src.etpc, 
                    target.etpc_kw=src.etpc_kw, 
                    target.exdt=src.exdt, 
                    target.ngpc=src.ngpc, 
                    target.ngpc_ref_compnr=src.ngpc_ref_compnr, 
                    target.ngpc_sepc_ref_compnr=src.ngpc_sepc_ref_compnr, 
                    target.ngpo=src.ngpo, 
                    target.ngpo_ref_compnr=src.ngpo_ref_compnr, 
                    target.ngpo_sepo_ref_compnr=src.ngpo_sepo_ref_compnr, 
                    target.ngpq=src.ngpq, 
                    target.ngpq_ref_compnr=src.ngpq_ref_compnr, 
                    target.ngpq_sepq_ref_compnr=src.ngpq_sepq_ref_compnr, 
                    target.pmsk=src.pmsk, 
                    target.potp=src.potp, 
                    target.proc=src.proc, 
                    target.reto=src.reto, 
                    target.reto_kw=src.reto_kw, 
                    target.sepc=src.sepc, 
                    target.sepo=src.sepo, 
                    target.sepq=src.sepq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.slcp=src.slcp, 
                    target.slcp_kw=src.slcp_kw, 
                    target.subc=src.subc, 
                    target.subc_kw=src.subc_kw, 
                    target.sund=src.sund, 
                    target.sund_kw=src.sund_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.wrhp=src.wrhp, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cbor, 
                    cbor_kw, 
                    cfnm, 
                    cfnm_kw, 
                    cnsp, 
                    cnsp_kw, 
                    cnsr, 
                    cnsr_kw, 
                    compnr, 
                    coun, 
                    coun_kw, 
                    deleted, 
                    drct, 
                    drct_kw, 
                    dsca, 
                    efdt, 
                    etpc, 
                    etpc_kw, 
                    exdt, 
                    ngpc, 
                    ngpc_ref_compnr, 
                    ngpc_sepc_ref_compnr, 
                    ngpo, 
                    ngpo_ref_compnr, 
                    ngpo_sepo_ref_compnr, 
                    ngpq, 
                    ngpq_ref_compnr, 
                    ngpq_sepq_ref_compnr, 
                    pmsk, 
                    potp, 
                    proc, 
                    reto, 
                    reto_kw, 
                    sepc, 
                    sepo, 
                    sepq, 
                    sequencenumber, 
                    slcp, 
                    slcp_kw, 
                    subc, 
                    subc_kw, 
                    sund, 
                    sund_kw, 
                    timestamp, 
                    username, 
                    wrhp, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cbor, 
                    src.cbor_kw, 
                    src.cfnm, 
                    src.cfnm_kw, 
                    src.cnsp, 
                    src.cnsp_kw, 
                    src.cnsr, 
                    src.cnsr_kw, 
                    src.compnr, 
                    src.coun, 
                    src.coun_kw, 
                    src.deleted, 
                    src.drct, 
                    src.drct_kw, 
                    src.dsca, 
                    src.efdt, 
                    src.etpc, 
                    src.etpc_kw, 
                    src.exdt, 
                    src.ngpc, 
                    src.ngpc_ref_compnr, 
                    src.ngpc_sepc_ref_compnr, 
                    src.ngpo, 
                    src.ngpo_ref_compnr, 
                    src.ngpo_sepo_ref_compnr, 
                    src.ngpq, 
                    src.ngpq_ref_compnr, 
                    src.ngpq_sepq_ref_compnr, 
                    src.pmsk, 
                    src.potp, 
                    src.proc, 
                    src.reto, 
                    src.reto_kw, 
                    src.sepc, 
                    src.sepo, 
                    src.sepq, 
                    src.sequencenumber, 
                    src.slcp, 
                    src.slcp_kw, 
                    src.subc, 
                    src.subc_kw, 
                    src.sund, 
                    src.sund_kw, 
                    src.timestamp, 
                    src.username, 
                    src.wrhp,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TDPUR094_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cbor, 
            strm.cbor_kw, 
            strm.cfnm, 
            strm.cfnm_kw, 
            strm.cnsp, 
            strm.cnsp_kw, 
            strm.cnsr, 
            strm.cnsr_kw, 
            strm.compnr, 
            strm.coun, 
            strm.coun_kw, 
            strm.deleted, 
            strm.drct, 
            strm.drct_kw, 
            strm.dsca, 
            strm.efdt, 
            strm.etpc, 
            strm.etpc_kw, 
            strm.exdt, 
            strm.ngpc, 
            strm.ngpc_ref_compnr, 
            strm.ngpc_sepc_ref_compnr, 
            strm.ngpo, 
            strm.ngpo_ref_compnr, 
            strm.ngpo_sepo_ref_compnr, 
            strm.ngpq, 
            strm.ngpq_ref_compnr, 
            strm.ngpq_sepq_ref_compnr, 
            strm.pmsk, 
            strm.potp, 
            strm.proc, 
            strm.reto, 
            strm.reto_kw, 
            strm.sepc, 
            strm.sepo, 
            strm.sepq, 
            strm.sequencenumber, 
            strm.slcp, 
            strm.slcp_kw, 
            strm.subc, 
            strm.subc_kw, 
            strm.sund, 
            strm.sund_kw, 
            strm.timestamp, 
            strm.username, 
            strm.wrhp, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.potp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR094 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.potp=src.potp) OR (target.potp IS NULL AND src.potp IS NULL)) 
                when matched then update set
                    target.cbor=src.cbor, 
                    target.cbor_kw=src.cbor_kw, 
                    target.cfnm=src.cfnm, 
                    target.cfnm_kw=src.cfnm_kw, 
                    target.cnsp=src.cnsp, 
                    target.cnsp_kw=src.cnsp_kw, 
                    target.cnsr=src.cnsr, 
                    target.cnsr_kw=src.cnsr_kw, 
                    target.compnr=src.compnr, 
                    target.coun=src.coun, 
                    target.coun_kw=src.coun_kw, 
                    target.deleted=src.deleted, 
                    target.drct=src.drct, 
                    target.drct_kw=src.drct_kw, 
                    target.dsca=src.dsca, 
                    target.efdt=src.efdt, 
                    target.etpc=src.etpc, 
                    target.etpc_kw=src.etpc_kw, 
                    target.exdt=src.exdt, 
                    target.ngpc=src.ngpc, 
                    target.ngpc_ref_compnr=src.ngpc_ref_compnr, 
                    target.ngpc_sepc_ref_compnr=src.ngpc_sepc_ref_compnr, 
                    target.ngpo=src.ngpo, 
                    target.ngpo_ref_compnr=src.ngpo_ref_compnr, 
                    target.ngpo_sepo_ref_compnr=src.ngpo_sepo_ref_compnr, 
                    target.ngpq=src.ngpq, 
                    target.ngpq_ref_compnr=src.ngpq_ref_compnr, 
                    target.ngpq_sepq_ref_compnr=src.ngpq_sepq_ref_compnr, 
                    target.pmsk=src.pmsk, 
                    target.potp=src.potp, 
                    target.proc=src.proc, 
                    target.reto=src.reto, 
                    target.reto_kw=src.reto_kw, 
                    target.sepc=src.sepc, 
                    target.sepo=src.sepo, 
                    target.sepq=src.sepq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.slcp=src.slcp, 
                    target.slcp_kw=src.slcp_kw, 
                    target.subc=src.subc, 
                    target.subc_kw=src.subc_kw, 
                    target.sund=src.sund, 
                    target.sund_kw=src.sund_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.wrhp=src.wrhp, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cbor, cbor_kw, cfnm, cfnm_kw, cnsp, cnsp_kw, cnsr, cnsr_kw, compnr, coun, coun_kw, deleted, drct, drct_kw, dsca, efdt, etpc, etpc_kw, exdt, ngpc, ngpc_ref_compnr, ngpc_sepc_ref_compnr, ngpo, ngpo_ref_compnr, ngpo_sepo_ref_compnr, ngpq, ngpq_ref_compnr, ngpq_sepq_ref_compnr, pmsk, potp, proc, reto, reto_kw, sepc, sepo, sepq, sequencenumber, slcp, slcp_kw, subc, subc_kw, sund, sund_kw, timestamp, username, wrhp, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cbor, 
                    src.cbor_kw, 
                    src.cfnm, 
                    src.cfnm_kw, 
                    src.cnsp, 
                    src.cnsp_kw, 
                    src.cnsr, 
                    src.cnsr_kw, 
                    src.compnr, 
                    src.coun, 
                    src.coun_kw, 
                    src.deleted, 
                    src.drct, 
                    src.drct_kw, 
                    src.dsca, 
                    src.efdt, 
                    src.etpc, 
                    src.etpc_kw, 
                    src.exdt, 
                    src.ngpc, 
                    src.ngpc_ref_compnr, 
                    src.ngpc_sepc_ref_compnr, 
                    src.ngpo, 
                    src.ngpo_ref_compnr, 
                    src.ngpo_sepo_ref_compnr, 
                    src.ngpq, 
                    src.ngpq_ref_compnr, 
                    src.ngpq_sepq_ref_compnr, 
                    src.pmsk, 
                    src.potp, 
                    src.proc, 
                    src.reto, 
                    src.reto_kw, 
                    src.sepc, 
                    src.sepo, 
                    src.sepq, 
                    src.sequencenumber, 
                    src.slcp, 
                    src.slcp_kw, 
                    src.subc, 
                    src.subc_kw, 
                    src.sund, 
                    src.sund_kw, 
                    src.timestamp, 
                    src.username, 
                    src.wrhp,     
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
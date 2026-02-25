create or replace procedure DATAHUB_INTEGRATION.SP_LN_TICST010()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TICST010_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TICST010 as target using (SELECT * FROM (SELECT 
            strm.aamt_1, 
            strm.aamt_2, 
            strm.aamt_3, 
            strm.aamt_dwhc, 
            strm.aamt_lclc, 
            strm.aamt_refc, 
            strm.aamt_rpc1, 
            strm.aamt_rpc2, 
            strm.addc, 
            strm.addc_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.csto, 
            strm.csto_kw, 
            strm.cstv, 
            strm.cstv_kw, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.deleted, 
            strm.dptm_fcmp, 
            strm.eamt_1, 
            strm.eamt_2, 
            strm.eamt_3, 
            strm.eamt_dwhc, 
            strm.eamt_lclc, 
            strm.eamt_refc, 
            strm.eamt_rpc1, 
            strm.eamt_rpc2, 
            strm.nuna, 
            strm.nune, 
            strm.pdno, 
            strm.pdno_ref_compnr, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pdno,
            strm.cstv,
            strm.cwoc,
            strm.cpcp,
            strm.addc,
            strm.csto ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TICST010 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pdno=src.pdno) OR (target.pdno IS NULL AND src.pdno IS NULL)) AND
            ((target.cstv=src.cstv) OR (target.cstv IS NULL AND src.cstv IS NULL)) AND
            ((target.cwoc=src.cwoc) OR (target.cwoc IS NULL AND src.cwoc IS NULL)) AND
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) AND
            ((target.addc=src.addc) OR (target.addc IS NULL AND src.addc IS NULL)) AND
            ((target.csto=src.csto) OR (target.csto IS NULL AND src.csto IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aamt_1=src.aamt_1, 
                    target.aamt_2=src.aamt_2, 
                    target.aamt_3=src.aamt_3, 
                    target.aamt_dwhc=src.aamt_dwhc, 
                    target.aamt_lclc=src.aamt_lclc, 
                    target.aamt_refc=src.aamt_refc, 
                    target.aamt_rpc1=src.aamt_rpc1, 
                    target.aamt_rpc2=src.aamt_rpc2, 
                    target.addc=src.addc, 
                    target.addc_kw=src.addc_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.csto=src.csto, 
                    target.csto_kw=src.csto_kw, 
                    target.cstv=src.cstv, 
                    target.cstv_kw=src.cstv_kw, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dptm_fcmp=src.dptm_fcmp, 
                    target.eamt_1=src.eamt_1, 
                    target.eamt_2=src.eamt_2, 
                    target.eamt_3=src.eamt_3, 
                    target.eamt_dwhc=src.eamt_dwhc, 
                    target.eamt_lclc=src.eamt_lclc, 
                    target.eamt_refc=src.eamt_refc, 
                    target.eamt_rpc1=src.eamt_rpc1, 
                    target.eamt_rpc2=src.eamt_rpc2, 
                    target.nuna=src.nuna, 
                    target.nune=src.nune, 
                    target.pdno=src.pdno, 
                    target.pdno_ref_compnr=src.pdno_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aamt_1, 
                    aamt_2, 
                    aamt_3, 
                    aamt_dwhc, 
                    aamt_lclc, 
                    aamt_refc, 
                    aamt_rpc1, 
                    aamt_rpc2, 
                    addc, 
                    addc_kw, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cpcp, 
                    cpcp_ref_compnr, 
                    csto, 
                    csto_kw, 
                    cstv, 
                    cstv_kw, 
                    cwoc, 
                    cwoc_ref_compnr, 
                    deleted, 
                    dptm_fcmp, 
                    eamt_1, 
                    eamt_2, 
                    eamt_3, 
                    eamt_dwhc, 
                    eamt_lclc, 
                    eamt_refc, 
                    eamt_rpc1, 
                    eamt_rpc2, 
                    nuna, 
                    nune, 
                    pdno, 
                    pdno_ref_compnr, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aamt_1, 
                    src.aamt_2, 
                    src.aamt_3, 
                    src.aamt_dwhc, 
                    src.aamt_lclc, 
                    src.aamt_refc, 
                    src.aamt_rpc1, 
                    src.aamt_rpc2, 
                    src.addc, 
                    src.addc_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.csto, 
                    src.csto_kw, 
                    src.cstv, 
                    src.cstv_kw, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.deleted, 
                    src.dptm_fcmp, 
                    src.eamt_1, 
                    src.eamt_2, 
                    src.eamt_3, 
                    src.eamt_dwhc, 
                    src.eamt_lclc, 
                    src.eamt_refc, 
                    src.eamt_rpc1, 
                    src.eamt_rpc2, 
                    src.nuna, 
                    src.nune, 
                    src.pdno, 
                    src.pdno_ref_compnr, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TICST010_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aamt_1, 
            strm.aamt_2, 
            strm.aamt_3, 
            strm.aamt_dwhc, 
            strm.aamt_lclc, 
            strm.aamt_refc, 
            strm.aamt_rpc1, 
            strm.aamt_rpc2, 
            strm.addc, 
            strm.addc_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.csto, 
            strm.csto_kw, 
            strm.cstv, 
            strm.cstv_kw, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.deleted, 
            strm.dptm_fcmp, 
            strm.eamt_1, 
            strm.eamt_2, 
            strm.eamt_3, 
            strm.eamt_dwhc, 
            strm.eamt_lclc, 
            strm.eamt_refc, 
            strm.eamt_rpc1, 
            strm.eamt_rpc2, 
            strm.nuna, 
            strm.nune, 
            strm.pdno, 
            strm.pdno_ref_compnr, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pdno,
            strm.cstv,
            strm.cwoc,
            strm.cpcp,
            strm.addc,
            strm.csto ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TICST010 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pdno=src.pdno) OR (target.pdno IS NULL AND src.pdno IS NULL)) AND
            ((target.cstv=src.cstv) OR (target.cstv IS NULL AND src.cstv IS NULL)) AND
            ((target.cwoc=src.cwoc) OR (target.cwoc IS NULL AND src.cwoc IS NULL)) AND
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) AND
            ((target.addc=src.addc) OR (target.addc IS NULL AND src.addc IS NULL)) AND
            ((target.csto=src.csto) OR (target.csto IS NULL AND src.csto IS NULL)) 
                when matched then update set
                    target.aamt_1=src.aamt_1, 
                    target.aamt_2=src.aamt_2, 
                    target.aamt_3=src.aamt_3, 
                    target.aamt_dwhc=src.aamt_dwhc, 
                    target.aamt_lclc=src.aamt_lclc, 
                    target.aamt_refc=src.aamt_refc, 
                    target.aamt_rpc1=src.aamt_rpc1, 
                    target.aamt_rpc2=src.aamt_rpc2, 
                    target.addc=src.addc, 
                    target.addc_kw=src.addc_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.csto=src.csto, 
                    target.csto_kw=src.csto_kw, 
                    target.cstv=src.cstv, 
                    target.cstv_kw=src.cstv_kw, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dptm_fcmp=src.dptm_fcmp, 
                    target.eamt_1=src.eamt_1, 
                    target.eamt_2=src.eamt_2, 
                    target.eamt_3=src.eamt_3, 
                    target.eamt_dwhc=src.eamt_dwhc, 
                    target.eamt_lclc=src.eamt_lclc, 
                    target.eamt_refc=src.eamt_refc, 
                    target.eamt_rpc1=src.eamt_rpc1, 
                    target.eamt_rpc2=src.eamt_rpc2, 
                    target.nuna=src.nuna, 
                    target.nune=src.nune, 
                    target.pdno=src.pdno, 
                    target.pdno_ref_compnr=src.pdno_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aamt_1, aamt_2, aamt_3, aamt_dwhc, aamt_lclc, aamt_refc, aamt_rpc1, aamt_rpc2, addc, addc_kw, ccur, ccur_ref_compnr, compnr, cpcp, cpcp_ref_compnr, csto, csto_kw, cstv, cstv_kw, cwoc, cwoc_ref_compnr, deleted, dptm_fcmp, eamt_1, eamt_2, eamt_3, eamt_dwhc, eamt_lclc, eamt_refc, eamt_rpc1, eamt_rpc2, nuna, nune, pdno, pdno_ref_compnr, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aamt_1, 
                    src.aamt_2, 
                    src.aamt_3, 
                    src.aamt_dwhc, 
                    src.aamt_lclc, 
                    src.aamt_refc, 
                    src.aamt_rpc1, 
                    src.aamt_rpc2, 
                    src.addc, 
                    src.addc_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.csto, 
                    src.csto_kw, 
                    src.cstv, 
                    src.cstv_kw, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.deleted, 
                    src.dptm_fcmp, 
                    src.eamt_1, 
                    src.eamt_2, 
                    src.eamt_3, 
                    src.eamt_dwhc, 
                    src.eamt_lclc, 
                    src.eamt_refc, 
                    src.eamt_rpc1, 
                    src.eamt_rpc2, 
                    src.nuna, 
                    src.nune, 
                    src.pdno, 
                    src.pdno_ref_compnr, 
                    src.sequencenumber, 
                    src.timestamp, 
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
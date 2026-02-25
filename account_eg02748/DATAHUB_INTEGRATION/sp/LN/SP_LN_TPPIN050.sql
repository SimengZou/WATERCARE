create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPIN050()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPIN050_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPIN050 as target using (SELECT * FROM (SELECT 
            strm.amos, 
            strm.amos_cntc, 
            strm.amos_dwhc, 
            strm.amos_homc, 
            strm.amos_prjc, 
            strm.amos_refc, 
            strm.amos_rpc1, 
            strm.amos_rpc2, 
            strm.cact, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.deleted, 
            strm.fcmp, 
            strm.idoc, 
            strm.ityp, 
            strm.pgdt, 
            strm.quan, 
            strm.quan_buar, 
            strm.quan_buln, 
            strm.quan_bupc, 
            strm.quan_butm, 
            strm.quan_buvl, 
            strm.quan_buwg, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.sequencenumber, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.cspa,
            strm.cpla,
            strm.cact,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPIN050 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cpla=src.cpla) OR (target.cpla IS NULL AND src.cpla IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amos=src.amos, 
                    target.amos_cntc=src.amos_cntc, 
                    target.amos_dwhc=src.amos_dwhc, 
                    target.amos_homc=src.amos_homc, 
                    target.amos_prjc=src.amos_prjc, 
                    target.amos_refc=src.amos_refc, 
                    target.amos_rpc1=src.amos_rpc1, 
                    target.amos_rpc2=src.amos_rpc2, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.fcmp=src.fcmp, 
                    target.idoc=src.idoc, 
                    target.ityp=src.ityp, 
                    target.pgdt=src.pgdt, 
                    target.quan=src.quan, 
                    target.quan_buar=src.quan_buar, 
                    target.quan_buln=src.quan_buln, 
                    target.quan_bupc=src.quan_bupc, 
                    target.quan_butm=src.quan_butm, 
                    target.quan_buvl=src.quan_buvl, 
                    target.quan_buwg=src.quan_buwg, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amos, 
                    amos_cntc, 
                    amos_dwhc, 
                    amos_homc, 
                    amos_prjc, 
                    amos_refc, 
                    amos_rpc1, 
                    amos_rpc2, 
                    cact, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cspa_ref_compnr, 
                    cprj_ref_compnr, 
                    cspa, 
                    cuni, 
                    cuni_ref_compnr, 
                    deleted, 
                    fcmp, 
                    idoc, 
                    ityp, 
                    pgdt, 
                    quan, 
                    quan_buar, 
                    quan_buln, 
                    quan_bupc, 
                    quan_butm, 
                    quan_buvl, 
                    quan_buwg, 
                    rate_1, 
                    rate_2, 
                    rate_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rdat, 
                    sequencenumber, 
                    sern, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amos, 
                    src.amos_cntc, 
                    src.amos_dwhc, 
                    src.amos_homc, 
                    src.amos_prjc, 
                    src.amos_refc, 
                    src.amos_rpc1, 
                    src.amos_rpc2, 
                    src.cact, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.deleted, 
                    src.fcmp, 
                    src.idoc, 
                    src.ityp, 
                    src.pgdt, 
                    src.quan, 
                    src.quan_buar, 
                    src.quan_buln, 
                    src.quan_bupc, 
                    src.quan_butm, 
                    src.quan_buvl, 
                    src.quan_buwg, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.sequencenumber, 
                    src.sern, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPIN050_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amos, 
            strm.amos_cntc, 
            strm.amos_dwhc, 
            strm.amos_homc, 
            strm.amos_prjc, 
            strm.amos_refc, 
            strm.amos_rpc1, 
            strm.amos_rpc2, 
            strm.cact, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cuni, 
            strm.cuni_ref_compnr, 
            strm.deleted, 
            strm.fcmp, 
            strm.idoc, 
            strm.ityp, 
            strm.pgdt, 
            strm.quan, 
            strm.quan_buar, 
            strm.quan_buln, 
            strm.quan_bupc, 
            strm.quan_butm, 
            strm.quan_buvl, 
            strm.quan_buwg, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.sequencenumber, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.cspa,
            strm.cpla,
            strm.cact,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPIN050 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cpla=src.cpla) OR (target.cpla IS NULL AND src.cpla IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.amos=src.amos, 
                    target.amos_cntc=src.amos_cntc, 
                    target.amos_dwhc=src.amos_dwhc, 
                    target.amos_homc=src.amos_homc, 
                    target.amos_prjc=src.amos_prjc, 
                    target.amos_refc=src.amos_refc, 
                    target.amos_rpc1=src.amos_rpc1, 
                    target.amos_rpc2=src.amos_rpc2, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cuni=src.cuni, 
                    target.cuni_ref_compnr=src.cuni_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.fcmp=src.fcmp, 
                    target.idoc=src.idoc, 
                    target.ityp=src.ityp, 
                    target.pgdt=src.pgdt, 
                    target.quan=src.quan, 
                    target.quan_buar=src.quan_buar, 
                    target.quan_buln=src.quan_buln, 
                    target.quan_bupc=src.quan_bupc, 
                    target.quan_butm=src.quan_butm, 
                    target.quan_buvl=src.quan_buvl, 
                    target.quan_buwg=src.quan_buwg, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amos, amos_cntc, amos_dwhc, amos_homc, amos_prjc, amos_refc, amos_rpc1, amos_rpc2, cact, ccur, ccur_ref_compnr, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cspa_ref_compnr, cprj_ref_compnr, cspa, cuni, cuni_ref_compnr, deleted, fcmp, idoc, ityp, pgdt, quan, quan_buar, quan_buln, quan_bupc, quan_butm, quan_buvl, quan_buwg, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rdat, sequencenumber, sern, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amos, 
                    src.amos_cntc, 
                    src.amos_dwhc, 
                    src.amos_homc, 
                    src.amos_prjc, 
                    src.amos_refc, 
                    src.amos_rpc1, 
                    src.amos_rpc2, 
                    src.cact, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cuni, 
                    src.cuni_ref_compnr, 
                    src.deleted, 
                    src.fcmp, 
                    src.idoc, 
                    src.ityp, 
                    src.pgdt, 
                    src.quan, 
                    src.quan_buar, 
                    src.quan_buln, 
                    src.quan_bupc, 
                    src.quan_butm, 
                    src.quan_buvl, 
                    src.quan_buwg, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.sequencenumber, 
                    src.sern, 
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
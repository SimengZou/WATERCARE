create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPPC232()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPPC232_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPPC232 as target using (SELECT * FROM (SELECT 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.cdoc, 
            strm.cfpo, 
            strm.cfpo_kw, 
            strm.compnr, 
            strm.cper, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cptc, 
            strm.cptc_cyea_cper_ref_compnr, 
            strm.cptc_ref_compnr, 
            strm.cpth, 
            strm.cpth_ref_compnr, 
            strm.cpth_year_peri_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.csub, 
            strm.cuni, 
            strm.cyea, 
            strm.deleted, 
            strm.desc, 
            strm.loco, 
            strm.ltdt, 
            strm.orno, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.peri, 
            strm.pono, 
            strm.quan, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.sern, 
            strm.task, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpth,
            strm.year,
            strm.peri,
            strm.otbp,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC232 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpth=src.cpth) OR (target.cpth IS NULL AND src.cpth IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.peri=src.peri) OR (target.peri IS NULL AND src.peri IS NULL)) AND
            ((target.otbp=src.otbp) OR (target.otbp IS NULL AND src.otbp IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.cdoc=src.cdoc, 
                    target.cfpo=src.cfpo, 
                    target.cfpo_kw=src.cfpo_kw, 
                    target.compnr=src.compnr, 
                    target.cper=src.cper, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cptc=src.cptc, 
                    target.cptc_cyea_cper_ref_compnr=src.cptc_cyea_cper_ref_compnr, 
                    target.cptc_ref_compnr=src.cptc_ref_compnr, 
                    target.cpth=src.cpth, 
                    target.cpth_ref_compnr=src.cpth_ref_compnr, 
                    target.cpth_year_peri_ref_compnr=src.cpth_year_peri_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.csub=src.csub, 
                    target.cuni=src.cuni, 
                    target.cyea=src.cyea, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.loco=src.loco, 
                    target.ltdt=src.ltdt, 
                    target.orno=src.orno, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.peri=src.peri, 
                    target.pono=src.pono, 
                    target.quan=src.quan, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.task=src.task, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cact, 
                    ccco, 
                    ccco_ref_compnr, 
                    cdoc, 
                    cfpo, 
                    cfpo_kw, 
                    compnr, 
                    cper, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cspa_ref_compnr, 
                    cprj_cstl_ref_compnr, 
                    cprj_ref_compnr, 
                    cptc, 
                    cptc_cyea_cper_ref_compnr, 
                    cptc_ref_compnr, 
                    cpth, 
                    cpth_ref_compnr, 
                    cpth_year_peri_ref_compnr, 
                    cspa, 
                    cstl, 
                    csub, 
                    cuni, 
                    cyea, 
                    deleted, 
                    desc, 
                    loco, 
                    ltdt, 
                    orno, 
                    otbp, 
                    otbp_ref_compnr, 
                    peri, 
                    pono, 
                    quan, 
                    rgdt, 
                    sequencenumber, 
                    sern, 
                    task, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.cdoc, 
                    src.cfpo, 
                    src.cfpo_kw, 
                    src.compnr, 
                    src.cper, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cptc, 
                    src.cptc_cyea_cper_ref_compnr, 
                    src.cptc_ref_compnr, 
                    src.cpth, 
                    src.cpth_ref_compnr, 
                    src.cpth_year_peri_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.csub, 
                    src.cuni, 
                    src.cyea, 
                    src.deleted, 
                    src.desc, 
                    src.loco, 
                    src.ltdt, 
                    src.orno, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.peri, 
                    src.pono, 
                    src.quan, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.sern, 
                    src.task, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPPC232_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.cdoc, 
            strm.cfpo, 
            strm.cfpo_kw, 
            strm.compnr, 
            strm.cper, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cptc, 
            strm.cptc_cyea_cper_ref_compnr, 
            strm.cptc_ref_compnr, 
            strm.cpth, 
            strm.cpth_ref_compnr, 
            strm.cpth_year_peri_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.csub, 
            strm.cuni, 
            strm.cyea, 
            strm.deleted, 
            strm.desc, 
            strm.loco, 
            strm.ltdt, 
            strm.orno, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.peri, 
            strm.pono, 
            strm.quan, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.sern, 
            strm.task, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cpth,
            strm.year,
            strm.peri,
            strm.otbp,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC232 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cpth=src.cpth) OR (target.cpth IS NULL AND src.cpth IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.peri=src.peri) OR (target.peri IS NULL AND src.peri IS NULL)) AND
            ((target.otbp=src.otbp) OR (target.otbp IS NULL AND src.otbp IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.cdoc=src.cdoc, 
                    target.cfpo=src.cfpo, 
                    target.cfpo_kw=src.cfpo_kw, 
                    target.compnr=src.compnr, 
                    target.cper=src.cper, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cptc=src.cptc, 
                    target.cptc_cyea_cper_ref_compnr=src.cptc_cyea_cper_ref_compnr, 
                    target.cptc_ref_compnr=src.cptc_ref_compnr, 
                    target.cpth=src.cpth, 
                    target.cpth_ref_compnr=src.cpth_ref_compnr, 
                    target.cpth_year_peri_ref_compnr=src.cpth_year_peri_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.csub=src.csub, 
                    target.cuni=src.cuni, 
                    target.cyea=src.cyea, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.loco=src.loco, 
                    target.ltdt=src.ltdt, 
                    target.orno=src.orno, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.peri=src.peri, 
                    target.pono=src.pono, 
                    target.quan=src.quan, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.task=src.task, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cact, ccco, ccco_ref_compnr, cdoc, cfpo, cfpo_kw, compnr, cper, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cspa_ref_compnr, cprj_cstl_ref_compnr, cprj_ref_compnr, cptc, cptc_cyea_cper_ref_compnr, cptc_ref_compnr, cpth, cpth_ref_compnr, cpth_year_peri_ref_compnr, cspa, cstl, csub, cuni, cyea, deleted, desc, loco, ltdt, orno, otbp, otbp_ref_compnr, peri, pono, quan, rgdt, sequencenumber, sern, task, timestamp, txta, txta_ref_compnr, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.cdoc, 
                    src.cfpo, 
                    src.cfpo_kw, 
                    src.compnr, 
                    src.cper, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cptc, 
                    src.cptc_cyea_cper_ref_compnr, 
                    src.cptc_ref_compnr, 
                    src.cpth, 
                    src.cpth_ref_compnr, 
                    src.cpth_year_peri_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.csub, 
                    src.cuni, 
                    src.cyea, 
                    src.deleted, 
                    src.desc, 
                    src.loco, 
                    src.ltdt, 
                    src.orno, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.peri, 
                    src.pono, 
                    src.quan, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.sern, 
                    src.task, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.year,     
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
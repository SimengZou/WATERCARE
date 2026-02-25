create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPPC420()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPPC420_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPPC420 as target using (SELECT * FROM (SELECT 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amac_4, 
            strm.amap_1, 
            strm.amap_2, 
            strm.amap_3, 
            strm.amap_4, 
            strm.ambg_1, 
            strm.ambg_2, 
            strm.ambg_3, 
            strm.ambg_4, 
            strm.amep_1, 
            strm.amep_2, 
            strm.amep_3, 
            strm.amep_4, 
            strm.amex_1, 
            strm.amex_2, 
            strm.amex_3, 
            strm.amex_4, 
            strm.ampe_1, 
            strm.ampe_2, 
            strm.ampe_3, 
            strm.ampe_4, 
            strm.ampm_1, 
            strm.ampm_2, 
            strm.ampm_3, 
            strm.ampm_4, 
            strm.ampp_1, 
            strm.ampp_2, 
            strm.ampp_3, 
            strm.ampp_4, 
            strm.amrf_1, 
            strm.amrf_2, 
            strm.amrf_3, 
            strm.amrf_4, 
            strm.amrp_1, 
            strm.amrp_2, 
            strm.amrp_3, 
            strm.amrp_4, 
            strm.amrs_1, 
            strm.amrs_2, 
            strm.amrs_3, 
            strm.amrs_4, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.deleted, 
            strm.mpto, 
            strm.mpto_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.mpto,
            strm.cspa ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC420 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.mpto=src.mpto) OR (target.mpto IS NULL AND src.mpto IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amac_4=src.amac_4, 
                    target.amap_1=src.amap_1, 
                    target.amap_2=src.amap_2, 
                    target.amap_3=src.amap_3, 
                    target.amap_4=src.amap_4, 
                    target.ambg_1=src.ambg_1, 
                    target.ambg_2=src.ambg_2, 
                    target.ambg_3=src.ambg_3, 
                    target.ambg_4=src.ambg_4, 
                    target.amep_1=src.amep_1, 
                    target.amep_2=src.amep_2, 
                    target.amep_3=src.amep_3, 
                    target.amep_4=src.amep_4, 
                    target.amex_1=src.amex_1, 
                    target.amex_2=src.amex_2, 
                    target.amex_3=src.amex_3, 
                    target.amex_4=src.amex_4, 
                    target.ampe_1=src.ampe_1, 
                    target.ampe_2=src.ampe_2, 
                    target.ampe_3=src.ampe_3, 
                    target.ampe_4=src.ampe_4, 
                    target.ampm_1=src.ampm_1, 
                    target.ampm_2=src.ampm_2, 
                    target.ampm_3=src.ampm_3, 
                    target.ampm_4=src.ampm_4, 
                    target.ampp_1=src.ampp_1, 
                    target.ampp_2=src.ampp_2, 
                    target.ampp_3=src.ampp_3, 
                    target.ampp_4=src.ampp_4, 
                    target.amrf_1=src.amrf_1, 
                    target.amrf_2=src.amrf_2, 
                    target.amrf_3=src.amrf_3, 
                    target.amrf_4=src.amrf_4, 
                    target.amrp_1=src.amrp_1, 
                    target.amrp_2=src.amrp_2, 
                    target.amrp_3=src.amrp_3, 
                    target.amrp_4=src.amrp_4, 
                    target.amrs_1=src.amrs_1, 
                    target.amrs_2=src.amrs_2, 
                    target.amrs_3=src.amrs_3, 
                    target.amrs_4=src.amrs_4, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.deleted=src.deleted, 
                    target.mpto=src.mpto, 
                    target.mpto_kw=src.mpto_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amac_1, 
                    amac_2, 
                    amac_3, 
                    amac_4, 
                    amap_1, 
                    amap_2, 
                    amap_3, 
                    amap_4, 
                    ambg_1, 
                    ambg_2, 
                    ambg_3, 
                    ambg_4, 
                    amep_1, 
                    amep_2, 
                    amep_3, 
                    amep_4, 
                    amex_1, 
                    amex_2, 
                    amex_3, 
                    amex_4, 
                    ampe_1, 
                    ampe_2, 
                    ampe_3, 
                    ampe_4, 
                    ampm_1, 
                    ampm_2, 
                    ampm_3, 
                    ampm_4, 
                    ampp_1, 
                    ampp_2, 
                    ampp_3, 
                    ampp_4, 
                    amrf_1, 
                    amrf_2, 
                    amrf_3, 
                    amrf_4, 
                    amrp_1, 
                    amrp_2, 
                    amrp_3, 
                    amrp_4, 
                    amrs_1, 
                    amrs_2, 
                    amrs_3, 
                    amrs_4, 
                    compnr, 
                    cprj, 
                    cprj_cspa_ref_compnr, 
                    cprj_ref_compnr, 
                    cspa, 
                    deleted, 
                    mpto, 
                    mpto_kw, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amac_4, 
                    src.amap_1, 
                    src.amap_2, 
                    src.amap_3, 
                    src.amap_4, 
                    src.ambg_1, 
                    src.ambg_2, 
                    src.ambg_3, 
                    src.ambg_4, 
                    src.amep_1, 
                    src.amep_2, 
                    src.amep_3, 
                    src.amep_4, 
                    src.amex_1, 
                    src.amex_2, 
                    src.amex_3, 
                    src.amex_4, 
                    src.ampe_1, 
                    src.ampe_2, 
                    src.ampe_3, 
                    src.ampe_4, 
                    src.ampm_1, 
                    src.ampm_2, 
                    src.ampm_3, 
                    src.ampm_4, 
                    src.ampp_1, 
                    src.ampp_2, 
                    src.ampp_3, 
                    src.ampp_4, 
                    src.amrf_1, 
                    src.amrf_2, 
                    src.amrf_3, 
                    src.amrf_4, 
                    src.amrp_1, 
                    src.amrp_2, 
                    src.amrp_3, 
                    src.amrp_4, 
                    src.amrs_1, 
                    src.amrs_2, 
                    src.amrs_3, 
                    src.amrs_4, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.deleted, 
                    src.mpto, 
                    src.mpto_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPPC420_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amac_4, 
            strm.amap_1, 
            strm.amap_2, 
            strm.amap_3, 
            strm.amap_4, 
            strm.ambg_1, 
            strm.ambg_2, 
            strm.ambg_3, 
            strm.ambg_4, 
            strm.amep_1, 
            strm.amep_2, 
            strm.amep_3, 
            strm.amep_4, 
            strm.amex_1, 
            strm.amex_2, 
            strm.amex_3, 
            strm.amex_4, 
            strm.ampe_1, 
            strm.ampe_2, 
            strm.ampe_3, 
            strm.ampe_4, 
            strm.ampm_1, 
            strm.ampm_2, 
            strm.ampm_3, 
            strm.ampm_4, 
            strm.ampp_1, 
            strm.ampp_2, 
            strm.ampp_3, 
            strm.ampp_4, 
            strm.amrf_1, 
            strm.amrf_2, 
            strm.amrf_3, 
            strm.amrf_4, 
            strm.amrp_1, 
            strm.amrp_2, 
            strm.amrp_3, 
            strm.amrp_4, 
            strm.amrs_1, 
            strm.amrs_2, 
            strm.amrs_3, 
            strm.amrs_4, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.deleted, 
            strm.mpto, 
            strm.mpto_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.mpto,
            strm.cspa ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC420 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.mpto=src.mpto) OR (target.mpto IS NULL AND src.mpto IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) 
                when matched then update set
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amac_4=src.amac_4, 
                    target.amap_1=src.amap_1, 
                    target.amap_2=src.amap_2, 
                    target.amap_3=src.amap_3, 
                    target.amap_4=src.amap_4, 
                    target.ambg_1=src.ambg_1, 
                    target.ambg_2=src.ambg_2, 
                    target.ambg_3=src.ambg_3, 
                    target.ambg_4=src.ambg_4, 
                    target.amep_1=src.amep_1, 
                    target.amep_2=src.amep_2, 
                    target.amep_3=src.amep_3, 
                    target.amep_4=src.amep_4, 
                    target.amex_1=src.amex_1, 
                    target.amex_2=src.amex_2, 
                    target.amex_3=src.amex_3, 
                    target.amex_4=src.amex_4, 
                    target.ampe_1=src.ampe_1, 
                    target.ampe_2=src.ampe_2, 
                    target.ampe_3=src.ampe_3, 
                    target.ampe_4=src.ampe_4, 
                    target.ampm_1=src.ampm_1, 
                    target.ampm_2=src.ampm_2, 
                    target.ampm_3=src.ampm_3, 
                    target.ampm_4=src.ampm_4, 
                    target.ampp_1=src.ampp_1, 
                    target.ampp_2=src.ampp_2, 
                    target.ampp_3=src.ampp_3, 
                    target.ampp_4=src.ampp_4, 
                    target.amrf_1=src.amrf_1, 
                    target.amrf_2=src.amrf_2, 
                    target.amrf_3=src.amrf_3, 
                    target.amrf_4=src.amrf_4, 
                    target.amrp_1=src.amrp_1, 
                    target.amrp_2=src.amrp_2, 
                    target.amrp_3=src.amrp_3, 
                    target.amrp_4=src.amrp_4, 
                    target.amrs_1=src.amrs_1, 
                    target.amrs_2=src.amrs_2, 
                    target.amrs_3=src.amrs_3, 
                    target.amrs_4=src.amrs_4, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.deleted=src.deleted, 
                    target.mpto=src.mpto, 
                    target.mpto_kw=src.mpto_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amac_1, amac_2, amac_3, amac_4, amap_1, amap_2, amap_3, amap_4, ambg_1, ambg_2, ambg_3, ambg_4, amep_1, amep_2, amep_3, amep_4, amex_1, amex_2, amex_3, amex_4, ampe_1, ampe_2, ampe_3, ampe_4, ampm_1, ampm_2, ampm_3, ampm_4, ampp_1, ampp_2, ampp_3, ampp_4, amrf_1, amrf_2, amrf_3, amrf_4, amrp_1, amrp_2, amrp_3, amrp_4, amrs_1, amrs_2, amrs_3, amrs_4, compnr, cprj, cprj_cspa_ref_compnr, cprj_ref_compnr, cspa, deleted, mpto, mpto_kw, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amac_4, 
                    src.amap_1, 
                    src.amap_2, 
                    src.amap_3, 
                    src.amap_4, 
                    src.ambg_1, 
                    src.ambg_2, 
                    src.ambg_3, 
                    src.ambg_4, 
                    src.amep_1, 
                    src.amep_2, 
                    src.amep_3, 
                    src.amep_4, 
                    src.amex_1, 
                    src.amex_2, 
                    src.amex_3, 
                    src.amex_4, 
                    src.ampe_1, 
                    src.ampe_2, 
                    src.ampe_3, 
                    src.ampe_4, 
                    src.ampm_1, 
                    src.ampm_2, 
                    src.ampm_3, 
                    src.ampm_4, 
                    src.ampp_1, 
                    src.ampp_2, 
                    src.ampp_3, 
                    src.ampp_4, 
                    src.amrf_1, 
                    src.amrf_2, 
                    src.amrf_3, 
                    src.amrf_4, 
                    src.amrp_1, 
                    src.amrp_2, 
                    src.amrp_3, 
                    src.amrp_4, 
                    src.amrs_1, 
                    src.amrs_2, 
                    src.amrs_3, 
                    src.amrs_4, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.deleted, 
                    src.mpto, 
                    src.mpto_kw, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC316()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC316_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC316 as target using (SELECT * FROM (SELECT 
            strm.aeqc_1, 
            strm.aeqc_2, 
            strm.aeqc_3, 
            strm.aeqc_4, 
            strm.aicc_1, 
            strm.aicc_2, 
            strm.aicc_3, 
            strm.aicc_4, 
            strm.aicl_1, 
            strm.aicl_2, 
            strm.aicl_3, 
            strm.aicl_4, 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amac_4, 
            strm.asbc_1, 
            strm.asbc_2, 
            strm.asbc_3, 
            strm.asbc_4, 
            strm.atac_1, 
            strm.atac_2, 
            strm.atac_3, 
            strm.atac_4, 
            strm.cact, 
            strm.ccal, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_ccal_ref_compnr, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.qtah, 
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
            strm.ccal,
            strm.cpla,
            strm.cact,
            strm.ccco ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC316 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) AND
            ((target.cpla=src.cpla) OR (target.cpla IS NULL AND src.cpla IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.ccco=src.ccco) OR (target.ccco IS NULL AND src.ccco IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aeqc_1=src.aeqc_1, 
                    target.aeqc_2=src.aeqc_2, 
                    target.aeqc_3=src.aeqc_3, 
                    target.aeqc_4=src.aeqc_4, 
                    target.aicc_1=src.aicc_1, 
                    target.aicc_2=src.aicc_2, 
                    target.aicc_3=src.aicc_3, 
                    target.aicc_4=src.aicc_4, 
                    target.aicl_1=src.aicl_1, 
                    target.aicl_2=src.aicl_2, 
                    target.aicl_3=src.aicl_3, 
                    target.aicl_4=src.aicl_4, 
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amac_4=src.amac_4, 
                    target.asbc_1=src.asbc_1, 
                    target.asbc_2=src.asbc_2, 
                    target.asbc_3=src.asbc_3, 
                    target.asbc_4=src.asbc_4, 
                    target.atac_1=src.atac_1, 
                    target.atac_2=src.atac_2, 
                    target.atac_3=src.atac_3, 
                    target.atac_4=src.atac_4, 
                    target.cact=src.cact, 
                    target.ccal=src.ccal, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_ccal_ref_compnr=src.cprj_ccal_ref_compnr, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.qtah=src.qtah, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aeqc_1, 
                    aeqc_2, 
                    aeqc_3, 
                    aeqc_4, 
                    aicc_1, 
                    aicc_2, 
                    aicc_3, 
                    aicc_4, 
                    aicl_1, 
                    aicl_2, 
                    aicl_3, 
                    aicl_4, 
                    amac_1, 
                    amac_2, 
                    amac_3, 
                    amac_4, 
                    asbc_1, 
                    asbc_2, 
                    asbc_3, 
                    asbc_4, 
                    atac_1, 
                    atac_2, 
                    atac_3, 
                    atac_4, 
                    cact, 
                    ccal, 
                    ccco, 
                    ccco_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_ccal_ref_compnr, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cpla_ref_compnr, 
                    cprj_ref_compnr, 
                    deleted, 
                    qtah, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aeqc_1, 
                    src.aeqc_2, 
                    src.aeqc_3, 
                    src.aeqc_4, 
                    src.aicc_1, 
                    src.aicc_2, 
                    src.aicc_3, 
                    src.aicc_4, 
                    src.aicl_1, 
                    src.aicl_2, 
                    src.aicl_3, 
                    src.aicl_4, 
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amac_4, 
                    src.asbc_1, 
                    src.asbc_2, 
                    src.asbc_3, 
                    src.asbc_4, 
                    src.atac_1, 
                    src.atac_2, 
                    src.atac_3, 
                    src.atac_4, 
                    src.cact, 
                    src.ccal, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_ccal_ref_compnr, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.qtah, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC316_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aeqc_1, 
            strm.aeqc_2, 
            strm.aeqc_3, 
            strm.aeqc_4, 
            strm.aicc_1, 
            strm.aicc_2, 
            strm.aicc_3, 
            strm.aicc_4, 
            strm.aicl_1, 
            strm.aicl_2, 
            strm.aicl_3, 
            strm.aicl_4, 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amac_4, 
            strm.asbc_1, 
            strm.asbc_2, 
            strm.asbc_3, 
            strm.asbc_4, 
            strm.atac_1, 
            strm.atac_2, 
            strm.atac_3, 
            strm.atac_4, 
            strm.cact, 
            strm.ccal, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_ccal_ref_compnr, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.qtah, 
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
            strm.ccal,
            strm.cpla,
            strm.cact,
            strm.ccco ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC316 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) AND
            ((target.cpla=src.cpla) OR (target.cpla IS NULL AND src.cpla IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.ccco=src.ccco) OR (target.ccco IS NULL AND src.ccco IS NULL)) 
                when matched then update set
                    target.aeqc_1=src.aeqc_1, 
                    target.aeqc_2=src.aeqc_2, 
                    target.aeqc_3=src.aeqc_3, 
                    target.aeqc_4=src.aeqc_4, 
                    target.aicc_1=src.aicc_1, 
                    target.aicc_2=src.aicc_2, 
                    target.aicc_3=src.aicc_3, 
                    target.aicc_4=src.aicc_4, 
                    target.aicl_1=src.aicl_1, 
                    target.aicl_2=src.aicl_2, 
                    target.aicl_3=src.aicl_3, 
                    target.aicl_4=src.aicl_4, 
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amac_4=src.amac_4, 
                    target.asbc_1=src.asbc_1, 
                    target.asbc_2=src.asbc_2, 
                    target.asbc_3=src.asbc_3, 
                    target.asbc_4=src.asbc_4, 
                    target.atac_1=src.atac_1, 
                    target.atac_2=src.atac_2, 
                    target.atac_3=src.atac_3, 
                    target.atac_4=src.atac_4, 
                    target.cact=src.cact, 
                    target.ccal=src.ccal, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_ccal_ref_compnr=src.cprj_ccal_ref_compnr, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.qtah=src.qtah, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aeqc_1, aeqc_2, aeqc_3, aeqc_4, aicc_1, aicc_2, aicc_3, aicc_4, aicl_1, aicl_2, aicl_3, aicl_4, amac_1, amac_2, amac_3, amac_4, asbc_1, asbc_2, asbc_3, asbc_4, atac_1, atac_2, atac_3, atac_4, cact, ccal, ccco, ccco_ref_compnr, compnr, cpla, cprj, cprj_ccal_ref_compnr, cprj_cpla_cact_ref_compnr, cprj_cpla_ref_compnr, cprj_ref_compnr, deleted, qtah, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aeqc_1, 
                    src.aeqc_2, 
                    src.aeqc_3, 
                    src.aeqc_4, 
                    src.aicc_1, 
                    src.aicc_2, 
                    src.aicc_3, 
                    src.aicc_4, 
                    src.aicl_1, 
                    src.aicl_2, 
                    src.aicl_3, 
                    src.aicl_4, 
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amac_4, 
                    src.asbc_1, 
                    src.asbc_2, 
                    src.asbc_3, 
                    src.asbc_4, 
                    src.atac_1, 
                    src.atac_2, 
                    src.atac_3, 
                    src.atac_4, 
                    src.cact, 
                    src.ccal, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_ccal_ref_compnr, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.qtah, 
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
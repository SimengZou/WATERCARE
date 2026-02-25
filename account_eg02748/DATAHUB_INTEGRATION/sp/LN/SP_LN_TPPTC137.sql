create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC137()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC137_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC137 as target using (SELECT * FROM (SELECT 
            strm.amoc, 
            strm.bsqn, 
            strm.btdt, 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.ccts, 
            strm.cocu, 
            strm.cocu_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.qtah, 
            strm.quan, 
            strm.rtcc_1, 
            strm.rtcc_2, 
            strm.rtcc_3, 
            strm.rtfc_1, 
            strm.rtfc_2, 
            strm.rtfc_3, 
            strm.sequencenumber, 
            strm.sern, 
            strm.task, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.task,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC137 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.task=src.task) OR (target.task IS NULL AND src.task IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amoc=src.amoc, 
                    target.bsqn=src.bsqn, 
                    target.btdt=src.btdt, 
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.ccts=src.ccts, 
                    target.cocu=src.cocu, 
                    target.cocu_ref_compnr=src.cocu_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.qtah=src.qtah, 
                    target.quan=src.quan, 
                    target.rtcc_1=src.rtcc_1, 
                    target.rtcc_2=src.rtcc_2, 
                    target.rtcc_3=src.rtcc_3, 
                    target.rtfc_1=src.rtfc_1, 
                    target.rtfc_2=src.rtfc_2, 
                    target.rtfc_3=src.rtfc_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.task=src.task, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amoc, 
                    bsqn, 
                    btdt, 
                    cact, 
                    ccco, 
                    ccco_ref_compnr, 
                    ccts, 
                    cocu, 
                    cocu_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cspa_ref_compnr, 
                    cprj_cstl_ref_compnr, 
                    cprj_ref_compnr, 
                    cspa, 
                    cstl, 
                    deleted, 
                    qtah, 
                    quan, 
                    rtcc_1, 
                    rtcc_2, 
                    rtcc_3, 
                    rtfc_1, 
                    rtfc_2, 
                    rtfc_3, 
                    sequencenumber, 
                    sern, 
                    task, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amoc, 
                    src.bsqn, 
                    src.btdt, 
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.ccts, 
                    src.cocu, 
                    src.cocu_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.qtah, 
                    src.quan, 
                    src.rtcc_1, 
                    src.rtcc_2, 
                    src.rtcc_3, 
                    src.rtfc_1, 
                    src.rtfc_2, 
                    src.rtfc_3, 
                    src.sequencenumber, 
                    src.sern, 
                    src.task, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC137_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amoc, 
            strm.bsqn, 
            strm.btdt, 
            strm.cact, 
            strm.ccco, 
            strm.ccco_ref_compnr, 
            strm.ccts, 
            strm.cocu, 
            strm.cocu_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.qtah, 
            strm.quan, 
            strm.rtcc_1, 
            strm.rtcc_2, 
            strm.rtcc_3, 
            strm.rtfc_1, 
            strm.rtfc_2, 
            strm.rtfc_3, 
            strm.sequencenumber, 
            strm.sern, 
            strm.task, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.task,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC137 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.task=src.task) OR (target.task IS NULL AND src.task IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.amoc=src.amoc, 
                    target.bsqn=src.bsqn, 
                    target.btdt=src.btdt, 
                    target.cact=src.cact, 
                    target.ccco=src.ccco, 
                    target.ccco_ref_compnr=src.ccco_ref_compnr, 
                    target.ccts=src.ccts, 
                    target.cocu=src.cocu, 
                    target.cocu_ref_compnr=src.cocu_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.qtah=src.qtah, 
                    target.quan=src.quan, 
                    target.rtcc_1=src.rtcc_1, 
                    target.rtcc_2=src.rtcc_2, 
                    target.rtcc_3=src.rtcc_3, 
                    target.rtfc_1=src.rtfc_1, 
                    target.rtfc_2=src.rtfc_2, 
                    target.rtfc_3=src.rtfc_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.task=src.task, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amoc, bsqn, btdt, cact, ccco, ccco_ref_compnr, ccts, cocu, cocu_ref_compnr, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cspa_ref_compnr, cprj_cstl_ref_compnr, cprj_ref_compnr, cspa, cstl, deleted, qtah, quan, rtcc_1, rtcc_2, rtcc_3, rtfc_1, rtfc_2, rtfc_3, sequencenumber, sern, task, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amoc, 
                    src.bsqn, 
                    src.btdt, 
                    src.cact, 
                    src.ccco, 
                    src.ccco_ref_compnr, 
                    src.ccts, 
                    src.cocu, 
                    src.cocu_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.qtah, 
                    src.quan, 
                    src.rtcc_1, 
                    src.rtcc_2, 
                    src.rtcc_3, 
                    src.rtfc_1, 
                    src.rtfc_2, 
                    src.rtfc_3, 
                    src.sequencenumber, 
                    src.sern, 
                    src.task, 
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
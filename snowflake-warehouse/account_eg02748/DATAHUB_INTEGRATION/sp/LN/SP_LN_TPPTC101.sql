create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC101()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC101_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC101 as target using (SELECT * FROM (SELECT 
            strm.cact, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_cspc_ref_compnr, 
            strm.cprj_cspp_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspc, 
            strm.cspp, 
            strm.deleted, 
            strm.qant, 
            strm.sequencenumber, 
            strm.sern, 
            strm.stat, 
            strm.stat_kw, 
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
            strm.cprj,
            strm.cspp,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC101 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.cspp=src.cspp) OR (target.cspp IS NULL AND src.cspp IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_cspc_ref_compnr=src.cprj_cspc_ref_compnr, 
                    target.cprj_cspp_ref_compnr=src.cprj_cspp_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspc=src.cspc, 
                    target.cspp=src.cspp, 
                    target.deleted=src.deleted, 
                    target.qant=src.qant, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cact, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cpla_ref_compnr, 
                    cprj_cspc_ref_compnr, 
                    cprj_cspp_ref_compnr, 
                    cprj_ref_compnr, 
                    cspc, 
                    cspp, 
                    deleted, 
                    qant, 
                    sequencenumber, 
                    sern, 
                    stat, 
                    stat_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cact, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_cspc_ref_compnr, 
                    src.cprj_cspp_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspc, 
                    src.cspp, 
                    src.deleted, 
                    src.qant, 
                    src.sequencenumber, 
                    src.sern, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC101_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cact, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cpla_ref_compnr, 
            strm.cprj_cspc_ref_compnr, 
            strm.cprj_cspp_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspc, 
            strm.cspp, 
            strm.deleted, 
            strm.qant, 
            strm.sequencenumber, 
            strm.sern, 
            strm.stat, 
            strm.stat_kw, 
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
            strm.cprj,
            strm.cspp,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC101 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.cspp=src.cspp) OR (target.cspp IS NULL AND src.cspp IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cpla_ref_compnr=src.cprj_cpla_ref_compnr, 
                    target.cprj_cspc_ref_compnr=src.cprj_cspc_ref_compnr, 
                    target.cprj_cspp_ref_compnr=src.cprj_cspp_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspc=src.cspc, 
                    target.cspp=src.cspp, 
                    target.deleted=src.deleted, 
                    target.qant=src.qant, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cact, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cpla_ref_compnr, cprj_cspc_ref_compnr, cprj_cspp_ref_compnr, cprj_ref_compnr, cspc, cspp, deleted, qant, sequencenumber, sern, stat, stat_kw, timestamp, txta, txta_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cact, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cpla_ref_compnr, 
                    src.cprj_cspc_ref_compnr, 
                    src.cprj_cspp_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspc, 
                    src.cspp, 
                    src.deleted, 
                    src.qant, 
                    src.sequencenumber, 
                    src.sern, 
                    src.stat, 
                    src.stat_kw, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TIPCS020()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TIPCS020_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TIPCS020 as target using (SELECT * FROM (SELECT 
            strm.bpid, 
            strm.bpid_ref_compnr, 
            strm.ccgr, 
            strm.ccgr_ref_compnr, 
            strm.cfdt, 
            strm.clco, 
            strm.clco_ref_compnr, 
            strm.compnr, 
            strm.cpcc, 
            strm.cpcc_ref_compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.dtfs, 
            strm.dtfs_kw, 
            strm.ffci, 
            strm.kopr, 
            strm.kopr_kw, 
            strm.pemp, 
            strm.pemp_ref_compnr, 
            strm.peng, 
            strm.peng_kw, 
            strm.refe, 
            strm.seak, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIPCS020 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bpid=src.bpid, 
                    target.bpid_ref_compnr=src.bpid_ref_compnr, 
                    target.ccgr=src.ccgr, 
                    target.ccgr_ref_compnr=src.ccgr_ref_compnr, 
                    target.cfdt=src.cfdt, 
                    target.clco=src.clco, 
                    target.clco_ref_compnr=src.clco_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcc=src.cpcc, 
                    target.cpcc_ref_compnr=src.cpcc_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dtfs=src.dtfs, 
                    target.dtfs_kw=src.dtfs_kw, 
                    target.ffci=src.ffci, 
                    target.kopr=src.kopr, 
                    target.kopr_kw=src.kopr_kw, 
                    target.pemp=src.pemp, 
                    target.pemp_ref_compnr=src.pemp_ref_compnr, 
                    target.peng=src.peng, 
                    target.peng_kw=src.peng_kw, 
                    target.refe=src.refe, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bpid, 
                    bpid_ref_compnr, 
                    ccgr, 
                    ccgr_ref_compnr, 
                    cfdt, 
                    clco, 
                    clco_ref_compnr, 
                    compnr, 
                    cpcc, 
                    cpcc_ref_compnr, 
                    cprj, 
                    cprj_ref_compnr, 
                    deleted, 
                    dtfs, 
                    dtfs_kw, 
                    ffci, 
                    kopr, 
                    kopr_kw, 
                    pemp, 
                    pemp_ref_compnr, 
                    peng, 
                    peng_kw, 
                    refe, 
                    seak, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bpid, 
                    src.bpid_ref_compnr, 
                    src.ccgr, 
                    src.ccgr_ref_compnr, 
                    src.cfdt, 
                    src.clco, 
                    src.clco_ref_compnr, 
                    src.compnr, 
                    src.cpcc, 
                    src.cpcc_ref_compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.dtfs, 
                    src.dtfs_kw, 
                    src.ffci, 
                    src.kopr, 
                    src.kopr_kw, 
                    src.pemp, 
                    src.pemp_ref_compnr, 
                    src.peng, 
                    src.peng_kw, 
                    src.refe, 
                    src.seak, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TIPCS020_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bpid, 
            strm.bpid_ref_compnr, 
            strm.ccgr, 
            strm.ccgr_ref_compnr, 
            strm.cfdt, 
            strm.clco, 
            strm.clco_ref_compnr, 
            strm.compnr, 
            strm.cpcc, 
            strm.cpcc_ref_compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.dtfs, 
            strm.dtfs_kw, 
            strm.ffci, 
            strm.kopr, 
            strm.kopr_kw, 
            strm.pemp, 
            strm.pemp_ref_compnr, 
            strm.peng, 
            strm.peng_kw, 
            strm.refe, 
            strm.seak, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TIPCS020 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) 
                when matched then update set
                    target.bpid=src.bpid, 
                    target.bpid_ref_compnr=src.bpid_ref_compnr, 
                    target.ccgr=src.ccgr, 
                    target.ccgr_ref_compnr=src.ccgr_ref_compnr, 
                    target.cfdt=src.cfdt, 
                    target.clco=src.clco, 
                    target.clco_ref_compnr=src.clco_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcc=src.cpcc, 
                    target.cpcc_ref_compnr=src.cpcc_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dtfs=src.dtfs, 
                    target.dtfs_kw=src.dtfs_kw, 
                    target.ffci=src.ffci, 
                    target.kopr=src.kopr, 
                    target.kopr_kw=src.kopr_kw, 
                    target.pemp=src.pemp, 
                    target.pemp_ref_compnr=src.pemp_ref_compnr, 
                    target.peng=src.peng, 
                    target.peng_kw=src.peng_kw, 
                    target.refe=src.refe, 
                    target.seak=src.seak, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bpid, bpid_ref_compnr, ccgr, ccgr_ref_compnr, cfdt, clco, clco_ref_compnr, compnr, cpcc, cpcc_ref_compnr, cprj, cprj_ref_compnr, deleted, dtfs, dtfs_kw, ffci, kopr, kopr_kw, pemp, pemp_ref_compnr, peng, peng_kw, refe, seak, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bpid, 
                    src.bpid_ref_compnr, 
                    src.ccgr, 
                    src.ccgr_ref_compnr, 
                    src.cfdt, 
                    src.clco, 
                    src.clco_ref_compnr, 
                    src.compnr, 
                    src.cpcc, 
                    src.cpcc_ref_compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.dtfs, 
                    src.dtfs_kw, 
                    src.ffci, 
                    src.kopr, 
                    src.kopr_kw, 
                    src.pemp, 
                    src.pemp_ref_compnr, 
                    src.peng, 
                    src.peng_kw, 
                    src.refe, 
                    src.seak, 
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
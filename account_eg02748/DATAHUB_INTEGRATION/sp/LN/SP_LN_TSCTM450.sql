create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM450()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM450_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM450 as target using (SELECT * FROM (SELECT 
            strm.acpp_1, 
            strm.acpp_2, 
            strm.acpp_3, 
            strm.cchn, 
            strm.cfln, 
            strm.compnr, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.cvdy, 
            strm.deleted, 
            strm.esco_1, 
            strm.esco_2, 
            strm.esco_3, 
            strm.esrv, 
            strm.esrv_dwhc, 
            strm.esrv_homc, 
            strm.esrv_refc, 
            strm.esrv_rpc1, 
            strm.esrv_rpc2, 
            strm.fspr, 
            strm.fsyr, 
            strm.nody, 
            strm.rcrv, 
            strm.rcrv_dwhc, 
            strm.rcrv_homc, 
            strm.rcrv_refc, 
            strm.rcrv_rpc1, 
            strm.rcrv_rpc2, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn,
            strm.cfln,
            strm.fsyr,
            strm.fspr ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM450 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.fsyr=src.fsyr) OR (target.fsyr IS NULL AND src.fsyr IS NULL)) AND
            ((target.fspr=src.fspr) OR (target.fspr IS NULL AND src.fspr IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acpp_1=src.acpp_1, 
                    target.acpp_2=src.acpp_2, 
                    target.acpp_3=src.acpp_3, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.cvdy=src.cvdy, 
                    target.deleted=src.deleted, 
                    target.esco_1=src.esco_1, 
                    target.esco_2=src.esco_2, 
                    target.esco_3=src.esco_3, 
                    target.esrv=src.esrv, 
                    target.esrv_dwhc=src.esrv_dwhc, 
                    target.esrv_homc=src.esrv_homc, 
                    target.esrv_refc=src.esrv_refc, 
                    target.esrv_rpc1=src.esrv_rpc1, 
                    target.esrv_rpc2=src.esrv_rpc2, 
                    target.fspr=src.fspr, 
                    target.fsyr=src.fsyr, 
                    target.nody=src.nody, 
                    target.rcrv=src.rcrv, 
                    target.rcrv_dwhc=src.rcrv_dwhc, 
                    target.rcrv_homc=src.rcrv_homc, 
                    target.rcrv_refc=src.rcrv_refc, 
                    target.rcrv_rpc1=src.rcrv_rpc1, 
                    target.rcrv_rpc2=src.rcrv_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acpp_1, 
                    acpp_2, 
                    acpp_3, 
                    cchn, 
                    cfln, 
                    compnr, 
                    csco, 
                    csco_cchn_ref_compnr, 
                    csco_ref_compnr, 
                    cvdy, 
                    deleted, 
                    esco_1, 
                    esco_2, 
                    esco_3, 
                    esrv, 
                    esrv_dwhc, 
                    esrv_homc, 
                    esrv_refc, 
                    esrv_rpc1, 
                    esrv_rpc2, 
                    fspr, 
                    fsyr, 
                    nody, 
                    rcrv, 
                    rcrv_dwhc, 
                    rcrv_homc, 
                    rcrv_refc, 
                    rcrv_rpc1, 
                    rcrv_rpc2, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acpp_1, 
                    src.acpp_2, 
                    src.acpp_3, 
                    src.cchn, 
                    src.cfln, 
                    src.compnr, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.cvdy, 
                    src.deleted, 
                    src.esco_1, 
                    src.esco_2, 
                    src.esco_3, 
                    src.esrv, 
                    src.esrv_dwhc, 
                    src.esrv_homc, 
                    src.esrv_refc, 
                    src.esrv_rpc1, 
                    src.esrv_rpc2, 
                    src.fspr, 
                    src.fsyr, 
                    src.nody, 
                    src.rcrv, 
                    src.rcrv_dwhc, 
                    src.rcrv_homc, 
                    src.rcrv_refc, 
                    src.rcrv_rpc1, 
                    src.rcrv_rpc2, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM450_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acpp_1, 
            strm.acpp_2, 
            strm.acpp_3, 
            strm.cchn, 
            strm.cfln, 
            strm.compnr, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.cvdy, 
            strm.deleted, 
            strm.esco_1, 
            strm.esco_2, 
            strm.esco_3, 
            strm.esrv, 
            strm.esrv_dwhc, 
            strm.esrv_homc, 
            strm.esrv_refc, 
            strm.esrv_rpc1, 
            strm.esrv_rpc2, 
            strm.fspr, 
            strm.fsyr, 
            strm.nody, 
            strm.rcrv, 
            strm.rcrv_dwhc, 
            strm.rcrv_homc, 
            strm.rcrv_refc, 
            strm.rcrv_rpc1, 
            strm.rcrv_rpc2, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn,
            strm.cfln,
            strm.fsyr,
            strm.fspr ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM450 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.fsyr=src.fsyr) OR (target.fsyr IS NULL AND src.fsyr IS NULL)) AND
            ((target.fspr=src.fspr) OR (target.fspr IS NULL AND src.fspr IS NULL)) 
                when matched then update set
                    target.acpp_1=src.acpp_1, 
                    target.acpp_2=src.acpp_2, 
                    target.acpp_3=src.acpp_3, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.compnr=src.compnr, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.cvdy=src.cvdy, 
                    target.deleted=src.deleted, 
                    target.esco_1=src.esco_1, 
                    target.esco_2=src.esco_2, 
                    target.esco_3=src.esco_3, 
                    target.esrv=src.esrv, 
                    target.esrv_dwhc=src.esrv_dwhc, 
                    target.esrv_homc=src.esrv_homc, 
                    target.esrv_refc=src.esrv_refc, 
                    target.esrv_rpc1=src.esrv_rpc1, 
                    target.esrv_rpc2=src.esrv_rpc2, 
                    target.fspr=src.fspr, 
                    target.fsyr=src.fsyr, 
                    target.nody=src.nody, 
                    target.rcrv=src.rcrv, 
                    target.rcrv_dwhc=src.rcrv_dwhc, 
                    target.rcrv_homc=src.rcrv_homc, 
                    target.rcrv_refc=src.rcrv_refc, 
                    target.rcrv_rpc1=src.rcrv_rpc1, 
                    target.rcrv_rpc2=src.rcrv_rpc2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acpp_1, acpp_2, acpp_3, cchn, cfln, compnr, csco, csco_cchn_ref_compnr, csco_ref_compnr, cvdy, deleted, esco_1, esco_2, esco_3, esrv, esrv_dwhc, esrv_homc, esrv_refc, esrv_rpc1, esrv_rpc2, fspr, fsyr, nody, rcrv, rcrv_dwhc, rcrv_homc, rcrv_refc, rcrv_rpc1, rcrv_rpc2, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acpp_1, 
                    src.acpp_2, 
                    src.acpp_3, 
                    src.cchn, 
                    src.cfln, 
                    src.compnr, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.cvdy, 
                    src.deleted, 
                    src.esco_1, 
                    src.esco_2, 
                    src.esco_3, 
                    src.esrv, 
                    src.esrv_dwhc, 
                    src.esrv_homc, 
                    src.esrv_refc, 
                    src.esrv_rpc1, 
                    src.esrv_rpc2, 
                    src.fspr, 
                    src.fsyr, 
                    src.nody, 
                    src.rcrv, 
                    src.rcrv_dwhc, 
                    src.rcrv_homc, 
                    src.rcrv_refc, 
                    src.rcrv_rpc1, 
                    src.rcrv_rpc2, 
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
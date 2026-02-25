create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM805()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM805_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM805 as target using (SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.book, 
            strm.bpid, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
            strm.lkey, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.rdpr_1, 
            strm.rdpr_2, 
            strm.rdpr_3, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.book,
            strm.tkey,
            strm.bpid,
            strm.lkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM805 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) AND
            ((target.lkey=src.lkey) OR (target.lkey IS NULL AND src.lkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.book=src.book, 
                    target.bpid=src.bpid, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
                    target.lkey=src.lkey, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.rdpr_1=src.rdpr_1, 
                    target.rdpr_2=src.rdpr_2, 
                    target.rdpr_3=src.rdpr_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aext, 
                    anbr, 
                    book, 
                    bpid, 
                    compnr, 
                    cost_1, 
                    cost_2, 
                    cost_3, 
                    deleted, 
                    depr_1, 
                    depr_2, 
                    depr_3, 
                    lkey, 
                    rcst_1, 
                    rcst_2, 
                    rcst_3, 
                    rdpr_1, 
                    rdpr_2, 
                    rdpr_3, 
                    sequencenumber, 
                    timestamp, 
                    tkey, 
                    tkey_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.book, 
                    src.bpid, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
                    src.lkey, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.rdpr_1, 
                    src.rdpr_2, 
                    src.rdpr_3, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM805_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.book, 
            strm.bpid, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
            strm.lkey, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.rdpr_1, 
            strm.rdpr_2, 
            strm.rdpr_3, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.book,
            strm.tkey,
            strm.bpid,
            strm.lkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM805 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) AND
            ((target.lkey=src.lkey) OR (target.lkey IS NULL AND src.lkey IS NULL)) 
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.book=src.book, 
                    target.bpid=src.bpid, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
                    target.lkey=src.lkey, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.rdpr_1=src.rdpr_1, 
                    target.rdpr_2=src.rdpr_2, 
                    target.rdpr_3=src.rdpr_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aext, anbr, book, bpid, compnr, cost_1, cost_2, cost_3, deleted, depr_1, depr_2, depr_3, lkey, rcst_1, rcst_2, rcst_3, rdpr_1, rdpr_2, rdpr_3, sequencenumber, timestamp, tkey, tkey_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.book, 
                    src.bpid, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
                    src.lkey, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.rdpr_1, 
                    src.rdpr_2, 
                    src.rdpr_3, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
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
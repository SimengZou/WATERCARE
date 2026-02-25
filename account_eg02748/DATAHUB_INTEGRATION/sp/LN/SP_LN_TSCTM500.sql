create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM500()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM500_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM500 as target using (SELECT * FROM (SELECT 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.cwte, 
            strm.cwte_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.efdt, 
            strm.exdt, 
            strm.gwte, 
            strm.nrpe, 
            strm.peru, 
            strm.peru_kw, 
            strm.sequencenumber, 
            strm.term, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.uprd, 
            strm.uprd_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.gwte ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM500 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.gwte=src.gwte) OR (target.gwte IS NULL AND src.gwte IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.cwte=src.cwte, 
                    target.cwte_ref_compnr=src.cwte_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.efdt=src.efdt, 
                    target.exdt=src.exdt, 
                    target.gwte=src.gwte, 
                    target.nrpe=src.nrpe, 
                    target.peru=src.peru, 
                    target.peru_kw=src.peru_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.term=src.term, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.uprd=src.uprd, 
                    target.uprd_kw=src.uprd_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cwoc, 
                    cwoc_ref_compnr, 
                    cwte, 
                    cwte_ref_compnr, 
                    deleted, 
                    desc, 
                    efdt, 
                    exdt, 
                    gwte, 
                    nrpe, 
                    peru, 
                    peru_kw, 
                    sequencenumber, 
                    term, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    uprd, 
                    uprd_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.cwte, 
                    src.cwte_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.efdt, 
                    src.exdt, 
                    src.gwte, 
                    src.nrpe, 
                    src.peru, 
                    src.peru_kw, 
                    src.sequencenumber, 
                    src.term, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.uprd, 
                    src.uprd_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM500_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cwoc, 
            strm.cwoc_ref_compnr, 
            strm.cwte, 
            strm.cwte_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.efdt, 
            strm.exdt, 
            strm.gwte, 
            strm.nrpe, 
            strm.peru, 
            strm.peru_kw, 
            strm.sequencenumber, 
            strm.term, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.uprd, 
            strm.uprd_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.gwte ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM500 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.gwte=src.gwte) OR (target.gwte IS NULL AND src.gwte IS NULL)) 
                when matched then update set
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwoc=src.cwoc, 
                    target.cwoc_ref_compnr=src.cwoc_ref_compnr, 
                    target.cwte=src.cwte, 
                    target.cwte_ref_compnr=src.cwte_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.efdt=src.efdt, 
                    target.exdt=src.exdt, 
                    target.gwte=src.gwte, 
                    target.nrpe=src.nrpe, 
                    target.peru=src.peru, 
                    target.peru_kw=src.peru_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.term=src.term, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.uprd=src.uprd, 
                    target.uprd_kw=src.uprd_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ccur, ccur_ref_compnr, compnr, cwoc, cwoc_ref_compnr, cwte, cwte_ref_compnr, deleted, desc, efdt, exdt, gwte, nrpe, peru, peru_kw, sequencenumber, term, timestamp, txta, txta_ref_compnr, uprd, uprd_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cwoc, 
                    src.cwoc_ref_compnr, 
                    src.cwte, 
                    src.cwte_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.efdt, 
                    src.exdt, 
                    src.gwte, 
                    src.nrpe, 
                    src.peru, 
                    src.peru_kw, 
                    src.sequencenumber, 
                    src.term, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.uprd, 
                    src.uprd_kw, 
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
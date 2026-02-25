create or replace procedure DATAHUB_INTEGRATION.SP_LN_TTDPM510()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TTDPM510_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TTDPM510 as target using (SELECT * FROM (SELECT 
            strm.comp, 
            strm.comp_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dset, 
            strm.dset_ref_compnr, 
            strm.pacc, 
            strm.pacc_dset_ref_compnr, 
            strm.pacc_ref_compnr, 
            strm.pubd, 
            strm.pubd_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pacc,
            strm.dset,
            strm.comp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TTDPM510 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pacc=src.pacc) OR (target.pacc IS NULL AND src.pacc IS NULL)) AND
            ((target.dset=src.dset) OR (target.dset IS NULL AND src.dset IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.comp=src.comp, 
                    target.comp_ref_compnr=src.comp_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dset=src.dset, 
                    target.dset_ref_compnr=src.dset_ref_compnr, 
                    target.pacc=src.pacc, 
                    target.pacc_dset_ref_compnr=src.pacc_dset_ref_compnr, 
                    target.pacc_ref_compnr=src.pacc_ref_compnr, 
                    target.pubd=src.pubd, 
                    target.pubd_kw=src.pubd_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    comp, 
                    comp_ref_compnr, 
                    compnr, 
                    deleted, 
                    dset, 
                    dset_ref_compnr, 
                    pacc, 
                    pacc_dset_ref_compnr, 
                    pacc_ref_compnr, 
                    pubd, 
                    pubd_kw, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.comp, 
                    src.comp_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dset, 
                    src.dset_ref_compnr, 
                    src.pacc, 
                    src.pacc_dset_ref_compnr, 
                    src.pacc_ref_compnr, 
                    src.pubd, 
                    src.pubd_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TTDPM510_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.comp, 
            strm.comp_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dset, 
            strm.dset_ref_compnr, 
            strm.pacc, 
            strm.pacc_dset_ref_compnr, 
            strm.pacc_ref_compnr, 
            strm.pubd, 
            strm.pubd_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.pacc,
            strm.dset,
            strm.comp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TTDPM510 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.pacc=src.pacc) OR (target.pacc IS NULL AND src.pacc IS NULL)) AND
            ((target.dset=src.dset) OR (target.dset IS NULL AND src.dset IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) 
                when matched then update set
                    target.comp=src.comp, 
                    target.comp_ref_compnr=src.comp_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dset=src.dset, 
                    target.dset_ref_compnr=src.dset_ref_compnr, 
                    target.pacc=src.pacc, 
                    target.pacc_dset_ref_compnr=src.pacc_dset_ref_compnr, 
                    target.pacc_ref_compnr=src.pacc_ref_compnr, 
                    target.pubd=src.pubd, 
                    target.pubd_kw=src.pubd_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( comp, comp_ref_compnr, compnr, deleted, dset, dset_ref_compnr, pacc, pacc_dset_ref_compnr, pacc_ref_compnr, pubd, pubd_kw, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.comp, 
                    src.comp_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dset, 
                    src.dset_ref_compnr, 
                    src.pacc, 
                    src.pacc_dset_ref_compnr, 
                    src.pacc_ref_compnr, 
                    src.pubd, 
                    src.pubd_kw, 
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
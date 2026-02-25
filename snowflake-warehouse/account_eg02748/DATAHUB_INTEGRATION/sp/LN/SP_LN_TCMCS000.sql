create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS000()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS000_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS000 as target using (SELECT * FROM (SELECT 
            strm.care, 
            strm.care_ref_compnr, 
            strm.clen, 
            strm.clen_ref_compnr, 
            strm.compnr, 
            strm.ctim, 
            strm.ctim_ref_compnr, 
            strm.cvol, 
            strm.cvol_ref_compnr, 
            strm.cwei, 
            strm.cwei_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.indt, 
            strm.logn, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.indt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS000 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.indt=src.indt) OR (target.indt IS NULL AND src.indt IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.care=src.care, 
                    target.care_ref_compnr=src.care_ref_compnr, 
                    target.clen=src.clen, 
                    target.clen_ref_compnr=src.clen_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.ctim=src.ctim, 
                    target.ctim_ref_compnr=src.ctim_ref_compnr, 
                    target.cvol=src.cvol, 
                    target.cvol_ref_compnr=src.cvol_ref_compnr, 
                    target.cwei=src.cwei, 
                    target.cwei_ref_compnr=src.cwei_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.indt=src.indt, 
                    target.logn=src.logn, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    care, 
                    care_ref_compnr, 
                    clen, 
                    clen_ref_compnr, 
                    compnr, 
                    ctim, 
                    ctim_ref_compnr, 
                    cvol, 
                    cvol_ref_compnr, 
                    cwei, 
                    cwei_ref_compnr, 
                    deleted, 
                    dsca, 
                    indt, 
                    logn, 
                    rtyp, 
                    rtyp_ref_compnr, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.care, 
                    src.care_ref_compnr, 
                    src.clen, 
                    src.clen_ref_compnr, 
                    src.compnr, 
                    src.ctim, 
                    src.ctim_ref_compnr, 
                    src.cvol, 
                    src.cvol_ref_compnr, 
                    src.cwei, 
                    src.cwei_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.indt, 
                    src.logn, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS000_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.care, 
            strm.care_ref_compnr, 
            strm.clen, 
            strm.clen_ref_compnr, 
            strm.compnr, 
            strm.ctim, 
            strm.ctim_ref_compnr, 
            strm.cvol, 
            strm.cvol_ref_compnr, 
            strm.cwei, 
            strm.cwei_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.indt, 
            strm.logn, 
            strm.rtyp, 
            strm.rtyp_ref_compnr, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.indt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS000 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.indt=src.indt) OR (target.indt IS NULL AND src.indt IS NULL)) 
                when matched then update set
                    target.care=src.care, 
                    target.care_ref_compnr=src.care_ref_compnr, 
                    target.clen=src.clen, 
                    target.clen_ref_compnr=src.clen_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.ctim=src.ctim, 
                    target.ctim_ref_compnr=src.ctim_ref_compnr, 
                    target.cvol=src.cvol, 
                    target.cvol_ref_compnr=src.cvol_ref_compnr, 
                    target.cwei=src.cwei, 
                    target.cwei_ref_compnr=src.cwei_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.indt=src.indt, 
                    target.logn=src.logn, 
                    target.rtyp=src.rtyp, 
                    target.rtyp_ref_compnr=src.rtyp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( care, care_ref_compnr, clen, clen_ref_compnr, compnr, ctim, ctim_ref_compnr, cvol, cvol_ref_compnr, cwei, cwei_ref_compnr, deleted, dsca, indt, logn, rtyp, rtyp_ref_compnr, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.care, 
                    src.care_ref_compnr, 
                    src.clen, 
                    src.clen_ref_compnr, 
                    src.compnr, 
                    src.ctim, 
                    src.ctim_ref_compnr, 
                    src.cvol, 
                    src.cvol_ref_compnr, 
                    src.cwei, 
                    src.cwei_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.indt, 
                    src.logn, 
                    src.rtyp, 
                    src.rtyp_ref_compnr, 
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
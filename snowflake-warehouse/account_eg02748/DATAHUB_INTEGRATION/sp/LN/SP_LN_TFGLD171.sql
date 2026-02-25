create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD171()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD171_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD171 as target using (SELECT * FROM (SELECT 
            strm.acid, 
            strm.acty, 
            strm.acty_kw, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.mapp, 
            strm.mapp_kw, 
            strm.paci, 
            strm.pseq, 
            strm.sequencenumber, 
            strm.subl, 
            strm.taxo, 
            strm.taxo_vers_ref_compnr, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.unas, 
            strm.unas_kw, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.taxo,
            strm.vers,
            strm.acid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD171 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.taxo=src.taxo) OR (target.taxo IS NULL AND src.taxo IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.acid=src.acid) OR (target.acid IS NULL AND src.acid IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acid=src.acid, 
                    target.acty=src.acty, 
                    target.acty_kw=src.acty_kw, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.mapp=src.mapp, 
                    target.mapp_kw=src.mapp_kw, 
                    target.paci=src.paci, 
                    target.pseq=src.pseq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.subl=src.subl, 
                    target.taxo=src.taxo, 
                    target.taxo_vers_ref_compnr=src.taxo_vers_ref_compnr, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.unas=src.unas, 
                    target.unas_kw=src.unas_kw, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acid, 
                    acty, 
                    acty_kw, 
                    compnr, 
                    deleted, 
                    desc, 
                    mapp, 
                    mapp_kw, 
                    paci, 
                    pseq, 
                    sequencenumber, 
                    subl, 
                    taxo, 
                    taxo_vers_ref_compnr, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    unas, 
                    unas_kw, 
                    username, 
                    vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acid, 
                    src.acty, 
                    src.acty_kw, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.mapp, 
                    src.mapp_kw, 
                    src.paci, 
                    src.pseq, 
                    src.sequencenumber, 
                    src.subl, 
                    src.taxo, 
                    src.taxo_vers_ref_compnr, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.unas, 
                    src.unas_kw, 
                    src.username, 
                    src.vers,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD171_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acid, 
            strm.acty, 
            strm.acty_kw, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.mapp, 
            strm.mapp_kw, 
            strm.paci, 
            strm.pseq, 
            strm.sequencenumber, 
            strm.subl, 
            strm.taxo, 
            strm.taxo_vers_ref_compnr, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.unas, 
            strm.unas_kw, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.taxo,
            strm.vers,
            strm.acid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD171 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.taxo=src.taxo) OR (target.taxo IS NULL AND src.taxo IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.acid=src.acid) OR (target.acid IS NULL AND src.acid IS NULL)) 
                when matched then update set
                    target.acid=src.acid, 
                    target.acty=src.acty, 
                    target.acty_kw=src.acty_kw, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.mapp=src.mapp, 
                    target.mapp_kw=src.mapp_kw, 
                    target.paci=src.paci, 
                    target.pseq=src.pseq, 
                    target.sequencenumber=src.sequencenumber, 
                    target.subl=src.subl, 
                    target.taxo=src.taxo, 
                    target.taxo_vers_ref_compnr=src.taxo_vers_ref_compnr, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.unas=src.unas, 
                    target.unas_kw=src.unas_kw, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acid, acty, acty_kw, compnr, deleted, desc, mapp, mapp_kw, paci, pseq, sequencenumber, subl, taxo, taxo_vers_ref_compnr, text, text_ref_compnr, timestamp, unas, unas_kw, username, vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acid, 
                    src.acty, 
                    src.acty_kw, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.mapp, 
                    src.mapp_kw, 
                    src.paci, 
                    src.pseq, 
                    src.sequencenumber, 
                    src.subl, 
                    src.taxo, 
                    src.taxo_vers_ref_compnr, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.unas, 
                    src.unas_kw, 
                    src.username, 
                    src.vers,     
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
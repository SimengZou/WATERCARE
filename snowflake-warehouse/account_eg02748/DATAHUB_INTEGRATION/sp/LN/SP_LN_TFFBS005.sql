create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFBS005()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFBS005_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFBS005 as target using (SELECT * FROM (SELECT 
            strm.budg, 
            strm.budg_ref_compnr, 
            strm.cbud, 
            strm.cbud_ref_compnr, 
            strm.cbyr, 
            strm.cbyr_cbud_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.dbud, 
            strm.dbud_ref_compnr, 
            strm.dbyr, 
            strm.dbyr_dbud_ref_compnr, 
            strm.defi, 
            strm.defi_kw, 
            strm.deleted, 
            strm.disb, 
            strm.disb_ref_compnr, 
            strm.lmdt, 
            strm.pbud, 
            strm.pbud_ref_compnr, 
            strm.pbyr, 
            strm.pbyr_pbud_ref_compnr, 
            strm.ratc_1, 
            strm.ratc_2, 
            strm.ratc_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.sequencenumber, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.year,
            strm.budg ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS005 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.budg=src.budg) OR (target.budg IS NULL AND src.budg IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.budg=src.budg, 
                    target.budg_ref_compnr=src.budg_ref_compnr, 
                    target.cbud=src.cbud, 
                    target.cbud_ref_compnr=src.cbud_ref_compnr, 
                    target.cbyr=src.cbyr, 
                    target.cbyr_cbud_ref_compnr=src.cbyr_cbud_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.dbud=src.dbud, 
                    target.dbud_ref_compnr=src.dbud_ref_compnr, 
                    target.dbyr=src.dbyr, 
                    target.dbyr_dbud_ref_compnr=src.dbyr_dbud_ref_compnr, 
                    target.defi=src.defi, 
                    target.defi_kw=src.defi_kw, 
                    target.deleted=src.deleted, 
                    target.disb=src.disb, 
                    target.disb_ref_compnr=src.disb_ref_compnr, 
                    target.lmdt=src.lmdt, 
                    target.pbud=src.pbud, 
                    target.pbud_ref_compnr=src.pbud_ref_compnr, 
                    target.pbyr=src.pbyr, 
                    target.pbyr_pbud_ref_compnr=src.pbyr_pbud_ref_compnr, 
                    target.ratc_1=src.ratc_1, 
                    target.ratc_2=src.ratc_2, 
                    target.ratc_3=src.ratc_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    budg, 
                    budg_ref_compnr, 
                    cbud, 
                    cbud_ref_compnr, 
                    cbyr, 
                    cbyr_cbud_ref_compnr, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    dbud, 
                    dbud_ref_compnr, 
                    dbyr, 
                    dbyr_dbud_ref_compnr, 
                    defi, 
                    defi_kw, 
                    deleted, 
                    disb, 
                    disb_ref_compnr, 
                    lmdt, 
                    pbud, 
                    pbud_ref_compnr, 
                    pbyr, 
                    pbyr_pbud_ref_compnr, 
                    ratc_1, 
                    ratc_2, 
                    ratc_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    sequencenumber, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.budg, 
                    src.budg_ref_compnr, 
                    src.cbud, 
                    src.cbud_ref_compnr, 
                    src.cbyr, 
                    src.cbyr_cbud_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.dbud, 
                    src.dbud_ref_compnr, 
                    src.dbyr, 
                    src.dbyr_dbud_ref_compnr, 
                    src.defi, 
                    src.defi_kw, 
                    src.deleted, 
                    src.disb, 
                    src.disb_ref_compnr, 
                    src.lmdt, 
                    src.pbud, 
                    src.pbud_ref_compnr, 
                    src.pbyr, 
                    src.pbyr_pbud_ref_compnr, 
                    src.ratc_1, 
                    src.ratc_2, 
                    src.ratc_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.sequencenumber, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFBS005_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.budg, 
            strm.budg_ref_compnr, 
            strm.cbud, 
            strm.cbud_ref_compnr, 
            strm.cbyr, 
            strm.cbyr_cbud_ref_compnr, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.dbud, 
            strm.dbud_ref_compnr, 
            strm.dbyr, 
            strm.dbyr_dbud_ref_compnr, 
            strm.defi, 
            strm.defi_kw, 
            strm.deleted, 
            strm.disb, 
            strm.disb_ref_compnr, 
            strm.lmdt, 
            strm.pbud, 
            strm.pbud_ref_compnr, 
            strm.pbyr, 
            strm.pbyr_pbud_ref_compnr, 
            strm.ratc_1, 
            strm.ratc_2, 
            strm.ratc_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.sequencenumber, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.year,
            strm.budg ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS005 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.budg=src.budg) OR (target.budg IS NULL AND src.budg IS NULL)) 
                when matched then update set
                    target.budg=src.budg, 
                    target.budg_ref_compnr=src.budg_ref_compnr, 
                    target.cbud=src.cbud, 
                    target.cbud_ref_compnr=src.cbud_ref_compnr, 
                    target.cbyr=src.cbyr, 
                    target.cbyr_cbud_ref_compnr=src.cbyr_cbud_ref_compnr, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.dbud=src.dbud, 
                    target.dbud_ref_compnr=src.dbud_ref_compnr, 
                    target.dbyr=src.dbyr, 
                    target.dbyr_dbud_ref_compnr=src.dbyr_dbud_ref_compnr, 
                    target.defi=src.defi, 
                    target.defi_kw=src.defi_kw, 
                    target.deleted=src.deleted, 
                    target.disb=src.disb, 
                    target.disb_ref_compnr=src.disb_ref_compnr, 
                    target.lmdt=src.lmdt, 
                    target.pbud=src.pbud, 
                    target.pbud_ref_compnr=src.pbud_ref_compnr, 
                    target.pbyr=src.pbyr, 
                    target.pbyr_pbud_ref_compnr=src.pbyr_pbud_ref_compnr, 
                    target.ratc_1=src.ratc_1, 
                    target.ratc_2=src.ratc_2, 
                    target.ratc_3=src.ratc_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( budg, budg_ref_compnr, cbud, cbud_ref_compnr, cbyr, cbyr_cbud_ref_compnr, ccur, ccur_ref_compnr, compnr, dbud, dbud_ref_compnr, dbyr, dbyr_dbud_ref_compnr, defi, defi_kw, deleted, disb, disb_ref_compnr, lmdt, pbud, pbud_ref_compnr, pbyr, pbyr_pbud_ref_compnr, ratc_1, ratc_2, ratc_3, ratf_1, ratf_2, ratf_3, sequencenumber, text, text_ref_compnr, timestamp, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.budg, 
                    src.budg_ref_compnr, 
                    src.cbud, 
                    src.cbud_ref_compnr, 
                    src.cbyr, 
                    src.cbyr_cbud_ref_compnr, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.dbud, 
                    src.dbud_ref_compnr, 
                    src.dbyr, 
                    src.dbyr_dbud_ref_compnr, 
                    src.defi, 
                    src.defi_kw, 
                    src.deleted, 
                    src.disb, 
                    src.disb_ref_compnr, 
                    src.lmdt, 
                    src.pbud, 
                    src.pbud_ref_compnr, 
                    src.pbyr, 
                    src.pbyr_pbud_ref_compnr, 
                    src.ratc_1, 
                    src.ratc_2, 
                    src.ratc_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.sequencenumber, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.username, 
                    src.year,     
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
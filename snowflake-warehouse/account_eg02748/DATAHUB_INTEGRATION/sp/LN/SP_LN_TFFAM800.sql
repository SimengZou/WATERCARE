create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM800()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM800_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM800 as target using (SELECT * FROM (SELECT 
            strm.aext, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.anbr, 
            strm.anbr_aext_book_ref_compnr, 
            strm.anbr_aext_ref_compnr, 
            strm.atty, 
            strm.atty_kw, 
            strm.book, 
            strm.book_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.docn, 
            strm.line, 
            strm.post, 
            strm.post_kw, 
            strm.prod, 
            strm.rort, 
            strm.rort_kw, 
            strm.sequencenumber, 
            strm.tdat, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.tkey, 
            strm.ttyp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM800 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aext=src.aext, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.anbr=src.anbr, 
                    target.anbr_aext_book_ref_compnr=src.anbr_aext_book_ref_compnr, 
                    target.anbr_aext_ref_compnr=src.anbr_aext_ref_compnr, 
                    target.atty=src.atty, 
                    target.atty_kw=src.atty_kw, 
                    target.book=src.book, 
                    target.book_ref_compnr=src.book_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.docn=src.docn, 
                    target.line=src.line, 
                    target.post=src.post, 
                    target.post_kw=src.post_kw, 
                    target.prod=src.prod, 
                    target.rort=src.rort, 
                    target.rort_kw=src.rort_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tdat=src.tdat, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.ttyp=src.ttyp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aext, 
                    amnt_1, 
                    amnt_2, 
                    amnt_3, 
                    anbr, 
                    anbr_aext_book_ref_compnr, 
                    anbr_aext_ref_compnr, 
                    atty, 
                    atty_kw, 
                    book, 
                    book_ref_compnr, 
                    compnr, 
                    deleted, 
                    docn, 
                    line, 
                    post, 
                    post_kw, 
                    prod, 
                    rort, 
                    rort_kw, 
                    sequencenumber, 
                    tdat, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    tkey, 
                    ttyp, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aext, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.anbr, 
                    src.anbr_aext_book_ref_compnr, 
                    src.anbr_aext_ref_compnr, 
                    src.atty, 
                    src.atty_kw, 
                    src.book, 
                    src.book_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.docn, 
                    src.line, 
                    src.post, 
                    src.post_kw, 
                    src.prod, 
                    src.rort, 
                    src.rort_kw, 
                    src.sequencenumber, 
                    src.tdat, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.tkey, 
                    src.ttyp, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM800_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aext, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.anbr, 
            strm.anbr_aext_book_ref_compnr, 
            strm.anbr_aext_ref_compnr, 
            strm.atty, 
            strm.atty_kw, 
            strm.book, 
            strm.book_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.docn, 
            strm.line, 
            strm.post, 
            strm.post_kw, 
            strm.prod, 
            strm.rort, 
            strm.rort_kw, 
            strm.sequencenumber, 
            strm.tdat, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.tkey, 
            strm.ttyp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM800 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched then update set
                    target.aext=src.aext, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.anbr=src.anbr, 
                    target.anbr_aext_book_ref_compnr=src.anbr_aext_book_ref_compnr, 
                    target.anbr_aext_ref_compnr=src.anbr_aext_ref_compnr, 
                    target.atty=src.atty, 
                    target.atty_kw=src.atty_kw, 
                    target.book=src.book, 
                    target.book_ref_compnr=src.book_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.docn=src.docn, 
                    target.line=src.line, 
                    target.post=src.post, 
                    target.post_kw=src.post_kw, 
                    target.prod=src.prod, 
                    target.rort=src.rort, 
                    target.rort_kw=src.rort_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tdat=src.tdat, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.ttyp=src.ttyp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aext, amnt_1, amnt_2, amnt_3, anbr, anbr_aext_book_ref_compnr, anbr_aext_ref_compnr, atty, atty_kw, book, book_ref_compnr, compnr, deleted, docn, line, post, post_kw, prod, rort, rort_kw, sequencenumber, tdat, text, text_ref_compnr, timestamp, tkey, ttyp, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aext, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.anbr, 
                    src.anbr_aext_book_ref_compnr, 
                    src.anbr_aext_ref_compnr, 
                    src.atty, 
                    src.atty_kw, 
                    src.book, 
                    src.book_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.docn, 
                    src.line, 
                    src.post, 
                    src.post_kw, 
                    src.prod, 
                    src.rort, 
                    src.rort_kw, 
                    src.sequencenumber, 
                    src.tdat, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.tkey, 
                    src.ttyp, 
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
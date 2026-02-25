create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM808()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM808_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM808 as target using (SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.book, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.deleted, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ocst_1, 
            strm.ocst_2, 
            strm.ocst_3, 
            strm.prod, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.s179_1, 
            strm.s179_2, 
            strm.s179_3, 
            strm.salv_1, 
            strm.salv_2, 
            strm.salv_3, 
            strm.sequencenumber, 
            strm.susp, 
            strm.susp_kw, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ytdd_1, 
            strm.ytdd_2, 
            strm.ytdd_3, 
            strm.ytdr_1, 
            strm.ytdr_2, 
            strm.ytdr_3, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.book,
            strm.year,
            strm.prod ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM808 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prod=src.prod) OR (target.prod IS NULL AND src.prod IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.book=src.book, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.deleted=src.deleted, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ocst_1=src.ocst_1, 
                    target.ocst_2=src.ocst_2, 
                    target.ocst_3=src.ocst_3, 
                    target.prod=src.prod, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.s179_1=src.s179_1, 
                    target.s179_2=src.s179_2, 
                    target.s179_3=src.s179_3, 
                    target.salv_1=src.salv_1, 
                    target.salv_2=src.salv_2, 
                    target.salv_3=src.salv_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.susp=src.susp, 
                    target.susp_kw=src.susp_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ytdd_1=src.ytdd_1, 
                    target.ytdd_2=src.ytdd_2, 
                    target.ytdd_3=src.ytdd_3, 
                    target.ytdr_1=src.ytdr_1, 
                    target.ytdr_2=src.ytdr_2, 
                    target.ytdr_3=src.ytdr_3, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    aext, 
                    anbr, 
                    book, 
                    compnr, 
                    cost_1, 
                    cost_2, 
                    cost_3, 
                    deleted, 
                    ltdd_1, 
                    ltdd_2, 
                    ltdd_3, 
                    ltdr_1, 
                    ltdr_2, 
                    ltdr_3, 
                    ocst_1, 
                    ocst_2, 
                    ocst_3, 
                    prod, 
                    rcst_1, 
                    rcst_2, 
                    rcst_3, 
                    s179_1, 
                    s179_2, 
                    s179_3, 
                    salv_1, 
                    salv_2, 
                    salv_3, 
                    sequencenumber, 
                    susp, 
                    susp_kw, 
                    timestamp, 
                    username, 
                    year, 
                    ytdd_1, 
                    ytdd_2, 
                    ytdd_3, 
                    ytdr_1, 
                    ytdr_2, 
                    ytdr_3, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.book, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.deleted, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ocst_1, 
                    src.ocst_2, 
                    src.ocst_3, 
                    src.prod, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.s179_1, 
                    src.s179_2, 
                    src.s179_3, 
                    src.salv_1, 
                    src.salv_2, 
                    src.salv_3, 
                    src.sequencenumber, 
                    src.susp, 
                    src.susp_kw, 
                    src.timestamp, 
                    src.username, 
                    src.year, 
                    src.ytdd_1, 
                    src.ytdd_2, 
                    src.ytdd_3, 
                    src.ytdr_1, 
                    src.ytdr_2, 
                    src.ytdr_3,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM808_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.aext, 
            strm.anbr, 
            strm.book, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.deleted, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ocst_1, 
            strm.ocst_2, 
            strm.ocst_3, 
            strm.prod, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.s179_1, 
            strm.s179_2, 
            strm.s179_3, 
            strm.salv_1, 
            strm.salv_2, 
            strm.salv_3, 
            strm.sequencenumber, 
            strm.susp, 
            strm.susp_kw, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ytdd_1, 
            strm.ytdd_2, 
            strm.ytdd_3, 
            strm.ytdr_1, 
            strm.ytdr_2, 
            strm.ytdr_3, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.anbr,
            strm.aext,
            strm.book,
            strm.year,
            strm.prod ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM808 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.anbr=src.anbr) OR (target.anbr IS NULL AND src.anbr IS NULL)) AND
            ((target.aext=src.aext) OR (target.aext IS NULL AND src.aext IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prod=src.prod) OR (target.prod IS NULL AND src.prod IS NULL)) 
                when matched then update set
                    target.aext=src.aext, 
                    target.anbr=src.anbr, 
                    target.book=src.book, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.deleted=src.deleted, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ocst_1=src.ocst_1, 
                    target.ocst_2=src.ocst_2, 
                    target.ocst_3=src.ocst_3, 
                    target.prod=src.prod, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.s179_1=src.s179_1, 
                    target.s179_2=src.s179_2, 
                    target.s179_3=src.s179_3, 
                    target.salv_1=src.salv_1, 
                    target.salv_2=src.salv_2, 
                    target.salv_3=src.salv_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.susp=src.susp, 
                    target.susp_kw=src.susp_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ytdd_1=src.ytdd_1, 
                    target.ytdd_2=src.ytdd_2, 
                    target.ytdd_3=src.ytdd_3, 
                    target.ytdr_1=src.ytdr_1, 
                    target.ytdr_2=src.ytdr_2, 
                    target.ytdr_3=src.ytdr_3, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( aext, anbr, book, compnr, cost_1, cost_2, cost_3, deleted, ltdd_1, ltdd_2, ltdd_3, ltdr_1, ltdr_2, ltdr_3, ocst_1, ocst_2, ocst_3, prod, rcst_1, rcst_2, rcst_3, s179_1, s179_2, s179_3, salv_1, salv_2, salv_3, sequencenumber, susp, susp_kw, timestamp, username, year, ytdd_1, ytdd_2, ytdd_3, ytdr_1, ytdr_2, ytdr_3, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.aext, 
                    src.anbr, 
                    src.book, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.deleted, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ocst_1, 
                    src.ocst_2, 
                    src.ocst_3, 
                    src.prod, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.s179_1, 
                    src.s179_2, 
                    src.s179_3, 
                    src.salv_1, 
                    src.salv_2, 
                    src.salv_3, 
                    src.sequencenumber, 
                    src.susp, 
                    src.susp_kw, 
                    src.timestamp, 
                    src.username, 
                    src.year, 
                    src.ytdd_1, 
                    src.ytdd_2, 
                    src.ytdd_3, 
                    src.ytdr_1, 
                    src.ytdr_2, 
                    src.ytdr_3,     
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
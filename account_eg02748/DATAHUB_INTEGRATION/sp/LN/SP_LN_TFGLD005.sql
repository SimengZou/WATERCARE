create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD005()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD005_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD005 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.corr, 
            strm.corr_kw, 
            strm.deleted, 
            strm.desc, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.stdt, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.year_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ptyp,
            strm.year,
            strm.prno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD005 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.corr=src.corr, 
                    target.corr_kw=src.corr_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stdt=src.stdt, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.year_ref_compnr=src.year_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    corr, 
                    corr_kw, 
                    deleted, 
                    desc, 
                    prno, 
                    ptyp, 
                    ptyp_kw, 
                    sequencenumber, 
                    stdt, 
                    timestamp, 
                    username, 
                    year, 
                    year_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.corr, 
                    src.corr_kw, 
                    src.deleted, 
                    src.desc, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
                    src.stdt, 
                    src.timestamp, 
                    src.username, 
                    src.year, 
                    src.year_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD005_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.corr, 
            strm.corr_kw, 
            strm.deleted, 
            strm.desc, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.stdt, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.year_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ptyp,
            strm.year,
            strm.prno ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD005 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.corr=src.corr, 
                    target.corr_kw=src.corr_kw, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stdt=src.stdt, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.year_ref_compnr=src.year_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, corr, corr_kw, deleted, desc, prno, ptyp, ptyp_kw, sequencenumber, stdt, timestamp, username, year, year_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.corr, 
                    src.corr_kw, 
                    src.deleted, 
                    src.desc, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
                    src.stdt, 
                    src.timestamp, 
                    src.username, 
                    src.year, 
                    src.year_ref_compnr,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINH439()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINH439_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINH439 as target using (SELECT * FROM (SELECT 
            strm.cdat, 
            strm.compnr, 
            strm.deleted, 
            strm.fshp, 
            strm.fshp_kw, 
            strm.ldat, 
            strm.mess, 
            strm.oorg, 
            strm.oorg_kw, 
            strm.orno, 
            strm.pono, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.shln, 
            strm.shpm, 
            strm.shpm_shln_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.oorg,
            strm.orno,
            strm.pono,
            strm.seqn,
            strm.sqnb,
            strm.shpm,
            strm.shln ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH439 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.oorg=src.oorg) OR (target.oorg IS NULL AND src.oorg IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.shln=src.shln) OR (target.shln IS NULL AND src.shln IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cdat=src.cdat, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.fshp=src.fshp, 
                    target.fshp_kw=src.fshp_kw, 
                    target.ldat=src.ldat, 
                    target.mess=src.mess, 
                    target.oorg=src.oorg, 
                    target.oorg_kw=src.oorg_kw, 
                    target.orno=src.orno, 
                    target.pono=src.pono, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shln=src.shln, 
                    target.shpm=src.shpm, 
                    target.shpm_shln_ref_compnr=src.shpm_shln_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cdat, 
                    compnr, 
                    deleted, 
                    fshp, 
                    fshp_kw, 
                    ldat, 
                    mess, 
                    oorg, 
                    oorg_kw, 
                    orno, 
                    pono, 
                    seqn, 
                    sequencenumber, 
                    shln, 
                    shpm, 
                    shpm_shln_ref_compnr, 
                    sqnb, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cdat, 
                    src.compnr, 
                    src.deleted, 
                    src.fshp, 
                    src.fshp_kw, 
                    src.ldat, 
                    src.mess, 
                    src.oorg, 
                    src.oorg_kw, 
                    src.orno, 
                    src.pono, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.shln, 
                    src.shpm, 
                    src.shpm_shln_ref_compnr, 
                    src.sqnb, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINH439_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cdat, 
            strm.compnr, 
            strm.deleted, 
            strm.fshp, 
            strm.fshp_kw, 
            strm.ldat, 
            strm.mess, 
            strm.oorg, 
            strm.oorg_kw, 
            strm.orno, 
            strm.pono, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.shln, 
            strm.shpm, 
            strm.shpm_shln_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.oorg,
            strm.orno,
            strm.pono,
            strm.seqn,
            strm.sqnb,
            strm.shpm,
            strm.shln ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINH439 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.oorg=src.oorg) OR (target.oorg IS NULL AND src.oorg IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) AND
            ((target.shpm=src.shpm) OR (target.shpm IS NULL AND src.shpm IS NULL)) AND
            ((target.shln=src.shln) OR (target.shln IS NULL AND src.shln IS NULL)) 
                when matched then update set
                    target.cdat=src.cdat, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.fshp=src.fshp, 
                    target.fshp_kw=src.fshp_kw, 
                    target.ldat=src.ldat, 
                    target.mess=src.mess, 
                    target.oorg=src.oorg, 
                    target.oorg_kw=src.oorg_kw, 
                    target.orno=src.orno, 
                    target.pono=src.pono, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shln=src.shln, 
                    target.shpm=src.shpm, 
                    target.shpm_shln_ref_compnr=src.shpm_shln_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cdat, compnr, deleted, fshp, fshp_kw, ldat, mess, oorg, oorg_kw, orno, pono, seqn, sequencenumber, shln, shpm, shpm_shln_ref_compnr, sqnb, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cdat, 
                    src.compnr, 
                    src.deleted, 
                    src.fshp, 
                    src.fshp_kw, 
                    src.ldat, 
                    src.mess, 
                    src.oorg, 
                    src.oorg_kw, 
                    src.orno, 
                    src.pono, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.shln, 
                    src.shpm, 
                    src.shpm_shln_ref_compnr, 
                    src.sqnb, 
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
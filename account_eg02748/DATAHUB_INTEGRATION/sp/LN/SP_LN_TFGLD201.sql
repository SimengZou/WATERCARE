create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD201()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD201_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD201 as target using (SELECT * FROM (SELECT 
            strm.bpid, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.deleted, 
            strm.duac, 
            strm.duac_kw, 
            strm.fcah_1, 
            strm.fcah_2, 
            strm.fcah_3, 
            strm.fcam, 
            strm.fdah_1, 
            strm.fdah_2, 
            strm.fdah_3, 
            strm.fdam, 
            strm.fqt1, 
            strm.fqt2, 
            strm.leac, 
            strm.ncah_1, 
            strm.ncah_2, 
            strm.ncah_3, 
            strm.ncam, 
            strm.ndah_1, 
            strm.ndah_2, 
            strm.ndah_3, 
            strm.ndam, 
            strm.nqt1, 
            strm.nqt2, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.ptyp,
            strm.year,
            strm.prno,
            strm.leac,
            strm.ccur,
            strm.duac,
            strm.bpid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD201 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) AND
            ((target.leac=src.leac) OR (target.leac IS NULL AND src.leac IS NULL)) AND
            ((target.ccur=src.ccur) OR (target.ccur IS NULL AND src.ccur IS NULL)) AND
            ((target.duac=src.duac) OR (target.duac IS NULL AND src.duac IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bpid=src.bpid, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.deleted=src.deleted, 
                    target.duac=src.duac, 
                    target.duac_kw=src.duac_kw, 
                    target.fcah_1=src.fcah_1, 
                    target.fcah_2=src.fcah_2, 
                    target.fcah_3=src.fcah_3, 
                    target.fcam=src.fcam, 
                    target.fdah_1=src.fdah_1, 
                    target.fdah_2=src.fdah_2, 
                    target.fdah_3=src.fdah_3, 
                    target.fdam=src.fdam, 
                    target.fqt1=src.fqt1, 
                    target.fqt2=src.fqt2, 
                    target.leac=src.leac, 
                    target.ncah_1=src.ncah_1, 
                    target.ncah_2=src.ncah_2, 
                    target.ncah_3=src.ncah_3, 
                    target.ncam=src.ncam, 
                    target.ndah_1=src.ndah_1, 
                    target.ndah_2=src.ndah_2, 
                    target.ndah_3=src.ndah_3, 
                    target.ndam=src.ndam, 
                    target.nqt1=src.nqt1, 
                    target.nqt2=src.nqt2, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bpid, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cono, 
                    deleted, 
                    duac, 
                    duac_kw, 
                    fcah_1, 
                    fcah_2, 
                    fcah_3, 
                    fcam, 
                    fdah_1, 
                    fdah_2, 
                    fdah_3, 
                    fdam, 
                    fqt1, 
                    fqt2, 
                    leac, 
                    ncah_1, 
                    ncah_2, 
                    ncah_3, 
                    ncam, 
                    ndah_1, 
                    ndah_2, 
                    ndah_3, 
                    ndam, 
                    nqt1, 
                    nqt2, 
                    prno, 
                    ptyp, 
                    ptyp_kw, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bpid, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.deleted, 
                    src.duac, 
                    src.duac_kw, 
                    src.fcah_1, 
                    src.fcah_2, 
                    src.fcah_3, 
                    src.fcam, 
                    src.fdah_1, 
                    src.fdah_2, 
                    src.fdah_3, 
                    src.fdam, 
                    src.fqt1, 
                    src.fqt2, 
                    src.leac, 
                    src.ncah_1, 
                    src.ncah_2, 
                    src.ncah_3, 
                    src.ncam, 
                    src.ndah_1, 
                    src.ndah_2, 
                    src.ndah_3, 
                    src.ndam, 
                    src.nqt1, 
                    src.nqt2, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD201_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bpid, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.deleted, 
            strm.duac, 
            strm.duac_kw, 
            strm.fcah_1, 
            strm.fcah_2, 
            strm.fcah_3, 
            strm.fcam, 
            strm.fdah_1, 
            strm.fdah_2, 
            strm.fdah_3, 
            strm.fdam, 
            strm.fqt1, 
            strm.fqt2, 
            strm.leac, 
            strm.ncah_1, 
            strm.ncah_2, 
            strm.ncah_3, 
            strm.ncam, 
            strm.ndah_1, 
            strm.ndah_2, 
            strm.ndah_3, 
            strm.ndam, 
            strm.nqt1, 
            strm.nqt2, 
            strm.prno, 
            strm.ptyp, 
            strm.ptyp_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.ptyp,
            strm.year,
            strm.prno,
            strm.leac,
            strm.ccur,
            strm.duac,
            strm.bpid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD201 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.ptyp=src.ptyp) OR (target.ptyp IS NULL AND src.ptyp IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prno=src.prno) OR (target.prno IS NULL AND src.prno IS NULL)) AND
            ((target.leac=src.leac) OR (target.leac IS NULL AND src.leac IS NULL)) AND
            ((target.ccur=src.ccur) OR (target.ccur IS NULL AND src.ccur IS NULL)) AND
            ((target.duac=src.duac) OR (target.duac IS NULL AND src.duac IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) 
                when matched then update set
                    target.bpid=src.bpid, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.deleted=src.deleted, 
                    target.duac=src.duac, 
                    target.duac_kw=src.duac_kw, 
                    target.fcah_1=src.fcah_1, 
                    target.fcah_2=src.fcah_2, 
                    target.fcah_3=src.fcah_3, 
                    target.fcam=src.fcam, 
                    target.fdah_1=src.fdah_1, 
                    target.fdah_2=src.fdah_2, 
                    target.fdah_3=src.fdah_3, 
                    target.fdam=src.fdam, 
                    target.fqt1=src.fqt1, 
                    target.fqt2=src.fqt2, 
                    target.leac=src.leac, 
                    target.ncah_1=src.ncah_1, 
                    target.ncah_2=src.ncah_2, 
                    target.ncah_3=src.ncah_3, 
                    target.ncam=src.ncam, 
                    target.ndah_1=src.ndah_1, 
                    target.ndah_2=src.ndah_2, 
                    target.ndah_3=src.ndah_3, 
                    target.ndam=src.ndam, 
                    target.nqt1=src.nqt1, 
                    target.nqt2=src.nqt2, 
                    target.prno=src.prno, 
                    target.ptyp=src.ptyp, 
                    target.ptyp_kw=src.ptyp_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bpid, ccur, ccur_ref_compnr, compnr, cono, deleted, duac, duac_kw, fcah_1, fcah_2, fcah_3, fcam, fdah_1, fdah_2, fdah_3, fdam, fqt1, fqt2, leac, ncah_1, ncah_2, ncah_3, ncam, ndah_1, ndah_2, ndah_3, ndam, nqt1, nqt2, prno, ptyp, ptyp_kw, sequencenumber, timestamp, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bpid, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.deleted, 
                    src.duac, 
                    src.duac_kw, 
                    src.fcah_1, 
                    src.fcah_2, 
                    src.fcah_3, 
                    src.fcam, 
                    src.fdah_1, 
                    src.fdah_2, 
                    src.fdah_3, 
                    src.fdam, 
                    src.fqt1, 
                    src.fqt2, 
                    src.leac, 
                    src.ncah_1, 
                    src.ncah_2, 
                    src.ncah_3, 
                    src.ncam, 
                    src.ndah_1, 
                    src.ndah_2, 
                    src.ndah_3, 
                    src.ndam, 
                    src.nqt1, 
                    src.nqt2, 
                    src.prno, 
                    src.ptyp, 
                    src.ptyp_kw, 
                    src.sequencenumber, 
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
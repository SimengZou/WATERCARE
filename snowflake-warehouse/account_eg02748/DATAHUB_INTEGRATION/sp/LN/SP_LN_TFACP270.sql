create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFACP270()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFACP270_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFACP270 as target using (SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amth_1, 
            strm.amth_2, 
            strm.amth_3, 
            strm.buid, 
            strm.cact, 
            strm.ccur, 
            strm.clss, 
            strm.compnr, 
            strm.cprj, 
            strm.cspa, 
            strm.cuni, 
            strm.dbcr, 
            strm.dbcr_kw, 
            strm.dcom, 
            strm.deleted, 
            strm.docn, 
            strm.fcom, 
            strm.guid, 
            strm.idtc, 
            strm.ktrn, 
            strm.ktrn_kw, 
            strm.nuni, 
            strm.obre, 
            strm.ocmp, 
            strm.rbid, 
            strm.rbon, 
            strm.reco, 
            strm.reco_kw, 
            strm.recs, 
            strm.rpon, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.ttyp, 
            strm.txin, 
            strm.txin_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.guid,
            strm.dbcr ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP270 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.guid=src.guid) OR (target.guid IS NULL AND src.guid IS NULL)) AND
            ((target.dbcr=src.dbcr) OR (target.dbcr IS NULL AND src.dbcr IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amth_1=src.amth_1, 
                    target.amth_2=src.amth_2, 
                    target.amth_3=src.amth_3, 
                    target.buid=src.buid, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.clss=src.clss, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cspa=src.cspa, 
                    target.cuni=src.cuni, 
                    target.dbcr=src.dbcr, 
                    target.dbcr_kw=src.dbcr_kw, 
                    target.dcom=src.dcom, 
                    target.deleted=src.deleted, 
                    target.docn=src.docn, 
                    target.fcom=src.fcom, 
                    target.guid=src.guid, 
                    target.idtc=src.idtc, 
                    target.ktrn=src.ktrn, 
                    target.ktrn_kw=src.ktrn_kw, 
                    target.nuni=src.nuni, 
                    target.obre=src.obre, 
                    target.ocmp=src.ocmp, 
                    target.rbid=src.rbid, 
                    target.rbon=src.rbon, 
                    target.reco=src.reco, 
                    target.reco_kw=src.reco_kw, 
                    target.recs=src.recs, 
                    target.rpon=src.rpon, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.ttyp=src.ttyp, 
                    target.txin=src.txin, 
                    target.txin_kw=src.txin_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amnt, 
                    amth_1, 
                    amth_2, 
                    amth_3, 
                    buid, 
                    cact, 
                    ccur, 
                    clss, 
                    compnr, 
                    cprj, 
                    cspa, 
                    cuni, 
                    dbcr, 
                    dbcr_kw, 
                    dcom, 
                    deleted, 
                    docn, 
                    fcom, 
                    guid, 
                    idtc, 
                    ktrn, 
                    ktrn_kw, 
                    nuni, 
                    obre, 
                    ocmp, 
                    rbid, 
                    rbon, 
                    reco, 
                    reco_kw, 
                    recs, 
                    rpon, 
                    sequencenumber, 
                    timestamp, 
                    ttyp, 
                    txin, 
                    txin_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amnt, 
                    src.amth_1, 
                    src.amth_2, 
                    src.amth_3, 
                    src.buid, 
                    src.cact, 
                    src.ccur, 
                    src.clss, 
                    src.compnr, 
                    src.cprj, 
                    src.cspa, 
                    src.cuni, 
                    src.dbcr, 
                    src.dbcr_kw, 
                    src.dcom, 
                    src.deleted, 
                    src.docn, 
                    src.fcom, 
                    src.guid, 
                    src.idtc, 
                    src.ktrn, 
                    src.ktrn_kw, 
                    src.nuni, 
                    src.obre, 
                    src.ocmp, 
                    src.rbid, 
                    src.rbon, 
                    src.reco, 
                    src.reco_kw, 
                    src.recs, 
                    src.rpon, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.ttyp, 
                    src.txin, 
                    src.txin_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFACP270_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amnt, 
            strm.amth_1, 
            strm.amth_2, 
            strm.amth_3, 
            strm.buid, 
            strm.cact, 
            strm.ccur, 
            strm.clss, 
            strm.compnr, 
            strm.cprj, 
            strm.cspa, 
            strm.cuni, 
            strm.dbcr, 
            strm.dbcr_kw, 
            strm.dcom, 
            strm.deleted, 
            strm.docn, 
            strm.fcom, 
            strm.guid, 
            strm.idtc, 
            strm.ktrn, 
            strm.ktrn_kw, 
            strm.nuni, 
            strm.obre, 
            strm.ocmp, 
            strm.rbid, 
            strm.rbon, 
            strm.reco, 
            strm.reco_kw, 
            strm.recs, 
            strm.rpon, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.ttyp, 
            strm.txin, 
            strm.txin_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.guid,
            strm.dbcr ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP270 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.guid=src.guid) OR (target.guid IS NULL AND src.guid IS NULL)) AND
            ((target.dbcr=src.dbcr) OR (target.dbcr IS NULL AND src.dbcr IS NULL)) 
                when matched then update set
                    target.amnt=src.amnt, 
                    target.amth_1=src.amth_1, 
                    target.amth_2=src.amth_2, 
                    target.amth_3=src.amth_3, 
                    target.buid=src.buid, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.clss=src.clss, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cspa=src.cspa, 
                    target.cuni=src.cuni, 
                    target.dbcr=src.dbcr, 
                    target.dbcr_kw=src.dbcr_kw, 
                    target.dcom=src.dcom, 
                    target.deleted=src.deleted, 
                    target.docn=src.docn, 
                    target.fcom=src.fcom, 
                    target.guid=src.guid, 
                    target.idtc=src.idtc, 
                    target.ktrn=src.ktrn, 
                    target.ktrn_kw=src.ktrn_kw, 
                    target.nuni=src.nuni, 
                    target.obre=src.obre, 
                    target.ocmp=src.ocmp, 
                    target.rbid=src.rbid, 
                    target.rbon=src.rbon, 
                    target.reco=src.reco, 
                    target.reco_kw=src.reco_kw, 
                    target.recs=src.recs, 
                    target.rpon=src.rpon, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.ttyp=src.ttyp, 
                    target.txin=src.txin, 
                    target.txin_kw=src.txin_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amnt, amth_1, amth_2, amth_3, buid, cact, ccur, clss, compnr, cprj, cspa, cuni, dbcr, dbcr_kw, dcom, deleted, docn, fcom, guid, idtc, ktrn, ktrn_kw, nuni, obre, ocmp, rbid, rbon, reco, reco_kw, recs, rpon, sequencenumber, timestamp, ttyp, txin, txin_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amnt, 
                    src.amth_1, 
                    src.amth_2, 
                    src.amth_3, 
                    src.buid, 
                    src.cact, 
                    src.ccur, 
                    src.clss, 
                    src.compnr, 
                    src.cprj, 
                    src.cspa, 
                    src.cuni, 
                    src.dbcr, 
                    src.dbcr_kw, 
                    src.dcom, 
                    src.deleted, 
                    src.docn, 
                    src.fcom, 
                    src.guid, 
                    src.idtc, 
                    src.ktrn, 
                    src.ktrn_kw, 
                    src.nuni, 
                    src.obre, 
                    src.ocmp, 
                    src.rbid, 
                    src.rbon, 
                    src.reco, 
                    src.reco_kw, 
                    src.recs, 
                    src.rpon, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.ttyp, 
                    src.txin, 
                    src.txin_kw, 
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
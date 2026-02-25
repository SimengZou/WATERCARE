create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM460()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM460_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM460 as target using (SELECT * FROM (SELECT 
            strm.acco_1, 
            strm.acco_2, 
            strm.acco_3, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.cchn, 
            strm.cfln, 
            strm.clrv, 
            strm.cmdt, 
            strm.cmur, 
            strm.compnr, 
            strm.corv, 
            strm.corv_dwhc, 
            strm.corv_refc, 
            strm.crdt, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.deleted, 
            strm.erfa, 
            strm.ndrc, 
            strm.plpr, 
            strm.plyr, 
            strm.popr, 
            strm.poyr, 
            strm.prov, 
            strm.rahc_1, 
            strm.rahc_2, 
            strm.rahc_3, 
            strm.ratc_1, 
            strm.ratc_2, 
            strm.ratc_3, 
            strm.ratd, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcdt, 
            strm.rcur, 
            strm.rtor, 
            strm.rtor_kw, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn,
            strm.cfln,
            strm.plyr,
            strm.plpr,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM460 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.plyr=src.plyr) OR (target.plyr IS NULL AND src.plyr IS NULL)) AND
            ((target.plpr=src.plpr) OR (target.plpr IS NULL AND src.plpr IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acco_1=src.acco_1, 
                    target.acco_2=src.acco_2, 
                    target.acco_3=src.acco_3, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.clrv=src.clrv, 
                    target.cmdt=src.cmdt, 
                    target.cmur=src.cmur, 
                    target.compnr=src.compnr, 
                    target.corv=src.corv, 
                    target.corv_dwhc=src.corv_dwhc, 
                    target.corv_refc=src.corv_refc, 
                    target.crdt=src.crdt, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.erfa=src.erfa, 
                    target.ndrc=src.ndrc, 
                    target.plpr=src.plpr, 
                    target.plyr=src.plyr, 
                    target.popr=src.popr, 
                    target.poyr=src.poyr, 
                    target.prov=src.prov, 
                    target.rahc_1=src.rahc_1, 
                    target.rahc_2=src.rahc_2, 
                    target.rahc_3=src.rahc_3, 
                    target.ratc_1=src.ratc_1, 
                    target.ratc_2=src.ratc_2, 
                    target.ratc_3=src.ratc_3, 
                    target.ratd=src.ratd, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcdt=src.rcdt, 
                    target.rcur=src.rcur, 
                    target.rtor=src.rtor, 
                    target.rtor_kw=src.rtor_kw, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acco_1, 
                    acco_2, 
                    acco_3, 
                    camt_1, 
                    camt_2, 
                    camt_3, 
                    cchn, 
                    cfln, 
                    clrv, 
                    cmdt, 
                    cmur, 
                    compnr, 
                    corv, 
                    corv_dwhc, 
                    corv_refc, 
                    crdt, 
                    csco, 
                    csco_cchn_ref_compnr, 
                    csco_ref_compnr, 
                    deleted, 
                    erfa, 
                    ndrc, 
                    plpr, 
                    plyr, 
                    popr, 
                    poyr, 
                    prov, 
                    rahc_1, 
                    rahc_2, 
                    rahc_3, 
                    ratc_1, 
                    ratc_2, 
                    ratc_3, 
                    ratd, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rcdt, 
                    rcur, 
                    rtor, 
                    rtor_kw, 
                    seqn, 
                    sequencenumber, 
                    stat, 
                    stat_kw, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acco_1, 
                    src.acco_2, 
                    src.acco_3, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.cchn, 
                    src.cfln, 
                    src.clrv, 
                    src.cmdt, 
                    src.cmur, 
                    src.compnr, 
                    src.corv, 
                    src.corv_dwhc, 
                    src.corv_refc, 
                    src.crdt, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.deleted, 
                    src.erfa, 
                    src.ndrc, 
                    src.plpr, 
                    src.plyr, 
                    src.popr, 
                    src.poyr, 
                    src.prov, 
                    src.rahc_1, 
                    src.rahc_2, 
                    src.rahc_3, 
                    src.ratc_1, 
                    src.ratc_2, 
                    src.ratc_3, 
                    src.ratd, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcdt, 
                    src.rcur, 
                    src.rtor, 
                    src.rtor_kw, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM460_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acco_1, 
            strm.acco_2, 
            strm.acco_3, 
            strm.camt_1, 
            strm.camt_2, 
            strm.camt_3, 
            strm.cchn, 
            strm.cfln, 
            strm.clrv, 
            strm.cmdt, 
            strm.cmur, 
            strm.compnr, 
            strm.corv, 
            strm.corv_dwhc, 
            strm.corv_refc, 
            strm.crdt, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.deleted, 
            strm.erfa, 
            strm.ndrc, 
            strm.plpr, 
            strm.plyr, 
            strm.popr, 
            strm.poyr, 
            strm.prov, 
            strm.rahc_1, 
            strm.rahc_2, 
            strm.rahc_3, 
            strm.ratc_1, 
            strm.ratc_2, 
            strm.ratc_3, 
            strm.ratd, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcdt, 
            strm.rcur, 
            strm.rtor, 
            strm.rtor_kw, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.csco,
            strm.cchn,
            strm.cfln,
            strm.plyr,
            strm.plpr,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM460 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.plyr=src.plyr) OR (target.plyr IS NULL AND src.plyr IS NULL)) AND
            ((target.plpr=src.plpr) OR (target.plpr IS NULL AND src.plpr IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.acco_1=src.acco_1, 
                    target.acco_2=src.acco_2, 
                    target.acco_3=src.acco_3, 
                    target.camt_1=src.camt_1, 
                    target.camt_2=src.camt_2, 
                    target.camt_3=src.camt_3, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.clrv=src.clrv, 
                    target.cmdt=src.cmdt, 
                    target.cmur=src.cmur, 
                    target.compnr=src.compnr, 
                    target.corv=src.corv, 
                    target.corv_dwhc=src.corv_dwhc, 
                    target.corv_refc=src.corv_refc, 
                    target.crdt=src.crdt, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.erfa=src.erfa, 
                    target.ndrc=src.ndrc, 
                    target.plpr=src.plpr, 
                    target.plyr=src.plyr, 
                    target.popr=src.popr, 
                    target.poyr=src.poyr, 
                    target.prov=src.prov, 
                    target.rahc_1=src.rahc_1, 
                    target.rahc_2=src.rahc_2, 
                    target.rahc_3=src.rahc_3, 
                    target.ratc_1=src.ratc_1, 
                    target.ratc_2=src.ratc_2, 
                    target.ratc_3=src.ratc_3, 
                    target.ratd=src.ratd, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcdt=src.rcdt, 
                    target.rcur=src.rcur, 
                    target.rtor=src.rtor, 
                    target.rtor_kw=src.rtor_kw, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acco_1, acco_2, acco_3, camt_1, camt_2, camt_3, cchn, cfln, clrv, cmdt, cmur, compnr, corv, corv_dwhc, corv_refc, crdt, csco, csco_cchn_ref_compnr, csco_ref_compnr, deleted, erfa, ndrc, plpr, plyr, popr, poyr, prov, rahc_1, rahc_2, rahc_3, ratc_1, ratc_2, ratc_3, ratd, ratf_1, ratf_2, ratf_3, rcdt, rcur, rtor, rtor_kw, seqn, sequencenumber, stat, stat_kw, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acco_1, 
                    src.acco_2, 
                    src.acco_3, 
                    src.camt_1, 
                    src.camt_2, 
                    src.camt_3, 
                    src.cchn, 
                    src.cfln, 
                    src.clrv, 
                    src.cmdt, 
                    src.cmur, 
                    src.compnr, 
                    src.corv, 
                    src.corv_dwhc, 
                    src.corv_refc, 
                    src.crdt, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.deleted, 
                    src.erfa, 
                    src.ndrc, 
                    src.plpr, 
                    src.plyr, 
                    src.popr, 
                    src.poyr, 
                    src.prov, 
                    src.rahc_1, 
                    src.rahc_2, 
                    src.rahc_3, 
                    src.ratc_1, 
                    src.ratc_2, 
                    src.ratc_3, 
                    src.ratd, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcdt, 
                    src.rcur, 
                    src.rtor, 
                    src.rtor_kw, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
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
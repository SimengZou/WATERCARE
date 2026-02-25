create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCTM480()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCTM480_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCTM480 as target using (SELECT * FROM (SELECT 
            strm.acco_1, 
            strm.acco_2, 
            strm.acco_3, 
            strm.acco_cntc, 
            strm.acco_dwhc, 
            strm.acco_refc, 
            strm.cchn, 
            strm.cfln, 
            strm.codt, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.cvln, 
            strm.deleted, 
            strm.ircl, 
            strm.ircl_kw, 
            strm.ivsq, 
            strm.orno, 
            strm.ortp, 
            strm.ortp_kw, 
            strm.plpr, 
            strm.plyr, 
            strm.pono, 
            strm.popr, 
            strm.poyr, 
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
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM480 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acco_1=src.acco_1, 
                    target.acco_2=src.acco_2, 
                    target.acco_3=src.acco_3, 
                    target.acco_cntc=src.acco_cntc, 
                    target.acco_dwhc=src.acco_dwhc, 
                    target.acco_refc=src.acco_refc, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.codt=src.codt, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.cvln=src.cvln, 
                    target.deleted=src.deleted, 
                    target.ircl=src.ircl, 
                    target.ircl_kw=src.ircl_kw, 
                    target.ivsq=src.ivsq, 
                    target.orno=src.orno, 
                    target.ortp=src.ortp, 
                    target.ortp_kw=src.ortp_kw, 
                    target.plpr=src.plpr, 
                    target.plyr=src.plyr, 
                    target.pono=src.pono, 
                    target.popr=src.popr, 
                    target.poyr=src.poyr, 
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
                    acco_cntc, 
                    acco_dwhc, 
                    acco_refc, 
                    cchn, 
                    cfln, 
                    codt, 
                    compnr, 
                    cotp, 
                    cotp_kw, 
                    csco, 
                    csco_cchn_ref_compnr, 
                    csco_ref_compnr, 
                    cvln, 
                    deleted, 
                    ircl, 
                    ircl_kw, 
                    ivsq, 
                    orno, 
                    ortp, 
                    ortp_kw, 
                    plpr, 
                    plyr, 
                    pono, 
                    popr, 
                    poyr, 
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
                    src.acco_cntc, 
                    src.acco_dwhc, 
                    src.acco_refc, 
                    src.cchn, 
                    src.cfln, 
                    src.codt, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.cvln, 
                    src.deleted, 
                    src.ircl, 
                    src.ircl_kw, 
                    src.ivsq, 
                    src.orno, 
                    src.ortp, 
                    src.ortp_kw, 
                    src.plpr, 
                    src.plyr, 
                    src.pono, 
                    src.popr, 
                    src.poyr, 
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

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCTM480_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acco_1, 
            strm.acco_2, 
            strm.acco_3, 
            strm.acco_cntc, 
            strm.acco_dwhc, 
            strm.acco_refc, 
            strm.cchn, 
            strm.cfln, 
            strm.codt, 
            strm.compnr, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.csco, 
            strm.csco_cchn_ref_compnr, 
            strm.csco_ref_compnr, 
            strm.cvln, 
            strm.deleted, 
            strm.ircl, 
            strm.ircl_kw, 
            strm.ivsq, 
            strm.orno, 
            strm.ortp, 
            strm.ortp_kw, 
            strm.plpr, 
            strm.plyr, 
            strm.pono, 
            strm.popr, 
            strm.poyr, 
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
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCTM480 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.csco=src.csco) OR (target.csco IS NULL AND src.csco IS NULL)) AND
            ((target.cchn=src.cchn) OR (target.cchn IS NULL AND src.cchn IS NULL)) AND
            ((target.cfln=src.cfln) OR (target.cfln IS NULL AND src.cfln IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.acco_1=src.acco_1, 
                    target.acco_2=src.acco_2, 
                    target.acco_3=src.acco_3, 
                    target.acco_cntc=src.acco_cntc, 
                    target.acco_dwhc=src.acco_dwhc, 
                    target.acco_refc=src.acco_refc, 
                    target.cchn=src.cchn, 
                    target.cfln=src.cfln, 
                    target.codt=src.codt, 
                    target.compnr=src.compnr, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.csco=src.csco, 
                    target.csco_cchn_ref_compnr=src.csco_cchn_ref_compnr, 
                    target.csco_ref_compnr=src.csco_ref_compnr, 
                    target.cvln=src.cvln, 
                    target.deleted=src.deleted, 
                    target.ircl=src.ircl, 
                    target.ircl_kw=src.ircl_kw, 
                    target.ivsq=src.ivsq, 
                    target.orno=src.orno, 
                    target.ortp=src.ortp, 
                    target.ortp_kw=src.ortp_kw, 
                    target.plpr=src.plpr, 
                    target.plyr=src.plyr, 
                    target.pono=src.pono, 
                    target.popr=src.popr, 
                    target.poyr=src.poyr, 
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
        when not matched   then insert ( acco_1, acco_2, acco_3, acco_cntc, acco_dwhc, acco_refc, cchn, cfln, codt, compnr, cotp, cotp_kw, csco, csco_cchn_ref_compnr, csco_ref_compnr, cvln, deleted, ircl, ircl_kw, ivsq, orno, ortp, ortp_kw, plpr, plyr, pono, popr, poyr, seqn, sequencenumber, stat, stat_kw, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acco_1, 
                    src.acco_2, 
                    src.acco_3, 
                    src.acco_cntc, 
                    src.acco_dwhc, 
                    src.acco_refc, 
                    src.cchn, 
                    src.cfln, 
                    src.codt, 
                    src.compnr, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.csco, 
                    src.csco_cchn_ref_compnr, 
                    src.csco_ref_compnr, 
                    src.cvln, 
                    src.deleted, 
                    src.ircl, 
                    src.ircl_kw, 
                    src.ivsq, 
                    src.orno, 
                    src.ortp, 
                    src.ortp_kw, 
                    src.plpr, 
                    src.plyr, 
                    src.pono, 
                    src.popr, 
                    src.poyr, 
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
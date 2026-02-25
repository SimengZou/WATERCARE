create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPPC600()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPPC600_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPPC600 as target using (SELECT * FROM (SELECT 
            strm.amnt, 
            strm.appl, 
            strm.appl_kw, 
            strm.bamt, 
            strm.base, 
            strm.base_vrsn_ref_compnr, 
            strm.bclc, 
            strm.bcur, 
            strm.bprc, 
            strm.brte, 
            strm.btba, 
            strm.btyp, 
            strm.btyp_kw, 
            strm.cact, 
            strm.calc, 
            strm.cccp, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cldt, 
            strm.compnr, 
            strm.coob, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.cprj, 
            strm.cspa, 
            strm.ctdt, 
            strm.deleted, 
            strm.hour, 
            strm.levl, 
            strm.ohcs, 
            strm.ohst, 
            strm.ohst_kw, 
            strm.perc, 
            strm.rate, 
            strm.sequencenumber, 
            strm.sern, 
            strm.tbap, 
            strm.tcob, 
            strm.tcob_ref_compnr, 
            strm.timestamp, 
            strm.trdt, 
            strm.unit, 
            strm.user, 
            strm.username, 
            strm.vrsn, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.user,
            strm.cprj,
            strm.base,
            strm.cspa,
            strm.cact,
            strm.cotp,
            strm.coob,
            strm.sern,
            strm.ohcs ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC600 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.base=src.base) OR (target.base IS NULL AND src.base IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.cotp=src.cotp) OR (target.cotp IS NULL AND src.cotp IS NULL)) AND
            ((target.coob=src.coob) OR (target.coob IS NULL AND src.coob IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.ohcs=src.ohcs) OR (target.ohcs IS NULL AND src.ohcs IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amnt=src.amnt, 
                    target.appl=src.appl, 
                    target.appl_kw=src.appl_kw, 
                    target.bamt=src.bamt, 
                    target.base=src.base, 
                    target.base_vrsn_ref_compnr=src.base_vrsn_ref_compnr, 
                    target.bclc=src.bclc, 
                    target.bcur=src.bcur, 
                    target.bprc=src.bprc, 
                    target.brte=src.brte, 
                    target.btba=src.btba, 
                    target.btyp=src.btyp, 
                    target.btyp_kw=src.btyp_kw, 
                    target.cact=src.cact, 
                    target.calc=src.calc, 
                    target.cccp=src.cccp, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cldt=src.cldt, 
                    target.compnr=src.compnr, 
                    target.coob=src.coob, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.cprj=src.cprj, 
                    target.cspa=src.cspa, 
                    target.ctdt=src.ctdt, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.levl=src.levl, 
                    target.ohcs=src.ohcs, 
                    target.ohst=src.ohst, 
                    target.ohst_kw=src.ohst_kw, 
                    target.perc=src.perc, 
                    target.rate=src.rate, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.tbap=src.tbap, 
                    target.tcob=src.tcob, 
                    target.tcob_ref_compnr=src.tcob_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.unit=src.unit, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.vrsn=src.vrsn, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amnt, 
                    appl, 
                    appl_kw, 
                    bamt, 
                    base, 
                    base_vrsn_ref_compnr, 
                    bclc, 
                    bcur, 
                    bprc, 
                    brte, 
                    btba, 
                    btyp, 
                    btyp_kw, 
                    cact, 
                    calc, 
                    cccp, 
                    ccur, 
                    ccur_ref_compnr, 
                    cldt, 
                    compnr, 
                    coob, 
                    cotp, 
                    cotp_kw, 
                    cprj, 
                    cspa, 
                    ctdt, 
                    deleted, 
                    hour, 
                    levl, 
                    ohcs, 
                    ohst, 
                    ohst_kw, 
                    perc, 
                    rate, 
                    sequencenumber, 
                    sern, 
                    tbap, 
                    tcob, 
                    tcob_ref_compnr, 
                    timestamp, 
                    trdt, 
                    unit, 
                    user, 
                    username, 
                    vrsn, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amnt, 
                    src.appl, 
                    src.appl_kw, 
                    src.bamt, 
                    src.base, 
                    src.base_vrsn_ref_compnr, 
                    src.bclc, 
                    src.bcur, 
                    src.bprc, 
                    src.brte, 
                    src.btba, 
                    src.btyp, 
                    src.btyp_kw, 
                    src.cact, 
                    src.calc, 
                    src.cccp, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cldt, 
                    src.compnr, 
                    src.coob, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.cprj, 
                    src.cspa, 
                    src.ctdt, 
                    src.deleted, 
                    src.hour, 
                    src.levl, 
                    src.ohcs, 
                    src.ohst, 
                    src.ohst_kw, 
                    src.perc, 
                    src.rate, 
                    src.sequencenumber, 
                    src.sern, 
                    src.tbap, 
                    src.tcob, 
                    src.tcob_ref_compnr, 
                    src.timestamp, 
                    src.trdt, 
                    src.unit, 
                    src.user, 
                    src.username, 
                    src.vrsn, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPPC600_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amnt, 
            strm.appl, 
            strm.appl_kw, 
            strm.bamt, 
            strm.base, 
            strm.base_vrsn_ref_compnr, 
            strm.bclc, 
            strm.bcur, 
            strm.bprc, 
            strm.brte, 
            strm.btba, 
            strm.btyp, 
            strm.btyp_kw, 
            strm.cact, 
            strm.calc, 
            strm.cccp, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cldt, 
            strm.compnr, 
            strm.coob, 
            strm.cotp, 
            strm.cotp_kw, 
            strm.cprj, 
            strm.cspa, 
            strm.ctdt, 
            strm.deleted, 
            strm.hour, 
            strm.levl, 
            strm.ohcs, 
            strm.ohst, 
            strm.ohst_kw, 
            strm.perc, 
            strm.rate, 
            strm.sequencenumber, 
            strm.sern, 
            strm.tbap, 
            strm.tcob, 
            strm.tcob_ref_compnr, 
            strm.timestamp, 
            strm.trdt, 
            strm.unit, 
            strm.user, 
            strm.username, 
            strm.vrsn, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.user,
            strm.cprj,
            strm.base,
            strm.cspa,
            strm.cact,
            strm.cotp,
            strm.coob,
            strm.sern,
            strm.ohcs ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC600 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.base=src.base) OR (target.base IS NULL AND src.base IS NULL)) AND
            ((target.cspa=src.cspa) OR (target.cspa IS NULL AND src.cspa IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.cotp=src.cotp) OR (target.cotp IS NULL AND src.cotp IS NULL)) AND
            ((target.coob=src.coob) OR (target.coob IS NULL AND src.coob IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) AND
            ((target.ohcs=src.ohcs) OR (target.ohcs IS NULL AND src.ohcs IS NULL)) 
                when matched then update set
                    target.amnt=src.amnt, 
                    target.appl=src.appl, 
                    target.appl_kw=src.appl_kw, 
                    target.bamt=src.bamt, 
                    target.base=src.base, 
                    target.base_vrsn_ref_compnr=src.base_vrsn_ref_compnr, 
                    target.bclc=src.bclc, 
                    target.bcur=src.bcur, 
                    target.bprc=src.bprc, 
                    target.brte=src.brte, 
                    target.btba=src.btba, 
                    target.btyp=src.btyp, 
                    target.btyp_kw=src.btyp_kw, 
                    target.cact=src.cact, 
                    target.calc=src.calc, 
                    target.cccp=src.cccp, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cldt=src.cldt, 
                    target.compnr=src.compnr, 
                    target.coob=src.coob, 
                    target.cotp=src.cotp, 
                    target.cotp_kw=src.cotp_kw, 
                    target.cprj=src.cprj, 
                    target.cspa=src.cspa, 
                    target.ctdt=src.ctdt, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.levl=src.levl, 
                    target.ohcs=src.ohcs, 
                    target.ohst=src.ohst, 
                    target.ohst_kw=src.ohst_kw, 
                    target.perc=src.perc, 
                    target.rate=src.rate, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.tbap=src.tbap, 
                    target.tcob=src.tcob, 
                    target.tcob_ref_compnr=src.tcob_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.unit=src.unit, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.vrsn=src.vrsn, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amnt, appl, appl_kw, bamt, base, base_vrsn_ref_compnr, bclc, bcur, bprc, brte, btba, btyp, btyp_kw, cact, calc, cccp, ccur, ccur_ref_compnr, cldt, compnr, coob, cotp, cotp_kw, cprj, cspa, ctdt, deleted, hour, levl, ohcs, ohst, ohst_kw, perc, rate, sequencenumber, sern, tbap, tcob, tcob_ref_compnr, timestamp, trdt, unit, user, username, vrsn, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amnt, 
                    src.appl, 
                    src.appl_kw, 
                    src.bamt, 
                    src.base, 
                    src.base_vrsn_ref_compnr, 
                    src.bclc, 
                    src.bcur, 
                    src.bprc, 
                    src.brte, 
                    src.btba, 
                    src.btyp, 
                    src.btyp_kw, 
                    src.cact, 
                    src.calc, 
                    src.cccp, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cldt, 
                    src.compnr, 
                    src.coob, 
                    src.cotp, 
                    src.cotp_kw, 
                    src.cprj, 
                    src.cspa, 
                    src.ctdt, 
                    src.deleted, 
                    src.hour, 
                    src.levl, 
                    src.ohcs, 
                    src.ohst, 
                    src.ohst_kw, 
                    src.perc, 
                    src.rate, 
                    src.sequencenumber, 
                    src.sern, 
                    src.tbap, 
                    src.tcob, 
                    src.tcob_ref_compnr, 
                    src.timestamp, 
                    src.trdt, 
                    src.unit, 
                    src.user, 
                    src.username, 
                    src.vrsn, 
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
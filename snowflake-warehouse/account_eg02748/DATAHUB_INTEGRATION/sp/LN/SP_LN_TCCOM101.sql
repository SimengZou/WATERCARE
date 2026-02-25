create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCCOM101()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCCOM101_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCCOM101 as target using (SELECT * FROM (SELECT 
            strm.bprl, 
            strm.bprl_kw, 
            strm.btbv, 
            strm.btbv_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cfcg, 
            strm.cfir, 
            strm.cfir_kw, 
            strm.cfsg, 
            strm.cinm, 
            strm.cinm_ref_compnr, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.compnr, 
            strm.cpay, 
            strm.cpay_ref_compnr, 
            strm.crat, 
            strm.crat_ref_compnr, 
            strm.ctyp, 
            strm.ctyp_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.lmdt, 
            strm.pcur, 
            strm.pcur_ref_compnr, 
            strm.scur, 
            strm.scur_ref_compnr, 
            strm.sequencenumber, 
            strm.sern, 
            strm.sfir, 
            strm.sfir_kw, 
            strm.spay, 
            strm.spay_ref_compnr, 
            strm.styp, 
            strm.styp_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCCOM101 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bprl=src.bprl, 
                    target.bprl_kw=src.bprl_kw, 
                    target.btbv=src.btbv, 
                    target.btbv_kw=src.btbv_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cfcg=src.cfcg, 
                    target.cfir=src.cfir, 
                    target.cfir_kw=src.cfir_kw, 
                    target.cfsg=src.cfsg, 
                    target.cinm=src.cinm, 
                    target.cinm_ref_compnr=src.cinm_ref_compnr, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpay=src.cpay, 
                    target.cpay_ref_compnr=src.cpay_ref_compnr, 
                    target.crat=src.crat, 
                    target.crat_ref_compnr=src.crat_ref_compnr, 
                    target.ctyp=src.ctyp, 
                    target.ctyp_ref_compnr=src.ctyp_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.lmdt=src.lmdt, 
                    target.pcur=src.pcur, 
                    target.pcur_ref_compnr=src.pcur_ref_compnr, 
                    target.scur=src.scur, 
                    target.scur_ref_compnr=src.scur_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.sfir=src.sfir, 
                    target.sfir_kw=src.sfir_kw, 
                    target.spay=src.spay, 
                    target.spay_ref_compnr=src.spay_ref_compnr, 
                    target.styp=src.styp, 
                    target.styp_ref_compnr=src.styp_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bprl, 
                    bprl_kw, 
                    btbv, 
                    btbv_kw, 
                    ccur, 
                    ccur_ref_compnr, 
                    cfcg, 
                    cfir, 
                    cfir_kw, 
                    cfsg, 
                    cinm, 
                    cinm_ref_compnr, 
                    clan, 
                    clan_ref_compnr, 
                    compnr, 
                    cpay, 
                    cpay_ref_compnr, 
                    crat, 
                    crat_ref_compnr, 
                    ctyp, 
                    ctyp_ref_compnr, 
                    deleted, 
                    dsca, 
                    lmdt, 
                    pcur, 
                    pcur_ref_compnr, 
                    scur, 
                    scur_ref_compnr, 
                    sequencenumber, 
                    sern, 
                    sfir, 
                    sfir_kw, 
                    spay, 
                    spay_ref_compnr, 
                    styp, 
                    styp_ref_compnr, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bprl, 
                    src.bprl_kw, 
                    src.btbv, 
                    src.btbv_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cfcg, 
                    src.cfir, 
                    src.cfir_kw, 
                    src.cfsg, 
                    src.cinm, 
                    src.cinm_ref_compnr, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.compnr, 
                    src.cpay, 
                    src.cpay_ref_compnr, 
                    src.crat, 
                    src.crat_ref_compnr, 
                    src.ctyp, 
                    src.ctyp_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.lmdt, 
                    src.pcur, 
                    src.pcur_ref_compnr, 
                    src.scur, 
                    src.scur_ref_compnr, 
                    src.sequencenumber, 
                    src.sern, 
                    src.sfir, 
                    src.sfir_kw, 
                    src.spay, 
                    src.spay_ref_compnr, 
                    src.styp, 
                    src.styp_ref_compnr, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCCOM101_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bprl, 
            strm.bprl_kw, 
            strm.btbv, 
            strm.btbv_kw, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.cfcg, 
            strm.cfir, 
            strm.cfir_kw, 
            strm.cfsg, 
            strm.cinm, 
            strm.cinm_ref_compnr, 
            strm.clan, 
            strm.clan_ref_compnr, 
            strm.compnr, 
            strm.cpay, 
            strm.cpay_ref_compnr, 
            strm.crat, 
            strm.crat_ref_compnr, 
            strm.ctyp, 
            strm.ctyp_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.lmdt, 
            strm.pcur, 
            strm.pcur_ref_compnr, 
            strm.scur, 
            strm.scur_ref_compnr, 
            strm.sequencenumber, 
            strm.sern, 
            strm.sfir, 
            strm.sfir_kw, 
            strm.spay, 
            strm.spay_ref_compnr, 
            strm.styp, 
            strm.styp_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCCOM101 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.bprl=src.bprl, 
                    target.bprl_kw=src.bprl_kw, 
                    target.btbv=src.btbv, 
                    target.btbv_kw=src.btbv_kw, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.cfcg=src.cfcg, 
                    target.cfir=src.cfir, 
                    target.cfir_kw=src.cfir_kw, 
                    target.cfsg=src.cfsg, 
                    target.cinm=src.cinm, 
                    target.cinm_ref_compnr=src.cinm_ref_compnr, 
                    target.clan=src.clan, 
                    target.clan_ref_compnr=src.clan_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpay=src.cpay, 
                    target.cpay_ref_compnr=src.cpay_ref_compnr, 
                    target.crat=src.crat, 
                    target.crat_ref_compnr=src.crat_ref_compnr, 
                    target.ctyp=src.ctyp, 
                    target.ctyp_ref_compnr=src.ctyp_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.lmdt=src.lmdt, 
                    target.pcur=src.pcur, 
                    target.pcur_ref_compnr=src.pcur_ref_compnr, 
                    target.scur=src.scur, 
                    target.scur_ref_compnr=src.scur_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.sfir=src.sfir, 
                    target.sfir_kw=src.sfir_kw, 
                    target.spay=src.spay, 
                    target.spay_ref_compnr=src.spay_ref_compnr, 
                    target.styp=src.styp, 
                    target.styp_ref_compnr=src.styp_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bprl, bprl_kw, btbv, btbv_kw, ccur, ccur_ref_compnr, cfcg, cfir, cfir_kw, cfsg, cinm, cinm_ref_compnr, clan, clan_ref_compnr, compnr, cpay, cpay_ref_compnr, crat, crat_ref_compnr, ctyp, ctyp_ref_compnr, deleted, dsca, lmdt, pcur, pcur_ref_compnr, scur, scur_ref_compnr, sequencenumber, sern, sfir, sfir_kw, spay, spay_ref_compnr, styp, styp_ref_compnr, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bprl, 
                    src.bprl_kw, 
                    src.btbv, 
                    src.btbv_kw, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.cfcg, 
                    src.cfir, 
                    src.cfir_kw, 
                    src.cfsg, 
                    src.cinm, 
                    src.cinm_ref_compnr, 
                    src.clan, 
                    src.clan_ref_compnr, 
                    src.compnr, 
                    src.cpay, 
                    src.cpay_ref_compnr, 
                    src.crat, 
                    src.crat_ref_compnr, 
                    src.ctyp, 
                    src.ctyp_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.lmdt, 
                    src.pcur, 
                    src.pcur_ref_compnr, 
                    src.scur, 
                    src.scur_ref_compnr, 
                    src.sequencenumber, 
                    src.sern, 
                    src.sfir, 
                    src.sfir_kw, 
                    src.spay, 
                    src.spay_ref_compnr, 
                    src.styp, 
                    src.styp_ref_compnr, 
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
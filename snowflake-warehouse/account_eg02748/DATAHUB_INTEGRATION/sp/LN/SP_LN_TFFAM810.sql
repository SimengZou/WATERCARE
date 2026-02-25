create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM810()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM810_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM810 as target using (SELECT * FROM (SELECT 
            strm.acmc_1, 
            strm.acmc_2, 
            strm.acmc_3, 
            strm.acmt, 
            strm.agrp, 
            strm.agrp_ref_compnr, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.amtt, 
            strm.compnr, 
            strm.ctgy, 
            strm.ctgy_ref_compnr, 
            strm.ctgy_sctg_ref_compnr, 
            strm.curr, 
            strm.deleted, 
            strm.jobn, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ratd, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.reas, 
            strm.reas_ref_compnr, 
            strm.rtyp, 
            strm.sctg, 
            strm.sequencenumber, 
            strm.tagn, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.trid, 
            strm.user, 
            strm.username, 
            strm.xaex, 
            strm.xanb, 
            strm.xcom, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM810 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acmc_1=src.acmc_1, 
                    target.acmc_2=src.acmc_2, 
                    target.acmc_3=src.acmc_3, 
                    target.acmt=src.acmt, 
                    target.agrp=src.agrp, 
                    target.agrp_ref_compnr=src.agrp_ref_compnr, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.amtt=src.amtt, 
                    target.compnr=src.compnr, 
                    target.ctgy=src.ctgy, 
                    target.ctgy_ref_compnr=src.ctgy_ref_compnr, 
                    target.ctgy_sctg_ref_compnr=src.ctgy_sctg_ref_compnr, 
                    target.curr=src.curr, 
                    target.deleted=src.deleted, 
                    target.jobn=src.jobn, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ratd=src.ratd, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.reas=src.reas, 
                    target.reas_ref_compnr=src.reas_ref_compnr, 
                    target.rtyp=src.rtyp, 
                    target.sctg=src.sctg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.trid=src.trid, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.xaex=src.xaex, 
                    target.xanb=src.xanb, 
                    target.xcom=src.xcom, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acmc_1, 
                    acmc_2, 
                    acmc_3, 
                    acmt, 
                    agrp, 
                    agrp_ref_compnr, 
                    amnt_1, 
                    amnt_2, 
                    amnt_3, 
                    amtt, 
                    compnr, 
                    ctgy, 
                    ctgy_ref_compnr, 
                    ctgy_sctg_ref_compnr, 
                    curr, 
                    deleted, 
                    jobn, 
                    ltdd_1, 
                    ltdd_2, 
                    ltdd_3, 
                    ltdr_1, 
                    ltdr_2, 
                    ltdr_3, 
                    ratd, 
                    rate_1, 
                    rate_2, 
                    rate_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rcst_1, 
                    rcst_2, 
                    rcst_3, 
                    reas, 
                    reas_ref_compnr, 
                    rtyp, 
                    sctg, 
                    sequencenumber, 
                    tagn, 
                    timestamp, 
                    tkey, 
                    tkey_ref_compnr, 
                    trid, 
                    user, 
                    username, 
                    xaex, 
                    xanb, 
                    xcom, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acmc_1, 
                    src.acmc_2, 
                    src.acmc_3, 
                    src.acmt, 
                    src.agrp, 
                    src.agrp_ref_compnr, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.amtt, 
                    src.compnr, 
                    src.ctgy, 
                    src.ctgy_ref_compnr, 
                    src.ctgy_sctg_ref_compnr, 
                    src.curr, 
                    src.deleted, 
                    src.jobn, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ratd, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.reas, 
                    src.reas_ref_compnr, 
                    src.rtyp, 
                    src.sctg, 
                    src.sequencenumber, 
                    src.tagn, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.trid, 
                    src.user, 
                    src.username, 
                    src.xaex, 
                    src.xanb, 
                    src.xcom,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM810_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acmc_1, 
            strm.acmc_2, 
            strm.acmc_3, 
            strm.acmt, 
            strm.agrp, 
            strm.agrp_ref_compnr, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.amtt, 
            strm.compnr, 
            strm.ctgy, 
            strm.ctgy_ref_compnr, 
            strm.ctgy_sctg_ref_compnr, 
            strm.curr, 
            strm.deleted, 
            strm.jobn, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ratd, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.reas, 
            strm.reas_ref_compnr, 
            strm.rtyp, 
            strm.sctg, 
            strm.sequencenumber, 
            strm.tagn, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.trid, 
            strm.user, 
            strm.username, 
            strm.xaex, 
            strm.xanb, 
            strm.xcom, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM810 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched then update set
                    target.acmc_1=src.acmc_1, 
                    target.acmc_2=src.acmc_2, 
                    target.acmc_3=src.acmc_3, 
                    target.acmt=src.acmt, 
                    target.agrp=src.agrp, 
                    target.agrp_ref_compnr=src.agrp_ref_compnr, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.amtt=src.amtt, 
                    target.compnr=src.compnr, 
                    target.ctgy=src.ctgy, 
                    target.ctgy_ref_compnr=src.ctgy_ref_compnr, 
                    target.ctgy_sctg_ref_compnr=src.ctgy_sctg_ref_compnr, 
                    target.curr=src.curr, 
                    target.deleted=src.deleted, 
                    target.jobn=src.jobn, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ratd=src.ratd, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.reas=src.reas, 
                    target.reas_ref_compnr=src.reas_ref_compnr, 
                    target.rtyp=src.rtyp, 
                    target.sctg=src.sctg, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.trid=src.trid, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.xaex=src.xaex, 
                    target.xanb=src.xanb, 
                    target.xcom=src.xcom, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acmc_1, acmc_2, acmc_3, acmt, agrp, agrp_ref_compnr, amnt_1, amnt_2, amnt_3, amtt, compnr, ctgy, ctgy_ref_compnr, ctgy_sctg_ref_compnr, curr, deleted, jobn, ltdd_1, ltdd_2, ltdd_3, ltdr_1, ltdr_2, ltdr_3, ratd, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rcst_1, rcst_2, rcst_3, reas, reas_ref_compnr, rtyp, sctg, sequencenumber, tagn, timestamp, tkey, tkey_ref_compnr, trid, user, username, xaex, xanb, xcom, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acmc_1, 
                    src.acmc_2, 
                    src.acmc_3, 
                    src.acmt, 
                    src.agrp, 
                    src.agrp_ref_compnr, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.amtt, 
                    src.compnr, 
                    src.ctgy, 
                    src.ctgy_ref_compnr, 
                    src.ctgy_sctg_ref_compnr, 
                    src.curr, 
                    src.deleted, 
                    src.jobn, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ratd, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.reas, 
                    src.reas_ref_compnr, 
                    src.rtyp, 
                    src.sctg, 
                    src.sequencenumber, 
                    src.tagn, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.trid, 
                    src.user, 
                    src.username, 
                    src.xaex, 
                    src.xanb, 
                    src.xcom,     
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
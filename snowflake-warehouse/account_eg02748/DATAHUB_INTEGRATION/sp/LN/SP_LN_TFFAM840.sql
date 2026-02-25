create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM840()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM840_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM840 as target using (SELECT * FROM (SELECT 
            strm.adrt, 
            strm.adrt_kw, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.curr, 
            strm.deleted, 
            strm.dqty, 
            strm.jobn, 
            strm.life, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.proc_1, 
            strm.proc_2, 
            strm.proc_3, 
            strm.prod, 
            strm.prot, 
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
            strm.refn, 
            strm.rtyp, 
            strm.salv_1, 
            strm.salv_2, 
            strm.salv_3, 
            strm.sequencenumber, 
            strm.tagn, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.type, 
            strm.type_kw, 
            strm.user, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM840 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.adrt=src.adrt, 
                    target.adrt_kw=src.adrt_kw, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.curr=src.curr, 
                    target.deleted=src.deleted, 
                    target.dqty=src.dqty, 
                    target.jobn=src.jobn, 
                    target.life=src.life, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.proc_1=src.proc_1, 
                    target.proc_2=src.proc_2, 
                    target.proc_3=src.proc_3, 
                    target.prod=src.prod, 
                    target.prot=src.prot, 
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
                    target.refn=src.refn, 
                    target.rtyp=src.rtyp, 
                    target.salv_1=src.salv_1, 
                    target.salv_2=src.salv_2, 
                    target.salv_3=src.salv_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.type=src.type, 
                    target.type_kw=src.type_kw, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    adrt, 
                    adrt_kw, 
                    compnr, 
                    cost_1, 
                    cost_2, 
                    cost_3, 
                    curr, 
                    deleted, 
                    dqty, 
                    jobn, 
                    life, 
                    ltdd_1, 
                    ltdd_2, 
                    ltdd_3, 
                    ltdr_1, 
                    ltdr_2, 
                    ltdr_3, 
                    proc_1, 
                    proc_2, 
                    proc_3, 
                    prod, 
                    prot, 
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
                    refn, 
                    rtyp, 
                    salv_1, 
                    salv_2, 
                    salv_3, 
                    sequencenumber, 
                    tagn, 
                    timestamp, 
                    tkey, 
                    tkey_ref_compnr, 
                    type, 
                    type_kw, 
                    user, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.adrt, 
                    src.adrt_kw, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.curr, 
                    src.deleted, 
                    src.dqty, 
                    src.jobn, 
                    src.life, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.proc_1, 
                    src.proc_2, 
                    src.proc_3, 
                    src.prod, 
                    src.prot, 
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
                    src.refn, 
                    src.rtyp, 
                    src.salv_1, 
                    src.salv_2, 
                    src.salv_3, 
                    src.sequencenumber, 
                    src.tagn, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.type, 
                    src.type_kw, 
                    src.user, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM840_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.adrt, 
            strm.adrt_kw, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.curr, 
            strm.deleted, 
            strm.dqty, 
            strm.jobn, 
            strm.life, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.proc_1, 
            strm.proc_2, 
            strm.proc_3, 
            strm.prod, 
            strm.prot, 
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
            strm.refn, 
            strm.rtyp, 
            strm.salv_1, 
            strm.salv_2, 
            strm.salv_3, 
            strm.sequencenumber, 
            strm.tagn, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.type, 
            strm.type_kw, 
            strm.user, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM840 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched then update set
                    target.adrt=src.adrt, 
                    target.adrt_kw=src.adrt_kw, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.curr=src.curr, 
                    target.deleted=src.deleted, 
                    target.dqty=src.dqty, 
                    target.jobn=src.jobn, 
                    target.life=src.life, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.proc_1=src.proc_1, 
                    target.proc_2=src.proc_2, 
                    target.proc_3=src.proc_3, 
                    target.prod=src.prod, 
                    target.prot=src.prot, 
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
                    target.refn=src.refn, 
                    target.rtyp=src.rtyp, 
                    target.salv_1=src.salv_1, 
                    target.salv_2=src.salv_2, 
                    target.salv_3=src.salv_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.tagn=src.tagn, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.type=src.type, 
                    target.type_kw=src.type_kw, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( adrt, adrt_kw, compnr, cost_1, cost_2, cost_3, curr, deleted, dqty, jobn, life, ltdd_1, ltdd_2, ltdd_3, ltdr_1, ltdr_2, ltdr_3, proc_1, proc_2, proc_3, prod, prot, ratd, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rcst_1, rcst_2, rcst_3, reas, reas_ref_compnr, refn, rtyp, salv_1, salv_2, salv_3, sequencenumber, tagn, timestamp, tkey, tkey_ref_compnr, type, type_kw, user, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.adrt, 
                    src.adrt_kw, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.curr, 
                    src.deleted, 
                    src.dqty, 
                    src.jobn, 
                    src.life, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.proc_1, 
                    src.proc_2, 
                    src.proc_3, 
                    src.prod, 
                    src.prot, 
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
                    src.refn, 
                    src.rtyp, 
                    src.salv_1, 
                    src.salv_2, 
                    src.salv_3, 
                    src.sequencenumber, 
                    src.tagn, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.type, 
                    src.type_kw, 
                    src.user, 
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
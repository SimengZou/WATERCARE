create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM830()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM830_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM830 as target using (SELECT * FROM (SELECT 
            strm.abdf, 
            strm.abdf_kw, 
            strm.adpc, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.curd, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
            strm.dtty, 
            strm.dtty_kw, 
            strm.jobn, 
            strm.last, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ltdu, 
            strm.meth, 
            strm.meth_ref_compnr, 
            strm.prop, 
            strm.prop_ref_compnr, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.sequencenumber, 
            strm.susp, 
            strm.susp_kw, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.unit, 
            strm.user, 
            strm.username, 
            strm.ytdd_1, 
            strm.ytdd_2, 
            strm.ytdd_3, 
            strm.ytdr_1, 
            strm.ytdr_2, 
            strm.ytdr_3, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM830 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.abdf=src.abdf, 
                    target.abdf_kw=src.abdf_kw, 
                    target.adpc=src.adpc, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.curd=src.curd, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
                    target.dtty=src.dtty, 
                    target.dtty_kw=src.dtty_kw, 
                    target.jobn=src.jobn, 
                    target.last=src.last, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ltdu=src.ltdu, 
                    target.meth=src.meth, 
                    target.meth_ref_compnr=src.meth_ref_compnr, 
                    target.prop=src.prop, 
                    target.prop_ref_compnr=src.prop_ref_compnr, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.susp=src.susp, 
                    target.susp_kw=src.susp_kw, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.unit=src.unit, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.ytdd_1=src.ytdd_1, 
                    target.ytdd_2=src.ytdd_2, 
                    target.ytdd_3=src.ytdd_3, 
                    target.ytdr_1=src.ytdr_1, 
                    target.ytdr_2=src.ytdr_2, 
                    target.ytdr_3=src.ytdr_3, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    abdf, 
                    abdf_kw, 
                    adpc, 
                    compnr, 
                    cost_1, 
                    cost_2, 
                    cost_3, 
                    curd, 
                    deleted, 
                    depr_1, 
                    depr_2, 
                    depr_3, 
                    dtty, 
                    dtty_kw, 
                    jobn, 
                    last, 
                    ltdd_1, 
                    ltdd_2, 
                    ltdd_3, 
                    ltdr_1, 
                    ltdr_2, 
                    ltdr_3, 
                    ltdu, 
                    meth, 
                    meth_ref_compnr, 
                    prop, 
                    prop_ref_compnr, 
                    rcst_1, 
                    rcst_2, 
                    rcst_3, 
                    sequencenumber, 
                    susp, 
                    susp_kw, 
                    timestamp, 
                    tkey, 
                    tkey_ref_compnr, 
                    unit, 
                    user, 
                    username, 
                    ytdd_1, 
                    ytdd_2, 
                    ytdd_3, 
                    ytdr_1, 
                    ytdr_2, 
                    ytdr_3, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.abdf, 
                    src.abdf_kw, 
                    src.adpc, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.curd, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
                    src.dtty, 
                    src.dtty_kw, 
                    src.jobn, 
                    src.last, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ltdu, 
                    src.meth, 
                    src.meth_ref_compnr, 
                    src.prop, 
                    src.prop_ref_compnr, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.sequencenumber, 
                    src.susp, 
                    src.susp_kw, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.unit, 
                    src.user, 
                    src.username, 
                    src.ytdd_1, 
                    src.ytdd_2, 
                    src.ytdd_3, 
                    src.ytdr_1, 
                    src.ytdr_2, 
                    src.ytdr_3,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM830_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.abdf, 
            strm.abdf_kw, 
            strm.adpc, 
            strm.compnr, 
            strm.cost_1, 
            strm.cost_2, 
            strm.cost_3, 
            strm.curd, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
            strm.dtty, 
            strm.dtty_kw, 
            strm.jobn, 
            strm.last, 
            strm.ltdd_1, 
            strm.ltdd_2, 
            strm.ltdd_3, 
            strm.ltdr_1, 
            strm.ltdr_2, 
            strm.ltdr_3, 
            strm.ltdu, 
            strm.meth, 
            strm.meth_ref_compnr, 
            strm.prop, 
            strm.prop_ref_compnr, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.sequencenumber, 
            strm.susp, 
            strm.susp_kw, 
            strm.timestamp, 
            strm.tkey, 
            strm.tkey_ref_compnr, 
            strm.unit, 
            strm.user, 
            strm.username, 
            strm.ytdd_1, 
            strm.ytdd_2, 
            strm.ytdd_3, 
            strm.ytdr_1, 
            strm.ytdr_2, 
            strm.ytdr_3, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.tkey ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM830 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.tkey=src.tkey) OR (target.tkey IS NULL AND src.tkey IS NULL)) 
                when matched then update set
                    target.abdf=src.abdf, 
                    target.abdf_kw=src.abdf_kw, 
                    target.adpc=src.adpc, 
                    target.compnr=src.compnr, 
                    target.cost_1=src.cost_1, 
                    target.cost_2=src.cost_2, 
                    target.cost_3=src.cost_3, 
                    target.curd=src.curd, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
                    target.dtty=src.dtty, 
                    target.dtty_kw=src.dtty_kw, 
                    target.jobn=src.jobn, 
                    target.last=src.last, 
                    target.ltdd_1=src.ltdd_1, 
                    target.ltdd_2=src.ltdd_2, 
                    target.ltdd_3=src.ltdd_3, 
                    target.ltdr_1=src.ltdr_1, 
                    target.ltdr_2=src.ltdr_2, 
                    target.ltdr_3=src.ltdr_3, 
                    target.ltdu=src.ltdu, 
                    target.meth=src.meth, 
                    target.meth_ref_compnr=src.meth_ref_compnr, 
                    target.prop=src.prop, 
                    target.prop_ref_compnr=src.prop_ref_compnr, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.susp=src.susp, 
                    target.susp_kw=src.susp_kw, 
                    target.timestamp=src.timestamp, 
                    target.tkey=src.tkey, 
                    target.tkey_ref_compnr=src.tkey_ref_compnr, 
                    target.unit=src.unit, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.ytdd_1=src.ytdd_1, 
                    target.ytdd_2=src.ytdd_2, 
                    target.ytdd_3=src.ytdd_3, 
                    target.ytdr_1=src.ytdr_1, 
                    target.ytdr_2=src.ytdr_2, 
                    target.ytdr_3=src.ytdr_3, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( abdf, abdf_kw, adpc, compnr, cost_1, cost_2, cost_3, curd, deleted, depr_1, depr_2, depr_3, dtty, dtty_kw, jobn, last, ltdd_1, ltdd_2, ltdd_3, ltdr_1, ltdr_2, ltdr_3, ltdu, meth, meth_ref_compnr, prop, prop_ref_compnr, rcst_1, rcst_2, rcst_3, sequencenumber, susp, susp_kw, timestamp, tkey, tkey_ref_compnr, unit, user, username, ytdd_1, ytdd_2, ytdd_3, ytdr_1, ytdr_2, ytdr_3, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.abdf, 
                    src.abdf_kw, 
                    src.adpc, 
                    src.compnr, 
                    src.cost_1, 
                    src.cost_2, 
                    src.cost_3, 
                    src.curd, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
                    src.dtty, 
                    src.dtty_kw, 
                    src.jobn, 
                    src.last, 
                    src.ltdd_1, 
                    src.ltdd_2, 
                    src.ltdd_3, 
                    src.ltdr_1, 
                    src.ltdr_2, 
                    src.ltdr_3, 
                    src.ltdu, 
                    src.meth, 
                    src.meth_ref_compnr, 
                    src.prop, 
                    src.prop_ref_compnr, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.sequencenumber, 
                    src.susp, 
                    src.susp_kw, 
                    src.timestamp, 
                    src.tkey, 
                    src.tkey_ref_compnr, 
                    src.unit, 
                    src.user, 
                    src.username, 
                    src.ytdd_1, 
                    src.ytdd_2, 
                    src.ytdd_3, 
                    src.ytdr_1, 
                    src.ytdr_2, 
                    src.ytdr_3,     
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
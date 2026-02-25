create or replace procedure DATAHUB_INTEGRATION.SP_LN_TDSMI113()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TDSMI113_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TDSMI113 as target using (SELECT * FROM (SELECT 
            strm.amta, 
            strm.amta_dtwc, 
            strm.amta_lclc, 
            strm.amta_rfrc, 
            strm.amta_rpc1, 
            strm.amta_rpc2, 
            strm.amtg_dtwc, 
            strm.amtg_lclc, 
            strm.amtg_rfrc, 
            strm.amtg_rpc1, 
            strm.amtg_rpc2, 
            strm.amtg_trnc, 
            strm.cact, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cuqs, 
            strm.cuqs_ref_compnr, 
            strm.cvqs, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.opty, 
            strm.opty_ref_compnr, 
            strm.pono, 
            strm.prid, 
            strm.qoor, 
            strm.qoor_buar, 
            strm.qoor_buln, 
            strm.qoor_bupc, 
            strm.qoor_butm, 
            strm.qoor_buvl, 
            strm.qoor_buwg, 
            strm.qoor_invu, 
            strm.seli, 
            strm.seli_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.opty,
            strm.pono ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDSMI113 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.opty=src.opty) OR (target.opty IS NULL AND src.opty IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amta=src.amta, 
                    target.amta_dtwc=src.amta_dtwc, 
                    target.amta_lclc=src.amta_lclc, 
                    target.amta_rfrc=src.amta_rfrc, 
                    target.amta_rpc1=src.amta_rpc1, 
                    target.amta_rpc2=src.amta_rpc2, 
                    target.amtg_dtwc=src.amtg_dtwc, 
                    target.amtg_lclc=src.amtg_lclc, 
                    target.amtg_rfrc=src.amtg_rfrc, 
                    target.amtg_rpc1=src.amtg_rpc1, 
                    target.amtg_rpc2=src.amtg_rpc2, 
                    target.amtg_trnc=src.amtg_trnc, 
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cuqs=src.cuqs, 
                    target.cuqs_ref_compnr=src.cuqs_ref_compnr, 
                    target.cvqs=src.cvqs, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.opty=src.opty, 
                    target.opty_ref_compnr=src.opty_ref_compnr, 
                    target.pono=src.pono, 
                    target.prid=src.prid, 
                    target.qoor=src.qoor, 
                    target.qoor_buar=src.qoor_buar, 
                    target.qoor_buln=src.qoor_buln, 
                    target.qoor_bupc=src.qoor_bupc, 
                    target.qoor_butm=src.qoor_butm, 
                    target.qoor_buvl=src.qoor_buvl, 
                    target.qoor_buwg=src.qoor_buwg, 
                    target.qoor_invu=src.qoor_invu, 
                    target.seli=src.seli, 
                    target.seli_kw=src.seli_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amta, 
                    amta_dtwc, 
                    amta_lclc, 
                    amta_rfrc, 
                    amta_rpc1, 
                    amta_rpc2, 
                    amtg_dtwc, 
                    amtg_lclc, 
                    amtg_rfrc, 
                    amtg_rpc1, 
                    amtg_rpc2, 
                    amtg_trnc, 
                    cact, 
                    compnr, 
                    cprj, 
                    cprj_ref_compnr, 
                    cspa, 
                    cuqs, 
                    cuqs_ref_compnr, 
                    cvqs, 
                    deleted, 
                    item, 
                    item_ref_compnr, 
                    opty, 
                    opty_ref_compnr, 
                    pono, 
                    prid, 
                    qoor, 
                    qoor_buar, 
                    qoor_buln, 
                    qoor_bupc, 
                    qoor_butm, 
                    qoor_buvl, 
                    qoor_buwg, 
                    qoor_invu, 
                    seli, 
                    seli_kw, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amta, 
                    src.amta_dtwc, 
                    src.amta_lclc, 
                    src.amta_rfrc, 
                    src.amta_rpc1, 
                    src.amta_rpc2, 
                    src.amtg_dtwc, 
                    src.amtg_lclc, 
                    src.amtg_rfrc, 
                    src.amtg_rpc1, 
                    src.amtg_rpc2, 
                    src.amtg_trnc, 
                    src.cact, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cuqs, 
                    src.cuqs_ref_compnr, 
                    src.cvqs, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.opty, 
                    src.opty_ref_compnr, 
                    src.pono, 
                    src.prid, 
                    src.qoor, 
                    src.qoor_buar, 
                    src.qoor_buln, 
                    src.qoor_bupc, 
                    src.qoor_butm, 
                    src.qoor_buvl, 
                    src.qoor_buwg, 
                    src.qoor_invu, 
                    src.seli, 
                    src.seli_kw, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TDSMI113_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amta, 
            strm.amta_dtwc, 
            strm.amta_lclc, 
            strm.amta_rfrc, 
            strm.amta_rpc1, 
            strm.amta_rpc2, 
            strm.amtg_dtwc, 
            strm.amtg_lclc, 
            strm.amtg_rfrc, 
            strm.amtg_rpc1, 
            strm.amtg_rpc2, 
            strm.amtg_trnc, 
            strm.cact, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cuqs, 
            strm.cuqs_ref_compnr, 
            strm.cvqs, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.opty, 
            strm.opty_ref_compnr, 
            strm.pono, 
            strm.prid, 
            strm.qoor, 
            strm.qoor_buar, 
            strm.qoor_buln, 
            strm.qoor_bupc, 
            strm.qoor_butm, 
            strm.qoor_buvl, 
            strm.qoor_buwg, 
            strm.qoor_invu, 
            strm.seli, 
            strm.seli_kw, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.opty,
            strm.pono ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDSMI113 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.opty=src.opty) OR (target.opty IS NULL AND src.opty IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) 
                when matched then update set
                    target.amta=src.amta, 
                    target.amta_dtwc=src.amta_dtwc, 
                    target.amta_lclc=src.amta_lclc, 
                    target.amta_rfrc=src.amta_rfrc, 
                    target.amta_rpc1=src.amta_rpc1, 
                    target.amta_rpc2=src.amta_rpc2, 
                    target.amtg_dtwc=src.amtg_dtwc, 
                    target.amtg_lclc=src.amtg_lclc, 
                    target.amtg_rfrc=src.amtg_rfrc, 
                    target.amtg_rpc1=src.amtg_rpc1, 
                    target.amtg_rpc2=src.amtg_rpc2, 
                    target.amtg_trnc=src.amtg_trnc, 
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cuqs=src.cuqs, 
                    target.cuqs_ref_compnr=src.cuqs_ref_compnr, 
                    target.cvqs=src.cvqs, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.opty=src.opty, 
                    target.opty_ref_compnr=src.opty_ref_compnr, 
                    target.pono=src.pono, 
                    target.prid=src.prid, 
                    target.qoor=src.qoor, 
                    target.qoor_buar=src.qoor_buar, 
                    target.qoor_buln=src.qoor_buln, 
                    target.qoor_bupc=src.qoor_bupc, 
                    target.qoor_butm=src.qoor_butm, 
                    target.qoor_buvl=src.qoor_buvl, 
                    target.qoor_buwg=src.qoor_buwg, 
                    target.qoor_invu=src.qoor_invu, 
                    target.seli=src.seli, 
                    target.seli_kw=src.seli_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amta, amta_dtwc, amta_lclc, amta_rfrc, amta_rpc1, amta_rpc2, amtg_dtwc, amtg_lclc, amtg_rfrc, amtg_rpc1, amtg_rpc2, amtg_trnc, cact, compnr, cprj, cprj_ref_compnr, cspa, cuqs, cuqs_ref_compnr, cvqs, deleted, item, item_ref_compnr, opty, opty_ref_compnr, pono, prid, qoor, qoor_buar, qoor_buln, qoor_bupc, qoor_butm, qoor_buvl, qoor_buwg, qoor_invu, seli, seli_kw, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amta, 
                    src.amta_dtwc, 
                    src.amta_lclc, 
                    src.amta_rfrc, 
                    src.amta_rpc1, 
                    src.amta_rpc2, 
                    src.amtg_dtwc, 
                    src.amtg_lclc, 
                    src.amtg_rfrc, 
                    src.amtg_rpc1, 
                    src.amtg_rpc2, 
                    src.amtg_trnc, 
                    src.cact, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cuqs, 
                    src.cuqs_ref_compnr, 
                    src.cvqs, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.opty, 
                    src.opty_ref_compnr, 
                    src.pono, 
                    src.prid, 
                    src.qoor, 
                    src.qoor_buar, 
                    src.qoor_buln, 
                    src.qoor_bupc, 
                    src.qoor_butm, 
                    src.qoor_buvl, 
                    src.qoor_buwg, 
                    src.qoor_invu, 
                    src.seli, 
                    src.seli_kw, 
                    src.sequencenumber, 
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
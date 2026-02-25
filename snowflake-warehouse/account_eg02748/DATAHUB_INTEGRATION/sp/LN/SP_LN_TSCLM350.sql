create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCLM350()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCLM350_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCLM350 as target using (SELECT * FROM (SELECT 
            strm.acln, 
            strm.ccll, 
            strm.ccll_ref_compnr, 
            strm.cgrp, 
            strm.cgrp_ref_compnr, 
            strm.clst, 
            strm.clst_ref_compnr, 
            strm.compnr, 
            strm.crac, 
            strm.crac_ref_compnr, 
            strm.csgr, 
            strm.csgr_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.espr, 
            strm.espr_ref_compnr, 
            strm.expr, 
            strm.expr_ref_compnr, 
            strm.exsl, 
            strm.exsl_ref_compnr, 
            strm.hidt, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.item_sern_ref_compnr, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.orig, 
            strm.orig_kw, 
            strm.orno, 
            strm.rprl, 
            strm.rprl_ref_compnr, 
            strm.scgr, 
            strm.scgr_ref_compnr, 
            strm.sear, 
            strm.sequencenumber, 
            strm.sern, 
            strm.sigr, 
            strm.sigr_ref_compnr, 
            strm.sltn, 
            strm.sltn_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orig,
            strm.orno,
            strm.acln,
            strm.ccll,
            strm.hidt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCLM350 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orig=src.orig) OR (target.orig IS NULL AND src.orig IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.acln=src.acln) OR (target.acln IS NULL AND src.acln IS NULL)) AND
            ((target.ccll=src.ccll) OR (target.ccll IS NULL AND src.ccll IS NULL)) AND
            ((target.hidt=src.hidt) OR (target.hidt IS NULL AND src.hidt IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acln=src.acln, 
                    target.ccll=src.ccll, 
                    target.ccll_ref_compnr=src.ccll_ref_compnr, 
                    target.cgrp=src.cgrp, 
                    target.cgrp_ref_compnr=src.cgrp_ref_compnr, 
                    target.clst=src.clst, 
                    target.clst_ref_compnr=src.clst_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.crac=src.crac, 
                    target.crac_ref_compnr=src.crac_ref_compnr, 
                    target.csgr=src.csgr, 
                    target.csgr_ref_compnr=src.csgr_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.espr=src.espr, 
                    target.espr_ref_compnr=src.espr_ref_compnr, 
                    target.expr=src.expr, 
                    target.expr_ref_compnr=src.expr_ref_compnr, 
                    target.exsl=src.exsl, 
                    target.exsl_ref_compnr=src.exsl_ref_compnr, 
                    target.hidt=src.hidt, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.item_sern_ref_compnr=src.item_sern_ref_compnr, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.orig=src.orig, 
                    target.orig_kw=src.orig_kw, 
                    target.orno=src.orno, 
                    target.rprl=src.rprl, 
                    target.rprl_ref_compnr=src.rprl_ref_compnr, 
                    target.scgr=src.scgr, 
                    target.scgr_ref_compnr=src.scgr_ref_compnr, 
                    target.sear=src.sear, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.sigr=src.sigr, 
                    target.sigr_ref_compnr=src.sigr_ref_compnr, 
                    target.sltn=src.sltn, 
                    target.sltn_ref_compnr=src.sltn_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acln, 
                    ccll, 
                    ccll_ref_compnr, 
                    cgrp, 
                    cgrp_ref_compnr, 
                    clst, 
                    clst_ref_compnr, 
                    compnr, 
                    crac, 
                    crac_ref_compnr, 
                    csgr, 
                    csgr_ref_compnr, 
                    deleted, 
                    desc, 
                    espr, 
                    espr_ref_compnr, 
                    expr, 
                    expr_ref_compnr, 
                    exsl, 
                    exsl_ref_compnr, 
                    hidt, 
                    item, 
                    item_ref_compnr, 
                    item_sern_ref_compnr, 
                    ofbp, 
                    ofbp_ref_compnr, 
                    orig, 
                    orig_kw, 
                    orno, 
                    rprl, 
                    rprl_ref_compnr, 
                    scgr, 
                    scgr_ref_compnr, 
                    sear, 
                    sequencenumber, 
                    sern, 
                    sigr, 
                    sigr_ref_compnr, 
                    sltn, 
                    sltn_ref_compnr, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acln, 
                    src.ccll, 
                    src.ccll_ref_compnr, 
                    src.cgrp, 
                    src.cgrp_ref_compnr, 
                    src.clst, 
                    src.clst_ref_compnr, 
                    src.compnr, 
                    src.crac, 
                    src.crac_ref_compnr, 
                    src.csgr, 
                    src.csgr_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.espr, 
                    src.espr_ref_compnr, 
                    src.expr, 
                    src.expr_ref_compnr, 
                    src.exsl, 
                    src.exsl_ref_compnr, 
                    src.hidt, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.item_sern_ref_compnr, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.orig, 
                    src.orig_kw, 
                    src.orno, 
                    src.rprl, 
                    src.rprl_ref_compnr, 
                    src.scgr, 
                    src.scgr_ref_compnr, 
                    src.sear, 
                    src.sequencenumber, 
                    src.sern, 
                    src.sigr, 
                    src.sigr_ref_compnr, 
                    src.sltn, 
                    src.sltn_ref_compnr, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCLM350_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acln, 
            strm.ccll, 
            strm.ccll_ref_compnr, 
            strm.cgrp, 
            strm.cgrp_ref_compnr, 
            strm.clst, 
            strm.clst_ref_compnr, 
            strm.compnr, 
            strm.crac, 
            strm.crac_ref_compnr, 
            strm.csgr, 
            strm.csgr_ref_compnr, 
            strm.deleted, 
            strm.desc, 
            strm.espr, 
            strm.espr_ref_compnr, 
            strm.expr, 
            strm.expr_ref_compnr, 
            strm.exsl, 
            strm.exsl_ref_compnr, 
            strm.hidt, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.item_sern_ref_compnr, 
            strm.ofbp, 
            strm.ofbp_ref_compnr, 
            strm.orig, 
            strm.orig_kw, 
            strm.orno, 
            strm.rprl, 
            strm.rprl_ref_compnr, 
            strm.scgr, 
            strm.scgr_ref_compnr, 
            strm.sear, 
            strm.sequencenumber, 
            strm.sern, 
            strm.sigr, 
            strm.sigr_ref_compnr, 
            strm.sltn, 
            strm.sltn_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orig,
            strm.orno,
            strm.acln,
            strm.ccll,
            strm.hidt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCLM350 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orig=src.orig) OR (target.orig IS NULL AND src.orig IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.acln=src.acln) OR (target.acln IS NULL AND src.acln IS NULL)) AND
            ((target.ccll=src.ccll) OR (target.ccll IS NULL AND src.ccll IS NULL)) AND
            ((target.hidt=src.hidt) OR (target.hidt IS NULL AND src.hidt IS NULL)) 
                when matched then update set
                    target.acln=src.acln, 
                    target.ccll=src.ccll, 
                    target.ccll_ref_compnr=src.ccll_ref_compnr, 
                    target.cgrp=src.cgrp, 
                    target.cgrp_ref_compnr=src.cgrp_ref_compnr, 
                    target.clst=src.clst, 
                    target.clst_ref_compnr=src.clst_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.crac=src.crac, 
                    target.crac_ref_compnr=src.crac_ref_compnr, 
                    target.csgr=src.csgr, 
                    target.csgr_ref_compnr=src.csgr_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.espr=src.espr, 
                    target.espr_ref_compnr=src.espr_ref_compnr, 
                    target.expr=src.expr, 
                    target.expr_ref_compnr=src.expr_ref_compnr, 
                    target.exsl=src.exsl, 
                    target.exsl_ref_compnr=src.exsl_ref_compnr, 
                    target.hidt=src.hidt, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.item_sern_ref_compnr=src.item_sern_ref_compnr, 
                    target.ofbp=src.ofbp, 
                    target.ofbp_ref_compnr=src.ofbp_ref_compnr, 
                    target.orig=src.orig, 
                    target.orig_kw=src.orig_kw, 
                    target.orno=src.orno, 
                    target.rprl=src.rprl, 
                    target.rprl_ref_compnr=src.rprl_ref_compnr, 
                    target.scgr=src.scgr, 
                    target.scgr_ref_compnr=src.scgr_ref_compnr, 
                    target.sear=src.sear, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.sigr=src.sigr, 
                    target.sigr_ref_compnr=src.sigr_ref_compnr, 
                    target.sltn=src.sltn, 
                    target.sltn_ref_compnr=src.sltn_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acln, ccll, ccll_ref_compnr, cgrp, cgrp_ref_compnr, clst, clst_ref_compnr, compnr, crac, crac_ref_compnr, csgr, csgr_ref_compnr, deleted, desc, espr, espr_ref_compnr, expr, expr_ref_compnr, exsl, exsl_ref_compnr, hidt, item, item_ref_compnr, item_sern_ref_compnr, ofbp, ofbp_ref_compnr, orig, orig_kw, orno, rprl, rprl_ref_compnr, scgr, scgr_ref_compnr, sear, sequencenumber, sern, sigr, sigr_ref_compnr, sltn, sltn_ref_compnr, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acln, 
                    src.ccll, 
                    src.ccll_ref_compnr, 
                    src.cgrp, 
                    src.cgrp_ref_compnr, 
                    src.clst, 
                    src.clst_ref_compnr, 
                    src.compnr, 
                    src.crac, 
                    src.crac_ref_compnr, 
                    src.csgr, 
                    src.csgr_ref_compnr, 
                    src.deleted, 
                    src.desc, 
                    src.espr, 
                    src.espr_ref_compnr, 
                    src.expr, 
                    src.expr_ref_compnr, 
                    src.exsl, 
                    src.exsl_ref_compnr, 
                    src.hidt, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.item_sern_ref_compnr, 
                    src.ofbp, 
                    src.ofbp_ref_compnr, 
                    src.orig, 
                    src.orig_kw, 
                    src.orno, 
                    src.rprl, 
                    src.rprl_ref_compnr, 
                    src.scgr, 
                    src.scgr_ref_compnr, 
                    src.sear, 
                    src.sequencenumber, 
                    src.sern, 
                    src.sigr, 
                    src.sigr_ref_compnr, 
                    src.sltn, 
                    src.sltn_ref_compnr, 
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
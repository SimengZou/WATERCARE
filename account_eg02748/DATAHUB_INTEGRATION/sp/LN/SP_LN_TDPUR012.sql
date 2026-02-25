create or replace procedure DATAHUB_INTEGRATION.SP_LN_TDPUR012()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TDPUR012_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TDPUR012 as target using (SELECT * FROM (SELECT 
            strm.cofc, 
            strm.cofc_ref_compnr, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.ngco, 
            strm.ngco_ref_compnr, 
            strm.ngco_scid_ref_compnr, 
            strm.ngpc, 
            strm.ngpc_ref_compnr, 
            strm.ngpc_sepc_ref_compnr, 
            strm.ngpo, 
            strm.ngpo_ref_compnr, 
            strm.ngpo_sepo_ref_compnr, 
            strm.ngpo_seps_ref_compnr, 
            strm.ngpq, 
            strm.ngpq_ref_compnr, 
            strm.ngpq_sepq_ref_compnr, 
            strm.ngpr, 
            strm.ngpr_ref_compnr, 
            strm.ngpr_sepr_ref_compnr, 
            strm.ngrl, 
            strm.ngrl_ref_compnr, 
            strm.ngrl_serl_ref_compnr, 
            strm.ngsp, 
            strm.ngsp_ref_compnr, 
            strm.ngsp_safp_ref_compnr, 
            strm.ngsp_seqo_ref_compnr, 
            strm.ngsp_sequ_ref_compnr, 
            strm.ngsp_sspo_ref_compnr, 
            strm.safp, 
            strm.scid, 
            strm.sepc, 
            strm.sepo, 
            strm.sepq, 
            strm.sepr, 
            strm.seps, 
            strm.seqo, 
            strm.sequ, 
            strm.sequencenumber, 
            strm.serl, 
            strm.site, 
            strm.site_ref_compnr, 
            strm.sspo, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cofc ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR012 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cofc=src.cofc) OR (target.cofc IS NULL AND src.cofc IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cofc=src.cofc, 
                    target.cofc_ref_compnr=src.cofc_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.ngco=src.ngco, 
                    target.ngco_ref_compnr=src.ngco_ref_compnr, 
                    target.ngco_scid_ref_compnr=src.ngco_scid_ref_compnr, 
                    target.ngpc=src.ngpc, 
                    target.ngpc_ref_compnr=src.ngpc_ref_compnr, 
                    target.ngpc_sepc_ref_compnr=src.ngpc_sepc_ref_compnr, 
                    target.ngpo=src.ngpo, 
                    target.ngpo_ref_compnr=src.ngpo_ref_compnr, 
                    target.ngpo_sepo_ref_compnr=src.ngpo_sepo_ref_compnr, 
                    target.ngpo_seps_ref_compnr=src.ngpo_seps_ref_compnr, 
                    target.ngpq=src.ngpq, 
                    target.ngpq_ref_compnr=src.ngpq_ref_compnr, 
                    target.ngpq_sepq_ref_compnr=src.ngpq_sepq_ref_compnr, 
                    target.ngpr=src.ngpr, 
                    target.ngpr_ref_compnr=src.ngpr_ref_compnr, 
                    target.ngpr_sepr_ref_compnr=src.ngpr_sepr_ref_compnr, 
                    target.ngrl=src.ngrl, 
                    target.ngrl_ref_compnr=src.ngrl_ref_compnr, 
                    target.ngrl_serl_ref_compnr=src.ngrl_serl_ref_compnr, 
                    target.ngsp=src.ngsp, 
                    target.ngsp_ref_compnr=src.ngsp_ref_compnr, 
                    target.ngsp_safp_ref_compnr=src.ngsp_safp_ref_compnr, 
                    target.ngsp_seqo_ref_compnr=src.ngsp_seqo_ref_compnr, 
                    target.ngsp_sequ_ref_compnr=src.ngsp_sequ_ref_compnr, 
                    target.ngsp_sspo_ref_compnr=src.ngsp_sspo_ref_compnr, 
                    target.safp=src.safp, 
                    target.scid=src.scid, 
                    target.sepc=src.sepc, 
                    target.sepo=src.sepo, 
                    target.sepq=src.sepq, 
                    target.sepr=src.sepr, 
                    target.seps=src.seps, 
                    target.seqo=src.seqo, 
                    target.sequ=src.sequ, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.site=src.site, 
                    target.site_ref_compnr=src.site_ref_compnr, 
                    target.sspo=src.sspo, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cofc, 
                    cofc_ref_compnr, 
                    compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    dsca, 
                    ngco, 
                    ngco_ref_compnr, 
                    ngco_scid_ref_compnr, 
                    ngpc, 
                    ngpc_ref_compnr, 
                    ngpc_sepc_ref_compnr, 
                    ngpo, 
                    ngpo_ref_compnr, 
                    ngpo_sepo_ref_compnr, 
                    ngpo_seps_ref_compnr, 
                    ngpq, 
                    ngpq_ref_compnr, 
                    ngpq_sepq_ref_compnr, 
                    ngpr, 
                    ngpr_ref_compnr, 
                    ngpr_sepr_ref_compnr, 
                    ngrl, 
                    ngrl_ref_compnr, 
                    ngrl_serl_ref_compnr, 
                    ngsp, 
                    ngsp_ref_compnr, 
                    ngsp_safp_ref_compnr, 
                    ngsp_seqo_ref_compnr, 
                    ngsp_sequ_ref_compnr, 
                    ngsp_sspo_ref_compnr, 
                    safp, 
                    scid, 
                    sepc, 
                    sepo, 
                    sepq, 
                    sepr, 
                    seps, 
                    seqo, 
                    sequ, 
                    sequencenumber, 
                    serl, 
                    site, 
                    site_ref_compnr, 
                    sspo, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cofc, 
                    src.cofc_ref_compnr, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.ngco, 
                    src.ngco_ref_compnr, 
                    src.ngco_scid_ref_compnr, 
                    src.ngpc, 
                    src.ngpc_ref_compnr, 
                    src.ngpc_sepc_ref_compnr, 
                    src.ngpo, 
                    src.ngpo_ref_compnr, 
                    src.ngpo_sepo_ref_compnr, 
                    src.ngpo_seps_ref_compnr, 
                    src.ngpq, 
                    src.ngpq_ref_compnr, 
                    src.ngpq_sepq_ref_compnr, 
                    src.ngpr, 
                    src.ngpr_ref_compnr, 
                    src.ngpr_sepr_ref_compnr, 
                    src.ngrl, 
                    src.ngrl_ref_compnr, 
                    src.ngrl_serl_ref_compnr, 
                    src.ngsp, 
                    src.ngsp_ref_compnr, 
                    src.ngsp_safp_ref_compnr, 
                    src.ngsp_seqo_ref_compnr, 
                    src.ngsp_sequ_ref_compnr, 
                    src.ngsp_sspo_ref_compnr, 
                    src.safp, 
                    src.scid, 
                    src.sepc, 
                    src.sepo, 
                    src.sepq, 
                    src.sepr, 
                    src.seps, 
                    src.seqo, 
                    src.sequ, 
                    src.sequencenumber, 
                    src.serl, 
                    src.site, 
                    src.site_ref_compnr, 
                    src.sspo, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TDPUR012_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cofc, 
            strm.cofc_ref_compnr, 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.ngco, 
            strm.ngco_ref_compnr, 
            strm.ngco_scid_ref_compnr, 
            strm.ngpc, 
            strm.ngpc_ref_compnr, 
            strm.ngpc_sepc_ref_compnr, 
            strm.ngpo, 
            strm.ngpo_ref_compnr, 
            strm.ngpo_sepo_ref_compnr, 
            strm.ngpo_seps_ref_compnr, 
            strm.ngpq, 
            strm.ngpq_ref_compnr, 
            strm.ngpq_sepq_ref_compnr, 
            strm.ngpr, 
            strm.ngpr_ref_compnr, 
            strm.ngpr_sepr_ref_compnr, 
            strm.ngrl, 
            strm.ngrl_ref_compnr, 
            strm.ngrl_serl_ref_compnr, 
            strm.ngsp, 
            strm.ngsp_ref_compnr, 
            strm.ngsp_safp_ref_compnr, 
            strm.ngsp_seqo_ref_compnr, 
            strm.ngsp_sequ_ref_compnr, 
            strm.ngsp_sspo_ref_compnr, 
            strm.safp, 
            strm.scid, 
            strm.sepc, 
            strm.sepo, 
            strm.sepq, 
            strm.sepr, 
            strm.seps, 
            strm.seqo, 
            strm.sequ, 
            strm.sequencenumber, 
            strm.serl, 
            strm.site, 
            strm.site_ref_compnr, 
            strm.sspo, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cofc ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR012 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cofc=src.cofc) OR (target.cofc IS NULL AND src.cofc IS NULL)) 
                when matched then update set
                    target.cofc=src.cofc, 
                    target.cofc_ref_compnr=src.cofc_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.ngco=src.ngco, 
                    target.ngco_ref_compnr=src.ngco_ref_compnr, 
                    target.ngco_scid_ref_compnr=src.ngco_scid_ref_compnr, 
                    target.ngpc=src.ngpc, 
                    target.ngpc_ref_compnr=src.ngpc_ref_compnr, 
                    target.ngpc_sepc_ref_compnr=src.ngpc_sepc_ref_compnr, 
                    target.ngpo=src.ngpo, 
                    target.ngpo_ref_compnr=src.ngpo_ref_compnr, 
                    target.ngpo_sepo_ref_compnr=src.ngpo_sepo_ref_compnr, 
                    target.ngpo_seps_ref_compnr=src.ngpo_seps_ref_compnr, 
                    target.ngpq=src.ngpq, 
                    target.ngpq_ref_compnr=src.ngpq_ref_compnr, 
                    target.ngpq_sepq_ref_compnr=src.ngpq_sepq_ref_compnr, 
                    target.ngpr=src.ngpr, 
                    target.ngpr_ref_compnr=src.ngpr_ref_compnr, 
                    target.ngpr_sepr_ref_compnr=src.ngpr_sepr_ref_compnr, 
                    target.ngrl=src.ngrl, 
                    target.ngrl_ref_compnr=src.ngrl_ref_compnr, 
                    target.ngrl_serl_ref_compnr=src.ngrl_serl_ref_compnr, 
                    target.ngsp=src.ngsp, 
                    target.ngsp_ref_compnr=src.ngsp_ref_compnr, 
                    target.ngsp_safp_ref_compnr=src.ngsp_safp_ref_compnr, 
                    target.ngsp_seqo_ref_compnr=src.ngsp_seqo_ref_compnr, 
                    target.ngsp_sequ_ref_compnr=src.ngsp_sequ_ref_compnr, 
                    target.ngsp_sspo_ref_compnr=src.ngsp_sspo_ref_compnr, 
                    target.safp=src.safp, 
                    target.scid=src.scid, 
                    target.sepc=src.sepc, 
                    target.sepo=src.sepo, 
                    target.sepq=src.sepq, 
                    target.sepr=src.sepr, 
                    target.seps=src.seps, 
                    target.seqo=src.seqo, 
                    target.sequ=src.sequ, 
                    target.sequencenumber=src.sequencenumber, 
                    target.serl=src.serl, 
                    target.site=src.site, 
                    target.site_ref_compnr=src.site_ref_compnr, 
                    target.sspo=src.sspo, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cofc, cofc_ref_compnr, compnr, cwar, cwar_ref_compnr, deleted, dsca, ngco, ngco_ref_compnr, ngco_scid_ref_compnr, ngpc, ngpc_ref_compnr, ngpc_sepc_ref_compnr, ngpo, ngpo_ref_compnr, ngpo_sepo_ref_compnr, ngpo_seps_ref_compnr, ngpq, ngpq_ref_compnr, ngpq_sepq_ref_compnr, ngpr, ngpr_ref_compnr, ngpr_sepr_ref_compnr, ngrl, ngrl_ref_compnr, ngrl_serl_ref_compnr, ngsp, ngsp_ref_compnr, ngsp_safp_ref_compnr, ngsp_seqo_ref_compnr, ngsp_sequ_ref_compnr, ngsp_sspo_ref_compnr, safp, scid, sepc, sepo, sepq, sepr, seps, seqo, sequ, sequencenumber, serl, site, site_ref_compnr, sspo, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cofc, 
                    src.cofc_ref_compnr, 
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.ngco, 
                    src.ngco_ref_compnr, 
                    src.ngco_scid_ref_compnr, 
                    src.ngpc, 
                    src.ngpc_ref_compnr, 
                    src.ngpc_sepc_ref_compnr, 
                    src.ngpo, 
                    src.ngpo_ref_compnr, 
                    src.ngpo_sepo_ref_compnr, 
                    src.ngpo_seps_ref_compnr, 
                    src.ngpq, 
                    src.ngpq_ref_compnr, 
                    src.ngpq_sepq_ref_compnr, 
                    src.ngpr, 
                    src.ngpr_ref_compnr, 
                    src.ngpr_sepr_ref_compnr, 
                    src.ngrl, 
                    src.ngrl_ref_compnr, 
                    src.ngrl_serl_ref_compnr, 
                    src.ngsp, 
                    src.ngsp_ref_compnr, 
                    src.ngsp_safp_ref_compnr, 
                    src.ngsp_seqo_ref_compnr, 
                    src.ngsp_sequ_ref_compnr, 
                    src.ngsp_sspo_ref_compnr, 
                    src.safp, 
                    src.scid, 
                    src.sepc, 
                    src.sepo, 
                    src.sepq, 
                    src.sepr, 
                    src.seps, 
                    src.seqo, 
                    src.sequ, 
                    src.sequencenumber, 
                    src.serl, 
                    src.site, 
                    src.site_ref_compnr, 
                    src.sspo, 
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
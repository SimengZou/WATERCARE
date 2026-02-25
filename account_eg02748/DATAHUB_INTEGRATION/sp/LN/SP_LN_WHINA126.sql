create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINA126()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINA126_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINA126 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.iror, 
            strm.iror_kw, 
            strm.item, 
            strm.item_cwar_trdt_seqn_inwp_ref_compnr, 
            strm.item_ref_compnr, 
            strm.ivsq, 
            strm.lgdt, 
            strm.reva, 
            strm.reva_kw, 
            strm.rorg, 
            strm.rorg_kw, 
            strm.rorn, 
            strm.rseq, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.vpdt, 
            strm.vseq, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item,
            strm.cwar,
            strm.trdt,
            strm.seqn,
            strm.inwp,
            strm.vpdt,
            strm.vseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA126 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.inwp=src.inwp) OR (target.inwp IS NULL AND src.inwp IS NULL)) AND
            ((target.vpdt=src.vpdt) OR (target.vpdt IS NULL AND src.vpdt IS NULL)) AND
            ((target.vseq=src.vseq) OR (target.vseq IS NULL AND src.vseq IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.iror=src.iror, 
                    target.iror_kw=src.iror_kw, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_inwp_ref_compnr=src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ivsq=src.ivsq, 
                    target.lgdt=src.lgdt, 
                    target.reva=src.reva, 
                    target.reva_kw=src.reva_kw, 
                    target.rorg=src.rorg, 
                    target.rorg_kw=src.rorg_kw, 
                    target.rorn=src.rorn, 
                    target.rseq=src.rseq, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.vpdt=src.vpdt, 
                    target.vseq=src.vseq, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    inwp, 
                    inwp_kw, 
                    iror, 
                    iror_kw, 
                    item, 
                    item_cwar_trdt_seqn_inwp_ref_compnr, 
                    item_ref_compnr, 
                    ivsq, 
                    lgdt, 
                    reva, 
                    reva_kw, 
                    rorg, 
                    rorg_kw, 
                    rorn, 
                    rseq, 
                    seqn, 
                    sequencenumber, 
                    timestamp, 
                    trdt, 
                    username, 
                    vpdt, 
                    vseq, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.iror, 
                    src.iror_kw, 
                    src.item, 
                    src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    src.item_ref_compnr, 
                    src.ivsq, 
                    src.lgdt, 
                    src.reva, 
                    src.reva_kw, 
                    src.rorg, 
                    src.rorg_kw, 
                    src.rorn, 
                    src.rseq, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.vpdt, 
                    src.vseq,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINA126_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.iror, 
            strm.iror_kw, 
            strm.item, 
            strm.item_cwar_trdt_seqn_inwp_ref_compnr, 
            strm.item_ref_compnr, 
            strm.ivsq, 
            strm.lgdt, 
            strm.reva, 
            strm.reva_kw, 
            strm.rorg, 
            strm.rorg_kw, 
            strm.rorn, 
            strm.rseq, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
            strm.vpdt, 
            strm.vseq, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.item,
            strm.cwar,
            strm.trdt,
            strm.seqn,
            strm.inwp,
            strm.vpdt,
            strm.vseq ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA126 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.inwp=src.inwp) OR (target.inwp IS NULL AND src.inwp IS NULL)) AND
            ((target.vpdt=src.vpdt) OR (target.vpdt IS NULL AND src.vpdt IS NULL)) AND
            ((target.vseq=src.vseq) OR (target.vseq IS NULL AND src.vseq IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.iror=src.iror, 
                    target.iror_kw=src.iror_kw, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_inwp_ref_compnr=src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.ivsq=src.ivsq, 
                    target.lgdt=src.lgdt, 
                    target.reva=src.reva, 
                    target.reva_kw=src.reva_kw, 
                    target.rorg=src.rorg, 
                    target.rorg_kw=src.rorg_kw, 
                    target.rorn=src.rorn, 
                    target.rseq=src.rseq, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.vpdt=src.vpdt, 
                    target.vseq=src.vseq, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, cwar, cwar_ref_compnr, deleted, inwp, inwp_kw, iror, iror_kw, item, item_cwar_trdt_seqn_inwp_ref_compnr, item_ref_compnr, ivsq, lgdt, reva, reva_kw, rorg, rorg_kw, rorn, rseq, seqn, sequencenumber, timestamp, trdt, username, vpdt, vseq, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.iror, 
                    src.iror_kw, 
                    src.item, 
                    src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    src.item_ref_compnr, 
                    src.ivsq, 
                    src.lgdt, 
                    src.reva, 
                    src.reva_kw, 
                    src.rorg, 
                    src.rorg_kw, 
                    src.rorn, 
                    src.rseq, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.trdt, 
                    src.username, 
                    src.vpdt, 
                    src.vseq,     
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
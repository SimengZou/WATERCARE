create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINA113()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINA113_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINA113 as target using (SELECT * FROM (SELECT 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amah, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.atmc_1, 
            strm.atmc_2, 
            strm.atmc_3, 
            strm.atmh, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hour, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.item, 
            strm.item_cwar_trdt_seqn_inwp_ref_compnr, 
            strm.item_ref_compnr, 
            strm.matc_1, 
            strm.matc_2, 
            strm.matc_3, 
            strm.math, 
            strm.mauc_1, 
            strm.mauc_2, 
            strm.mauc_3, 
            strm.mauh, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
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
            strm.cpcp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA113 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.inwp=src.inwp) OR (target.inwp IS NULL AND src.inwp IS NULL)) AND
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amah=src.amah, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.atmc_1=src.atmc_1, 
                    target.atmc_2=src.atmc_2, 
                    target.atmc_3=src.atmc_3, 
                    target.atmh=src.atmh, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_inwp_ref_compnr=src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.matc_1=src.matc_1, 
                    target.matc_2=src.matc_2, 
                    target.matc_3=src.matc_3, 
                    target.math=src.math, 
                    target.mauc_1=src.mauc_1, 
                    target.mauc_2=src.mauc_2, 
                    target.mauc_3=src.mauc_3, 
                    target.mauh=src.mauh, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amac_1, 
                    amac_2, 
                    amac_3, 
                    amah, 
                    amnt_1, 
                    amnt_2, 
                    amnt_3, 
                    atmc_1, 
                    atmc_2, 
                    atmc_3, 
                    atmh, 
                    compnr, 
                    cpcp, 
                    cpcp_ref_compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    hour, 
                    inwp, 
                    inwp_kw, 
                    item, 
                    item_cwar_trdt_seqn_inwp_ref_compnr, 
                    item_ref_compnr, 
                    matc_1, 
                    matc_2, 
                    matc_3, 
                    math, 
                    mauc_1, 
                    mauc_2, 
                    mauc_3, 
                    mauh, 
                    seqn, 
                    sequencenumber, 
                    timestamp, 
                    trdt, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amah, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.atmc_1, 
                    src.atmc_2, 
                    src.atmc_3, 
                    src.atmh, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hour, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.item, 
                    src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    src.item_ref_compnr, 
                    src.matc_1, 
                    src.matc_2, 
                    src.matc_3, 
                    src.math, 
                    src.mauc_1, 
                    src.mauc_2, 
                    src.mauc_3, 
                    src.mauh, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.trdt, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINA113_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amac_1, 
            strm.amac_2, 
            strm.amac_3, 
            strm.amah, 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.atmc_1, 
            strm.atmc_2, 
            strm.atmc_3, 
            strm.atmh, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hour, 
            strm.inwp, 
            strm.inwp_kw, 
            strm.item, 
            strm.item_cwar_trdt_seqn_inwp_ref_compnr, 
            strm.item_ref_compnr, 
            strm.matc_1, 
            strm.matc_2, 
            strm.matc_3, 
            strm.math, 
            strm.mauc_1, 
            strm.mauc_2, 
            strm.mauc_3, 
            strm.mauh, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.trdt, 
            strm.username, 
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
            strm.cpcp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA113 as  strm
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
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) 
                when matched then update set
                    target.amac_1=src.amac_1, 
                    target.amac_2=src.amac_2, 
                    target.amac_3=src.amac_3, 
                    target.amah=src.amah, 
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.atmc_1=src.atmc_1, 
                    target.atmc_2=src.atmc_2, 
                    target.atmc_3=src.atmc_3, 
                    target.atmh=src.atmh, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.inwp=src.inwp, 
                    target.inwp_kw=src.inwp_kw, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_inwp_ref_compnr=src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.matc_1=src.matc_1, 
                    target.matc_2=src.matc_2, 
                    target.matc_3=src.matc_3, 
                    target.math=src.math, 
                    target.mauc_1=src.mauc_1, 
                    target.mauc_2=src.mauc_2, 
                    target.mauc_3=src.mauc_3, 
                    target.mauh=src.mauh, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amac_1, amac_2, amac_3, amah, amnt_1, amnt_2, amnt_3, atmc_1, atmc_2, atmc_3, atmh, compnr, cpcp, cpcp_ref_compnr, cwar, cwar_ref_compnr, deleted, hour, inwp, inwp_kw, item, item_cwar_trdt_seqn_inwp_ref_compnr, item_ref_compnr, matc_1, matc_2, matc_3, math, mauc_1, mauc_2, mauc_3, mauh, seqn, sequencenumber, timestamp, trdt, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amac_1, 
                    src.amac_2, 
                    src.amac_3, 
                    src.amah, 
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.atmc_1, 
                    src.atmc_2, 
                    src.atmc_3, 
                    src.atmh, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hour, 
                    src.inwp, 
                    src.inwp_kw, 
                    src.item, 
                    src.item_cwar_trdt_seqn_inwp_ref_compnr, 
                    src.item_ref_compnr, 
                    src.matc_1, 
                    src.matc_2, 
                    src.matc_3, 
                    src.math, 
                    src.mauc_1, 
                    src.mauc_2, 
                    src.mauc_3, 
                    src.mauh, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.trdt, 
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
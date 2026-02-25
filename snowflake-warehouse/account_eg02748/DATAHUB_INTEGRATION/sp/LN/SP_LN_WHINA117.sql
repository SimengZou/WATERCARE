create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHINA117()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHINA117_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHINA117 as target using (SELECT * FROM (SELECT 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hour, 
            strm.item, 
            strm.item_cwar_trdt_seqn_ref_compnr, 
            strm.item_ref_compnr, 
            strm.namt_1, 
            strm.namt_2, 
            strm.namt_3, 
            strm.oamt_1, 
            strm.oamt_2, 
            strm.oamt_3, 
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
            strm.cpcp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA117 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_ref_compnr=src.item_cwar_trdt_seqn_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.namt_1=src.namt_1, 
                    target.namt_2=src.namt_2, 
                    target.namt_3=src.namt_3, 
                    target.oamt_1=src.oamt_1, 
                    target.oamt_2=src.oamt_2, 
                    target.oamt_3=src.oamt_3, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amnt_1, 
                    amnt_2, 
                    amnt_3, 
                    compnr, 
                    cpcp, 
                    cpcp_ref_compnr, 
                    cwar, 
                    cwar_ref_compnr, 
                    deleted, 
                    hour, 
                    item, 
                    item_cwar_trdt_seqn_ref_compnr, 
                    item_ref_compnr, 
                    namt_1, 
                    namt_2, 
                    namt_3, 
                    oamt_1, 
                    oamt_2, 
                    oamt_3, 
                    seqn, 
                    sequencenumber, 
                    timestamp, 
                    trdt, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hour, 
                    src.item, 
                    src.item_cwar_trdt_seqn_ref_compnr, 
                    src.item_ref_compnr, 
                    src.namt_1, 
                    src.namt_2, 
                    src.namt_3, 
                    src.oamt_1, 
                    src.oamt_2, 
                    src.oamt_3, 
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

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHINA117_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amnt_1, 
            strm.amnt_2, 
            strm.amnt_3, 
            strm.compnr, 
            strm.cpcp, 
            strm.cpcp_ref_compnr, 
            strm.cwar, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hour, 
            strm.item, 
            strm.item_cwar_trdt_seqn_ref_compnr, 
            strm.item_ref_compnr, 
            strm.namt_1, 
            strm.namt_2, 
            strm.namt_3, 
            strm.oamt_1, 
            strm.oamt_2, 
            strm.oamt_3, 
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
            strm.cpcp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHINA117 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.trdt=src.trdt) OR (target.trdt IS NULL AND src.trdt IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) AND
            ((target.cpcp=src.cpcp) OR (target.cpcp IS NULL AND src.cpcp IS NULL)) 
                when matched then update set
                    target.amnt_1=src.amnt_1, 
                    target.amnt_2=src.amnt_2, 
                    target.amnt_3=src.amnt_3, 
                    target.compnr=src.compnr, 
                    target.cpcp=src.cpcp, 
                    target.cpcp_ref_compnr=src.cpcp_ref_compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hour=src.hour, 
                    target.item=src.item, 
                    target.item_cwar_trdt_seqn_ref_compnr=src.item_cwar_trdt_seqn_ref_compnr, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.namt_1=src.namt_1, 
                    target.namt_2=src.namt_2, 
                    target.namt_3=src.namt_3, 
                    target.oamt_1=src.oamt_1, 
                    target.oamt_2=src.oamt_2, 
                    target.oamt_3=src.oamt_3, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.trdt=src.trdt, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amnt_1, amnt_2, amnt_3, compnr, cpcp, cpcp_ref_compnr, cwar, cwar_ref_compnr, deleted, hour, item, item_cwar_trdt_seqn_ref_compnr, item_ref_compnr, namt_1, namt_2, namt_3, oamt_1, oamt_2, oamt_3, seqn, sequencenumber, timestamp, trdt, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amnt_1, 
                    src.amnt_2, 
                    src.amnt_3, 
                    src.compnr, 
                    src.cpcp, 
                    src.cpcp_ref_compnr, 
                    src.cwar, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hour, 
                    src.item, 
                    src.item_cwar_trdt_seqn_ref_compnr, 
                    src.item_ref_compnr, 
                    src.namt_1, 
                    src.namt_2, 
                    src.namt_3, 
                    src.oamt_1, 
                    src.oamt_2, 
                    src.oamt_3, 
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
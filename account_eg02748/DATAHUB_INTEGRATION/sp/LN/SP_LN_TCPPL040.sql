create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCPPL040()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCPPL040_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCPPL040 as target using (SELECT * FROM (SELECT 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.cgrp, 
            strm.clrt, 
            strm.clrt_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.sequencenumber, 
            strm.spvr, 
            strm.spvr_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.wtsc, 
            strm.wtsc_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cgrp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCPPL040 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cgrp=src.cgrp) OR (target.cgrp IS NULL AND src.cgrp IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.cgrp=src.cgrp, 
                    target.clrt=src.clrt, 
                    target.clrt_ref_compnr=src.clrt_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.sequencenumber=src.sequencenumber, 
                    target.spvr=src.spvr, 
                    target.spvr_ref_compnr=src.spvr_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.wtsc=src.wtsc, 
                    target.wtsc_ref_compnr=src.wtsc_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ccal, 
                    ccal_ref_compnr, 
                    cgrp, 
                    clrt, 
                    clrt_ref_compnr, 
                    compnr, 
                    deleted, 
                    dsca, 
                    sequencenumber, 
                    spvr, 
                    spvr_ref_compnr, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    wtsc, 
                    wtsc_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.cgrp, 
                    src.clrt, 
                    src.clrt_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.sequencenumber, 
                    src.spvr, 
                    src.spvr_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.wtsc, 
                    src.wtsc_ref_compnr,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCPPL040_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ccal, 
            strm.ccal_ref_compnr, 
            strm.cgrp, 
            strm.clrt, 
            strm.clrt_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.dsca, 
            strm.sequencenumber, 
            strm.spvr, 
            strm.spvr_ref_compnr, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.wtsc, 
            strm.wtsc_ref_compnr, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cgrp ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCPPL040 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cgrp=src.cgrp) OR (target.cgrp IS NULL AND src.cgrp IS NULL)) 
                when matched then update set
                    target.ccal=src.ccal, 
                    target.ccal_ref_compnr=src.ccal_ref_compnr, 
                    target.cgrp=src.cgrp, 
                    target.clrt=src.clrt, 
                    target.clrt_ref_compnr=src.clrt_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.sequencenumber=src.sequencenumber, 
                    target.spvr=src.spvr, 
                    target.spvr_ref_compnr=src.spvr_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.wtsc=src.wtsc, 
                    target.wtsc_ref_compnr=src.wtsc_ref_compnr, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ccal, ccal_ref_compnr, cgrp, clrt, clrt_ref_compnr, compnr, deleted, dsca, sequencenumber, spvr, spvr_ref_compnr, timestamp, txta, txta_ref_compnr, username, wtsc, wtsc_ref_compnr, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ccal, 
                    src.ccal_ref_compnr, 
                    src.cgrp, 
                    src.clrt, 
                    src.clrt_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.dsca, 
                    src.sequencenumber, 
                    src.spvr, 
                    src.spvr_ref_compnr, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.wtsc, 
                    src.wtsc_ref_compnr,     
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
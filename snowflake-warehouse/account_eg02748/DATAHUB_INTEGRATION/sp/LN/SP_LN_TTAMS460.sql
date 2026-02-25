create or replace procedure DATAHUB_INTEGRATION.SP_LN_TTAMS460()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TTAMS460_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TTAMS460 as target using (SELECT * FROM (SELECT 
            strm.allc, 
            strm.allc_kw, 
            strm.comp, 
            strm.comp_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.user, 
            strm.user_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.user,
            strm.comp,
            strm.allc ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TTAMS460 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) AND
            ((target.allc=src.allc) OR (target.allc IS NULL AND src.allc IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.allc=src.allc, 
                    target.allc_kw=src.allc_kw, 
                    target.comp=src.comp, 
                    target.comp_ref_compnr=src.comp_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.user_ref_compnr=src.user_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    allc, 
                    allc_kw, 
                    comp, 
                    comp_ref_compnr, 
                    compnr, 
                    deleted, 
                    sequencenumber, 
                    timestamp, 
                    user, 
                    user_ref_compnr, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.allc, 
                    src.allc_kw, 
                    src.comp, 
                    src.comp_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.user, 
                    src.user_ref_compnr, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TTAMS460_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.allc, 
            strm.allc_kw, 
            strm.comp, 
            strm.comp_ref_compnr, 
            strm.compnr, 
            strm.deleted, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.user, 
            strm.user_ref_compnr, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.user,
            strm.comp,
            strm.allc ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TTAMS460 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) AND
            ((target.allc=src.allc) OR (target.allc IS NULL AND src.allc IS NULL)) 
                when matched then update set
                    target.allc=src.allc, 
                    target.allc_kw=src.allc_kw, 
                    target.comp=src.comp, 
                    target.comp_ref_compnr=src.comp_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.user_ref_compnr=src.user_ref_compnr, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( allc, allc_kw, comp, comp_ref_compnr, compnr, deleted, sequencenumber, timestamp, user, user_ref_compnr, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.allc, 
                    src.allc_kw, 
                    src.comp, 
                    src.comp_ref_compnr, 
                    src.compnr, 
                    src.deleted, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.user, 
                    src.user_ref_compnr, 
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
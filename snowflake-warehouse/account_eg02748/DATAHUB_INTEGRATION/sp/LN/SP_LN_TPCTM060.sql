create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPCTM060()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPCTM060_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPCTM060 as target using (SELECT * FROM (SELECT 
            strm.cact, 
            strm.cnln, 
            strm.compnr, 
            strm.cono, 
            strm.cono_cnln_ref_compnr, 
            strm.cono_ref_compnr, 
            strm.cprj, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.sequencenumber, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPCTM060 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cact=src.cact, 
                    target.cnln=src.cnln, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.cono_cnln_ref_compnr=src.cono_cnln_ref_compnr, 
                    target.cono_ref_compnr=src.cono_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cact, 
                    cnln, 
                    compnr, 
                    cono, 
                    cono_cnln_ref_compnr, 
                    cono_ref_compnr, 
                    cprj, 
                    cprj_cspa_ref_compnr, 
                    cprj_cstl_ref_compnr, 
                    cprj_ref_compnr, 
                    cspa, 
                    cstl, 
                    deleted, 
                    sequencenumber, 
                    sern, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cact, 
                    src.cnln, 
                    src.compnr, 
                    src.cono, 
                    src.cono_cnln_ref_compnr, 
                    src.cono_ref_compnr, 
                    src.cprj, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.sequencenumber, 
                    src.sern, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPCTM060_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cact, 
            strm.cnln, 
            strm.compnr, 
            strm.cono, 
            strm.cono_cnln_ref_compnr, 
            strm.cono_ref_compnr, 
            strm.cprj, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.sequencenumber, 
            strm.sern, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.sern ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPCTM060 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.sern=src.sern) OR (target.sern IS NULL AND src.sern IS NULL)) 
                when matched then update set
                    target.cact=src.cact, 
                    target.cnln=src.cnln, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.cono_cnln_ref_compnr=src.cono_cnln_ref_compnr, 
                    target.cono_ref_compnr=src.cono_ref_compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.sequencenumber=src.sequencenumber, 
                    target.sern=src.sern, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cact, cnln, compnr, cono, cono_cnln_ref_compnr, cono_ref_compnr, cprj, cprj_cspa_ref_compnr, cprj_cstl_ref_compnr, cprj_ref_compnr, cspa, cstl, deleted, sequencenumber, sern, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cact, 
                    src.cnln, 
                    src.compnr, 
                    src.cono, 
                    src.cono_cnln_ref_compnr, 
                    src.cono_ref_compnr, 
                    src.cprj, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.sequencenumber, 
                    src.sern, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5FIRSTACT()
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
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5FIRSTACT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5FIRSTACT as target using (SELECT * FROM (SELECT 
            strm.ACT_ACT, 
            strm.ACT_ASMLEVEL, 
            strm.ACT_COMPLEVEL, 
            strm.ACT_COMPLOCATION, 
            strm.ACT_DURATION, 
            strm.ACT_EST, 
            strm.ACT_EVENT, 
            strm.ACT_LASTSAVED, 
            strm.ACT_MANUFACT, 
            strm.ACT_MATLIST, 
            strm.ACT_MULTIPLETRADES, 
            strm.ACT_PERSONS, 
            strm.ACT_REM, 
            strm.ACT_RPC, 
            strm.ACT_START, 
            strm.ACT_SYSLEVEL, 
            strm.ACT_TASK, 
            strm.ACT_TPF, 
            strm.ACT_TRADE, 
            strm.ACT_WAP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACT_EVENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACT_EVENT=src.ACT_EVENT) OR (target.ACT_EVENT IS NULL AND src.ACT_EVENT IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACT_ACT=src.ACT_ACT, 
                    target.ACT_ASMLEVEL=src.ACT_ASMLEVEL, 
                    target.ACT_COMPLEVEL=src.ACT_COMPLEVEL, 
                    target.ACT_COMPLOCATION=src.ACT_COMPLOCATION, 
                    target.ACT_DURATION=src.ACT_DURATION, 
                    target.ACT_EST=src.ACT_EST, 
                    target.ACT_EVENT=src.ACT_EVENT, 
                    target.ACT_LASTSAVED=src.ACT_LASTSAVED, 
                    target.ACT_MANUFACT=src.ACT_MANUFACT, 
                    target.ACT_MATLIST=src.ACT_MATLIST, 
                    target.ACT_MULTIPLETRADES=src.ACT_MULTIPLETRADES, 
                    target.ACT_PERSONS=src.ACT_PERSONS, 
                    target.ACT_REM=src.ACT_REM, 
                    target.ACT_RPC=src.ACT_RPC, 
                    target.ACT_START=src.ACT_START, 
                    target.ACT_SYSLEVEL=src.ACT_SYSLEVEL, 
                    target.ACT_TASK=src.ACT_TASK, 
                    target.ACT_TPF=src.ACT_TPF, 
                    target.ACT_TRADE=src.ACT_TRADE, 
                    target.ACT_WAP=src.ACT_WAP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACT_ACT, 
                    ACT_ASMLEVEL, 
                    ACT_COMPLEVEL, 
                    ACT_COMPLOCATION, 
                    ACT_DURATION, 
                    ACT_EST, 
                    ACT_EVENT, 
                    ACT_LASTSAVED, 
                    ACT_MANUFACT, 
                    ACT_MATLIST, 
                    ACT_MULTIPLETRADES, 
                    ACT_PERSONS, 
                    ACT_REM, 
                    ACT_RPC, 
                    ACT_START, 
                    ACT_SYSLEVEL, 
                    ACT_TASK, 
                    ACT_TPF, 
                    ACT_TRADE, 
                    ACT_WAP, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACT_ACT, 
                    src.ACT_ASMLEVEL, 
                    src.ACT_COMPLEVEL, 
                    src.ACT_COMPLOCATION, 
                    src.ACT_DURATION, 
                    src.ACT_EST, 
                    src.ACT_EVENT, 
                    src.ACT_LASTSAVED, 
                    src.ACT_MANUFACT, 
                    src.ACT_MATLIST, 
                    src.ACT_MULTIPLETRADES, 
                    src.ACT_PERSONS, 
                    src.ACT_REM, 
                    src.ACT_RPC, 
                    src.ACT_START, 
                    src.ACT_SYSLEVEL, 
                    src.ACT_TASK, 
                    src.ACT_TPF, 
                    src.ACT_TRADE, 
                    src.ACT_WAP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5FIRSTACT_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ACT_ACT, 
            strm.ACT_ASMLEVEL, 
            strm.ACT_COMPLEVEL, 
            strm.ACT_COMPLOCATION, 
            strm.ACT_DURATION, 
            strm.ACT_EST, 
            strm.ACT_EVENT, 
            strm.ACT_LASTSAVED, 
            strm.ACT_MANUFACT, 
            strm.ACT_MATLIST, 
            strm.ACT_MULTIPLETRADES, 
            strm.ACT_PERSONS, 
            strm.ACT_REM, 
            strm.ACT_RPC, 
            strm.ACT_START, 
            strm.ACT_SYSLEVEL, 
            strm.ACT_TASK, 
            strm.ACT_TPF, 
            strm.ACT_TRADE, 
            strm.ACT_WAP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACT_EVENT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACT_EVENT=src.ACT_EVENT) OR (target.ACT_EVENT IS NULL AND src.ACT_EVENT IS NULL)) 
                when matched then update set
                    target.ACT_ACT=src.ACT_ACT, 
                    target.ACT_ASMLEVEL=src.ACT_ASMLEVEL, 
                    target.ACT_COMPLEVEL=src.ACT_COMPLEVEL, 
                    target.ACT_COMPLOCATION=src.ACT_COMPLOCATION, 
                    target.ACT_DURATION=src.ACT_DURATION, 
                    target.ACT_EST=src.ACT_EST, 
                    target.ACT_EVENT=src.ACT_EVENT, 
                    target.ACT_LASTSAVED=src.ACT_LASTSAVED, 
                    target.ACT_MANUFACT=src.ACT_MANUFACT, 
                    target.ACT_MATLIST=src.ACT_MATLIST, 
                    target.ACT_MULTIPLETRADES=src.ACT_MULTIPLETRADES, 
                    target.ACT_PERSONS=src.ACT_PERSONS, 
                    target.ACT_REM=src.ACT_REM, 
                    target.ACT_RPC=src.ACT_RPC, 
                    target.ACT_START=src.ACT_START, 
                    target.ACT_SYSLEVEL=src.ACT_SYSLEVEL, 
                    target.ACT_TASK=src.ACT_TASK, 
                    target.ACT_TPF=src.ACT_TPF, 
                    target.ACT_TRADE=src.ACT_TRADE, 
                    target.ACT_WAP=src.ACT_WAP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACT_ACT, ACT_ASMLEVEL, ACT_COMPLEVEL, ACT_COMPLOCATION, ACT_DURATION, ACT_EST, ACT_EVENT, ACT_LASTSAVED, ACT_MANUFACT, ACT_MATLIST, ACT_MULTIPLETRADES, ACT_PERSONS, ACT_REM, ACT_RPC, ACT_START, ACT_SYSLEVEL, ACT_TASK, ACT_TPF, ACT_TRADE, ACT_WAP, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACT_ACT, 
                    src.ACT_ASMLEVEL, 
                    src.ACT_COMPLEVEL, 
                    src.ACT_COMPLOCATION, 
                    src.ACT_DURATION, 
                    src.ACT_EST, 
                    src.ACT_EVENT, 
                    src.ACT_LASTSAVED, 
                    src.ACT_MANUFACT, 
                    src.ACT_MATLIST, 
                    src.ACT_MULTIPLETRADES, 
                    src.ACT_PERSONS, 
                    src.ACT_REM, 
                    src.ACT_RPC, 
                    src.ACT_START, 
                    src.ACT_SYSLEVEL, 
                    src.ACT_TASK, 
                    src.ACT_TPF, 
                    src.ACT_TRADE, 
                    src.ACT_WAP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
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
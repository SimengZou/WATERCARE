create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DRIVERCTRL()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DRIVERCTRL_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DRIVERCTRL as target using (SELECT * FROM (SELECT 
            strm.DRV_ACTION, 
            strm.DRV_CODE, 
            strm.DRV_FREQUENCY, 
            strm.DRV_LASTRAN, 
            strm.DRV_LASTSAVED, 
            strm.DRV_NEXTRUN, 
            strm.DRV_QUEUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DRV_CODE,
            strm.DRV_QUEUE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DRIVERCTRL as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DRV_CODE=src.DRV_CODE) OR (target.DRV_CODE IS NULL AND src.DRV_CODE IS NULL)) AND
            ((target.DRV_QUEUE=src.DRV_QUEUE) OR (target.DRV_QUEUE IS NULL AND src.DRV_QUEUE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DRV_ACTION=src.DRV_ACTION, 
                    target.DRV_CODE=src.DRV_CODE, 
                    target.DRV_FREQUENCY=src.DRV_FREQUENCY, 
                    target.DRV_LASTRAN=src.DRV_LASTRAN, 
                    target.DRV_LASTSAVED=src.DRV_LASTSAVED, 
                    target.DRV_NEXTRUN=src.DRV_NEXTRUN, 
                    target.DRV_QUEUE=src.DRV_QUEUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DRV_ACTION, 
                    DRV_CODE, 
                    DRV_FREQUENCY, 
                    DRV_LASTRAN, 
                    DRV_LASTSAVED, 
                    DRV_NEXTRUN, 
                    DRV_QUEUE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DRV_ACTION, 
                    src.DRV_CODE, 
                    src.DRV_FREQUENCY, 
                    src.DRV_LASTRAN, 
                    src.DRV_LASTSAVED, 
                    src.DRV_NEXTRUN, 
                    src.DRV_QUEUE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DRIVERCTRL_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DRV_ACTION, 
            strm.DRV_CODE, 
            strm.DRV_FREQUENCY, 
            strm.DRV_LASTRAN, 
            strm.DRV_LASTSAVED, 
            strm.DRV_NEXTRUN, 
            strm.DRV_QUEUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DRV_CODE,
            strm.DRV_QUEUE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DRIVERCTRL as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DRV_CODE=src.DRV_CODE) OR (target.DRV_CODE IS NULL AND src.DRV_CODE IS NULL)) AND
            ((target.DRV_QUEUE=src.DRV_QUEUE) OR (target.DRV_QUEUE IS NULL AND src.DRV_QUEUE IS NULL)) 
                when matched then update set
                    target.DRV_ACTION=src.DRV_ACTION, 
                    target.DRV_CODE=src.DRV_CODE, 
                    target.DRV_FREQUENCY=src.DRV_FREQUENCY, 
                    target.DRV_LASTRAN=src.DRV_LASTRAN, 
                    target.DRV_LASTSAVED=src.DRV_LASTSAVED, 
                    target.DRV_NEXTRUN=src.DRV_NEXTRUN, 
                    target.DRV_QUEUE=src.DRV_QUEUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DRV_ACTION, DRV_CODE, DRV_FREQUENCY, DRV_LASTRAN, DRV_LASTSAVED, DRV_NEXTRUN, DRV_QUEUE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DRV_ACTION, 
                    src.DRV_CODE, 
                    src.DRV_FREQUENCY, 
                    src.DRV_LASTRAN, 
                    src.DRV_LASTSAVED, 
                    src.DRV_NEXTRUN, 
                    src.DRV_QUEUE, 
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
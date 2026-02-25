create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DEVICES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DEVICES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DEVICES as target using (SELECT * FROM (SELECT 
            strm.DEV_CATEGORY, 
            strm.DEV_CATFLAG, 
            strm.DEV_CODE, 
            strm.DEV_DESC, 
            strm.DEV_DESTINATION, 
            strm.DEV_DRIVER, 
            strm.DEV_LASTLOGINDATE, 
            strm.DEV_LASTSAVED, 
            strm.DEV_MODE, 
            strm.DEV_NOTUSED, 
            strm.DEV_ORG, 
            strm.DEV_ORIENTATION, 
            strm.DEV_RTYPE, 
            strm.DEV_SPECIAL, 
            strm.DEV_TYPE, 
            strm.DEV_UPDATECOUNT, 
            strm.DEV_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DEV_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DEVICES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DEV_CODE=src.DEV_CODE) OR (target.DEV_CODE IS NULL AND src.DEV_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DEV_CATEGORY=src.DEV_CATEGORY, 
                    target.DEV_CATFLAG=src.DEV_CATFLAG, 
                    target.DEV_CODE=src.DEV_CODE, 
                    target.DEV_DESC=src.DEV_DESC, 
                    target.DEV_DESTINATION=src.DEV_DESTINATION, 
                    target.DEV_DRIVER=src.DEV_DRIVER, 
                    target.DEV_LASTLOGINDATE=src.DEV_LASTLOGINDATE, 
                    target.DEV_LASTSAVED=src.DEV_LASTSAVED, 
                    target.DEV_MODE=src.DEV_MODE, 
                    target.DEV_NOTUSED=src.DEV_NOTUSED, 
                    target.DEV_ORG=src.DEV_ORG, 
                    target.DEV_ORIENTATION=src.DEV_ORIENTATION, 
                    target.DEV_RTYPE=src.DEV_RTYPE, 
                    target.DEV_SPECIAL=src.DEV_SPECIAL, 
                    target.DEV_TYPE=src.DEV_TYPE, 
                    target.DEV_UPDATECOUNT=src.DEV_UPDATECOUNT, 
                    target.DEV_UPDATED=src.DEV_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DEV_CATEGORY, 
                    DEV_CATFLAG, 
                    DEV_CODE, 
                    DEV_DESC, 
                    DEV_DESTINATION, 
                    DEV_DRIVER, 
                    DEV_LASTLOGINDATE, 
                    DEV_LASTSAVED, 
                    DEV_MODE, 
                    DEV_NOTUSED, 
                    DEV_ORG, 
                    DEV_ORIENTATION, 
                    DEV_RTYPE, 
                    DEV_SPECIAL, 
                    DEV_TYPE, 
                    DEV_UPDATECOUNT, 
                    DEV_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DEV_CATEGORY, 
                    src.DEV_CATFLAG, 
                    src.DEV_CODE, 
                    src.DEV_DESC, 
                    src.DEV_DESTINATION, 
                    src.DEV_DRIVER, 
                    src.DEV_LASTLOGINDATE, 
                    src.DEV_LASTSAVED, 
                    src.DEV_MODE, 
                    src.DEV_NOTUSED, 
                    src.DEV_ORG, 
                    src.DEV_ORIENTATION, 
                    src.DEV_RTYPE, 
                    src.DEV_SPECIAL, 
                    src.DEV_TYPE, 
                    src.DEV_UPDATECOUNT, 
                    src.DEV_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DEVICES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DEV_CATEGORY, 
            strm.DEV_CATFLAG, 
            strm.DEV_CODE, 
            strm.DEV_DESC, 
            strm.DEV_DESTINATION, 
            strm.DEV_DRIVER, 
            strm.DEV_LASTLOGINDATE, 
            strm.DEV_LASTSAVED, 
            strm.DEV_MODE, 
            strm.DEV_NOTUSED, 
            strm.DEV_ORG, 
            strm.DEV_ORIENTATION, 
            strm.DEV_RTYPE, 
            strm.DEV_SPECIAL, 
            strm.DEV_TYPE, 
            strm.DEV_UPDATECOUNT, 
            strm.DEV_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DEV_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DEVICES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DEV_CODE=src.DEV_CODE) OR (target.DEV_CODE IS NULL AND src.DEV_CODE IS NULL)) 
                when matched then update set
                    target.DEV_CATEGORY=src.DEV_CATEGORY, 
                    target.DEV_CATFLAG=src.DEV_CATFLAG, 
                    target.DEV_CODE=src.DEV_CODE, 
                    target.DEV_DESC=src.DEV_DESC, 
                    target.DEV_DESTINATION=src.DEV_DESTINATION, 
                    target.DEV_DRIVER=src.DEV_DRIVER, 
                    target.DEV_LASTLOGINDATE=src.DEV_LASTLOGINDATE, 
                    target.DEV_LASTSAVED=src.DEV_LASTSAVED, 
                    target.DEV_MODE=src.DEV_MODE, 
                    target.DEV_NOTUSED=src.DEV_NOTUSED, 
                    target.DEV_ORG=src.DEV_ORG, 
                    target.DEV_ORIENTATION=src.DEV_ORIENTATION, 
                    target.DEV_RTYPE=src.DEV_RTYPE, 
                    target.DEV_SPECIAL=src.DEV_SPECIAL, 
                    target.DEV_TYPE=src.DEV_TYPE, 
                    target.DEV_UPDATECOUNT=src.DEV_UPDATECOUNT, 
                    target.DEV_UPDATED=src.DEV_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DEV_CATEGORY, DEV_CATFLAG, DEV_CODE, DEV_DESC, DEV_DESTINATION, DEV_DRIVER, DEV_LASTLOGINDATE, DEV_LASTSAVED, DEV_MODE, DEV_NOTUSED, DEV_ORG, DEV_ORIENTATION, DEV_RTYPE, DEV_SPECIAL, DEV_TYPE, DEV_UPDATECOUNT, DEV_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DEV_CATEGORY, 
                    src.DEV_CATFLAG, 
                    src.DEV_CODE, 
                    src.DEV_DESC, 
                    src.DEV_DESTINATION, 
                    src.DEV_DRIVER, 
                    src.DEV_LASTLOGINDATE, 
                    src.DEV_LASTSAVED, 
                    src.DEV_MODE, 
                    src.DEV_NOTUSED, 
                    src.DEV_ORG, 
                    src.DEV_ORIENTATION, 
                    src.DEV_RTYPE, 
                    src.DEV_SPECIAL, 
                    src.DEV_TYPE, 
                    src.DEV_UPDATECOUNT, 
                    src.DEV_UPDATED, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ALERTWO()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ALERTWO_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ALERTWO as target using (SELECT * FROM (SELECT 
            strm.ALW_ALERT, 
            strm.ALW_DELAY, 
            strm.ALW_DELAYUOM, 
            strm.ALW_DUENONCONFORMITIESONLY, 
            strm.ALW_DURATIONFIELDID, 
            strm.ALW_INCLUDENONCONFORMITIES, 
            strm.ALW_JOBTYPEFIELDID, 
            strm.ALW_LASTSAVED, 
            strm.ALW_OBJECTFIELDID, 
            strm.ALW_OBJECTORGFIELDID, 
            strm.ALW_PRIORITYFIELDID, 
            strm.ALW_PROBLEMCODEFIELDID, 
            strm.ALW_REQUESTENDFIELDID, 
            strm.ALW_REQUESTSTARTFIELDID, 
            strm.ALW_SCHEDSTARTFIELDID, 
            strm.ALW_STANDWORK, 
            strm.ALW_UPDATECOUNT, 
            strm.ALW_WOCOMMENT, 
            strm.ALW_WODESC, 
            strm.ALW_WORKORDERORG, 
            strm.ALW_WORKORDERORGFIELDID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ALW_ALERT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTWO as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ALW_ALERT=src.ALW_ALERT) OR (target.ALW_ALERT IS NULL AND src.ALW_ALERT IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ALW_ALERT=src.ALW_ALERT, 
                    target.ALW_DELAY=src.ALW_DELAY, 
                    target.ALW_DELAYUOM=src.ALW_DELAYUOM, 
                    target.ALW_DUENONCONFORMITIESONLY=src.ALW_DUENONCONFORMITIESONLY, 
                    target.ALW_DURATIONFIELDID=src.ALW_DURATIONFIELDID, 
                    target.ALW_INCLUDENONCONFORMITIES=src.ALW_INCLUDENONCONFORMITIES, 
                    target.ALW_JOBTYPEFIELDID=src.ALW_JOBTYPEFIELDID, 
                    target.ALW_LASTSAVED=src.ALW_LASTSAVED, 
                    target.ALW_OBJECTFIELDID=src.ALW_OBJECTFIELDID, 
                    target.ALW_OBJECTORGFIELDID=src.ALW_OBJECTORGFIELDID, 
                    target.ALW_PRIORITYFIELDID=src.ALW_PRIORITYFIELDID, 
                    target.ALW_PROBLEMCODEFIELDID=src.ALW_PROBLEMCODEFIELDID, 
                    target.ALW_REQUESTENDFIELDID=src.ALW_REQUESTENDFIELDID, 
                    target.ALW_REQUESTSTARTFIELDID=src.ALW_REQUESTSTARTFIELDID, 
                    target.ALW_SCHEDSTARTFIELDID=src.ALW_SCHEDSTARTFIELDID, 
                    target.ALW_STANDWORK=src.ALW_STANDWORK, 
                    target.ALW_UPDATECOUNT=src.ALW_UPDATECOUNT, 
                    target.ALW_WOCOMMENT=src.ALW_WOCOMMENT, 
                    target.ALW_WODESC=src.ALW_WODESC, 
                    target.ALW_WORKORDERORG=src.ALW_WORKORDERORG, 
                    target.ALW_WORKORDERORGFIELDID=src.ALW_WORKORDERORGFIELDID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ALW_ALERT, 
                    ALW_DELAY, 
                    ALW_DELAYUOM, 
                    ALW_DUENONCONFORMITIESONLY, 
                    ALW_DURATIONFIELDID, 
                    ALW_INCLUDENONCONFORMITIES, 
                    ALW_JOBTYPEFIELDID, 
                    ALW_LASTSAVED, 
                    ALW_OBJECTFIELDID, 
                    ALW_OBJECTORGFIELDID, 
                    ALW_PRIORITYFIELDID, 
                    ALW_PROBLEMCODEFIELDID, 
                    ALW_REQUESTENDFIELDID, 
                    ALW_REQUESTSTARTFIELDID, 
                    ALW_SCHEDSTARTFIELDID, 
                    ALW_STANDWORK, 
                    ALW_UPDATECOUNT, 
                    ALW_WOCOMMENT, 
                    ALW_WODESC, 
                    ALW_WORKORDERORG, 
                    ALW_WORKORDERORGFIELDID, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ALW_ALERT, 
                    src.ALW_DELAY, 
                    src.ALW_DELAYUOM, 
                    src.ALW_DUENONCONFORMITIESONLY, 
                    src.ALW_DURATIONFIELDID, 
                    src.ALW_INCLUDENONCONFORMITIES, 
                    src.ALW_JOBTYPEFIELDID, 
                    src.ALW_LASTSAVED, 
                    src.ALW_OBJECTFIELDID, 
                    src.ALW_OBJECTORGFIELDID, 
                    src.ALW_PRIORITYFIELDID, 
                    src.ALW_PROBLEMCODEFIELDID, 
                    src.ALW_REQUESTENDFIELDID, 
                    src.ALW_REQUESTSTARTFIELDID, 
                    src.ALW_SCHEDSTARTFIELDID, 
                    src.ALW_STANDWORK, 
                    src.ALW_UPDATECOUNT, 
                    src.ALW_WOCOMMENT, 
                    src.ALW_WODESC, 
                    src.ALW_WORKORDERORG, 
                    src.ALW_WORKORDERORGFIELDID, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ALERTWO_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ALW_ALERT, 
            strm.ALW_DELAY, 
            strm.ALW_DELAYUOM, 
            strm.ALW_DUENONCONFORMITIESONLY, 
            strm.ALW_DURATIONFIELDID, 
            strm.ALW_INCLUDENONCONFORMITIES, 
            strm.ALW_JOBTYPEFIELDID, 
            strm.ALW_LASTSAVED, 
            strm.ALW_OBJECTFIELDID, 
            strm.ALW_OBJECTORGFIELDID, 
            strm.ALW_PRIORITYFIELDID, 
            strm.ALW_PROBLEMCODEFIELDID, 
            strm.ALW_REQUESTENDFIELDID, 
            strm.ALW_REQUESTSTARTFIELDID, 
            strm.ALW_SCHEDSTARTFIELDID, 
            strm.ALW_STANDWORK, 
            strm.ALW_UPDATECOUNT, 
            strm.ALW_WOCOMMENT, 
            strm.ALW_WODESC, 
            strm.ALW_WORKORDERORG, 
            strm.ALW_WORKORDERORGFIELDID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ALW_ALERT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTWO as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ALW_ALERT=src.ALW_ALERT) OR (target.ALW_ALERT IS NULL AND src.ALW_ALERT IS NULL)) 
                when matched then update set
                    target.ALW_ALERT=src.ALW_ALERT, 
                    target.ALW_DELAY=src.ALW_DELAY, 
                    target.ALW_DELAYUOM=src.ALW_DELAYUOM, 
                    target.ALW_DUENONCONFORMITIESONLY=src.ALW_DUENONCONFORMITIESONLY, 
                    target.ALW_DURATIONFIELDID=src.ALW_DURATIONFIELDID, 
                    target.ALW_INCLUDENONCONFORMITIES=src.ALW_INCLUDENONCONFORMITIES, 
                    target.ALW_JOBTYPEFIELDID=src.ALW_JOBTYPEFIELDID, 
                    target.ALW_LASTSAVED=src.ALW_LASTSAVED, 
                    target.ALW_OBJECTFIELDID=src.ALW_OBJECTFIELDID, 
                    target.ALW_OBJECTORGFIELDID=src.ALW_OBJECTORGFIELDID, 
                    target.ALW_PRIORITYFIELDID=src.ALW_PRIORITYFIELDID, 
                    target.ALW_PROBLEMCODEFIELDID=src.ALW_PROBLEMCODEFIELDID, 
                    target.ALW_REQUESTENDFIELDID=src.ALW_REQUESTENDFIELDID, 
                    target.ALW_REQUESTSTARTFIELDID=src.ALW_REQUESTSTARTFIELDID, 
                    target.ALW_SCHEDSTARTFIELDID=src.ALW_SCHEDSTARTFIELDID, 
                    target.ALW_STANDWORK=src.ALW_STANDWORK, 
                    target.ALW_UPDATECOUNT=src.ALW_UPDATECOUNT, 
                    target.ALW_WOCOMMENT=src.ALW_WOCOMMENT, 
                    target.ALW_WODESC=src.ALW_WODESC, 
                    target.ALW_WORKORDERORG=src.ALW_WORKORDERORG, 
                    target.ALW_WORKORDERORGFIELDID=src.ALW_WORKORDERORGFIELDID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ALW_ALERT, ALW_DELAY, ALW_DELAYUOM, ALW_DUENONCONFORMITIESONLY, ALW_DURATIONFIELDID, ALW_INCLUDENONCONFORMITIES, ALW_JOBTYPEFIELDID, ALW_LASTSAVED, ALW_OBJECTFIELDID, ALW_OBJECTORGFIELDID, ALW_PRIORITYFIELDID, ALW_PROBLEMCODEFIELDID, ALW_REQUESTENDFIELDID, ALW_REQUESTSTARTFIELDID, ALW_SCHEDSTARTFIELDID, ALW_STANDWORK, ALW_UPDATECOUNT, ALW_WOCOMMENT, ALW_WODESC, ALW_WORKORDERORG, ALW_WORKORDERORGFIELDID, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ALW_ALERT, 
                    src.ALW_DELAY, 
                    src.ALW_DELAYUOM, 
                    src.ALW_DUENONCONFORMITIESONLY, 
                    src.ALW_DURATIONFIELDID, 
                    src.ALW_INCLUDENONCONFORMITIES, 
                    src.ALW_JOBTYPEFIELDID, 
                    src.ALW_LASTSAVED, 
                    src.ALW_OBJECTFIELDID, 
                    src.ALW_OBJECTORGFIELDID, 
                    src.ALW_PRIORITYFIELDID, 
                    src.ALW_PROBLEMCODEFIELDID, 
                    src.ALW_REQUESTENDFIELDID, 
                    src.ALW_REQUESTSTARTFIELDID, 
                    src.ALW_SCHEDSTARTFIELDID, 
                    src.ALW_STANDWORK, 
                    src.ALW_UPDATECOUNT, 
                    src.ALW_WOCOMMENT, 
                    src.ALW_WODESC, 
                    src.ALW_WORKORDERORG, 
                    src.ALW_WORKORDERORGFIELDID, 
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
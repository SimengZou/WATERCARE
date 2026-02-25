create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5USAGE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5USAGE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5USAGE as target using (SELECT * FROM (SELECT 
            strm.SUS_ACTION, 
            strm.SUS_ACTION_DATE, 
            strm.SUS_ID, 
            strm.SUS_LASTSAVED, 
            strm.SUS_PROCESSING_TIME, 
            strm.SUS_SERVERID, 
            strm.SUS_SESSIONID, 
            strm.SUS_SUBTYPE, 
            strm.SUS_TARGET, 
            strm.SUS_TARGET_PARENT, 
            strm.SUS_TARGET_TAB, 
            strm.SUS_TENANT, 
            strm.SUS_TYPE, 
            strm.SUS_UPDATECOUNT, 
            strm.SUS_USERAGENT, 
            strm.SUS_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SUS_ID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USAGE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SUS_ID=src.SUS_ID) OR (target.SUS_ID IS NULL AND src.SUS_ID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.SUS_ACTION=src.SUS_ACTION, 
                    target.SUS_ACTION_DATE=src.SUS_ACTION_DATE, 
                    target.SUS_ID=src.SUS_ID, 
                    target.SUS_LASTSAVED=src.SUS_LASTSAVED, 
                    target.SUS_PROCESSING_TIME=src.SUS_PROCESSING_TIME, 
                    target.SUS_SERVERID=src.SUS_SERVERID, 
                    target.SUS_SESSIONID=src.SUS_SESSIONID, 
                    target.SUS_SUBTYPE=src.SUS_SUBTYPE, 
                    target.SUS_TARGET=src.SUS_TARGET, 
                    target.SUS_TARGET_PARENT=src.SUS_TARGET_PARENT, 
                    target.SUS_TARGET_TAB=src.SUS_TARGET_TAB, 
                    target.SUS_TENANT=src.SUS_TENANT, 
                    target.SUS_TYPE=src.SUS_TYPE, 
                    target.SUS_UPDATECOUNT=src.SUS_UPDATECOUNT, 
                    target.SUS_USERAGENT=src.SUS_USERAGENT, 
                    target.SUS_USERID=src.SUS_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    SUS_ACTION, 
                    SUS_ACTION_DATE, 
                    SUS_ID, 
                    SUS_LASTSAVED, 
                    SUS_PROCESSING_TIME, 
                    SUS_SERVERID, 
                    SUS_SESSIONID, 
                    SUS_SUBTYPE, 
                    SUS_TARGET, 
                    SUS_TARGET_PARENT, 
                    SUS_TARGET_TAB, 
                    SUS_TENANT, 
                    SUS_TYPE, 
                    SUS_UPDATECOUNT, 
                    SUS_USERAGENT, 
                    SUS_USERID, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.SUS_ACTION, 
                    src.SUS_ACTION_DATE, 
                    src.SUS_ID, 
                    src.SUS_LASTSAVED, 
                    src.SUS_PROCESSING_TIME, 
                    src.SUS_SERVERID, 
                    src.SUS_SESSIONID, 
                    src.SUS_SUBTYPE, 
                    src.SUS_TARGET, 
                    src.SUS_TARGET_PARENT, 
                    src.SUS_TARGET_TAB, 
                    src.SUS_TENANT, 
                    src.SUS_TYPE, 
                    src.SUS_UPDATECOUNT, 
                    src.SUS_USERAGENT, 
                    src.SUS_USERID, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5USAGE_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.SUS_ACTION, 
            strm.SUS_ACTION_DATE, 
            strm.SUS_ID, 
            strm.SUS_LASTSAVED, 
            strm.SUS_PROCESSING_TIME, 
            strm.SUS_SERVERID, 
            strm.SUS_SESSIONID, 
            strm.SUS_SUBTYPE, 
            strm.SUS_TARGET, 
            strm.SUS_TARGET_PARENT, 
            strm.SUS_TARGET_TAB, 
            strm.SUS_TENANT, 
            strm.SUS_TYPE, 
            strm.SUS_UPDATECOUNT, 
            strm.SUS_USERAGENT, 
            strm.SUS_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SUS_ID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USAGE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SUS_ID=src.SUS_ID) OR (target.SUS_ID IS NULL AND src.SUS_ID IS NULL)) 
                when matched then update set
                    target.SUS_ACTION=src.SUS_ACTION, 
                    target.SUS_ACTION_DATE=src.SUS_ACTION_DATE, 
                    target.SUS_ID=src.SUS_ID, 
                    target.SUS_LASTSAVED=src.SUS_LASTSAVED, 
                    target.SUS_PROCESSING_TIME=src.SUS_PROCESSING_TIME, 
                    target.SUS_SERVERID=src.SUS_SERVERID, 
                    target.SUS_SESSIONID=src.SUS_SESSIONID, 
                    target.SUS_SUBTYPE=src.SUS_SUBTYPE, 
                    target.SUS_TARGET=src.SUS_TARGET, 
                    target.SUS_TARGET_PARENT=src.SUS_TARGET_PARENT, 
                    target.SUS_TARGET_TAB=src.SUS_TARGET_TAB, 
                    target.SUS_TENANT=src.SUS_TENANT, 
                    target.SUS_TYPE=src.SUS_TYPE, 
                    target.SUS_UPDATECOUNT=src.SUS_UPDATECOUNT, 
                    target.SUS_USERAGENT=src.SUS_USERAGENT, 
                    target.SUS_USERID=src.SUS_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( SUS_ACTION, SUS_ACTION_DATE, SUS_ID, SUS_LASTSAVED, SUS_PROCESSING_TIME, SUS_SERVERID, SUS_SESSIONID, SUS_SUBTYPE, SUS_TARGET, SUS_TARGET_PARENT, SUS_TARGET_TAB, SUS_TENANT, SUS_TYPE, SUS_UPDATECOUNT, SUS_USERAGENT, SUS_USERID, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.SUS_ACTION, 
                    src.SUS_ACTION_DATE, 
                    src.SUS_ID, 
                    src.SUS_LASTSAVED, 
                    src.SUS_PROCESSING_TIME, 
                    src.SUS_SERVERID, 
                    src.SUS_SESSIONID, 
                    src.SUS_SUBTYPE, 
                    src.SUS_TARGET, 
                    src.SUS_TARGET_PARENT, 
                    src.SUS_TARGET_TAB, 
                    src.SUS_TENANT, 
                    src.SUS_TYPE, 
                    src.SUS_UPDATECOUNT, 
                    src.SUS_USERAGENT, 
                    src.SUS_USERID, 
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
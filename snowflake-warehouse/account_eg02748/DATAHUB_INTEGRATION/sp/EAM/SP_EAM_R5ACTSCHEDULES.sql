create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ACTSCHEDULES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ACTSCHEDULES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ACTSCHEDULES as target using (SELECT * FROM (SELECT 
            strm.ACS_ACTIVITY, 
            strm.ACS_CODE, 
            strm.ACS_COMMENT, 
            strm.ACS_ENDTIME, 
            strm.ACS_EVENT, 
            strm.ACS_FROZEN, 
            strm.ACS_HOURS, 
            strm.ACS_LASTSAVED, 
            strm.ACS_MPPROJ, 
            strm.ACS_MRC, 
            strm.ACS_OBJECT, 
            strm.ACS_OBJECT_ORG, 
            strm.ACS_RESPONSIBLE, 
            strm.ACS_SCHED, 
            strm.ACS_SCHEDULER, 
            strm.ACS_SHIFT, 
            strm.ACS_SHIFTSCHEDULESESSION, 
            strm.ACS_STARTTIME, 
            strm.ACS_TRADE, 
            strm.ACS_UPDATECOUNT, 
            strm.ACS_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTSCHEDULES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACS_CODE=src.ACS_CODE) OR (target.ACS_CODE IS NULL AND src.ACS_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACS_ACTIVITY=src.ACS_ACTIVITY, 
                    target.ACS_CODE=src.ACS_CODE, 
                    target.ACS_COMMENT=src.ACS_COMMENT, 
                    target.ACS_ENDTIME=src.ACS_ENDTIME, 
                    target.ACS_EVENT=src.ACS_EVENT, 
                    target.ACS_FROZEN=src.ACS_FROZEN, 
                    target.ACS_HOURS=src.ACS_HOURS, 
                    target.ACS_LASTSAVED=src.ACS_LASTSAVED, 
                    target.ACS_MPPROJ=src.ACS_MPPROJ, 
                    target.ACS_MRC=src.ACS_MRC, 
                    target.ACS_OBJECT=src.ACS_OBJECT, 
                    target.ACS_OBJECT_ORG=src.ACS_OBJECT_ORG, 
                    target.ACS_RESPONSIBLE=src.ACS_RESPONSIBLE, 
                    target.ACS_SCHED=src.ACS_SCHED, 
                    target.ACS_SCHEDULER=src.ACS_SCHEDULER, 
                    target.ACS_SHIFT=src.ACS_SHIFT, 
                    target.ACS_SHIFTSCHEDULESESSION=src.ACS_SHIFTSCHEDULESESSION, 
                    target.ACS_STARTTIME=src.ACS_STARTTIME, 
                    target.ACS_TRADE=src.ACS_TRADE, 
                    target.ACS_UPDATECOUNT=src.ACS_UPDATECOUNT, 
                    target.ACS_UPDATED=src.ACS_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACS_ACTIVITY, 
                    ACS_CODE, 
                    ACS_COMMENT, 
                    ACS_ENDTIME, 
                    ACS_EVENT, 
                    ACS_FROZEN, 
                    ACS_HOURS, 
                    ACS_LASTSAVED, 
                    ACS_MPPROJ, 
                    ACS_MRC, 
                    ACS_OBJECT, 
                    ACS_OBJECT_ORG, 
                    ACS_RESPONSIBLE, 
                    ACS_SCHED, 
                    ACS_SCHEDULER, 
                    ACS_SHIFT, 
                    ACS_SHIFTSCHEDULESESSION, 
                    ACS_STARTTIME, 
                    ACS_TRADE, 
                    ACS_UPDATECOUNT, 
                    ACS_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACS_ACTIVITY, 
                    src.ACS_CODE, 
                    src.ACS_COMMENT, 
                    src.ACS_ENDTIME, 
                    src.ACS_EVENT, 
                    src.ACS_FROZEN, 
                    src.ACS_HOURS, 
                    src.ACS_LASTSAVED, 
                    src.ACS_MPPROJ, 
                    src.ACS_MRC, 
                    src.ACS_OBJECT, 
                    src.ACS_OBJECT_ORG, 
                    src.ACS_RESPONSIBLE, 
                    src.ACS_SCHED, 
                    src.ACS_SCHEDULER, 
                    src.ACS_SHIFT, 
                    src.ACS_SHIFTSCHEDULESESSION, 
                    src.ACS_STARTTIME, 
                    src.ACS_TRADE, 
                    src.ACS_UPDATECOUNT, 
                    src.ACS_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ACTSCHEDULES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ACS_ACTIVITY, 
            strm.ACS_CODE, 
            strm.ACS_COMMENT, 
            strm.ACS_ENDTIME, 
            strm.ACS_EVENT, 
            strm.ACS_FROZEN, 
            strm.ACS_HOURS, 
            strm.ACS_LASTSAVED, 
            strm.ACS_MPPROJ, 
            strm.ACS_MRC, 
            strm.ACS_OBJECT, 
            strm.ACS_OBJECT_ORG, 
            strm.ACS_RESPONSIBLE, 
            strm.ACS_SCHED, 
            strm.ACS_SCHEDULER, 
            strm.ACS_SHIFT, 
            strm.ACS_SHIFTSCHEDULESESSION, 
            strm.ACS_STARTTIME, 
            strm.ACS_TRADE, 
            strm.ACS_UPDATECOUNT, 
            strm.ACS_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTSCHEDULES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACS_CODE=src.ACS_CODE) OR (target.ACS_CODE IS NULL AND src.ACS_CODE IS NULL)) 
                when matched then update set
                    target.ACS_ACTIVITY=src.ACS_ACTIVITY, 
                    target.ACS_CODE=src.ACS_CODE, 
                    target.ACS_COMMENT=src.ACS_COMMENT, 
                    target.ACS_ENDTIME=src.ACS_ENDTIME, 
                    target.ACS_EVENT=src.ACS_EVENT, 
                    target.ACS_FROZEN=src.ACS_FROZEN, 
                    target.ACS_HOURS=src.ACS_HOURS, 
                    target.ACS_LASTSAVED=src.ACS_LASTSAVED, 
                    target.ACS_MPPROJ=src.ACS_MPPROJ, 
                    target.ACS_MRC=src.ACS_MRC, 
                    target.ACS_OBJECT=src.ACS_OBJECT, 
                    target.ACS_OBJECT_ORG=src.ACS_OBJECT_ORG, 
                    target.ACS_RESPONSIBLE=src.ACS_RESPONSIBLE, 
                    target.ACS_SCHED=src.ACS_SCHED, 
                    target.ACS_SCHEDULER=src.ACS_SCHEDULER, 
                    target.ACS_SHIFT=src.ACS_SHIFT, 
                    target.ACS_SHIFTSCHEDULESESSION=src.ACS_SHIFTSCHEDULESESSION, 
                    target.ACS_STARTTIME=src.ACS_STARTTIME, 
                    target.ACS_TRADE=src.ACS_TRADE, 
                    target.ACS_UPDATECOUNT=src.ACS_UPDATECOUNT, 
                    target.ACS_UPDATED=src.ACS_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACS_ACTIVITY, ACS_CODE, ACS_COMMENT, ACS_ENDTIME, ACS_EVENT, ACS_FROZEN, ACS_HOURS, ACS_LASTSAVED, ACS_MPPROJ, ACS_MRC, ACS_OBJECT, ACS_OBJECT_ORG, ACS_RESPONSIBLE, ACS_SCHED, ACS_SCHEDULER, ACS_SHIFT, ACS_SHIFTSCHEDULESESSION, ACS_STARTTIME, ACS_TRADE, ACS_UPDATECOUNT, ACS_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACS_ACTIVITY, 
                    src.ACS_CODE, 
                    src.ACS_COMMENT, 
                    src.ACS_ENDTIME, 
                    src.ACS_EVENT, 
                    src.ACS_FROZEN, 
                    src.ACS_HOURS, 
                    src.ACS_LASTSAVED, 
                    src.ACS_MPPROJ, 
                    src.ACS_MRC, 
                    src.ACS_OBJECT, 
                    src.ACS_OBJECT_ORG, 
                    src.ACS_RESPONSIBLE, 
                    src.ACS_SCHED, 
                    src.ACS_SCHEDULER, 
                    src.ACS_SHIFT, 
                    src.ACS_SHIFTSCHEDULESESSION, 
                    src.ACS_STARTTIME, 
                    src.ACS_TRADE, 
                    src.ACS_UPDATECOUNT, 
                    src.ACS_UPDATED, 
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
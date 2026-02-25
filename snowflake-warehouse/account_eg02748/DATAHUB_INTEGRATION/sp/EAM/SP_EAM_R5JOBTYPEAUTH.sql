create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5JOBTYPEAUTH()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5JOBTYPEAUTH_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5JOBTYPEAUTH as target using (SELECT * FROM (SELECT 
            strm.JTA_DELETE, 
            strm.JTA_GROUP, 
            strm.JTA_INSERT, 
            strm.JTA_JOBTYPE, 
            strm.JTA_LASTSAVED, 
            strm.JTA_STATUS, 
            strm.JTA_UPDATE, 
            strm.JTA_UPDATECOUNT, 
            strm.JTA_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JTA_GROUP,
            strm.JTA_JOBTYPE,
            strm.JTA_STATUS ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBTYPEAUTH as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JTA_GROUP=src.JTA_GROUP) OR (target.JTA_GROUP IS NULL AND src.JTA_GROUP IS NULL)) AND
            ((target.JTA_JOBTYPE=src.JTA_JOBTYPE) OR (target.JTA_JOBTYPE IS NULL AND src.JTA_JOBTYPE IS NULL)) AND
            ((target.JTA_STATUS=src.JTA_STATUS) OR (target.JTA_STATUS IS NULL AND src.JTA_STATUS IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.JTA_DELETE=src.JTA_DELETE, 
                    target.JTA_GROUP=src.JTA_GROUP, 
                    target.JTA_INSERT=src.JTA_INSERT, 
                    target.JTA_JOBTYPE=src.JTA_JOBTYPE, 
                    target.JTA_LASTSAVED=src.JTA_LASTSAVED, 
                    target.JTA_STATUS=src.JTA_STATUS, 
                    target.JTA_UPDATE=src.JTA_UPDATE, 
                    target.JTA_UPDATECOUNT=src.JTA_UPDATECOUNT, 
                    target.JTA_UPDATED=src.JTA_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    JTA_DELETE, 
                    JTA_GROUP, 
                    JTA_INSERT, 
                    JTA_JOBTYPE, 
                    JTA_LASTSAVED, 
                    JTA_STATUS, 
                    JTA_UPDATE, 
                    JTA_UPDATECOUNT, 
                    JTA_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.JTA_DELETE, 
                    src.JTA_GROUP, 
                    src.JTA_INSERT, 
                    src.JTA_JOBTYPE, 
                    src.JTA_LASTSAVED, 
                    src.JTA_STATUS, 
                    src.JTA_UPDATE, 
                    src.JTA_UPDATECOUNT, 
                    src.JTA_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5JOBTYPEAUTH_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.JTA_DELETE, 
            strm.JTA_GROUP, 
            strm.JTA_INSERT, 
            strm.JTA_JOBTYPE, 
            strm.JTA_LASTSAVED, 
            strm.JTA_STATUS, 
            strm.JTA_UPDATE, 
            strm.JTA_UPDATECOUNT, 
            strm.JTA_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JTA_GROUP,
            strm.JTA_JOBTYPE,
            strm.JTA_STATUS ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBTYPEAUTH as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JTA_GROUP=src.JTA_GROUP) OR (target.JTA_GROUP IS NULL AND src.JTA_GROUP IS NULL)) AND
            ((target.JTA_JOBTYPE=src.JTA_JOBTYPE) OR (target.JTA_JOBTYPE IS NULL AND src.JTA_JOBTYPE IS NULL)) AND
            ((target.JTA_STATUS=src.JTA_STATUS) OR (target.JTA_STATUS IS NULL AND src.JTA_STATUS IS NULL)) 
                when matched then update set
                    target.JTA_DELETE=src.JTA_DELETE, 
                    target.JTA_GROUP=src.JTA_GROUP, 
                    target.JTA_INSERT=src.JTA_INSERT, 
                    target.JTA_JOBTYPE=src.JTA_JOBTYPE, 
                    target.JTA_LASTSAVED=src.JTA_LASTSAVED, 
                    target.JTA_STATUS=src.JTA_STATUS, 
                    target.JTA_UPDATE=src.JTA_UPDATE, 
                    target.JTA_UPDATECOUNT=src.JTA_UPDATECOUNT, 
                    target.JTA_UPDATED=src.JTA_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( JTA_DELETE, JTA_GROUP, JTA_INSERT, JTA_JOBTYPE, JTA_LASTSAVED, JTA_STATUS, JTA_UPDATE, JTA_UPDATECOUNT, JTA_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.JTA_DELETE, 
                    src.JTA_GROUP, 
                    src.JTA_INSERT, 
                    src.JTA_JOBTYPE, 
                    src.JTA_LASTSAVED, 
                    src.JTA_STATUS, 
                    src.JTA_UPDATE, 
                    src.JTA_UPDATECOUNT, 
                    src.JTA_UPDATED, 
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
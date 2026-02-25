create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5AUTH()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5AUTH_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5AUTH as target using (SELECT * FROM (SELECT 
            strm.AUT_CREATED, 
            strm.AUT_ENTITY, 
            strm.AUT_GROUP, 
            strm.AUT_LASTSAVED, 
            strm.AUT_RENTITY, 
            strm.AUT_STATNEW, 
            strm.AUT_STATUS, 
            strm.AUT_TYPE, 
            strm.AUT_UPDATECOUNT, 
            strm.AUT_UPDATED, 
            strm.AUT_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AUT_GROUP,
            strm.AUT_USER,
            strm.AUT_ENTITY,
            strm.AUT_STATUS,
            strm.AUT_STATNEW ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUTH as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AUT_GROUP=src.AUT_GROUP) OR (target.AUT_GROUP IS NULL AND src.AUT_GROUP IS NULL)) AND
            ((target.AUT_USER=src.AUT_USER) OR (target.AUT_USER IS NULL AND src.AUT_USER IS NULL)) AND
            ((target.AUT_ENTITY=src.AUT_ENTITY) OR (target.AUT_ENTITY IS NULL AND src.AUT_ENTITY IS NULL)) AND
            ((target.AUT_STATUS=src.AUT_STATUS) OR (target.AUT_STATUS IS NULL AND src.AUT_STATUS IS NULL)) AND
            ((target.AUT_STATNEW=src.AUT_STATNEW) OR (target.AUT_STATNEW IS NULL AND src.AUT_STATNEW IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.AUT_CREATED=src.AUT_CREATED, 
                    target.AUT_ENTITY=src.AUT_ENTITY, 
                    target.AUT_GROUP=src.AUT_GROUP, 
                    target.AUT_LASTSAVED=src.AUT_LASTSAVED, 
                    target.AUT_RENTITY=src.AUT_RENTITY, 
                    target.AUT_STATNEW=src.AUT_STATNEW, 
                    target.AUT_STATUS=src.AUT_STATUS, 
                    target.AUT_TYPE=src.AUT_TYPE, 
                    target.AUT_UPDATECOUNT=src.AUT_UPDATECOUNT, 
                    target.AUT_UPDATED=src.AUT_UPDATED, 
                    target.AUT_USER=src.AUT_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    AUT_CREATED, 
                    AUT_ENTITY, 
                    AUT_GROUP, 
                    AUT_LASTSAVED, 
                    AUT_RENTITY, 
                    AUT_STATNEW, 
                    AUT_STATUS, 
                    AUT_TYPE, 
                    AUT_UPDATECOUNT, 
                    AUT_UPDATED, 
                    AUT_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.AUT_CREATED, 
                    src.AUT_ENTITY, 
                    src.AUT_GROUP, 
                    src.AUT_LASTSAVED, 
                    src.AUT_RENTITY, 
                    src.AUT_STATNEW, 
                    src.AUT_STATUS, 
                    src.AUT_TYPE, 
                    src.AUT_UPDATECOUNT, 
                    src.AUT_UPDATED, 
                    src.AUT_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5AUTH_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.AUT_CREATED, 
            strm.AUT_ENTITY, 
            strm.AUT_GROUP, 
            strm.AUT_LASTSAVED, 
            strm.AUT_RENTITY, 
            strm.AUT_STATNEW, 
            strm.AUT_STATUS, 
            strm.AUT_TYPE, 
            strm.AUT_UPDATECOUNT, 
            strm.AUT_UPDATED, 
            strm.AUT_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AUT_GROUP,
            strm.AUT_USER,
            strm.AUT_ENTITY,
            strm.AUT_STATUS,
            strm.AUT_STATNEW ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUTH as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AUT_GROUP=src.AUT_GROUP) OR (target.AUT_GROUP IS NULL AND src.AUT_GROUP IS NULL)) AND
            ((target.AUT_USER=src.AUT_USER) OR (target.AUT_USER IS NULL AND src.AUT_USER IS NULL)) AND
            ((target.AUT_ENTITY=src.AUT_ENTITY) OR (target.AUT_ENTITY IS NULL AND src.AUT_ENTITY IS NULL)) AND
            ((target.AUT_STATUS=src.AUT_STATUS) OR (target.AUT_STATUS IS NULL AND src.AUT_STATUS IS NULL)) AND
            ((target.AUT_STATNEW=src.AUT_STATNEW) OR (target.AUT_STATNEW IS NULL AND src.AUT_STATNEW IS NULL)) 
                when matched then update set
                    target.AUT_CREATED=src.AUT_CREATED, 
                    target.AUT_ENTITY=src.AUT_ENTITY, 
                    target.AUT_GROUP=src.AUT_GROUP, 
                    target.AUT_LASTSAVED=src.AUT_LASTSAVED, 
                    target.AUT_RENTITY=src.AUT_RENTITY, 
                    target.AUT_STATNEW=src.AUT_STATNEW, 
                    target.AUT_STATUS=src.AUT_STATUS, 
                    target.AUT_TYPE=src.AUT_TYPE, 
                    target.AUT_UPDATECOUNT=src.AUT_UPDATECOUNT, 
                    target.AUT_UPDATED=src.AUT_UPDATED, 
                    target.AUT_USER=src.AUT_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( AUT_CREATED, AUT_ENTITY, AUT_GROUP, AUT_LASTSAVED, AUT_RENTITY, AUT_STATNEW, AUT_STATUS, AUT_TYPE, AUT_UPDATECOUNT, AUT_UPDATED, AUT_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.AUT_CREATED, 
                    src.AUT_ENTITY, 
                    src.AUT_GROUP, 
                    src.AUT_LASTSAVED, 
                    src.AUT_RENTITY, 
                    src.AUT_STATNEW, 
                    src.AUT_STATUS, 
                    src.AUT_TYPE, 
                    src.AUT_UPDATECOUNT, 
                    src.AUT_UPDATED, 
                    src.AUT_USER, 
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
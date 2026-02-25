create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5SCHEDGROUPS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5SCHEDGROUPS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5SCHEDGROUPS as target using (SELECT * FROM (SELECT 
            strm.SCG_CLASS, 
            strm.SCG_CLASS_ORG, 
            strm.SCG_CODE, 
            strm.SCG_DESC, 
            strm.SCG_LASTSAVED, 
            strm.SCG_NOTUSED, 
            strm.SCG_ORG, 
            strm.SCG_PROFILEPICTURE, 
            strm.SCG_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SCG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SCHEDGROUPS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SCG_CODE=src.SCG_CODE) OR (target.SCG_CODE IS NULL AND src.SCG_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.SCG_CLASS=src.SCG_CLASS, 
                    target.SCG_CLASS_ORG=src.SCG_CLASS_ORG, 
                    target.SCG_CODE=src.SCG_CODE, 
                    target.SCG_DESC=src.SCG_DESC, 
                    target.SCG_LASTSAVED=src.SCG_LASTSAVED, 
                    target.SCG_NOTUSED=src.SCG_NOTUSED, 
                    target.SCG_ORG=src.SCG_ORG, 
                    target.SCG_PROFILEPICTURE=src.SCG_PROFILEPICTURE, 
                    target.SCG_UPDATECOUNT=src.SCG_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    SCG_CLASS, 
                    SCG_CLASS_ORG, 
                    SCG_CODE, 
                    SCG_DESC, 
                    SCG_LASTSAVED, 
                    SCG_NOTUSED, 
                    SCG_ORG, 
                    SCG_PROFILEPICTURE, 
                    SCG_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.SCG_CLASS, 
                    src.SCG_CLASS_ORG, 
                    src.SCG_CODE, 
                    src.SCG_DESC, 
                    src.SCG_LASTSAVED, 
                    src.SCG_NOTUSED, 
                    src.SCG_ORG, 
                    src.SCG_PROFILEPICTURE, 
                    src.SCG_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5SCHEDGROUPS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.SCG_CLASS, 
            strm.SCG_CLASS_ORG, 
            strm.SCG_CODE, 
            strm.SCG_DESC, 
            strm.SCG_LASTSAVED, 
            strm.SCG_NOTUSED, 
            strm.SCG_ORG, 
            strm.SCG_PROFILEPICTURE, 
            strm.SCG_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SCG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SCHEDGROUPS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SCG_CODE=src.SCG_CODE) OR (target.SCG_CODE IS NULL AND src.SCG_CODE IS NULL)) 
                when matched then update set
                    target.SCG_CLASS=src.SCG_CLASS, 
                    target.SCG_CLASS_ORG=src.SCG_CLASS_ORG, 
                    target.SCG_CODE=src.SCG_CODE, 
                    target.SCG_DESC=src.SCG_DESC, 
                    target.SCG_LASTSAVED=src.SCG_LASTSAVED, 
                    target.SCG_NOTUSED=src.SCG_NOTUSED, 
                    target.SCG_ORG=src.SCG_ORG, 
                    target.SCG_PROFILEPICTURE=src.SCG_PROFILEPICTURE, 
                    target.SCG_UPDATECOUNT=src.SCG_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( SCG_CLASS, SCG_CLASS_ORG, SCG_CODE, SCG_DESC, SCG_LASTSAVED, SCG_NOTUSED, SCG_ORG, SCG_PROFILEPICTURE, SCG_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.SCG_CLASS, 
                    src.SCG_CLASS_ORG, 
                    src.SCG_CODE, 
                    src.SCG_DESC, 
                    src.SCG_LASTSAVED, 
                    src.SCG_NOTUSED, 
                    src.SCG_ORG, 
                    src.SCG_PROFILEPICTURE, 
                    src.SCG_UPDATECOUNT, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ACTIONCODES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ACTIONCODES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ACTIONCODES as target using (SELECT * FROM (SELECT 
            strm.ACC_CLASS, 
            strm.ACC_CLASS_ORG, 
            strm.ACC_CODE, 
            strm.ACC_CREATED, 
            strm.ACC_DESC, 
            strm.ACC_ENABLEWORKORDERS, 
            strm.ACC_GEN, 
            strm.ACC_GROUP, 
            strm.ACC_LASTSAVED, 
            strm.ACC_NOTUSED, 
            strm.ACC_PARTFAILURE, 
            strm.ACC_UPDATECOUNT, 
            strm.ACC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTIONCODES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACC_CODE=src.ACC_CODE) OR (target.ACC_CODE IS NULL AND src.ACC_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACC_CLASS=src.ACC_CLASS, 
                    target.ACC_CLASS_ORG=src.ACC_CLASS_ORG, 
                    target.ACC_CODE=src.ACC_CODE, 
                    target.ACC_CREATED=src.ACC_CREATED, 
                    target.ACC_DESC=src.ACC_DESC, 
                    target.ACC_ENABLEWORKORDERS=src.ACC_ENABLEWORKORDERS, 
                    target.ACC_GEN=src.ACC_GEN, 
                    target.ACC_GROUP=src.ACC_GROUP, 
                    target.ACC_LASTSAVED=src.ACC_LASTSAVED, 
                    target.ACC_NOTUSED=src.ACC_NOTUSED, 
                    target.ACC_PARTFAILURE=src.ACC_PARTFAILURE, 
                    target.ACC_UPDATECOUNT=src.ACC_UPDATECOUNT, 
                    target.ACC_UPDATED=src.ACC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACC_CLASS, 
                    ACC_CLASS_ORG, 
                    ACC_CODE, 
                    ACC_CREATED, 
                    ACC_DESC, 
                    ACC_ENABLEWORKORDERS, 
                    ACC_GEN, 
                    ACC_GROUP, 
                    ACC_LASTSAVED, 
                    ACC_NOTUSED, 
                    ACC_PARTFAILURE, 
                    ACC_UPDATECOUNT, 
                    ACC_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACC_CLASS, 
                    src.ACC_CLASS_ORG, 
                    src.ACC_CODE, 
                    src.ACC_CREATED, 
                    src.ACC_DESC, 
                    src.ACC_ENABLEWORKORDERS, 
                    src.ACC_GEN, 
                    src.ACC_GROUP, 
                    src.ACC_LASTSAVED, 
                    src.ACC_NOTUSED, 
                    src.ACC_PARTFAILURE, 
                    src.ACC_UPDATECOUNT, 
                    src.ACC_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ACTIONCODES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ACC_CLASS, 
            strm.ACC_CLASS_ORG, 
            strm.ACC_CODE, 
            strm.ACC_CREATED, 
            strm.ACC_DESC, 
            strm.ACC_ENABLEWORKORDERS, 
            strm.ACC_GEN, 
            strm.ACC_GROUP, 
            strm.ACC_LASTSAVED, 
            strm.ACC_NOTUSED, 
            strm.ACC_PARTFAILURE, 
            strm.ACC_UPDATECOUNT, 
            strm.ACC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTIONCODES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACC_CODE=src.ACC_CODE) OR (target.ACC_CODE IS NULL AND src.ACC_CODE IS NULL)) 
                when matched then update set
                    target.ACC_CLASS=src.ACC_CLASS, 
                    target.ACC_CLASS_ORG=src.ACC_CLASS_ORG, 
                    target.ACC_CODE=src.ACC_CODE, 
                    target.ACC_CREATED=src.ACC_CREATED, 
                    target.ACC_DESC=src.ACC_DESC, 
                    target.ACC_ENABLEWORKORDERS=src.ACC_ENABLEWORKORDERS, 
                    target.ACC_GEN=src.ACC_GEN, 
                    target.ACC_GROUP=src.ACC_GROUP, 
                    target.ACC_LASTSAVED=src.ACC_LASTSAVED, 
                    target.ACC_NOTUSED=src.ACC_NOTUSED, 
                    target.ACC_PARTFAILURE=src.ACC_PARTFAILURE, 
                    target.ACC_UPDATECOUNT=src.ACC_UPDATECOUNT, 
                    target.ACC_UPDATED=src.ACC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACC_CLASS, ACC_CLASS_ORG, ACC_CODE, ACC_CREATED, ACC_DESC, ACC_ENABLEWORKORDERS, ACC_GEN, ACC_GROUP, ACC_LASTSAVED, ACC_NOTUSED, ACC_PARTFAILURE, ACC_UPDATECOUNT, ACC_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACC_CLASS, 
                    src.ACC_CLASS_ORG, 
                    src.ACC_CODE, 
                    src.ACC_CREATED, 
                    src.ACC_DESC, 
                    src.ACC_ENABLEWORKORDERS, 
                    src.ACC_GEN, 
                    src.ACC_GROUP, 
                    src.ACC_LASTSAVED, 
                    src.ACC_NOTUSED, 
                    src.ACC_PARTFAILURE, 
                    src.ACC_UPDATECOUNT, 
                    src.ACC_UPDATED, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REQUIRCODES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REQUIRCODES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REQUIRCODES as target using (SELECT * FROM (SELECT 
            strm.RQM_CLASS, 
            strm.RQM_CLASS_ORG, 
            strm.RQM_CODE, 
            strm.RQM_CREATED, 
            strm.RQM_DESC, 
            strm.RQM_ENABLEWORKORDERS, 
            strm.RQM_GEN, 
            strm.RQM_GROUP, 
            strm.RQM_LASTSAVED, 
            strm.RQM_NOTUSED, 
            strm.RQM_PARTFAILURE, 
            strm.RQM_UPDATECOUNT, 
            strm.RQM_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RQM_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REQUIRCODES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RQM_CODE=src.RQM_CODE) OR (target.RQM_CODE IS NULL AND src.RQM_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RQM_CLASS=src.RQM_CLASS, 
                    target.RQM_CLASS_ORG=src.RQM_CLASS_ORG, 
                    target.RQM_CODE=src.RQM_CODE, 
                    target.RQM_CREATED=src.RQM_CREATED, 
                    target.RQM_DESC=src.RQM_DESC, 
                    target.RQM_ENABLEWORKORDERS=src.RQM_ENABLEWORKORDERS, 
                    target.RQM_GEN=src.RQM_GEN, 
                    target.RQM_GROUP=src.RQM_GROUP, 
                    target.RQM_LASTSAVED=src.RQM_LASTSAVED, 
                    target.RQM_NOTUSED=src.RQM_NOTUSED, 
                    target.RQM_PARTFAILURE=src.RQM_PARTFAILURE, 
                    target.RQM_UPDATECOUNT=src.RQM_UPDATECOUNT, 
                    target.RQM_UPDATED=src.RQM_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RQM_CLASS, 
                    RQM_CLASS_ORG, 
                    RQM_CODE, 
                    RQM_CREATED, 
                    RQM_DESC, 
                    RQM_ENABLEWORKORDERS, 
                    RQM_GEN, 
                    RQM_GROUP, 
                    RQM_LASTSAVED, 
                    RQM_NOTUSED, 
                    RQM_PARTFAILURE, 
                    RQM_UPDATECOUNT, 
                    RQM_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RQM_CLASS, 
                    src.RQM_CLASS_ORG, 
                    src.RQM_CODE, 
                    src.RQM_CREATED, 
                    src.RQM_DESC, 
                    src.RQM_ENABLEWORKORDERS, 
                    src.RQM_GEN, 
                    src.RQM_GROUP, 
                    src.RQM_LASTSAVED, 
                    src.RQM_NOTUSED, 
                    src.RQM_PARTFAILURE, 
                    src.RQM_UPDATECOUNT, 
                    src.RQM_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REQUIRCODES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RQM_CLASS, 
            strm.RQM_CLASS_ORG, 
            strm.RQM_CODE, 
            strm.RQM_CREATED, 
            strm.RQM_DESC, 
            strm.RQM_ENABLEWORKORDERS, 
            strm.RQM_GEN, 
            strm.RQM_GROUP, 
            strm.RQM_LASTSAVED, 
            strm.RQM_NOTUSED, 
            strm.RQM_PARTFAILURE, 
            strm.RQM_UPDATECOUNT, 
            strm.RQM_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RQM_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REQUIRCODES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RQM_CODE=src.RQM_CODE) OR (target.RQM_CODE IS NULL AND src.RQM_CODE IS NULL)) 
                when matched then update set
                    target.RQM_CLASS=src.RQM_CLASS, 
                    target.RQM_CLASS_ORG=src.RQM_CLASS_ORG, 
                    target.RQM_CODE=src.RQM_CODE, 
                    target.RQM_CREATED=src.RQM_CREATED, 
                    target.RQM_DESC=src.RQM_DESC, 
                    target.RQM_ENABLEWORKORDERS=src.RQM_ENABLEWORKORDERS, 
                    target.RQM_GEN=src.RQM_GEN, 
                    target.RQM_GROUP=src.RQM_GROUP, 
                    target.RQM_LASTSAVED=src.RQM_LASTSAVED, 
                    target.RQM_NOTUSED=src.RQM_NOTUSED, 
                    target.RQM_PARTFAILURE=src.RQM_PARTFAILURE, 
                    target.RQM_UPDATECOUNT=src.RQM_UPDATECOUNT, 
                    target.RQM_UPDATED=src.RQM_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RQM_CLASS, RQM_CLASS_ORG, RQM_CODE, RQM_CREATED, RQM_DESC, RQM_ENABLEWORKORDERS, RQM_GEN, RQM_GROUP, RQM_LASTSAVED, RQM_NOTUSED, RQM_PARTFAILURE, RQM_UPDATECOUNT, RQM_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RQM_CLASS, 
                    src.RQM_CLASS_ORG, 
                    src.RQM_CODE, 
                    src.RQM_CREATED, 
                    src.RQM_DESC, 
                    src.RQM_ENABLEWORKORDERS, 
                    src.RQM_GEN, 
                    src.RQM_GROUP, 
                    src.RQM_LASTSAVED, 
                    src.RQM_NOTUSED, 
                    src.RQM_PARTFAILURE, 
                    src.RQM_UPDATECOUNT, 
                    src.RQM_UPDATED, 
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
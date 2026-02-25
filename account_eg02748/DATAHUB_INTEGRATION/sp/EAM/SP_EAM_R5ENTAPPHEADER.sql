create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ENTAPPHEADER()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ENTAPPHEADER_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ENTAPPHEADER as target using (SELECT * FROM (SELECT 
            strm.EAH_APPDATE, 
            strm.EAH_APPLIST, 
            strm.EAH_CODE, 
            strm.EAH_DATE, 
            strm.EAH_ENTITY, 
            strm.EAH_LASTSAVED, 
            strm.EAH_PK, 
            strm.EAH_REASON, 
            strm.EAH_RENTITY, 
            strm.EAH_REVISION, 
            strm.EAH_UPDATECOUNT, 
            strm.EAH_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EAH_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTAPPHEADER as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EAH_PK=src.EAH_PK) OR (target.EAH_PK IS NULL AND src.EAH_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.EAH_APPDATE=src.EAH_APPDATE, 
                    target.EAH_APPLIST=src.EAH_APPLIST, 
                    target.EAH_CODE=src.EAH_CODE, 
                    target.EAH_DATE=src.EAH_DATE, 
                    target.EAH_ENTITY=src.EAH_ENTITY, 
                    target.EAH_LASTSAVED=src.EAH_LASTSAVED, 
                    target.EAH_PK=src.EAH_PK, 
                    target.EAH_REASON=src.EAH_REASON, 
                    target.EAH_RENTITY=src.EAH_RENTITY, 
                    target.EAH_REVISION=src.EAH_REVISION, 
                    target.EAH_UPDATECOUNT=src.EAH_UPDATECOUNT, 
                    target.EAH_USER=src.EAH_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    EAH_APPDATE, 
                    EAH_APPLIST, 
                    EAH_CODE, 
                    EAH_DATE, 
                    EAH_ENTITY, 
                    EAH_LASTSAVED, 
                    EAH_PK, 
                    EAH_REASON, 
                    EAH_RENTITY, 
                    EAH_REVISION, 
                    EAH_UPDATECOUNT, 
                    EAH_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.EAH_APPDATE, 
                    src.EAH_APPLIST, 
                    src.EAH_CODE, 
                    src.EAH_DATE, 
                    src.EAH_ENTITY, 
                    src.EAH_LASTSAVED, 
                    src.EAH_PK, 
                    src.EAH_REASON, 
                    src.EAH_RENTITY, 
                    src.EAH_REVISION, 
                    src.EAH_UPDATECOUNT, 
                    src.EAH_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ENTAPPHEADER_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.EAH_APPDATE, 
            strm.EAH_APPLIST, 
            strm.EAH_CODE, 
            strm.EAH_DATE, 
            strm.EAH_ENTITY, 
            strm.EAH_LASTSAVED, 
            strm.EAH_PK, 
            strm.EAH_REASON, 
            strm.EAH_RENTITY, 
            strm.EAH_REVISION, 
            strm.EAH_UPDATECOUNT, 
            strm.EAH_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EAH_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTAPPHEADER as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EAH_PK=src.EAH_PK) OR (target.EAH_PK IS NULL AND src.EAH_PK IS NULL)) 
                when matched then update set
                    target.EAH_APPDATE=src.EAH_APPDATE, 
                    target.EAH_APPLIST=src.EAH_APPLIST, 
                    target.EAH_CODE=src.EAH_CODE, 
                    target.EAH_DATE=src.EAH_DATE, 
                    target.EAH_ENTITY=src.EAH_ENTITY, 
                    target.EAH_LASTSAVED=src.EAH_LASTSAVED, 
                    target.EAH_PK=src.EAH_PK, 
                    target.EAH_REASON=src.EAH_REASON, 
                    target.EAH_RENTITY=src.EAH_RENTITY, 
                    target.EAH_REVISION=src.EAH_REVISION, 
                    target.EAH_UPDATECOUNT=src.EAH_UPDATECOUNT, 
                    target.EAH_USER=src.EAH_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( EAH_APPDATE, EAH_APPLIST, EAH_CODE, EAH_DATE, EAH_ENTITY, EAH_LASTSAVED, EAH_PK, EAH_REASON, EAH_RENTITY, EAH_REVISION, EAH_UPDATECOUNT, EAH_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.EAH_APPDATE, 
                    src.EAH_APPLIST, 
                    src.EAH_CODE, 
                    src.EAH_DATE, 
                    src.EAH_ENTITY, 
                    src.EAH_LASTSAVED, 
                    src.EAH_PK, 
                    src.EAH_REASON, 
                    src.EAH_RENTITY, 
                    src.EAH_REVISION, 
                    src.EAH_UPDATECOUNT, 
                    src.EAH_USER, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_U5JOBPRRESPTIME()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_U5JOBPRRESPTIME_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_U5JOBPRRESPTIME as target using (SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.JBP_BUSINESSDAYS, 
            strm.JBP_CONTRACTOR, 
            strm.JBP_ORGANIZATION, 
            strm.JBP_WOCOMPLETIONTIME, 
            strm.JBP_WOPRIORITY, 
            strm.JBP_WOPRIORITY_DESC, 
            strm.JBP_WORESOLUTIONTIME, 
            strm.JBP_WORESPONSETIME, 
            strm.JBP_WOTYPE, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JBP_WOPRIORITY,
            strm.JBP_WOTYPE,
            strm.JBP_CONTRACTOR,
            strm.JBP_ORGANIZATION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5JOBPRRESPTIME as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JBP_WOPRIORITY=src.JBP_WOPRIORITY) OR (target.JBP_WOPRIORITY IS NULL AND src.JBP_WOPRIORITY IS NULL)) AND
            ((target.JBP_WOTYPE=src.JBP_WOTYPE) OR (target.JBP_WOTYPE IS NULL AND src.JBP_WOTYPE IS NULL)) AND
            ((target.JBP_CONTRACTOR=src.JBP_CONTRACTOR) OR (target.JBP_CONTRACTOR IS NULL AND src.JBP_CONTRACTOR IS NULL)) AND
            ((target.JBP_ORGANIZATION=src.JBP_ORGANIZATION) OR (target.JBP_ORGANIZATION IS NULL AND src.JBP_ORGANIZATION IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.JBP_BUSINESSDAYS=src.JBP_BUSINESSDAYS, 
                    target.JBP_CONTRACTOR=src.JBP_CONTRACTOR, 
                    target.JBP_ORGANIZATION=src.JBP_ORGANIZATION, 
                    target.JBP_WOCOMPLETIONTIME=src.JBP_WOCOMPLETIONTIME, 
                    target.JBP_WOPRIORITY=src.JBP_WOPRIORITY, 
                    target.JBP_WOPRIORITY_DESC=src.JBP_WOPRIORITY_DESC, 
                    target.JBP_WORESOLUTIONTIME=src.JBP_WORESOLUTIONTIME, 
                    target.JBP_WORESPONSETIME=src.JBP_WORESPONSETIME, 
                    target.JBP_WOTYPE=src.JBP_WOTYPE, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CREATED, 
                    CREATEDBY, 
                    JBP_BUSINESSDAYS, 
                    JBP_CONTRACTOR, 
                    JBP_ORGANIZATION, 
                    JBP_WOCOMPLETIONTIME, 
                    JBP_WOPRIORITY, 
                    JBP_WOPRIORITY_DESC, 
                    JBP_WORESOLUTIONTIME, 
                    JBP_WORESPONSETIME, 
                    JBP_WOTYPE, 
                    LASTSAVED, 
                    UPDATECOUNT, 
                    UPDATED, 
                    UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.JBP_BUSINESSDAYS, 
                    src.JBP_CONTRACTOR, 
                    src.JBP_ORGANIZATION, 
                    src.JBP_WOCOMPLETIONTIME, 
                    src.JBP_WOPRIORITY, 
                    src.JBP_WOPRIORITY_DESC, 
                    src.JBP_WORESOLUTIONTIME, 
                    src.JBP_WORESPONSETIME, 
                    src.JBP_WOTYPE, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_U5JOBPRRESPTIME_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.JBP_BUSINESSDAYS, 
            strm.JBP_CONTRACTOR, 
            strm.JBP_ORGANIZATION, 
            strm.JBP_WOCOMPLETIONTIME, 
            strm.JBP_WOPRIORITY, 
            strm.JBP_WOPRIORITY_DESC, 
            strm.JBP_WORESOLUTIONTIME, 
            strm.JBP_WORESPONSETIME, 
            strm.JBP_WOTYPE, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.JBP_WOPRIORITY,
            strm.JBP_WOTYPE,
            strm.JBP_CONTRACTOR,
            strm.JBP_ORGANIZATION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5JOBPRRESPTIME as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.JBP_WOPRIORITY=src.JBP_WOPRIORITY) OR (target.JBP_WOPRIORITY IS NULL AND src.JBP_WOPRIORITY IS NULL)) AND
            ((target.JBP_WOTYPE=src.JBP_WOTYPE) OR (target.JBP_WOTYPE IS NULL AND src.JBP_WOTYPE IS NULL)) AND
            ((target.JBP_CONTRACTOR=src.JBP_CONTRACTOR) OR (target.JBP_CONTRACTOR IS NULL AND src.JBP_CONTRACTOR IS NULL)) AND
            ((target.JBP_ORGANIZATION=src.JBP_ORGANIZATION) OR (target.JBP_ORGANIZATION IS NULL AND src.JBP_ORGANIZATION IS NULL)) 
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.JBP_BUSINESSDAYS=src.JBP_BUSINESSDAYS, 
                    target.JBP_CONTRACTOR=src.JBP_CONTRACTOR, 
                    target.JBP_ORGANIZATION=src.JBP_ORGANIZATION, 
                    target.JBP_WOCOMPLETIONTIME=src.JBP_WOCOMPLETIONTIME, 
                    target.JBP_WOPRIORITY=src.JBP_WOPRIORITY, 
                    target.JBP_WOPRIORITY_DESC=src.JBP_WOPRIORITY_DESC, 
                    target.JBP_WORESOLUTIONTIME=src.JBP_WORESOLUTIONTIME, 
                    target.JBP_WORESPONSETIME=src.JBP_WORESPONSETIME, 
                    target.JBP_WOTYPE=src.JBP_WOTYPE, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CREATED, CREATEDBY, JBP_BUSINESSDAYS, JBP_CONTRACTOR, JBP_ORGANIZATION, JBP_WOCOMPLETIONTIME, JBP_WOPRIORITY, JBP_WOPRIORITY_DESC, JBP_WORESOLUTIONTIME, JBP_WORESPONSETIME, JBP_WOTYPE, LASTSAVED, UPDATECOUNT, UPDATED, UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.JBP_BUSINESSDAYS, 
                    src.JBP_CONTRACTOR, 
                    src.JBP_ORGANIZATION, 
                    src.JBP_WOCOMPLETIONTIME, 
                    src.JBP_WOPRIORITY, 
                    src.JBP_WOPRIORITY_DESC, 
                    src.JBP_WORESOLUTIONTIME, 
                    src.JBP_WORESPONSETIME, 
                    src.JBP_WOTYPE, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5RELIABILITYRANKINGANSWER()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGANSWER_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5RELIABILITYRANKINGANSWER as target using (SELECT * FROM (SELECT 
            strm.RRW_ADJUSTED, 
            strm.RRW_CODE, 
            strm.RRW_DESC, 
            strm.RRW_FINDING, 
            strm.RRW_GOOD, 
            strm.RRW_LASTSAVED, 
            strm.RRW_LEVELPK, 
            strm.RRW_NO, 
            strm.RRW_NONCONFORMITY, 
            strm.RRW_OK, 
            strm.RRW_PK, 
            strm.RRW_POOR, 
            strm.RRW_REPAIRSNEEDED, 
            strm.RRW_RESOLUTION, 
            strm.RRW_SEVERITY, 
            strm.RRW_UPDATECOUNT, 
            strm.RRW_VALUE, 
            strm.RRW_YES, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRW_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGANSWER as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRW_PK=src.RRW_PK) OR (target.RRW_PK IS NULL AND src.RRW_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RRW_ADJUSTED=src.RRW_ADJUSTED, 
                    target.RRW_CODE=src.RRW_CODE, 
                    target.RRW_DESC=src.RRW_DESC, 
                    target.RRW_FINDING=src.RRW_FINDING, 
                    target.RRW_GOOD=src.RRW_GOOD, 
                    target.RRW_LASTSAVED=src.RRW_LASTSAVED, 
                    target.RRW_LEVELPK=src.RRW_LEVELPK, 
                    target.RRW_NO=src.RRW_NO, 
                    target.RRW_NONCONFORMITY=src.RRW_NONCONFORMITY, 
                    target.RRW_OK=src.RRW_OK, 
                    target.RRW_PK=src.RRW_PK, 
                    target.RRW_POOR=src.RRW_POOR, 
                    target.RRW_REPAIRSNEEDED=src.RRW_REPAIRSNEEDED, 
                    target.RRW_RESOLUTION=src.RRW_RESOLUTION, 
                    target.RRW_SEVERITY=src.RRW_SEVERITY, 
                    target.RRW_UPDATECOUNT=src.RRW_UPDATECOUNT, 
                    target.RRW_VALUE=src.RRW_VALUE, 
                    target.RRW_YES=src.RRW_YES, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RRW_ADJUSTED, 
                    RRW_CODE, 
                    RRW_DESC, 
                    RRW_FINDING, 
                    RRW_GOOD, 
                    RRW_LASTSAVED, 
                    RRW_LEVELPK, 
                    RRW_NO, 
                    RRW_NONCONFORMITY, 
                    RRW_OK, 
                    RRW_PK, 
                    RRW_POOR, 
                    RRW_REPAIRSNEEDED, 
                    RRW_RESOLUTION, 
                    RRW_SEVERITY, 
                    RRW_UPDATECOUNT, 
                    RRW_VALUE, 
                    RRW_YES, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RRW_ADJUSTED, 
                    src.RRW_CODE, 
                    src.RRW_DESC, 
                    src.RRW_FINDING, 
                    src.RRW_GOOD, 
                    src.RRW_LASTSAVED, 
                    src.RRW_LEVELPK, 
                    src.RRW_NO, 
                    src.RRW_NONCONFORMITY, 
                    src.RRW_OK, 
                    src.RRW_PK, 
                    src.RRW_POOR, 
                    src.RRW_REPAIRSNEEDED, 
                    src.RRW_RESOLUTION, 
                    src.RRW_SEVERITY, 
                    src.RRW_UPDATECOUNT, 
                    src.RRW_VALUE, 
                    src.RRW_YES, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5RELIABILITYRANKINGANSWER_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RRW_ADJUSTED, 
            strm.RRW_CODE, 
            strm.RRW_DESC, 
            strm.RRW_FINDING, 
            strm.RRW_GOOD, 
            strm.RRW_LASTSAVED, 
            strm.RRW_LEVELPK, 
            strm.RRW_NO, 
            strm.RRW_NONCONFORMITY, 
            strm.RRW_OK, 
            strm.RRW_PK, 
            strm.RRW_POOR, 
            strm.RRW_REPAIRSNEEDED, 
            strm.RRW_RESOLUTION, 
            strm.RRW_SEVERITY, 
            strm.RRW_UPDATECOUNT, 
            strm.RRW_VALUE, 
            strm.RRW_YES, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRW_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGANSWER as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRW_PK=src.RRW_PK) OR (target.RRW_PK IS NULL AND src.RRW_PK IS NULL)) 
                when matched then update set
                    target.RRW_ADJUSTED=src.RRW_ADJUSTED, 
                    target.RRW_CODE=src.RRW_CODE, 
                    target.RRW_DESC=src.RRW_DESC, 
                    target.RRW_FINDING=src.RRW_FINDING, 
                    target.RRW_GOOD=src.RRW_GOOD, 
                    target.RRW_LASTSAVED=src.RRW_LASTSAVED, 
                    target.RRW_LEVELPK=src.RRW_LEVELPK, 
                    target.RRW_NO=src.RRW_NO, 
                    target.RRW_NONCONFORMITY=src.RRW_NONCONFORMITY, 
                    target.RRW_OK=src.RRW_OK, 
                    target.RRW_PK=src.RRW_PK, 
                    target.RRW_POOR=src.RRW_POOR, 
                    target.RRW_REPAIRSNEEDED=src.RRW_REPAIRSNEEDED, 
                    target.RRW_RESOLUTION=src.RRW_RESOLUTION, 
                    target.RRW_SEVERITY=src.RRW_SEVERITY, 
                    target.RRW_UPDATECOUNT=src.RRW_UPDATECOUNT, 
                    target.RRW_VALUE=src.RRW_VALUE, 
                    target.RRW_YES=src.RRW_YES, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RRW_ADJUSTED, RRW_CODE, RRW_DESC, RRW_FINDING, RRW_GOOD, RRW_LASTSAVED, RRW_LEVELPK, RRW_NO, RRW_NONCONFORMITY, RRW_OK, RRW_PK, RRW_POOR, RRW_REPAIRSNEEDED, RRW_RESOLUTION, RRW_SEVERITY, RRW_UPDATECOUNT, RRW_VALUE, RRW_YES, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RRW_ADJUSTED, 
                    src.RRW_CODE, 
                    src.RRW_DESC, 
                    src.RRW_FINDING, 
                    src.RRW_GOOD, 
                    src.RRW_LASTSAVED, 
                    src.RRW_LEVELPK, 
                    src.RRW_NO, 
                    src.RRW_NONCONFORMITY, 
                    src.RRW_OK, 
                    src.RRW_PK, 
                    src.RRW_POOR, 
                    src.RRW_REPAIRSNEEDED, 
                    src.RRW_RESOLUTION, 
                    src.RRW_SEVERITY, 
                    src.RRW_UPDATECOUNT, 
                    src.RRW_VALUE, 
                    src.RRW_YES, 
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
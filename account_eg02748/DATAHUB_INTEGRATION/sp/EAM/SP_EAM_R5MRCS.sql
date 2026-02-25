create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MRCS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MRCS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MRCS as target using (SELECT * FROM (SELECT 
            strm.MRC_CAPACITY, 
            strm.MRC_CLASS, 
            strm.MRC_CLASS_ORG, 
            strm.MRC_CODE, 
            strm.MRC_CREATED, 
            strm.MRC_DEPOT, 
            strm.MRC_DEPOT_ORG, 
            strm.MRC_DESC, 
            strm.MRC_DFLTSCREENER, 
            strm.MRC_LASTSAVED, 
            strm.MRC_NOTUSED, 
            strm.MRC_ORG, 
            strm.MRC_SCHEDGROUP, 
            strm.MRC_SEGMENTVALUE, 
            strm.MRC_STORE, 
            strm.MRC_UPDATECOUNT, 
            strm.MRC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MRC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MRCS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MRC_CODE=src.MRC_CODE) OR (target.MRC_CODE IS NULL AND src.MRC_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MRC_CAPACITY=src.MRC_CAPACITY, 
                    target.MRC_CLASS=src.MRC_CLASS, 
                    target.MRC_CLASS_ORG=src.MRC_CLASS_ORG, 
                    target.MRC_CODE=src.MRC_CODE, 
                    target.MRC_CREATED=src.MRC_CREATED, 
                    target.MRC_DEPOT=src.MRC_DEPOT, 
                    target.MRC_DEPOT_ORG=src.MRC_DEPOT_ORG, 
                    target.MRC_DESC=src.MRC_DESC, 
                    target.MRC_DFLTSCREENER=src.MRC_DFLTSCREENER, 
                    target.MRC_LASTSAVED=src.MRC_LASTSAVED, 
                    target.MRC_NOTUSED=src.MRC_NOTUSED, 
                    target.MRC_ORG=src.MRC_ORG, 
                    target.MRC_SCHEDGROUP=src.MRC_SCHEDGROUP, 
                    target.MRC_SEGMENTVALUE=src.MRC_SEGMENTVALUE, 
                    target.MRC_STORE=src.MRC_STORE, 
                    target.MRC_UPDATECOUNT=src.MRC_UPDATECOUNT, 
                    target.MRC_UPDATED=src.MRC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MRC_CAPACITY, 
                    MRC_CLASS, 
                    MRC_CLASS_ORG, 
                    MRC_CODE, 
                    MRC_CREATED, 
                    MRC_DEPOT, 
                    MRC_DEPOT_ORG, 
                    MRC_DESC, 
                    MRC_DFLTSCREENER, 
                    MRC_LASTSAVED, 
                    MRC_NOTUSED, 
                    MRC_ORG, 
                    MRC_SCHEDGROUP, 
                    MRC_SEGMENTVALUE, 
                    MRC_STORE, 
                    MRC_UPDATECOUNT, 
                    MRC_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MRC_CAPACITY, 
                    src.MRC_CLASS, 
                    src.MRC_CLASS_ORG, 
                    src.MRC_CODE, 
                    src.MRC_CREATED, 
                    src.MRC_DEPOT, 
                    src.MRC_DEPOT_ORG, 
                    src.MRC_DESC, 
                    src.MRC_DFLTSCREENER, 
                    src.MRC_LASTSAVED, 
                    src.MRC_NOTUSED, 
                    src.MRC_ORG, 
                    src.MRC_SCHEDGROUP, 
                    src.MRC_SEGMENTVALUE, 
                    src.MRC_STORE, 
                    src.MRC_UPDATECOUNT, 
                    src.MRC_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MRCS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MRC_CAPACITY, 
            strm.MRC_CLASS, 
            strm.MRC_CLASS_ORG, 
            strm.MRC_CODE, 
            strm.MRC_CREATED, 
            strm.MRC_DEPOT, 
            strm.MRC_DEPOT_ORG, 
            strm.MRC_DESC, 
            strm.MRC_DFLTSCREENER, 
            strm.MRC_LASTSAVED, 
            strm.MRC_NOTUSED, 
            strm.MRC_ORG, 
            strm.MRC_SCHEDGROUP, 
            strm.MRC_SEGMENTVALUE, 
            strm.MRC_STORE, 
            strm.MRC_UPDATECOUNT, 
            strm.MRC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MRC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MRCS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MRC_CODE=src.MRC_CODE) OR (target.MRC_CODE IS NULL AND src.MRC_CODE IS NULL)) 
                when matched then update set
                    target.MRC_CAPACITY=src.MRC_CAPACITY, 
                    target.MRC_CLASS=src.MRC_CLASS, 
                    target.MRC_CLASS_ORG=src.MRC_CLASS_ORG, 
                    target.MRC_CODE=src.MRC_CODE, 
                    target.MRC_CREATED=src.MRC_CREATED, 
                    target.MRC_DEPOT=src.MRC_DEPOT, 
                    target.MRC_DEPOT_ORG=src.MRC_DEPOT_ORG, 
                    target.MRC_DESC=src.MRC_DESC, 
                    target.MRC_DFLTSCREENER=src.MRC_DFLTSCREENER, 
                    target.MRC_LASTSAVED=src.MRC_LASTSAVED, 
                    target.MRC_NOTUSED=src.MRC_NOTUSED, 
                    target.MRC_ORG=src.MRC_ORG, 
                    target.MRC_SCHEDGROUP=src.MRC_SCHEDGROUP, 
                    target.MRC_SEGMENTVALUE=src.MRC_SEGMENTVALUE, 
                    target.MRC_STORE=src.MRC_STORE, 
                    target.MRC_UPDATECOUNT=src.MRC_UPDATECOUNT, 
                    target.MRC_UPDATED=src.MRC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MRC_CAPACITY, MRC_CLASS, MRC_CLASS_ORG, MRC_CODE, MRC_CREATED, MRC_DEPOT, MRC_DEPOT_ORG, MRC_DESC, MRC_DFLTSCREENER, MRC_LASTSAVED, MRC_NOTUSED, MRC_ORG, MRC_SCHEDGROUP, MRC_SEGMENTVALUE, MRC_STORE, MRC_UPDATECOUNT, MRC_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MRC_CAPACITY, 
                    src.MRC_CLASS, 
                    src.MRC_CLASS_ORG, 
                    src.MRC_CODE, 
                    src.MRC_CREATED, 
                    src.MRC_DEPOT, 
                    src.MRC_DEPOT_ORG, 
                    src.MRC_DESC, 
                    src.MRC_DFLTSCREENER, 
                    src.MRC_LASTSAVED, 
                    src.MRC_NOTUSED, 
                    src.MRC_ORG, 
                    src.MRC_SCHEDGROUP, 
                    src.MRC_SEGMENTVALUE, 
                    src.MRC_STORE, 
                    src.MRC_UPDATECOUNT, 
                    src.MRC_UPDATED, 
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
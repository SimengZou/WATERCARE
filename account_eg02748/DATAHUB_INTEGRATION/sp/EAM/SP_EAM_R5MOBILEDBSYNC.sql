create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MOBILEDBSYNC()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MOBILEDBSYNC_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MOBILEDBSYNC as target using (SELECT * FROM (SELECT 
            strm.MDS_DBLASTUPDATEDDATE, 
            strm.MDS_DOWNLOAD, 
            strm.MDS_FILENAME, 
            strm.MDS_GRIDS_PROCESSED, 
            strm.MDS_LASTSAVED, 
            strm.MDS_ORG, 
            strm.MDS_STATUS, 
            strm.MDS_STATUS_MSG, 
            strm.MDS_SYNCID, 
            strm.MDS_SYNC_REQUEST, 
            strm.MDS_UPDATECOUNT, 
            strm.MDS_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MDS_SYNCID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEDBSYNC as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MDS_SYNCID=src.MDS_SYNCID) OR (target.MDS_SYNCID IS NULL AND src.MDS_SYNCID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MDS_DBLASTUPDATEDDATE=src.MDS_DBLASTUPDATEDDATE, 
                    target.MDS_DOWNLOAD=src.MDS_DOWNLOAD, 
                    target.MDS_FILENAME=src.MDS_FILENAME, 
                    target.MDS_GRIDS_PROCESSED=src.MDS_GRIDS_PROCESSED, 
                    target.MDS_LASTSAVED=src.MDS_LASTSAVED, 
                    target.MDS_ORG=src.MDS_ORG, 
                    target.MDS_STATUS=src.MDS_STATUS, 
                    target.MDS_STATUS_MSG=src.MDS_STATUS_MSG, 
                    target.MDS_SYNCID=src.MDS_SYNCID, 
                    target.MDS_SYNC_REQUEST=src.MDS_SYNC_REQUEST, 
                    target.MDS_UPDATECOUNT=src.MDS_UPDATECOUNT, 
                    target.MDS_USER=src.MDS_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MDS_DBLASTUPDATEDDATE, 
                    MDS_DOWNLOAD, 
                    MDS_FILENAME, 
                    MDS_GRIDS_PROCESSED, 
                    MDS_LASTSAVED, 
                    MDS_ORG, 
                    MDS_STATUS, 
                    MDS_STATUS_MSG, 
                    MDS_SYNCID, 
                    MDS_SYNC_REQUEST, 
                    MDS_UPDATECOUNT, 
                    MDS_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MDS_DBLASTUPDATEDDATE, 
                    src.MDS_DOWNLOAD, 
                    src.MDS_FILENAME, 
                    src.MDS_GRIDS_PROCESSED, 
                    src.MDS_LASTSAVED, 
                    src.MDS_ORG, 
                    src.MDS_STATUS, 
                    src.MDS_STATUS_MSG, 
                    src.MDS_SYNCID, 
                    src.MDS_SYNC_REQUEST, 
                    src.MDS_UPDATECOUNT, 
                    src.MDS_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MOBILEDBSYNC_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MDS_DBLASTUPDATEDDATE, 
            strm.MDS_DOWNLOAD, 
            strm.MDS_FILENAME, 
            strm.MDS_GRIDS_PROCESSED, 
            strm.MDS_LASTSAVED, 
            strm.MDS_ORG, 
            strm.MDS_STATUS, 
            strm.MDS_STATUS_MSG, 
            strm.MDS_SYNCID, 
            strm.MDS_SYNC_REQUEST, 
            strm.MDS_UPDATECOUNT, 
            strm.MDS_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MDS_SYNCID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEDBSYNC as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MDS_SYNCID=src.MDS_SYNCID) OR (target.MDS_SYNCID IS NULL AND src.MDS_SYNCID IS NULL)) 
                when matched then update set
                    target.MDS_DBLASTUPDATEDDATE=src.MDS_DBLASTUPDATEDDATE, 
                    target.MDS_DOWNLOAD=src.MDS_DOWNLOAD, 
                    target.MDS_FILENAME=src.MDS_FILENAME, 
                    target.MDS_GRIDS_PROCESSED=src.MDS_GRIDS_PROCESSED, 
                    target.MDS_LASTSAVED=src.MDS_LASTSAVED, 
                    target.MDS_ORG=src.MDS_ORG, 
                    target.MDS_STATUS=src.MDS_STATUS, 
                    target.MDS_STATUS_MSG=src.MDS_STATUS_MSG, 
                    target.MDS_SYNCID=src.MDS_SYNCID, 
                    target.MDS_SYNC_REQUEST=src.MDS_SYNC_REQUEST, 
                    target.MDS_UPDATECOUNT=src.MDS_UPDATECOUNT, 
                    target.MDS_USER=src.MDS_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MDS_DBLASTUPDATEDDATE, MDS_DOWNLOAD, MDS_FILENAME, MDS_GRIDS_PROCESSED, MDS_LASTSAVED, MDS_ORG, MDS_STATUS, MDS_STATUS_MSG, MDS_SYNCID, MDS_SYNC_REQUEST, MDS_UPDATECOUNT, MDS_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MDS_DBLASTUPDATEDDATE, 
                    src.MDS_DOWNLOAD, 
                    src.MDS_FILENAME, 
                    src.MDS_GRIDS_PROCESSED, 
                    src.MDS_LASTSAVED, 
                    src.MDS_ORG, 
                    src.MDS_STATUS, 
                    src.MDS_STATUS_MSG, 
                    src.MDS_SYNCID, 
                    src.MDS_SYNC_REQUEST, 
                    src.MDS_UPDATECOUNT, 
                    src.MDS_USER, 
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
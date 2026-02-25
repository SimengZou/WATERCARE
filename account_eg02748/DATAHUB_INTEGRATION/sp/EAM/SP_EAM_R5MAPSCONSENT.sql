create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MAPSCONSENT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MAPSCONSENT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MAPSCONSENT as target using (SELECT * FROM (SELECT 
            strm.MCT_APPID, 
            strm.MCT_DEVICEID, 
            strm.MCT_LASTSAVED, 
            strm.MCT_LASTUPDATED, 
            strm.MCT_PRODUCT, 
            strm.MCT_UPDATECOUNT, 
            strm.MCT_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MCT_APPID,
            strm.MCT_USERID,
            strm.MCT_DEVICEID,
            strm.MCT_PRODUCT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAPSCONSENT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MCT_APPID=src.MCT_APPID) OR (target.MCT_APPID IS NULL AND src.MCT_APPID IS NULL)) AND
            ((target.MCT_USERID=src.MCT_USERID) OR (target.MCT_USERID IS NULL AND src.MCT_USERID IS NULL)) AND
            ((target.MCT_DEVICEID=src.MCT_DEVICEID) OR (target.MCT_DEVICEID IS NULL AND src.MCT_DEVICEID IS NULL)) AND
            ((target.MCT_PRODUCT=src.MCT_PRODUCT) OR (target.MCT_PRODUCT IS NULL AND src.MCT_PRODUCT IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MCT_APPID=src.MCT_APPID, 
                    target.MCT_DEVICEID=src.MCT_DEVICEID, 
                    target.MCT_LASTSAVED=src.MCT_LASTSAVED, 
                    target.MCT_LASTUPDATED=src.MCT_LASTUPDATED, 
                    target.MCT_PRODUCT=src.MCT_PRODUCT, 
                    target.MCT_UPDATECOUNT=src.MCT_UPDATECOUNT, 
                    target.MCT_USERID=src.MCT_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MCT_APPID, 
                    MCT_DEVICEID, 
                    MCT_LASTSAVED, 
                    MCT_LASTUPDATED, 
                    MCT_PRODUCT, 
                    MCT_UPDATECOUNT, 
                    MCT_USERID, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MCT_APPID, 
                    src.MCT_DEVICEID, 
                    src.MCT_LASTSAVED, 
                    src.MCT_LASTUPDATED, 
                    src.MCT_PRODUCT, 
                    src.MCT_UPDATECOUNT, 
                    src.MCT_USERID, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MAPSCONSENT_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MCT_APPID, 
            strm.MCT_DEVICEID, 
            strm.MCT_LASTSAVED, 
            strm.MCT_LASTUPDATED, 
            strm.MCT_PRODUCT, 
            strm.MCT_UPDATECOUNT, 
            strm.MCT_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MCT_APPID,
            strm.MCT_USERID,
            strm.MCT_DEVICEID,
            strm.MCT_PRODUCT ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAPSCONSENT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MCT_APPID=src.MCT_APPID) OR (target.MCT_APPID IS NULL AND src.MCT_APPID IS NULL)) AND
            ((target.MCT_USERID=src.MCT_USERID) OR (target.MCT_USERID IS NULL AND src.MCT_USERID IS NULL)) AND
            ((target.MCT_DEVICEID=src.MCT_DEVICEID) OR (target.MCT_DEVICEID IS NULL AND src.MCT_DEVICEID IS NULL)) AND
            ((target.MCT_PRODUCT=src.MCT_PRODUCT) OR (target.MCT_PRODUCT IS NULL AND src.MCT_PRODUCT IS NULL)) 
                when matched then update set
                    target.MCT_APPID=src.MCT_APPID, 
                    target.MCT_DEVICEID=src.MCT_DEVICEID, 
                    target.MCT_LASTSAVED=src.MCT_LASTSAVED, 
                    target.MCT_LASTUPDATED=src.MCT_LASTUPDATED, 
                    target.MCT_PRODUCT=src.MCT_PRODUCT, 
                    target.MCT_UPDATECOUNT=src.MCT_UPDATECOUNT, 
                    target.MCT_USERID=src.MCT_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MCT_APPID, MCT_DEVICEID, MCT_LASTSAVED, MCT_LASTUPDATED, MCT_PRODUCT, MCT_UPDATECOUNT, MCT_USERID, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MCT_APPID, 
                    src.MCT_DEVICEID, 
                    src.MCT_LASTSAVED, 
                    src.MCT_LASTUPDATED, 
                    src.MCT_PRODUCT, 
                    src.MCT_UPDATECOUNT, 
                    src.MCT_USERID, 
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
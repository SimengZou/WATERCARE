create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PRODUCTVERSIONHISTORY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PRODUCTVERSIONHISTORY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PRODUCTVERSIONHISTORY as target using (SELECT * FROM (SELECT 
            strm.PVH_BUILD, 
            strm.PVH_BUILDDATE, 
            strm.PVH_CHANGED, 
            strm.PVH_DESC, 
            strm.PVH_LASTSAVED, 
            strm.PVH_PATCH, 
            strm.PVH_PRODUCTCODE, 
            strm.PVH_UPDATECOUNT, 
            strm.PVH_VERSION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PVH_PRODUCTCODE,
            strm.PVH_CHANGED ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PRODUCTVERSIONHISTORY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PVH_PRODUCTCODE=src.PVH_PRODUCTCODE) OR (target.PVH_PRODUCTCODE IS NULL AND src.PVH_PRODUCTCODE IS NULL)) AND
            ((target.PVH_CHANGED=src.PVH_CHANGED) OR (target.PVH_CHANGED IS NULL AND src.PVH_CHANGED IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PVH_BUILD=src.PVH_BUILD, 
                    target.PVH_BUILDDATE=src.PVH_BUILDDATE, 
                    target.PVH_CHANGED=src.PVH_CHANGED, 
                    target.PVH_DESC=src.PVH_DESC, 
                    target.PVH_LASTSAVED=src.PVH_LASTSAVED, 
                    target.PVH_PATCH=src.PVH_PATCH, 
                    target.PVH_PRODUCTCODE=src.PVH_PRODUCTCODE, 
                    target.PVH_UPDATECOUNT=src.PVH_UPDATECOUNT, 
                    target.PVH_VERSION=src.PVH_VERSION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PVH_BUILD, 
                    PVH_BUILDDATE, 
                    PVH_CHANGED, 
                    PVH_DESC, 
                    PVH_LASTSAVED, 
                    PVH_PATCH, 
                    PVH_PRODUCTCODE, 
                    PVH_UPDATECOUNT, 
                    PVH_VERSION, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PVH_BUILD, 
                    src.PVH_BUILDDATE, 
                    src.PVH_CHANGED, 
                    src.PVH_DESC, 
                    src.PVH_LASTSAVED, 
                    src.PVH_PATCH, 
                    src.PVH_PRODUCTCODE, 
                    src.PVH_UPDATECOUNT, 
                    src.PVH_VERSION, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PRODUCTVERSIONHISTORY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PVH_BUILD, 
            strm.PVH_BUILDDATE, 
            strm.PVH_CHANGED, 
            strm.PVH_DESC, 
            strm.PVH_LASTSAVED, 
            strm.PVH_PATCH, 
            strm.PVH_PRODUCTCODE, 
            strm.PVH_UPDATECOUNT, 
            strm.PVH_VERSION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PVH_PRODUCTCODE,
            strm.PVH_CHANGED ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PRODUCTVERSIONHISTORY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PVH_PRODUCTCODE=src.PVH_PRODUCTCODE) OR (target.PVH_PRODUCTCODE IS NULL AND src.PVH_PRODUCTCODE IS NULL)) AND
            ((target.PVH_CHANGED=src.PVH_CHANGED) OR (target.PVH_CHANGED IS NULL AND src.PVH_CHANGED IS NULL)) 
                when matched then update set
                    target.PVH_BUILD=src.PVH_BUILD, 
                    target.PVH_BUILDDATE=src.PVH_BUILDDATE, 
                    target.PVH_CHANGED=src.PVH_CHANGED, 
                    target.PVH_DESC=src.PVH_DESC, 
                    target.PVH_LASTSAVED=src.PVH_LASTSAVED, 
                    target.PVH_PATCH=src.PVH_PATCH, 
                    target.PVH_PRODUCTCODE=src.PVH_PRODUCTCODE, 
                    target.PVH_UPDATECOUNT=src.PVH_UPDATECOUNT, 
                    target.PVH_VERSION=src.PVH_VERSION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PVH_BUILD, PVH_BUILDDATE, PVH_CHANGED, PVH_DESC, PVH_LASTSAVED, PVH_PATCH, PVH_PRODUCTCODE, PVH_UPDATECOUNT, PVH_VERSION, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PVH_BUILD, 
                    src.PVH_BUILDDATE, 
                    src.PVH_CHANGED, 
                    src.PVH_DESC, 
                    src.PVH_LASTSAVED, 
                    src.PVH_PATCH, 
                    src.PVH_PRODUCTCODE, 
                    src.PVH_UPDATECOUNT, 
                    src.PVH_VERSION, 
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
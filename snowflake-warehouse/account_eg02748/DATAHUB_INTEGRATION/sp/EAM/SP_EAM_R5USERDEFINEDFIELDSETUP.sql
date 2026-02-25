create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5USERDEFINEDFIELDSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5USERDEFINEDFIELDSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5USERDEFINEDFIELDSETUP as target using (SELECT * FROM (SELECT 
            strm.UDF_CURR, 
            strm.UDF_DATETYPE, 
            strm.UDF_ENABLEFORADDONS, 
            strm.UDF_FIELD, 
            strm.UDF_LASTSAVED, 
            strm.UDF_LOOKUPRENTITY, 
            strm.UDF_LOOKUPTYPE, 
            strm.UDF_LOOKUPVALIDATE, 
            strm.UDF_MAX, 
            strm.UDF_MIN, 
            strm.UDF_NUMBERTYPE, 
            strm.UDF_PRINT, 
            strm.UDF_RENTITY, 
            strm.UDF_UOM, 
            strm.UDF_UPDATECOUNT, 
            strm.UDF_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UDF_RENTITY,
            strm.UDF_FIELD ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERDEFINEDFIELDSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UDF_RENTITY=src.UDF_RENTITY) OR (target.UDF_RENTITY IS NULL AND src.UDF_RENTITY IS NULL)) AND
            ((target.UDF_FIELD=src.UDF_FIELD) OR (target.UDF_FIELD IS NULL AND src.UDF_FIELD IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.UDF_CURR=src.UDF_CURR, 
                    target.UDF_DATETYPE=src.UDF_DATETYPE, 
                    target.UDF_ENABLEFORADDONS=src.UDF_ENABLEFORADDONS, 
                    target.UDF_FIELD=src.UDF_FIELD, 
                    target.UDF_LASTSAVED=src.UDF_LASTSAVED, 
                    target.UDF_LOOKUPRENTITY=src.UDF_LOOKUPRENTITY, 
                    target.UDF_LOOKUPTYPE=src.UDF_LOOKUPTYPE, 
                    target.UDF_LOOKUPVALIDATE=src.UDF_LOOKUPVALIDATE, 
                    target.UDF_MAX=src.UDF_MAX, 
                    target.UDF_MIN=src.UDF_MIN, 
                    target.UDF_NUMBERTYPE=src.UDF_NUMBERTYPE, 
                    target.UDF_PRINT=src.UDF_PRINT, 
                    target.UDF_RENTITY=src.UDF_RENTITY, 
                    target.UDF_UOM=src.UDF_UOM, 
                    target.UDF_UPDATECOUNT=src.UDF_UPDATECOUNT, 
                    target.UDF_UPDATED=src.UDF_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    UDF_CURR, 
                    UDF_DATETYPE, 
                    UDF_ENABLEFORADDONS, 
                    UDF_FIELD, 
                    UDF_LASTSAVED, 
                    UDF_LOOKUPRENTITY, 
                    UDF_LOOKUPTYPE, 
                    UDF_LOOKUPVALIDATE, 
                    UDF_MAX, 
                    UDF_MIN, 
                    UDF_NUMBERTYPE, 
                    UDF_PRINT, 
                    UDF_RENTITY, 
                    UDF_UOM, 
                    UDF_UPDATECOUNT, 
                    UDF_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.UDF_CURR, 
                    src.UDF_DATETYPE, 
                    src.UDF_ENABLEFORADDONS, 
                    src.UDF_FIELD, 
                    src.UDF_LASTSAVED, 
                    src.UDF_LOOKUPRENTITY, 
                    src.UDF_LOOKUPTYPE, 
                    src.UDF_LOOKUPVALIDATE, 
                    src.UDF_MAX, 
                    src.UDF_MIN, 
                    src.UDF_NUMBERTYPE, 
                    src.UDF_PRINT, 
                    src.UDF_RENTITY, 
                    src.UDF_UOM, 
                    src.UDF_UPDATECOUNT, 
                    src.UDF_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5USERDEFINEDFIELDSETUP_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.UDF_CURR, 
            strm.UDF_DATETYPE, 
            strm.UDF_ENABLEFORADDONS, 
            strm.UDF_FIELD, 
            strm.UDF_LASTSAVED, 
            strm.UDF_LOOKUPRENTITY, 
            strm.UDF_LOOKUPTYPE, 
            strm.UDF_LOOKUPVALIDATE, 
            strm.UDF_MAX, 
            strm.UDF_MIN, 
            strm.UDF_NUMBERTYPE, 
            strm.UDF_PRINT, 
            strm.UDF_RENTITY, 
            strm.UDF_UOM, 
            strm.UDF_UPDATECOUNT, 
            strm.UDF_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UDF_RENTITY,
            strm.UDF_FIELD ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERDEFINEDFIELDSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UDF_RENTITY=src.UDF_RENTITY) OR (target.UDF_RENTITY IS NULL AND src.UDF_RENTITY IS NULL)) AND
            ((target.UDF_FIELD=src.UDF_FIELD) OR (target.UDF_FIELD IS NULL AND src.UDF_FIELD IS NULL)) 
                when matched then update set
                    target.UDF_CURR=src.UDF_CURR, 
                    target.UDF_DATETYPE=src.UDF_DATETYPE, 
                    target.UDF_ENABLEFORADDONS=src.UDF_ENABLEFORADDONS, 
                    target.UDF_FIELD=src.UDF_FIELD, 
                    target.UDF_LASTSAVED=src.UDF_LASTSAVED, 
                    target.UDF_LOOKUPRENTITY=src.UDF_LOOKUPRENTITY, 
                    target.UDF_LOOKUPTYPE=src.UDF_LOOKUPTYPE, 
                    target.UDF_LOOKUPVALIDATE=src.UDF_LOOKUPVALIDATE, 
                    target.UDF_MAX=src.UDF_MAX, 
                    target.UDF_MIN=src.UDF_MIN, 
                    target.UDF_NUMBERTYPE=src.UDF_NUMBERTYPE, 
                    target.UDF_PRINT=src.UDF_PRINT, 
                    target.UDF_RENTITY=src.UDF_RENTITY, 
                    target.UDF_UOM=src.UDF_UOM, 
                    target.UDF_UPDATECOUNT=src.UDF_UPDATECOUNT, 
                    target.UDF_UPDATED=src.UDF_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( UDF_CURR, UDF_DATETYPE, UDF_ENABLEFORADDONS, UDF_FIELD, UDF_LASTSAVED, UDF_LOOKUPRENTITY, UDF_LOOKUPTYPE, UDF_LOOKUPVALIDATE, UDF_MAX, UDF_MIN, UDF_NUMBERTYPE, UDF_PRINT, UDF_RENTITY, UDF_UOM, UDF_UPDATECOUNT, UDF_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.UDF_CURR, 
                    src.UDF_DATETYPE, 
                    src.UDF_ENABLEFORADDONS, 
                    src.UDF_FIELD, 
                    src.UDF_LASTSAVED, 
                    src.UDF_LOOKUPRENTITY, 
                    src.UDF_LOOKUPTYPE, 
                    src.UDF_LOOKUPVALIDATE, 
                    src.UDF_MAX, 
                    src.UDF_MIN, 
                    src.UDF_NUMBERTYPE, 
                    src.UDF_PRINT, 
                    src.UDF_RENTITY, 
                    src.UDF_UOM, 
                    src.UDF_UPDATECOUNT, 
                    src.UDF_UPDATED, 
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
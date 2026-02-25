create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PERMISSIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PERMISSIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PERMISSIONS as target using (SELECT * FROM (SELECT 
            strm.PRM_CREATED, 
            strm.PRM_DEFQUERY, 
            strm.PRM_DELETE, 
            strm.PRM_FUNCTION, 
            strm.PRM_GROUP, 
            strm.PRM_INPUT, 
            strm.PRM_INSERT, 
            strm.PRM_LASTSAVED, 
            strm.PRM_LOCAL, 
            strm.PRM_OVERRULE, 
            strm.PRM_PRFILE, 
            strm.PRM_PRINTER, 
            strm.PRM_SCREEN, 
            strm.PRM_SECURITYDDSPYID, 
            strm.PRM_SELECT, 
            strm.PRM_UPDATE, 
            strm.PRM_UPDATECOUNT, 
            strm.PRM_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRM_FUNCTION,
            strm.PRM_GROUP ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PERMISSIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRM_FUNCTION=src.PRM_FUNCTION) OR (target.PRM_FUNCTION IS NULL AND src.PRM_FUNCTION IS NULL)) AND
            ((target.PRM_GROUP=src.PRM_GROUP) OR (target.PRM_GROUP IS NULL AND src.PRM_GROUP IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PRM_CREATED=src.PRM_CREATED, 
                    target.PRM_DEFQUERY=src.PRM_DEFQUERY, 
                    target.PRM_DELETE=src.PRM_DELETE, 
                    target.PRM_FUNCTION=src.PRM_FUNCTION, 
                    target.PRM_GROUP=src.PRM_GROUP, 
                    target.PRM_INPUT=src.PRM_INPUT, 
                    target.PRM_INSERT=src.PRM_INSERT, 
                    target.PRM_LASTSAVED=src.PRM_LASTSAVED, 
                    target.PRM_LOCAL=src.PRM_LOCAL, 
                    target.PRM_OVERRULE=src.PRM_OVERRULE, 
                    target.PRM_PRFILE=src.PRM_PRFILE, 
                    target.PRM_PRINTER=src.PRM_PRINTER, 
                    target.PRM_SCREEN=src.PRM_SCREEN, 
                    target.PRM_SECURITYDDSPYID=src.PRM_SECURITYDDSPYID, 
                    target.PRM_SELECT=src.PRM_SELECT, 
                    target.PRM_UPDATE=src.PRM_UPDATE, 
                    target.PRM_UPDATECOUNT=src.PRM_UPDATECOUNT, 
                    target.PRM_UPDATED=src.PRM_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PRM_CREATED, 
                    PRM_DEFQUERY, 
                    PRM_DELETE, 
                    PRM_FUNCTION, 
                    PRM_GROUP, 
                    PRM_INPUT, 
                    PRM_INSERT, 
                    PRM_LASTSAVED, 
                    PRM_LOCAL, 
                    PRM_OVERRULE, 
                    PRM_PRFILE, 
                    PRM_PRINTER, 
                    PRM_SCREEN, 
                    PRM_SECURITYDDSPYID, 
                    PRM_SELECT, 
                    PRM_UPDATE, 
                    PRM_UPDATECOUNT, 
                    PRM_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PRM_CREATED, 
                    src.PRM_DEFQUERY, 
                    src.PRM_DELETE, 
                    src.PRM_FUNCTION, 
                    src.PRM_GROUP, 
                    src.PRM_INPUT, 
                    src.PRM_INSERT, 
                    src.PRM_LASTSAVED, 
                    src.PRM_LOCAL, 
                    src.PRM_OVERRULE, 
                    src.PRM_PRFILE, 
                    src.PRM_PRINTER, 
                    src.PRM_SCREEN, 
                    src.PRM_SECURITYDDSPYID, 
                    src.PRM_SELECT, 
                    src.PRM_UPDATE, 
                    src.PRM_UPDATECOUNT, 
                    src.PRM_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PERMISSIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PRM_CREATED, 
            strm.PRM_DEFQUERY, 
            strm.PRM_DELETE, 
            strm.PRM_FUNCTION, 
            strm.PRM_GROUP, 
            strm.PRM_INPUT, 
            strm.PRM_INSERT, 
            strm.PRM_LASTSAVED, 
            strm.PRM_LOCAL, 
            strm.PRM_OVERRULE, 
            strm.PRM_PRFILE, 
            strm.PRM_PRINTER, 
            strm.PRM_SCREEN, 
            strm.PRM_SECURITYDDSPYID, 
            strm.PRM_SELECT, 
            strm.PRM_UPDATE, 
            strm.PRM_UPDATECOUNT, 
            strm.PRM_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRM_FUNCTION,
            strm.PRM_GROUP ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PERMISSIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRM_FUNCTION=src.PRM_FUNCTION) OR (target.PRM_FUNCTION IS NULL AND src.PRM_FUNCTION IS NULL)) AND
            ((target.PRM_GROUP=src.PRM_GROUP) OR (target.PRM_GROUP IS NULL AND src.PRM_GROUP IS NULL)) 
                when matched then update set
                    target.PRM_CREATED=src.PRM_CREATED, 
                    target.PRM_DEFQUERY=src.PRM_DEFQUERY, 
                    target.PRM_DELETE=src.PRM_DELETE, 
                    target.PRM_FUNCTION=src.PRM_FUNCTION, 
                    target.PRM_GROUP=src.PRM_GROUP, 
                    target.PRM_INPUT=src.PRM_INPUT, 
                    target.PRM_INSERT=src.PRM_INSERT, 
                    target.PRM_LASTSAVED=src.PRM_LASTSAVED, 
                    target.PRM_LOCAL=src.PRM_LOCAL, 
                    target.PRM_OVERRULE=src.PRM_OVERRULE, 
                    target.PRM_PRFILE=src.PRM_PRFILE, 
                    target.PRM_PRINTER=src.PRM_PRINTER, 
                    target.PRM_SCREEN=src.PRM_SCREEN, 
                    target.PRM_SECURITYDDSPYID=src.PRM_SECURITYDDSPYID, 
                    target.PRM_SELECT=src.PRM_SELECT, 
                    target.PRM_UPDATE=src.PRM_UPDATE, 
                    target.PRM_UPDATECOUNT=src.PRM_UPDATECOUNT, 
                    target.PRM_UPDATED=src.PRM_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PRM_CREATED, PRM_DEFQUERY, PRM_DELETE, PRM_FUNCTION, PRM_GROUP, PRM_INPUT, PRM_INSERT, PRM_LASTSAVED, PRM_LOCAL, PRM_OVERRULE, PRM_PRFILE, PRM_PRINTER, PRM_SCREEN, PRM_SECURITYDDSPYID, PRM_SELECT, PRM_UPDATE, PRM_UPDATECOUNT, PRM_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PRM_CREATED, 
                    src.PRM_DEFQUERY, 
                    src.PRM_DELETE, 
                    src.PRM_FUNCTION, 
                    src.PRM_GROUP, 
                    src.PRM_INPUT, 
                    src.PRM_INSERT, 
                    src.PRM_LASTSAVED, 
                    src.PRM_LOCAL, 
                    src.PRM_OVERRULE, 
                    src.PRM_PRFILE, 
                    src.PRM_PRINTER, 
                    src.PRM_SCREEN, 
                    src.PRM_SECURITYDDSPYID, 
                    src.PRM_SELECT, 
                    src.PRM_UPDATE, 
                    src.PRM_UPDATECOUNT, 
                    src.PRM_UPDATED, 
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
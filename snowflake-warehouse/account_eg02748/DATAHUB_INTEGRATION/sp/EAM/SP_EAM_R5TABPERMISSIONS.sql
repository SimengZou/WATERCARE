create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5TABPERMISSIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5TABPERMISSIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5TABPERMISSIONS as target using (SELECT * FROM (SELECT 
            strm.TRP_ALTERSAVE, 
            strm.TRP_DELETE, 
            strm.TRP_FUNCTION, 
            strm.TRP_GROUP, 
            strm.TRP_INSERT, 
            strm.TRP_LASTSAVED, 
            strm.TRP_SECURITYDDSPYID, 
            strm.TRP_SELECT, 
            strm.TRP_SEQUENCE, 
            strm.TRP_SYSREQUIRED, 
            strm.TRP_TAB, 
            strm.TRP_UPDATE, 
            strm.TRP_UPDATECOUNT, 
            strm.TRP_UPDATED, 
            strm.TRP_VISIBLE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TRP_FUNCTION,
            strm.TRP_GROUP,
            strm.TRP_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TABPERMISSIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TRP_FUNCTION=src.TRP_FUNCTION) OR (target.TRP_FUNCTION IS NULL AND src.TRP_FUNCTION IS NULL)) AND
            ((target.TRP_GROUP=src.TRP_GROUP) OR (target.TRP_GROUP IS NULL AND src.TRP_GROUP IS NULL)) AND
            ((target.TRP_TAB=src.TRP_TAB) OR (target.TRP_TAB IS NULL AND src.TRP_TAB IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.TRP_ALTERSAVE=src.TRP_ALTERSAVE, 
                    target.TRP_DELETE=src.TRP_DELETE, 
                    target.TRP_FUNCTION=src.TRP_FUNCTION, 
                    target.TRP_GROUP=src.TRP_GROUP, 
                    target.TRP_INSERT=src.TRP_INSERT, 
                    target.TRP_LASTSAVED=src.TRP_LASTSAVED, 
                    target.TRP_SECURITYDDSPYID=src.TRP_SECURITYDDSPYID, 
                    target.TRP_SELECT=src.TRP_SELECT, 
                    target.TRP_SEQUENCE=src.TRP_SEQUENCE, 
                    target.TRP_SYSREQUIRED=src.TRP_SYSREQUIRED, 
                    target.TRP_TAB=src.TRP_TAB, 
                    target.TRP_UPDATE=src.TRP_UPDATE, 
                    target.TRP_UPDATECOUNT=src.TRP_UPDATECOUNT, 
                    target.TRP_UPDATED=src.TRP_UPDATED, 
                    target.TRP_VISIBLE=src.TRP_VISIBLE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    TRP_ALTERSAVE, 
                    TRP_DELETE, 
                    TRP_FUNCTION, 
                    TRP_GROUP, 
                    TRP_INSERT, 
                    TRP_LASTSAVED, 
                    TRP_SECURITYDDSPYID, 
                    TRP_SELECT, 
                    TRP_SEQUENCE, 
                    TRP_SYSREQUIRED, 
                    TRP_TAB, 
                    TRP_UPDATE, 
                    TRP_UPDATECOUNT, 
                    TRP_UPDATED, 
                    TRP_VISIBLE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.TRP_ALTERSAVE, 
                    src.TRP_DELETE, 
                    src.TRP_FUNCTION, 
                    src.TRP_GROUP, 
                    src.TRP_INSERT, 
                    src.TRP_LASTSAVED, 
                    src.TRP_SECURITYDDSPYID, 
                    src.TRP_SELECT, 
                    src.TRP_SEQUENCE, 
                    src.TRP_SYSREQUIRED, 
                    src.TRP_TAB, 
                    src.TRP_UPDATE, 
                    src.TRP_UPDATECOUNT, 
                    src.TRP_UPDATED, 
                    src.TRP_VISIBLE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5TABPERMISSIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.TRP_ALTERSAVE, 
            strm.TRP_DELETE, 
            strm.TRP_FUNCTION, 
            strm.TRP_GROUP, 
            strm.TRP_INSERT, 
            strm.TRP_LASTSAVED, 
            strm.TRP_SECURITYDDSPYID, 
            strm.TRP_SELECT, 
            strm.TRP_SEQUENCE, 
            strm.TRP_SYSREQUIRED, 
            strm.TRP_TAB, 
            strm.TRP_UPDATE, 
            strm.TRP_UPDATECOUNT, 
            strm.TRP_UPDATED, 
            strm.TRP_VISIBLE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TRP_FUNCTION,
            strm.TRP_GROUP,
            strm.TRP_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TABPERMISSIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TRP_FUNCTION=src.TRP_FUNCTION) OR (target.TRP_FUNCTION IS NULL AND src.TRP_FUNCTION IS NULL)) AND
            ((target.TRP_GROUP=src.TRP_GROUP) OR (target.TRP_GROUP IS NULL AND src.TRP_GROUP IS NULL)) AND
            ((target.TRP_TAB=src.TRP_TAB) OR (target.TRP_TAB IS NULL AND src.TRP_TAB IS NULL)) 
                when matched then update set
                    target.TRP_ALTERSAVE=src.TRP_ALTERSAVE, 
                    target.TRP_DELETE=src.TRP_DELETE, 
                    target.TRP_FUNCTION=src.TRP_FUNCTION, 
                    target.TRP_GROUP=src.TRP_GROUP, 
                    target.TRP_INSERT=src.TRP_INSERT, 
                    target.TRP_LASTSAVED=src.TRP_LASTSAVED, 
                    target.TRP_SECURITYDDSPYID=src.TRP_SECURITYDDSPYID, 
                    target.TRP_SELECT=src.TRP_SELECT, 
                    target.TRP_SEQUENCE=src.TRP_SEQUENCE, 
                    target.TRP_SYSREQUIRED=src.TRP_SYSREQUIRED, 
                    target.TRP_TAB=src.TRP_TAB, 
                    target.TRP_UPDATE=src.TRP_UPDATE, 
                    target.TRP_UPDATECOUNT=src.TRP_UPDATECOUNT, 
                    target.TRP_UPDATED=src.TRP_UPDATED, 
                    target.TRP_VISIBLE=src.TRP_VISIBLE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( TRP_ALTERSAVE, TRP_DELETE, TRP_FUNCTION, TRP_GROUP, TRP_INSERT, TRP_LASTSAVED, TRP_SECURITYDDSPYID, TRP_SELECT, TRP_SEQUENCE, TRP_SYSREQUIRED, TRP_TAB, TRP_UPDATE, TRP_UPDATECOUNT, TRP_UPDATED, TRP_VISIBLE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.TRP_ALTERSAVE, 
                    src.TRP_DELETE, 
                    src.TRP_FUNCTION, 
                    src.TRP_GROUP, 
                    src.TRP_INSERT, 
                    src.TRP_LASTSAVED, 
                    src.TRP_SECURITYDDSPYID, 
                    src.TRP_SELECT, 
                    src.TRP_SEQUENCE, 
                    src.TRP_SYSREQUIRED, 
                    src.TRP_TAB, 
                    src.TRP_UPDATE, 
                    src.TRP_UPDATECOUNT, 
                    src.TRP_UPDATED, 
                    src.TRP_VISIBLE, 
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
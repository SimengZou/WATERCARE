create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CLASSES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CLASSES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CLASSES as target using (SELECT * FROM (SELECT 
            strm.CLS_CODE, 
            strm.CLS_CREATED, 
            strm.CLS_DESC, 
            strm.CLS_ENTITY, 
            strm.CLS_ICON, 
            strm.CLS_ICONPATH, 
            strm.CLS_LASTSAVED, 
            strm.CLS_LEVEL, 
            strm.CLS_MOBILE_NOTEBOOK_DEFINITION, 
            strm.CLS_NOTUSED, 
            strm.CLS_ORG, 
            strm.CLS_PROPERTY_DEFINITION, 
            strm.CLS_RENTITY, 
            strm.CLS_RENTITYCODE, 
            strm.CLS_UPDATECOUNT, 
            strm.CLS_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLS_ENTITY,
            strm.CLS_CODE,
            strm.CLS_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLASSES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLS_ENTITY=src.CLS_ENTITY) OR (target.CLS_ENTITY IS NULL AND src.CLS_ENTITY IS NULL)) AND
            ((target.CLS_CODE=src.CLS_CODE) OR (target.CLS_CODE IS NULL AND src.CLS_CODE IS NULL)) AND
            ((target.CLS_ORG=src.CLS_ORG) OR (target.CLS_ORG IS NULL AND src.CLS_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CLS_CODE=src.CLS_CODE, 
                    target.CLS_CREATED=src.CLS_CREATED, 
                    target.CLS_DESC=src.CLS_DESC, 
                    target.CLS_ENTITY=src.CLS_ENTITY, 
                    target.CLS_ICON=src.CLS_ICON, 
                    target.CLS_ICONPATH=src.CLS_ICONPATH, 
                    target.CLS_LASTSAVED=src.CLS_LASTSAVED, 
                    target.CLS_LEVEL=src.CLS_LEVEL, 
                    target.CLS_MOBILE_NOTEBOOK_DEFINITION=src.CLS_MOBILE_NOTEBOOK_DEFINITION, 
                    target.CLS_NOTUSED=src.CLS_NOTUSED, 
                    target.CLS_ORG=src.CLS_ORG, 
                    target.CLS_PROPERTY_DEFINITION=src.CLS_PROPERTY_DEFINITION, 
                    target.CLS_RENTITY=src.CLS_RENTITY, 
                    target.CLS_RENTITYCODE=src.CLS_RENTITYCODE, 
                    target.CLS_UPDATECOUNT=src.CLS_UPDATECOUNT, 
                    target.CLS_UPDATED=src.CLS_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CLS_CODE, 
                    CLS_CREATED, 
                    CLS_DESC, 
                    CLS_ENTITY, 
                    CLS_ICON, 
                    CLS_ICONPATH, 
                    CLS_LASTSAVED, 
                    CLS_LEVEL, 
                    CLS_MOBILE_NOTEBOOK_DEFINITION, 
                    CLS_NOTUSED, 
                    CLS_ORG, 
                    CLS_PROPERTY_DEFINITION, 
                    CLS_RENTITY, 
                    CLS_RENTITYCODE, 
                    CLS_UPDATECOUNT, 
                    CLS_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CLS_CODE, 
                    src.CLS_CREATED, 
                    src.CLS_DESC, 
                    src.CLS_ENTITY, 
                    src.CLS_ICON, 
                    src.CLS_ICONPATH, 
                    src.CLS_LASTSAVED, 
                    src.CLS_LEVEL, 
                    src.CLS_MOBILE_NOTEBOOK_DEFINITION, 
                    src.CLS_NOTUSED, 
                    src.CLS_ORG, 
                    src.CLS_PROPERTY_DEFINITION, 
                    src.CLS_RENTITY, 
                    src.CLS_RENTITYCODE, 
                    src.CLS_UPDATECOUNT, 
                    src.CLS_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CLASSES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CLS_CODE, 
            strm.CLS_CREATED, 
            strm.CLS_DESC, 
            strm.CLS_ENTITY, 
            strm.CLS_ICON, 
            strm.CLS_ICONPATH, 
            strm.CLS_LASTSAVED, 
            strm.CLS_LEVEL, 
            strm.CLS_MOBILE_NOTEBOOK_DEFINITION, 
            strm.CLS_NOTUSED, 
            strm.CLS_ORG, 
            strm.CLS_PROPERTY_DEFINITION, 
            strm.CLS_RENTITY, 
            strm.CLS_RENTITYCODE, 
            strm.CLS_UPDATECOUNT, 
            strm.CLS_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLS_ENTITY,
            strm.CLS_CODE,
            strm.CLS_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLASSES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLS_ENTITY=src.CLS_ENTITY) OR (target.CLS_ENTITY IS NULL AND src.CLS_ENTITY IS NULL)) AND
            ((target.CLS_CODE=src.CLS_CODE) OR (target.CLS_CODE IS NULL AND src.CLS_CODE IS NULL)) AND
            ((target.CLS_ORG=src.CLS_ORG) OR (target.CLS_ORG IS NULL AND src.CLS_ORG IS NULL)) 
                when matched then update set
                    target.CLS_CODE=src.CLS_CODE, 
                    target.CLS_CREATED=src.CLS_CREATED, 
                    target.CLS_DESC=src.CLS_DESC, 
                    target.CLS_ENTITY=src.CLS_ENTITY, 
                    target.CLS_ICON=src.CLS_ICON, 
                    target.CLS_ICONPATH=src.CLS_ICONPATH, 
                    target.CLS_LASTSAVED=src.CLS_LASTSAVED, 
                    target.CLS_LEVEL=src.CLS_LEVEL, 
                    target.CLS_MOBILE_NOTEBOOK_DEFINITION=src.CLS_MOBILE_NOTEBOOK_DEFINITION, 
                    target.CLS_NOTUSED=src.CLS_NOTUSED, 
                    target.CLS_ORG=src.CLS_ORG, 
                    target.CLS_PROPERTY_DEFINITION=src.CLS_PROPERTY_DEFINITION, 
                    target.CLS_RENTITY=src.CLS_RENTITY, 
                    target.CLS_RENTITYCODE=src.CLS_RENTITYCODE, 
                    target.CLS_UPDATECOUNT=src.CLS_UPDATECOUNT, 
                    target.CLS_UPDATED=src.CLS_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CLS_CODE, CLS_CREATED, CLS_DESC, CLS_ENTITY, CLS_ICON, CLS_ICONPATH, CLS_LASTSAVED, CLS_LEVEL, CLS_MOBILE_NOTEBOOK_DEFINITION, CLS_NOTUSED, CLS_ORG, CLS_PROPERTY_DEFINITION, CLS_RENTITY, CLS_RENTITYCODE, CLS_UPDATECOUNT, CLS_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CLS_CODE, 
                    src.CLS_CREATED, 
                    src.CLS_DESC, 
                    src.CLS_ENTITY, 
                    src.CLS_ICON, 
                    src.CLS_ICONPATH, 
                    src.CLS_LASTSAVED, 
                    src.CLS_LEVEL, 
                    src.CLS_MOBILE_NOTEBOOK_DEFINITION, 
                    src.CLS_NOTUSED, 
                    src.CLS_ORG, 
                    src.CLS_PROPERTY_DEFINITION, 
                    src.CLS_RENTITY, 
                    src.CLS_RENTITYCODE, 
                    src.CLS_UPDATECOUNT, 
                    src.CLS_UPDATED, 
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
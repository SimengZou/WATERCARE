create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MAILCONDITIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MAILCONDITIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MAILCONDITIONS as target using (SELECT * FROM (SELECT 
            strm.MAC_ANDOR, 
            strm.MAC_ATTRIBPK, 
            strm.MAC_COLUMN, 
            strm.MAC_COLUMNGRO, 
            strm.MAC_CRITERIA, 
            strm.MAC_LASTSAVED, 
            strm.MAC_PK, 
            strm.MAC_TABLE, 
            strm.MAC_TEMPLATE, 
            strm.MAC_UPDATECOUNT, 
            strm.MAC_VALUE1, 
            strm.MAC_VALUE2, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAC_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILCONDITIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAC_PK=src.MAC_PK) OR (target.MAC_PK IS NULL AND src.MAC_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MAC_ANDOR=src.MAC_ANDOR, 
                    target.MAC_ATTRIBPK=src.MAC_ATTRIBPK, 
                    target.MAC_COLUMN=src.MAC_COLUMN, 
                    target.MAC_COLUMNGRO=src.MAC_COLUMNGRO, 
                    target.MAC_CRITERIA=src.MAC_CRITERIA, 
                    target.MAC_LASTSAVED=src.MAC_LASTSAVED, 
                    target.MAC_PK=src.MAC_PK, 
                    target.MAC_TABLE=src.MAC_TABLE, 
                    target.MAC_TEMPLATE=src.MAC_TEMPLATE, 
                    target.MAC_UPDATECOUNT=src.MAC_UPDATECOUNT, 
                    target.MAC_VALUE1=src.MAC_VALUE1, 
                    target.MAC_VALUE2=src.MAC_VALUE2, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MAC_ANDOR, 
                    MAC_ATTRIBPK, 
                    MAC_COLUMN, 
                    MAC_COLUMNGRO, 
                    MAC_CRITERIA, 
                    MAC_LASTSAVED, 
                    MAC_PK, 
                    MAC_TABLE, 
                    MAC_TEMPLATE, 
                    MAC_UPDATECOUNT, 
                    MAC_VALUE1, 
                    MAC_VALUE2, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MAC_ANDOR, 
                    src.MAC_ATTRIBPK, 
                    src.MAC_COLUMN, 
                    src.MAC_COLUMNGRO, 
                    src.MAC_CRITERIA, 
                    src.MAC_LASTSAVED, 
                    src.MAC_PK, 
                    src.MAC_TABLE, 
                    src.MAC_TEMPLATE, 
                    src.MAC_UPDATECOUNT, 
                    src.MAC_VALUE1, 
                    src.MAC_VALUE2, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MAILCONDITIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MAC_ANDOR, 
            strm.MAC_ATTRIBPK, 
            strm.MAC_COLUMN, 
            strm.MAC_COLUMNGRO, 
            strm.MAC_CRITERIA, 
            strm.MAC_LASTSAVED, 
            strm.MAC_PK, 
            strm.MAC_TABLE, 
            strm.MAC_TEMPLATE, 
            strm.MAC_UPDATECOUNT, 
            strm.MAC_VALUE1, 
            strm.MAC_VALUE2, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAC_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILCONDITIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAC_PK=src.MAC_PK) OR (target.MAC_PK IS NULL AND src.MAC_PK IS NULL)) 
                when matched then update set
                    target.MAC_ANDOR=src.MAC_ANDOR, 
                    target.MAC_ATTRIBPK=src.MAC_ATTRIBPK, 
                    target.MAC_COLUMN=src.MAC_COLUMN, 
                    target.MAC_COLUMNGRO=src.MAC_COLUMNGRO, 
                    target.MAC_CRITERIA=src.MAC_CRITERIA, 
                    target.MAC_LASTSAVED=src.MAC_LASTSAVED, 
                    target.MAC_PK=src.MAC_PK, 
                    target.MAC_TABLE=src.MAC_TABLE, 
                    target.MAC_TEMPLATE=src.MAC_TEMPLATE, 
                    target.MAC_UPDATECOUNT=src.MAC_UPDATECOUNT, 
                    target.MAC_VALUE1=src.MAC_VALUE1, 
                    target.MAC_VALUE2=src.MAC_VALUE2, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MAC_ANDOR, MAC_ATTRIBPK, MAC_COLUMN, MAC_COLUMNGRO, MAC_CRITERIA, MAC_LASTSAVED, MAC_PK, MAC_TABLE, MAC_TEMPLATE, MAC_UPDATECOUNT, MAC_VALUE1, MAC_VALUE2, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MAC_ANDOR, 
                    src.MAC_ATTRIBPK, 
                    src.MAC_COLUMN, 
                    src.MAC_COLUMNGRO, 
                    src.MAC_CRITERIA, 
                    src.MAC_LASTSAVED, 
                    src.MAC_PK, 
                    src.MAC_TABLE, 
                    src.MAC_TEMPLATE, 
                    src.MAC_UPDATECOUNT, 
                    src.MAC_VALUE1, 
                    src.MAC_VALUE2, 
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
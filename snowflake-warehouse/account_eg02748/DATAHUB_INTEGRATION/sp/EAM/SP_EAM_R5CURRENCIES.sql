create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CURRENCIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CURRENCIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CURRENCIES as target using (SELECT * FROM (SELECT 
            strm.CUR_CLASS, 
            strm.CUR_CLASS_ORG, 
            strm.CUR_CODE, 
            strm.CUR_CREATED, 
            strm.CUR_DESC, 
            strm.CUR_DUAL, 
            strm.CUR_INTERFACE, 
            strm.CUR_LASTSAVED, 
            strm.CUR_NOTUSED, 
            strm.CUR_SOURCECODE, 
            strm.CUR_SOURCESYSTEM, 
            strm.CUR_UPDATECOUNT, 
            strm.CUR_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CUR_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CURRENCIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CUR_CODE=src.CUR_CODE) OR (target.CUR_CODE IS NULL AND src.CUR_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CUR_CLASS=src.CUR_CLASS, 
                    target.CUR_CLASS_ORG=src.CUR_CLASS_ORG, 
                    target.CUR_CODE=src.CUR_CODE, 
                    target.CUR_CREATED=src.CUR_CREATED, 
                    target.CUR_DESC=src.CUR_DESC, 
                    target.CUR_DUAL=src.CUR_DUAL, 
                    target.CUR_INTERFACE=src.CUR_INTERFACE, 
                    target.CUR_LASTSAVED=src.CUR_LASTSAVED, 
                    target.CUR_NOTUSED=src.CUR_NOTUSED, 
                    target.CUR_SOURCECODE=src.CUR_SOURCECODE, 
                    target.CUR_SOURCESYSTEM=src.CUR_SOURCESYSTEM, 
                    target.CUR_UPDATECOUNT=src.CUR_UPDATECOUNT, 
                    target.CUR_UPDATED=src.CUR_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CUR_CLASS, 
                    CUR_CLASS_ORG, 
                    CUR_CODE, 
                    CUR_CREATED, 
                    CUR_DESC, 
                    CUR_DUAL, 
                    CUR_INTERFACE, 
                    CUR_LASTSAVED, 
                    CUR_NOTUSED, 
                    CUR_SOURCECODE, 
                    CUR_SOURCESYSTEM, 
                    CUR_UPDATECOUNT, 
                    CUR_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CUR_CLASS, 
                    src.CUR_CLASS_ORG, 
                    src.CUR_CODE, 
                    src.CUR_CREATED, 
                    src.CUR_DESC, 
                    src.CUR_DUAL, 
                    src.CUR_INTERFACE, 
                    src.CUR_LASTSAVED, 
                    src.CUR_NOTUSED, 
                    src.CUR_SOURCECODE, 
                    src.CUR_SOURCESYSTEM, 
                    src.CUR_UPDATECOUNT, 
                    src.CUR_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CURRENCIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CUR_CLASS, 
            strm.CUR_CLASS_ORG, 
            strm.CUR_CODE, 
            strm.CUR_CREATED, 
            strm.CUR_DESC, 
            strm.CUR_DUAL, 
            strm.CUR_INTERFACE, 
            strm.CUR_LASTSAVED, 
            strm.CUR_NOTUSED, 
            strm.CUR_SOURCECODE, 
            strm.CUR_SOURCESYSTEM, 
            strm.CUR_UPDATECOUNT, 
            strm.CUR_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CUR_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CURRENCIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CUR_CODE=src.CUR_CODE) OR (target.CUR_CODE IS NULL AND src.CUR_CODE IS NULL)) 
                when matched then update set
                    target.CUR_CLASS=src.CUR_CLASS, 
                    target.CUR_CLASS_ORG=src.CUR_CLASS_ORG, 
                    target.CUR_CODE=src.CUR_CODE, 
                    target.CUR_CREATED=src.CUR_CREATED, 
                    target.CUR_DESC=src.CUR_DESC, 
                    target.CUR_DUAL=src.CUR_DUAL, 
                    target.CUR_INTERFACE=src.CUR_INTERFACE, 
                    target.CUR_LASTSAVED=src.CUR_LASTSAVED, 
                    target.CUR_NOTUSED=src.CUR_NOTUSED, 
                    target.CUR_SOURCECODE=src.CUR_SOURCECODE, 
                    target.CUR_SOURCESYSTEM=src.CUR_SOURCESYSTEM, 
                    target.CUR_UPDATECOUNT=src.CUR_UPDATECOUNT, 
                    target.CUR_UPDATED=src.CUR_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CUR_CLASS, CUR_CLASS_ORG, CUR_CODE, CUR_CREATED, CUR_DESC, CUR_DUAL, CUR_INTERFACE, CUR_LASTSAVED, CUR_NOTUSED, CUR_SOURCECODE, CUR_SOURCESYSTEM, CUR_UPDATECOUNT, CUR_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CUR_CLASS, 
                    src.CUR_CLASS_ORG, 
                    src.CUR_CODE, 
                    src.CUR_CREATED, 
                    src.CUR_DESC, 
                    src.CUR_DUAL, 
                    src.CUR_INTERFACE, 
                    src.CUR_LASTSAVED, 
                    src.CUR_NOTUSED, 
                    src.CUR_SOURCECODE, 
                    src.CUR_SOURCESYSTEM, 
                    src.CUR_UPDATECOUNT, 
                    src.CUR_UPDATED, 
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
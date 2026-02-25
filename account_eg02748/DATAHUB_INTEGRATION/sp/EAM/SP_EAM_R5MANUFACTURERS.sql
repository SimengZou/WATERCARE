create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MANUFACTURERS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MANUFACTURERS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MANUFACTURERS as target using (SELECT * FROM (SELECT 
            strm.MFG_CLASS, 
            strm.MFG_CLASS_ORG, 
            strm.MFG_CODE, 
            strm.MFG_DESC, 
            strm.MFG_LASTSAVED, 
            strm.MFG_NOTUSED, 
            strm.MFG_ORG, 
            strm.MFG_SOURCECODE, 
            strm.MFG_SOURCESYSTEM, 
            strm.MFG_SUPPLIER, 
            strm.MFG_SUPPLIER_ORG, 
            strm.MFG_UPDATECOUNT, 
            strm.MFG_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MFG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MANUFACTURERS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MFG_CODE=src.MFG_CODE) OR (target.MFG_CODE IS NULL AND src.MFG_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MFG_CLASS=src.MFG_CLASS, 
                    target.MFG_CLASS_ORG=src.MFG_CLASS_ORG, 
                    target.MFG_CODE=src.MFG_CODE, 
                    target.MFG_DESC=src.MFG_DESC, 
                    target.MFG_LASTSAVED=src.MFG_LASTSAVED, 
                    target.MFG_NOTUSED=src.MFG_NOTUSED, 
                    target.MFG_ORG=src.MFG_ORG, 
                    target.MFG_SOURCECODE=src.MFG_SOURCECODE, 
                    target.MFG_SOURCESYSTEM=src.MFG_SOURCESYSTEM, 
                    target.MFG_SUPPLIER=src.MFG_SUPPLIER, 
                    target.MFG_SUPPLIER_ORG=src.MFG_SUPPLIER_ORG, 
                    target.MFG_UPDATECOUNT=src.MFG_UPDATECOUNT, 
                    target.MFG_UPDATED=src.MFG_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MFG_CLASS, 
                    MFG_CLASS_ORG, 
                    MFG_CODE, 
                    MFG_DESC, 
                    MFG_LASTSAVED, 
                    MFG_NOTUSED, 
                    MFG_ORG, 
                    MFG_SOURCECODE, 
                    MFG_SOURCESYSTEM, 
                    MFG_SUPPLIER, 
                    MFG_SUPPLIER_ORG, 
                    MFG_UPDATECOUNT, 
                    MFG_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MFG_CLASS, 
                    src.MFG_CLASS_ORG, 
                    src.MFG_CODE, 
                    src.MFG_DESC, 
                    src.MFG_LASTSAVED, 
                    src.MFG_NOTUSED, 
                    src.MFG_ORG, 
                    src.MFG_SOURCECODE, 
                    src.MFG_SOURCESYSTEM, 
                    src.MFG_SUPPLIER, 
                    src.MFG_SUPPLIER_ORG, 
                    src.MFG_UPDATECOUNT, 
                    src.MFG_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MANUFACTURERS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MFG_CLASS, 
            strm.MFG_CLASS_ORG, 
            strm.MFG_CODE, 
            strm.MFG_DESC, 
            strm.MFG_LASTSAVED, 
            strm.MFG_NOTUSED, 
            strm.MFG_ORG, 
            strm.MFG_SOURCECODE, 
            strm.MFG_SOURCESYSTEM, 
            strm.MFG_SUPPLIER, 
            strm.MFG_SUPPLIER_ORG, 
            strm.MFG_UPDATECOUNT, 
            strm.MFG_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MFG_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MANUFACTURERS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MFG_CODE=src.MFG_CODE) OR (target.MFG_CODE IS NULL AND src.MFG_CODE IS NULL)) 
                when matched then update set
                    target.MFG_CLASS=src.MFG_CLASS, 
                    target.MFG_CLASS_ORG=src.MFG_CLASS_ORG, 
                    target.MFG_CODE=src.MFG_CODE, 
                    target.MFG_DESC=src.MFG_DESC, 
                    target.MFG_LASTSAVED=src.MFG_LASTSAVED, 
                    target.MFG_NOTUSED=src.MFG_NOTUSED, 
                    target.MFG_ORG=src.MFG_ORG, 
                    target.MFG_SOURCECODE=src.MFG_SOURCECODE, 
                    target.MFG_SOURCESYSTEM=src.MFG_SOURCESYSTEM, 
                    target.MFG_SUPPLIER=src.MFG_SUPPLIER, 
                    target.MFG_SUPPLIER_ORG=src.MFG_SUPPLIER_ORG, 
                    target.MFG_UPDATECOUNT=src.MFG_UPDATECOUNT, 
                    target.MFG_UPDATED=src.MFG_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MFG_CLASS, MFG_CLASS_ORG, MFG_CODE, MFG_DESC, MFG_LASTSAVED, MFG_NOTUSED, MFG_ORG, MFG_SOURCECODE, MFG_SOURCESYSTEM, MFG_SUPPLIER, MFG_SUPPLIER_ORG, MFG_UPDATECOUNT, MFG_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MFG_CLASS, 
                    src.MFG_CLASS_ORG, 
                    src.MFG_CODE, 
                    src.MFG_DESC, 
                    src.MFG_LASTSAVED, 
                    src.MFG_NOTUSED, 
                    src.MFG_ORG, 
                    src.MFG_SOURCECODE, 
                    src.MFG_SOURCESYSTEM, 
                    src.MFG_SUPPLIER, 
                    src.MFG_SUPPLIER_ORG, 
                    src.MFG_UPDATECOUNT, 
                    src.MFG_UPDATED, 
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
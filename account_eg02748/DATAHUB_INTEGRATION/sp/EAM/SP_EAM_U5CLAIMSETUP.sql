create or replace procedure DATAHUB_INTEGRATION.SP_EAM_U5CLAIMSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_U5CLAIMSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_U5CLAIMSETUP as target using (SELECT * FROM (SELECT 
            strm.CLS_CODE, 
            strm.CLS_CONTRACT, 
            strm.CLS_COSTITEM, 
            strm.CLS_PROJACTIVITY, 
            strm.CLS_PROJECT, 
            strm.CLS_STAGINGCOSTCENTER, 
            strm.CLS_SUPPLIER, 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLS_CODE=src.CLS_CODE) OR (target.CLS_CODE IS NULL AND src.CLS_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CLS_CODE=src.CLS_CODE, 
                    target.CLS_CONTRACT=src.CLS_CONTRACT, 
                    target.CLS_COSTITEM=src.CLS_COSTITEM, 
                    target.CLS_PROJACTIVITY=src.CLS_PROJACTIVITY, 
                    target.CLS_PROJECT=src.CLS_PROJECT, 
                    target.CLS_STAGINGCOSTCENTER=src.CLS_STAGINGCOSTCENTER, 
                    target.CLS_SUPPLIER=src.CLS_SUPPLIER, 
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CLS_CODE, 
                    CLS_CONTRACT, 
                    CLS_COSTITEM, 
                    CLS_PROJACTIVITY, 
                    CLS_PROJECT, 
                    CLS_STAGINGCOSTCENTER, 
                    CLS_SUPPLIER, 
                    CREATED, 
                    CREATEDBY, 
                    LASTSAVED, 
                    UPDATECOUNT, 
                    UPDATED, 
                    UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CLS_CODE, 
                    src.CLS_CONTRACT, 
                    src.CLS_COSTITEM, 
                    src.CLS_PROJACTIVITY, 
                    src.CLS_PROJECT, 
                    src.CLS_STAGINGCOSTCENTER, 
                    src.CLS_SUPPLIER, 
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_U5CLAIMSETUP_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CLS_CODE, 
            strm.CLS_CONTRACT, 
            strm.CLS_COSTITEM, 
            strm.CLS_PROJACTIVITY, 
            strm.CLS_PROJECT, 
            strm.CLS_STAGINGCOSTCENTER, 
            strm.CLS_SUPPLIER, 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CLS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CLS_CODE=src.CLS_CODE) OR (target.CLS_CODE IS NULL AND src.CLS_CODE IS NULL)) 
                when matched then update set
                    target.CLS_CODE=src.CLS_CODE, 
                    target.CLS_CONTRACT=src.CLS_CONTRACT, 
                    target.CLS_COSTITEM=src.CLS_COSTITEM, 
                    target.CLS_PROJACTIVITY=src.CLS_PROJACTIVITY, 
                    target.CLS_PROJECT=src.CLS_PROJECT, 
                    target.CLS_STAGINGCOSTCENTER=src.CLS_STAGINGCOSTCENTER, 
                    target.CLS_SUPPLIER=src.CLS_SUPPLIER, 
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CLS_CODE, CLS_CONTRACT, CLS_COSTITEM, CLS_PROJACTIVITY, CLS_PROJECT, CLS_STAGINGCOSTCENTER, CLS_SUPPLIER, CREATED, CREATEDBY, LASTSAVED, UPDATECOUNT, UPDATED, UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CLS_CODE, 
                    src.CLS_CONTRACT, 
                    src.CLS_COSTITEM, 
                    src.CLS_PROJACTIVITY, 
                    src.CLS_PROJECT, 
                    src.CLS_STAGINGCOSTCENTER, 
                    src.CLS_SUPPLIER, 
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
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
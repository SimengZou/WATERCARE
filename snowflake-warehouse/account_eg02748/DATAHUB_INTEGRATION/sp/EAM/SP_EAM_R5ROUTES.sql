create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ROUTES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ROUTES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ROUTES as target using (SELECT * FROM (SELECT 
            strm.ROU_CATEGORY, 
            strm.ROU_CLASS, 
            strm.ROU_CLASS_ORG, 
            strm.ROU_CODE, 
            strm.ROU_DESC, 
            strm.ROU_LASTSAVED, 
            strm.ROU_LINKGISWO, 
            strm.ROU_ORG, 
            strm.ROU_REVISION, 
            strm.ROU_REVRSTATUS, 
            strm.ROU_REVSTATUS, 
            strm.ROU_TEMPLATE, 
            strm.ROU_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROU_CODE,
            strm.ROU_REVISION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROUTES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROU_CODE=src.ROU_CODE) OR (target.ROU_CODE IS NULL AND src.ROU_CODE IS NULL)) AND
            ((target.ROU_REVISION=src.ROU_REVISION) OR (target.ROU_REVISION IS NULL AND src.ROU_REVISION IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ROU_CATEGORY=src.ROU_CATEGORY, 
                    target.ROU_CLASS=src.ROU_CLASS, 
                    target.ROU_CLASS_ORG=src.ROU_CLASS_ORG, 
                    target.ROU_CODE=src.ROU_CODE, 
                    target.ROU_DESC=src.ROU_DESC, 
                    target.ROU_LASTSAVED=src.ROU_LASTSAVED, 
                    target.ROU_LINKGISWO=src.ROU_LINKGISWO, 
                    target.ROU_ORG=src.ROU_ORG, 
                    target.ROU_REVISION=src.ROU_REVISION, 
                    target.ROU_REVRSTATUS=src.ROU_REVRSTATUS, 
                    target.ROU_REVSTATUS=src.ROU_REVSTATUS, 
                    target.ROU_TEMPLATE=src.ROU_TEMPLATE, 
                    target.ROU_UPDATECOUNT=src.ROU_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ROU_CATEGORY, 
                    ROU_CLASS, 
                    ROU_CLASS_ORG, 
                    ROU_CODE, 
                    ROU_DESC, 
                    ROU_LASTSAVED, 
                    ROU_LINKGISWO, 
                    ROU_ORG, 
                    ROU_REVISION, 
                    ROU_REVRSTATUS, 
                    ROU_REVSTATUS, 
                    ROU_TEMPLATE, 
                    ROU_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ROU_CATEGORY, 
                    src.ROU_CLASS, 
                    src.ROU_CLASS_ORG, 
                    src.ROU_CODE, 
                    src.ROU_DESC, 
                    src.ROU_LASTSAVED, 
                    src.ROU_LINKGISWO, 
                    src.ROU_ORG, 
                    src.ROU_REVISION, 
                    src.ROU_REVRSTATUS, 
                    src.ROU_REVSTATUS, 
                    src.ROU_TEMPLATE, 
                    src.ROU_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ROUTES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ROU_CATEGORY, 
            strm.ROU_CLASS, 
            strm.ROU_CLASS_ORG, 
            strm.ROU_CODE, 
            strm.ROU_DESC, 
            strm.ROU_LASTSAVED, 
            strm.ROU_LINKGISWO, 
            strm.ROU_ORG, 
            strm.ROU_REVISION, 
            strm.ROU_REVRSTATUS, 
            strm.ROU_REVSTATUS, 
            strm.ROU_TEMPLATE, 
            strm.ROU_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROU_CODE,
            strm.ROU_REVISION ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROUTES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROU_CODE=src.ROU_CODE) OR (target.ROU_CODE IS NULL AND src.ROU_CODE IS NULL)) AND
            ((target.ROU_REVISION=src.ROU_REVISION) OR (target.ROU_REVISION IS NULL AND src.ROU_REVISION IS NULL)) 
                when matched then update set
                    target.ROU_CATEGORY=src.ROU_CATEGORY, 
                    target.ROU_CLASS=src.ROU_CLASS, 
                    target.ROU_CLASS_ORG=src.ROU_CLASS_ORG, 
                    target.ROU_CODE=src.ROU_CODE, 
                    target.ROU_DESC=src.ROU_DESC, 
                    target.ROU_LASTSAVED=src.ROU_LASTSAVED, 
                    target.ROU_LINKGISWO=src.ROU_LINKGISWO, 
                    target.ROU_ORG=src.ROU_ORG, 
                    target.ROU_REVISION=src.ROU_REVISION, 
                    target.ROU_REVRSTATUS=src.ROU_REVRSTATUS, 
                    target.ROU_REVSTATUS=src.ROU_REVSTATUS, 
                    target.ROU_TEMPLATE=src.ROU_TEMPLATE, 
                    target.ROU_UPDATECOUNT=src.ROU_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ROU_CATEGORY, ROU_CLASS, ROU_CLASS_ORG, ROU_CODE, ROU_DESC, ROU_LASTSAVED, ROU_LINKGISWO, ROU_ORG, ROU_REVISION, ROU_REVRSTATUS, ROU_REVSTATUS, ROU_TEMPLATE, ROU_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ROU_CATEGORY, 
                    src.ROU_CLASS, 
                    src.ROU_CLASS_ORG, 
                    src.ROU_CODE, 
                    src.ROU_DESC, 
                    src.ROU_LASTSAVED, 
                    src.ROU_LINKGISWO, 
                    src.ROU_ORG, 
                    src.ROU_REVISION, 
                    src.ROU_REVRSTATUS, 
                    src.ROU_REVSTATUS, 
                    src.ROU_TEMPLATE, 
                    src.ROU_UPDATECOUNT, 
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
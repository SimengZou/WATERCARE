create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5HELPTEXTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5HELPTEXTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5HELPTEXTS as target using (SELECT * FROM (SELECT 
            strm.HEL_CHANGED, 
            strm.HEL_DRILLDOWN, 
            strm.HEL_FUNCTION, 
            strm.HEL_ITEM, 
            strm.HEL_LANG, 
            strm.HEL_LASTSAVED, 
            strm.HEL_POOL, 
            strm.HEL_TEXT, 
            strm.HEL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.HEL_FUNCTION,
            strm.HEL_ITEM,
            strm.HEL_LANG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HELPTEXTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.HEL_FUNCTION=src.HEL_FUNCTION) OR (target.HEL_FUNCTION IS NULL AND src.HEL_FUNCTION IS NULL)) AND
            ((target.HEL_ITEM=src.HEL_ITEM) OR (target.HEL_ITEM IS NULL AND src.HEL_ITEM IS NULL)) AND
            ((target.HEL_LANG=src.HEL_LANG) OR (target.HEL_LANG IS NULL AND src.HEL_LANG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.HEL_CHANGED=src.HEL_CHANGED, 
                    target.HEL_DRILLDOWN=src.HEL_DRILLDOWN, 
                    target.HEL_FUNCTION=src.HEL_FUNCTION, 
                    target.HEL_ITEM=src.HEL_ITEM, 
                    target.HEL_LANG=src.HEL_LANG, 
                    target.HEL_LASTSAVED=src.HEL_LASTSAVED, 
                    target.HEL_POOL=src.HEL_POOL, 
                    target.HEL_TEXT=src.HEL_TEXT, 
                    target.HEL_UPDATECOUNT=src.HEL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    HEL_CHANGED, 
                    HEL_DRILLDOWN, 
                    HEL_FUNCTION, 
                    HEL_ITEM, 
                    HEL_LANG, 
                    HEL_LASTSAVED, 
                    HEL_POOL, 
                    HEL_TEXT, 
                    HEL_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.HEL_CHANGED, 
                    src.HEL_DRILLDOWN, 
                    src.HEL_FUNCTION, 
                    src.HEL_ITEM, 
                    src.HEL_LANG, 
                    src.HEL_LASTSAVED, 
                    src.HEL_POOL, 
                    src.HEL_TEXT, 
                    src.HEL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5HELPTEXTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.HEL_CHANGED, 
            strm.HEL_DRILLDOWN, 
            strm.HEL_FUNCTION, 
            strm.HEL_ITEM, 
            strm.HEL_LANG, 
            strm.HEL_LASTSAVED, 
            strm.HEL_POOL, 
            strm.HEL_TEXT, 
            strm.HEL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.HEL_FUNCTION,
            strm.HEL_ITEM,
            strm.HEL_LANG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HELPTEXTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.HEL_FUNCTION=src.HEL_FUNCTION) OR (target.HEL_FUNCTION IS NULL AND src.HEL_FUNCTION IS NULL)) AND
            ((target.HEL_ITEM=src.HEL_ITEM) OR (target.HEL_ITEM IS NULL AND src.HEL_ITEM IS NULL)) AND
            ((target.HEL_LANG=src.HEL_LANG) OR (target.HEL_LANG IS NULL AND src.HEL_LANG IS NULL)) 
                when matched then update set
                    target.HEL_CHANGED=src.HEL_CHANGED, 
                    target.HEL_DRILLDOWN=src.HEL_DRILLDOWN, 
                    target.HEL_FUNCTION=src.HEL_FUNCTION, 
                    target.HEL_ITEM=src.HEL_ITEM, 
                    target.HEL_LANG=src.HEL_LANG, 
                    target.HEL_LASTSAVED=src.HEL_LASTSAVED, 
                    target.HEL_POOL=src.HEL_POOL, 
                    target.HEL_TEXT=src.HEL_TEXT, 
                    target.HEL_UPDATECOUNT=src.HEL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( HEL_CHANGED, HEL_DRILLDOWN, HEL_FUNCTION, HEL_ITEM, HEL_LANG, HEL_LASTSAVED, HEL_POOL, HEL_TEXT, HEL_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.HEL_CHANGED, 
                    src.HEL_DRILLDOWN, 
                    src.HEL_FUNCTION, 
                    src.HEL_ITEM, 
                    src.HEL_LANG, 
                    src.HEL_LASTSAVED, 
                    src.HEL_POOL, 
                    src.HEL_TEXT, 
                    src.HEL_UPDATECOUNT, 
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
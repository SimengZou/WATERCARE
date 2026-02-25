create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPORTGROUPBY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPORTGROUPBY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPORTGROUPBY as target using (SELECT * FROM (SELECT 
            strm.RGP_DEFAULT, 
            strm.RGP_FUNCTION, 
            strm.RGP_GROUPFIELDS, 
            strm.RGP_LASTSAVED, 
            strm.RGP_LINE, 
            strm.RGP_UPDATECOUNT, 
            strm.RGP_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RGP_FUNCTION,
            strm.RGP_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTGROUPBY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RGP_FUNCTION=src.RGP_FUNCTION) OR (target.RGP_FUNCTION IS NULL AND src.RGP_FUNCTION IS NULL)) AND
            ((target.RGP_LINE=src.RGP_LINE) OR (target.RGP_LINE IS NULL AND src.RGP_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RGP_DEFAULT=src.RGP_DEFAULT, 
                    target.RGP_FUNCTION=src.RGP_FUNCTION, 
                    target.RGP_GROUPFIELDS=src.RGP_GROUPFIELDS, 
                    target.RGP_LASTSAVED=src.RGP_LASTSAVED, 
                    target.RGP_LINE=src.RGP_LINE, 
                    target.RGP_UPDATECOUNT=src.RGP_UPDATECOUNT, 
                    target.RGP_UPDATED=src.RGP_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RGP_DEFAULT, 
                    RGP_FUNCTION, 
                    RGP_GROUPFIELDS, 
                    RGP_LASTSAVED, 
                    RGP_LINE, 
                    RGP_UPDATECOUNT, 
                    RGP_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RGP_DEFAULT, 
                    src.RGP_FUNCTION, 
                    src.RGP_GROUPFIELDS, 
                    src.RGP_LASTSAVED, 
                    src.RGP_LINE, 
                    src.RGP_UPDATECOUNT, 
                    src.RGP_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPORTGROUPBY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RGP_DEFAULT, 
            strm.RGP_FUNCTION, 
            strm.RGP_GROUPFIELDS, 
            strm.RGP_LASTSAVED, 
            strm.RGP_LINE, 
            strm.RGP_UPDATECOUNT, 
            strm.RGP_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RGP_FUNCTION,
            strm.RGP_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTGROUPBY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RGP_FUNCTION=src.RGP_FUNCTION) OR (target.RGP_FUNCTION IS NULL AND src.RGP_FUNCTION IS NULL)) AND
            ((target.RGP_LINE=src.RGP_LINE) OR (target.RGP_LINE IS NULL AND src.RGP_LINE IS NULL)) 
                when matched then update set
                    target.RGP_DEFAULT=src.RGP_DEFAULT, 
                    target.RGP_FUNCTION=src.RGP_FUNCTION, 
                    target.RGP_GROUPFIELDS=src.RGP_GROUPFIELDS, 
                    target.RGP_LASTSAVED=src.RGP_LASTSAVED, 
                    target.RGP_LINE=src.RGP_LINE, 
                    target.RGP_UPDATECOUNT=src.RGP_UPDATECOUNT, 
                    target.RGP_UPDATED=src.RGP_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RGP_DEFAULT, RGP_FUNCTION, RGP_GROUPFIELDS, RGP_LASTSAVED, RGP_LINE, RGP_UPDATECOUNT, RGP_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RGP_DEFAULT, 
                    src.RGP_FUNCTION, 
                    src.RGP_GROUPFIELDS, 
                    src.RGP_LASTSAVED, 
                    src.RGP_LINE, 
                    src.RGP_UPDATECOUNT, 
                    src.RGP_UPDATED, 
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
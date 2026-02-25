create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ALERTGRIDPARAMS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ALERTGRIDPARAMS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ALERTGRIDPARAMS as target using (SELECT * FROM (SELECT 
            strm.AGP_ALERT, 
            strm.AGP_DVALUE, 
            strm.AGP_LASTSAVED, 
            strm.AGP_NVALUE, 
            strm.AGP_PARAM, 
            strm.AGP_UPDATECOUNT, 
            strm.AGP_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AGP_ALERT,
            strm.AGP_PARAM ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTGRIDPARAMS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AGP_ALERT=src.AGP_ALERT) OR (target.AGP_ALERT IS NULL AND src.AGP_ALERT IS NULL)) AND
            ((target.AGP_PARAM=src.AGP_PARAM) OR (target.AGP_PARAM IS NULL AND src.AGP_PARAM IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.AGP_ALERT=src.AGP_ALERT, 
                    target.AGP_DVALUE=src.AGP_DVALUE, 
                    target.AGP_LASTSAVED=src.AGP_LASTSAVED, 
                    target.AGP_NVALUE=src.AGP_NVALUE, 
                    target.AGP_PARAM=src.AGP_PARAM, 
                    target.AGP_UPDATECOUNT=src.AGP_UPDATECOUNT, 
                    target.AGP_VALUE=src.AGP_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    AGP_ALERT, 
                    AGP_DVALUE, 
                    AGP_LASTSAVED, 
                    AGP_NVALUE, 
                    AGP_PARAM, 
                    AGP_UPDATECOUNT, 
                    AGP_VALUE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.AGP_ALERT, 
                    src.AGP_DVALUE, 
                    src.AGP_LASTSAVED, 
                    src.AGP_NVALUE, 
                    src.AGP_PARAM, 
                    src.AGP_UPDATECOUNT, 
                    src.AGP_VALUE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ALERTGRIDPARAMS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.AGP_ALERT, 
            strm.AGP_DVALUE, 
            strm.AGP_LASTSAVED, 
            strm.AGP_NVALUE, 
            strm.AGP_PARAM, 
            strm.AGP_UPDATECOUNT, 
            strm.AGP_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AGP_ALERT,
            strm.AGP_PARAM ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTGRIDPARAMS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AGP_ALERT=src.AGP_ALERT) OR (target.AGP_ALERT IS NULL AND src.AGP_ALERT IS NULL)) AND
            ((target.AGP_PARAM=src.AGP_PARAM) OR (target.AGP_PARAM IS NULL AND src.AGP_PARAM IS NULL)) 
                when matched then update set
                    target.AGP_ALERT=src.AGP_ALERT, 
                    target.AGP_DVALUE=src.AGP_DVALUE, 
                    target.AGP_LASTSAVED=src.AGP_LASTSAVED, 
                    target.AGP_NVALUE=src.AGP_NVALUE, 
                    target.AGP_PARAM=src.AGP_PARAM, 
                    target.AGP_UPDATECOUNT=src.AGP_UPDATECOUNT, 
                    target.AGP_VALUE=src.AGP_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( AGP_ALERT, AGP_DVALUE, AGP_LASTSAVED, AGP_NVALUE, AGP_PARAM, AGP_UPDATECOUNT, AGP_VALUE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.AGP_ALERT, 
                    src.AGP_DVALUE, 
                    src.AGP_LASTSAVED, 
                    src.AGP_NVALUE, 
                    src.AGP_PARAM, 
                    src.AGP_UPDATECOUNT, 
                    src.AGP_VALUE, 
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
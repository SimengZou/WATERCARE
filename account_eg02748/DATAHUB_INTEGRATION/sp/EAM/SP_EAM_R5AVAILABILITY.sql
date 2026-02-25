create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5AVAILABILITY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5AVAILABILITY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5AVAILABILITY as target using (SELECT * FROM (SELECT 
            strm.AVL_DATE, 
            strm.AVL_HIRHOURS, 
            strm.AVL_LASTSAVED, 
            strm.AVL_MRC, 
            strm.AVL_NETHIRED, 
            strm.AVL_NETHOURS, 
            strm.AVL_OWNHOURS, 
            strm.AVL_SPECIAL, 
            strm.AVL_TRADE, 
            strm.AVL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AVL_DATE,
            strm.AVL_MRC,
            strm.AVL_TRADE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AVAILABILITY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AVL_DATE=src.AVL_DATE) OR (target.AVL_DATE IS NULL AND src.AVL_DATE IS NULL)) AND
            ((target.AVL_MRC=src.AVL_MRC) OR (target.AVL_MRC IS NULL AND src.AVL_MRC IS NULL)) AND
            ((target.AVL_TRADE=src.AVL_TRADE) OR (target.AVL_TRADE IS NULL AND src.AVL_TRADE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.AVL_DATE=src.AVL_DATE, 
                    target.AVL_HIRHOURS=src.AVL_HIRHOURS, 
                    target.AVL_LASTSAVED=src.AVL_LASTSAVED, 
                    target.AVL_MRC=src.AVL_MRC, 
                    target.AVL_NETHIRED=src.AVL_NETHIRED, 
                    target.AVL_NETHOURS=src.AVL_NETHOURS, 
                    target.AVL_OWNHOURS=src.AVL_OWNHOURS, 
                    target.AVL_SPECIAL=src.AVL_SPECIAL, 
                    target.AVL_TRADE=src.AVL_TRADE, 
                    target.AVL_UPDATECOUNT=src.AVL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    AVL_DATE, 
                    AVL_HIRHOURS, 
                    AVL_LASTSAVED, 
                    AVL_MRC, 
                    AVL_NETHIRED, 
                    AVL_NETHOURS, 
                    AVL_OWNHOURS, 
                    AVL_SPECIAL, 
                    AVL_TRADE, 
                    AVL_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.AVL_DATE, 
                    src.AVL_HIRHOURS, 
                    src.AVL_LASTSAVED, 
                    src.AVL_MRC, 
                    src.AVL_NETHIRED, 
                    src.AVL_NETHOURS, 
                    src.AVL_OWNHOURS, 
                    src.AVL_SPECIAL, 
                    src.AVL_TRADE, 
                    src.AVL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5AVAILABILITY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.AVL_DATE, 
            strm.AVL_HIRHOURS, 
            strm.AVL_LASTSAVED, 
            strm.AVL_MRC, 
            strm.AVL_NETHIRED, 
            strm.AVL_NETHOURS, 
            strm.AVL_OWNHOURS, 
            strm.AVL_SPECIAL, 
            strm.AVL_TRADE, 
            strm.AVL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.AVL_DATE,
            strm.AVL_MRC,
            strm.AVL_TRADE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AVAILABILITY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.AVL_DATE=src.AVL_DATE) OR (target.AVL_DATE IS NULL AND src.AVL_DATE IS NULL)) AND
            ((target.AVL_MRC=src.AVL_MRC) OR (target.AVL_MRC IS NULL AND src.AVL_MRC IS NULL)) AND
            ((target.AVL_TRADE=src.AVL_TRADE) OR (target.AVL_TRADE IS NULL AND src.AVL_TRADE IS NULL)) 
                when matched then update set
                    target.AVL_DATE=src.AVL_DATE, 
                    target.AVL_HIRHOURS=src.AVL_HIRHOURS, 
                    target.AVL_LASTSAVED=src.AVL_LASTSAVED, 
                    target.AVL_MRC=src.AVL_MRC, 
                    target.AVL_NETHIRED=src.AVL_NETHIRED, 
                    target.AVL_NETHOURS=src.AVL_NETHOURS, 
                    target.AVL_OWNHOURS=src.AVL_OWNHOURS, 
                    target.AVL_SPECIAL=src.AVL_SPECIAL, 
                    target.AVL_TRADE=src.AVL_TRADE, 
                    target.AVL_UPDATECOUNT=src.AVL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( AVL_DATE, AVL_HIRHOURS, AVL_LASTSAVED, AVL_MRC, AVL_NETHIRED, AVL_NETHOURS, AVL_OWNHOURS, AVL_SPECIAL, AVL_TRADE, AVL_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.AVL_DATE, 
                    src.AVL_HIRHOURS, 
                    src.AVL_LASTSAVED, 
                    src.AVL_MRC, 
                    src.AVL_NETHIRED, 
                    src.AVL_NETHOURS, 
                    src.AVL_OWNHOURS, 
                    src.AVL_SPECIAL, 
                    src.AVL_TRADE, 
                    src.AVL_UPDATECOUNT, 
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
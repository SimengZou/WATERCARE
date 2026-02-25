create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5SHFPERS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5SHFPERS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5SHFPERS as target using (SELECT * FROM (SELECT 
            strm.SHP_END, 
            strm.SHP_LASTSAVED, 
            strm.SHP_PERSON, 
            strm.SHP_SHIFT, 
            strm.SHP_START, 
            strm.SHP_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SHP_SHIFT,
            strm.SHP_PERSON,
            strm.SHP_START,
            strm.SHP_END ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SHFPERS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SHP_SHIFT=src.SHP_SHIFT) OR (target.SHP_SHIFT IS NULL AND src.SHP_SHIFT IS NULL)) AND
            ((target.SHP_PERSON=src.SHP_PERSON) OR (target.SHP_PERSON IS NULL AND src.SHP_PERSON IS NULL)) AND
            ((target.SHP_START=src.SHP_START) OR (target.SHP_START IS NULL AND src.SHP_START IS NULL)) AND
            ((target.SHP_END=src.SHP_END) OR (target.SHP_END IS NULL AND src.SHP_END IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.SHP_END=src.SHP_END, 
                    target.SHP_LASTSAVED=src.SHP_LASTSAVED, 
                    target.SHP_PERSON=src.SHP_PERSON, 
                    target.SHP_SHIFT=src.SHP_SHIFT, 
                    target.SHP_START=src.SHP_START, 
                    target.SHP_UPDATECOUNT=src.SHP_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    SHP_END, 
                    SHP_LASTSAVED, 
                    SHP_PERSON, 
                    SHP_SHIFT, 
                    SHP_START, 
                    SHP_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.SHP_END, 
                    src.SHP_LASTSAVED, 
                    src.SHP_PERSON, 
                    src.SHP_SHIFT, 
                    src.SHP_START, 
                    src.SHP_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5SHFPERS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.SHP_END, 
            strm.SHP_LASTSAVED, 
            strm.SHP_PERSON, 
            strm.SHP_SHIFT, 
            strm.SHP_START, 
            strm.SHP_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SHP_SHIFT,
            strm.SHP_PERSON,
            strm.SHP_START,
            strm.SHP_END ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SHFPERS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SHP_SHIFT=src.SHP_SHIFT) OR (target.SHP_SHIFT IS NULL AND src.SHP_SHIFT IS NULL)) AND
            ((target.SHP_PERSON=src.SHP_PERSON) OR (target.SHP_PERSON IS NULL AND src.SHP_PERSON IS NULL)) AND
            ((target.SHP_START=src.SHP_START) OR (target.SHP_START IS NULL AND src.SHP_START IS NULL)) AND
            ((target.SHP_END=src.SHP_END) OR (target.SHP_END IS NULL AND src.SHP_END IS NULL)) 
                when matched then update set
                    target.SHP_END=src.SHP_END, 
                    target.SHP_LASTSAVED=src.SHP_LASTSAVED, 
                    target.SHP_PERSON=src.SHP_PERSON, 
                    target.SHP_SHIFT=src.SHP_SHIFT, 
                    target.SHP_START=src.SHP_START, 
                    target.SHP_UPDATECOUNT=src.SHP_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( SHP_END, SHP_LASTSAVED, SHP_PERSON, SHP_SHIFT, SHP_START, SHP_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.SHP_END, 
                    src.SHP_LASTSAVED, 
                    src.SHP_PERSON, 
                    src.SHP_SHIFT, 
                    src.SHP_START, 
                    src.SHP_UPDATECOUNT, 
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
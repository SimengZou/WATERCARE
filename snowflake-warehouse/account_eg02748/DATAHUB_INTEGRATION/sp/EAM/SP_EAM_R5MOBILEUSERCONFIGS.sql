create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MOBILEUSERCONFIGS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MOBILEUSERCONFIGS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MOBILEUSERCONFIGS as target using (SELECT * FROM (SELECT 
            strm.MUC_CODE, 
            strm.MUC_COMPTYPE, 
            strm.MUC_CREATED, 
            strm.MUC_DESK, 
            strm.MUC_GROUP, 
            strm.MUC_LASTSAVED, 
            strm.MUC_RCODE, 
            strm.MUC_UPDATECOUNT, 
            strm.MUC_UPDATED, 
            strm.MUC_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MUC_USER,
            strm.MUC_GROUP,
            strm.MUC_CODE,
            strm.MUC_DESK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEUSERCONFIGS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MUC_USER=src.MUC_USER) OR (target.MUC_USER IS NULL AND src.MUC_USER IS NULL)) AND
            ((target.MUC_GROUP=src.MUC_GROUP) OR (target.MUC_GROUP IS NULL AND src.MUC_GROUP IS NULL)) AND
            ((target.MUC_CODE=src.MUC_CODE) OR (target.MUC_CODE IS NULL AND src.MUC_CODE IS NULL)) AND
            ((target.MUC_DESK=src.MUC_DESK) OR (target.MUC_DESK IS NULL AND src.MUC_DESK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MUC_CODE=src.MUC_CODE, 
                    target.MUC_COMPTYPE=src.MUC_COMPTYPE, 
                    target.MUC_CREATED=src.MUC_CREATED, 
                    target.MUC_DESK=src.MUC_DESK, 
                    target.MUC_GROUP=src.MUC_GROUP, 
                    target.MUC_LASTSAVED=src.MUC_LASTSAVED, 
                    target.MUC_RCODE=src.MUC_RCODE, 
                    target.MUC_UPDATECOUNT=src.MUC_UPDATECOUNT, 
                    target.MUC_UPDATED=src.MUC_UPDATED, 
                    target.MUC_USER=src.MUC_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MUC_CODE, 
                    MUC_COMPTYPE, 
                    MUC_CREATED, 
                    MUC_DESK, 
                    MUC_GROUP, 
                    MUC_LASTSAVED, 
                    MUC_RCODE, 
                    MUC_UPDATECOUNT, 
                    MUC_UPDATED, 
                    MUC_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MUC_CODE, 
                    src.MUC_COMPTYPE, 
                    src.MUC_CREATED, 
                    src.MUC_DESK, 
                    src.MUC_GROUP, 
                    src.MUC_LASTSAVED, 
                    src.MUC_RCODE, 
                    src.MUC_UPDATECOUNT, 
                    src.MUC_UPDATED, 
                    src.MUC_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MOBILEUSERCONFIGS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MUC_CODE, 
            strm.MUC_COMPTYPE, 
            strm.MUC_CREATED, 
            strm.MUC_DESK, 
            strm.MUC_GROUP, 
            strm.MUC_LASTSAVED, 
            strm.MUC_RCODE, 
            strm.MUC_UPDATECOUNT, 
            strm.MUC_UPDATED, 
            strm.MUC_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MUC_USER,
            strm.MUC_GROUP,
            strm.MUC_CODE,
            strm.MUC_DESK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEUSERCONFIGS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MUC_USER=src.MUC_USER) OR (target.MUC_USER IS NULL AND src.MUC_USER IS NULL)) AND
            ((target.MUC_GROUP=src.MUC_GROUP) OR (target.MUC_GROUP IS NULL AND src.MUC_GROUP IS NULL)) AND
            ((target.MUC_CODE=src.MUC_CODE) OR (target.MUC_CODE IS NULL AND src.MUC_CODE IS NULL)) AND
            ((target.MUC_DESK=src.MUC_DESK) OR (target.MUC_DESK IS NULL AND src.MUC_DESK IS NULL)) 
                when matched then update set
                    target.MUC_CODE=src.MUC_CODE, 
                    target.MUC_COMPTYPE=src.MUC_COMPTYPE, 
                    target.MUC_CREATED=src.MUC_CREATED, 
                    target.MUC_DESK=src.MUC_DESK, 
                    target.MUC_GROUP=src.MUC_GROUP, 
                    target.MUC_LASTSAVED=src.MUC_LASTSAVED, 
                    target.MUC_RCODE=src.MUC_RCODE, 
                    target.MUC_UPDATECOUNT=src.MUC_UPDATECOUNT, 
                    target.MUC_UPDATED=src.MUC_UPDATED, 
                    target.MUC_USER=src.MUC_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MUC_CODE, MUC_COMPTYPE, MUC_CREATED, MUC_DESK, MUC_GROUP, MUC_LASTSAVED, MUC_RCODE, MUC_UPDATECOUNT, MUC_UPDATED, MUC_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MUC_CODE, 
                    src.MUC_COMPTYPE, 
                    src.MUC_CREATED, 
                    src.MUC_DESK, 
                    src.MUC_GROUP, 
                    src.MUC_LASTSAVED, 
                    src.MUC_RCODE, 
                    src.MUC_UPDATECOUNT, 
                    src.MUC_UPDATED, 
                    src.MUC_USER, 
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
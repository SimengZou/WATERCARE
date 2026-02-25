create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5UCODES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5UCODES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5UCODES as target using (SELECT * FROM (SELECT 
            strm.UCO_CATEGORY, 
            strm.UCO_CODE, 
            strm.UCO_COLOR, 
            strm.UCO_CREATED, 
            strm.UCO_DESC, 
            strm.UCO_ENTITY, 
            strm.UCO_GISDISPATCHRANKING, 
            strm.UCO_ICON, 
            strm.UCO_ICONPATH, 
            strm.UCO_LASTSAVED, 
            strm.UCO_NOTUSED, 
            strm.UCO_RCODE, 
            strm.UCO_RENTITY, 
            strm.UCO_SYSTEM, 
            strm.UCO_TEXTCODE, 
            strm.UCO_UPDATECOUNT, 
            strm.UCO_UPDATED, 
            strm.UCO_WEIGHT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UCO_ENTITY,
            strm.UCO_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UCODES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UCO_ENTITY=src.UCO_ENTITY) OR (target.UCO_ENTITY IS NULL AND src.UCO_ENTITY IS NULL)) AND
            ((target.UCO_CODE=src.UCO_CODE) OR (target.UCO_CODE IS NULL AND src.UCO_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.UCO_CATEGORY=src.UCO_CATEGORY, 
                    target.UCO_CODE=src.UCO_CODE, 
                    target.UCO_COLOR=src.UCO_COLOR, 
                    target.UCO_CREATED=src.UCO_CREATED, 
                    target.UCO_DESC=src.UCO_DESC, 
                    target.UCO_ENTITY=src.UCO_ENTITY, 
                    target.UCO_GISDISPATCHRANKING=src.UCO_GISDISPATCHRANKING, 
                    target.UCO_ICON=src.UCO_ICON, 
                    target.UCO_ICONPATH=src.UCO_ICONPATH, 
                    target.UCO_LASTSAVED=src.UCO_LASTSAVED, 
                    target.UCO_NOTUSED=src.UCO_NOTUSED, 
                    target.UCO_RCODE=src.UCO_RCODE, 
                    target.UCO_RENTITY=src.UCO_RENTITY, 
                    target.UCO_SYSTEM=src.UCO_SYSTEM, 
                    target.UCO_TEXTCODE=src.UCO_TEXTCODE, 
                    target.UCO_UPDATECOUNT=src.UCO_UPDATECOUNT, 
                    target.UCO_UPDATED=src.UCO_UPDATED, 
                    target.UCO_WEIGHT=src.UCO_WEIGHT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    UCO_CATEGORY, 
                    UCO_CODE, 
                    UCO_COLOR, 
                    UCO_CREATED, 
                    UCO_DESC, 
                    UCO_ENTITY, 
                    UCO_GISDISPATCHRANKING, 
                    UCO_ICON, 
                    UCO_ICONPATH, 
                    UCO_LASTSAVED, 
                    UCO_NOTUSED, 
                    UCO_RCODE, 
                    UCO_RENTITY, 
                    UCO_SYSTEM, 
                    UCO_TEXTCODE, 
                    UCO_UPDATECOUNT, 
                    UCO_UPDATED, 
                    UCO_WEIGHT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.UCO_CATEGORY, 
                    src.UCO_CODE, 
                    src.UCO_COLOR, 
                    src.UCO_CREATED, 
                    src.UCO_DESC, 
                    src.UCO_ENTITY, 
                    src.UCO_GISDISPATCHRANKING, 
                    src.UCO_ICON, 
                    src.UCO_ICONPATH, 
                    src.UCO_LASTSAVED, 
                    src.UCO_NOTUSED, 
                    src.UCO_RCODE, 
                    src.UCO_RENTITY, 
                    src.UCO_SYSTEM, 
                    src.UCO_TEXTCODE, 
                    src.UCO_UPDATECOUNT, 
                    src.UCO_UPDATED, 
                    src.UCO_WEIGHT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5UCODES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.UCO_CATEGORY, 
            strm.UCO_CODE, 
            strm.UCO_COLOR, 
            strm.UCO_CREATED, 
            strm.UCO_DESC, 
            strm.UCO_ENTITY, 
            strm.UCO_GISDISPATCHRANKING, 
            strm.UCO_ICON, 
            strm.UCO_ICONPATH, 
            strm.UCO_LASTSAVED, 
            strm.UCO_NOTUSED, 
            strm.UCO_RCODE, 
            strm.UCO_RENTITY, 
            strm.UCO_SYSTEM, 
            strm.UCO_TEXTCODE, 
            strm.UCO_UPDATECOUNT, 
            strm.UCO_UPDATED, 
            strm.UCO_WEIGHT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UCO_ENTITY,
            strm.UCO_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UCODES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UCO_ENTITY=src.UCO_ENTITY) OR (target.UCO_ENTITY IS NULL AND src.UCO_ENTITY IS NULL)) AND
            ((target.UCO_CODE=src.UCO_CODE) OR (target.UCO_CODE IS NULL AND src.UCO_CODE IS NULL)) 
                when matched then update set
                    target.UCO_CATEGORY=src.UCO_CATEGORY, 
                    target.UCO_CODE=src.UCO_CODE, 
                    target.UCO_COLOR=src.UCO_COLOR, 
                    target.UCO_CREATED=src.UCO_CREATED, 
                    target.UCO_DESC=src.UCO_DESC, 
                    target.UCO_ENTITY=src.UCO_ENTITY, 
                    target.UCO_GISDISPATCHRANKING=src.UCO_GISDISPATCHRANKING, 
                    target.UCO_ICON=src.UCO_ICON, 
                    target.UCO_ICONPATH=src.UCO_ICONPATH, 
                    target.UCO_LASTSAVED=src.UCO_LASTSAVED, 
                    target.UCO_NOTUSED=src.UCO_NOTUSED, 
                    target.UCO_RCODE=src.UCO_RCODE, 
                    target.UCO_RENTITY=src.UCO_RENTITY, 
                    target.UCO_SYSTEM=src.UCO_SYSTEM, 
                    target.UCO_TEXTCODE=src.UCO_TEXTCODE, 
                    target.UCO_UPDATECOUNT=src.UCO_UPDATECOUNT, 
                    target.UCO_UPDATED=src.UCO_UPDATED, 
                    target.UCO_WEIGHT=src.UCO_WEIGHT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( UCO_CATEGORY, UCO_CODE, UCO_COLOR, UCO_CREATED, UCO_DESC, UCO_ENTITY, UCO_GISDISPATCHRANKING, UCO_ICON, UCO_ICONPATH, UCO_LASTSAVED, UCO_NOTUSED, UCO_RCODE, UCO_RENTITY, UCO_SYSTEM, UCO_TEXTCODE, UCO_UPDATECOUNT, UCO_UPDATED, UCO_WEIGHT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.UCO_CATEGORY, 
                    src.UCO_CODE, 
                    src.UCO_COLOR, 
                    src.UCO_CREATED, 
                    src.UCO_DESC, 
                    src.UCO_ENTITY, 
                    src.UCO_GISDISPATCHRANKING, 
                    src.UCO_ICON, 
                    src.UCO_ICONPATH, 
                    src.UCO_LASTSAVED, 
                    src.UCO_NOTUSED, 
                    src.UCO_RCODE, 
                    src.UCO_RENTITY, 
                    src.UCO_SYSTEM, 
                    src.UCO_TEXTCODE, 
                    src.UCO_UPDATECOUNT, 
                    src.UCO_UPDATED, 
                    src.UCO_WEIGHT, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DOCENTITIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DOCENTITIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DOCENTITIES as target using (SELECT * FROM (SELECT 
            strm.DAE_CODE, 
            strm.DAE_COPYTOPO, 
            strm.DAE_COPYTOWO, 
            strm.DAE_CREATECOPYTOWO, 
            strm.DAE_DOCUMENT, 
            strm.DAE_ENTITY, 
            strm.DAE_IDMCOPY, 
            strm.DAE_LASTSAVED, 
            strm.DAE_PRINTONPO, 
            strm.DAE_PRINTONWO, 
            strm.DAE_RENTITY, 
            strm.DAE_RTYPE, 
            strm.DAE_TYPE, 
            strm.DAE_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DAE_DOCUMENT,
            strm.DAE_CODE,
            strm.DAE_RENTITY ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCENTITIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DAE_DOCUMENT=src.DAE_DOCUMENT) OR (target.DAE_DOCUMENT IS NULL AND src.DAE_DOCUMENT IS NULL)) AND
            ((target.DAE_CODE=src.DAE_CODE) OR (target.DAE_CODE IS NULL AND src.DAE_CODE IS NULL)) AND
            ((target.DAE_RENTITY=src.DAE_RENTITY) OR (target.DAE_RENTITY IS NULL AND src.DAE_RENTITY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DAE_CODE=src.DAE_CODE, 
                    target.DAE_COPYTOPO=src.DAE_COPYTOPO, 
                    target.DAE_COPYTOWO=src.DAE_COPYTOWO, 
                    target.DAE_CREATECOPYTOWO=src.DAE_CREATECOPYTOWO, 
                    target.DAE_DOCUMENT=src.DAE_DOCUMENT, 
                    target.DAE_ENTITY=src.DAE_ENTITY, 
                    target.DAE_IDMCOPY=src.DAE_IDMCOPY, 
                    target.DAE_LASTSAVED=src.DAE_LASTSAVED, 
                    target.DAE_PRINTONPO=src.DAE_PRINTONPO, 
                    target.DAE_PRINTONWO=src.DAE_PRINTONWO, 
                    target.DAE_RENTITY=src.DAE_RENTITY, 
                    target.DAE_RTYPE=src.DAE_RTYPE, 
                    target.DAE_TYPE=src.DAE_TYPE, 
                    target.DAE_UPDATECOUNT=src.DAE_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DAE_CODE, 
                    DAE_COPYTOPO, 
                    DAE_COPYTOWO, 
                    DAE_CREATECOPYTOWO, 
                    DAE_DOCUMENT, 
                    DAE_ENTITY, 
                    DAE_IDMCOPY, 
                    DAE_LASTSAVED, 
                    DAE_PRINTONPO, 
                    DAE_PRINTONWO, 
                    DAE_RENTITY, 
                    DAE_RTYPE, 
                    DAE_TYPE, 
                    DAE_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DAE_CODE, 
                    src.DAE_COPYTOPO, 
                    src.DAE_COPYTOWO, 
                    src.DAE_CREATECOPYTOWO, 
                    src.DAE_DOCUMENT, 
                    src.DAE_ENTITY, 
                    src.DAE_IDMCOPY, 
                    src.DAE_LASTSAVED, 
                    src.DAE_PRINTONPO, 
                    src.DAE_PRINTONWO, 
                    src.DAE_RENTITY, 
                    src.DAE_RTYPE, 
                    src.DAE_TYPE, 
                    src.DAE_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DOCENTITIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DAE_CODE, 
            strm.DAE_COPYTOPO, 
            strm.DAE_COPYTOWO, 
            strm.DAE_CREATECOPYTOWO, 
            strm.DAE_DOCUMENT, 
            strm.DAE_ENTITY, 
            strm.DAE_IDMCOPY, 
            strm.DAE_LASTSAVED, 
            strm.DAE_PRINTONPO, 
            strm.DAE_PRINTONWO, 
            strm.DAE_RENTITY, 
            strm.DAE_RTYPE, 
            strm.DAE_TYPE, 
            strm.DAE_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DAE_DOCUMENT,
            strm.DAE_CODE,
            strm.DAE_RENTITY ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCENTITIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DAE_DOCUMENT=src.DAE_DOCUMENT) OR (target.DAE_DOCUMENT IS NULL AND src.DAE_DOCUMENT IS NULL)) AND
            ((target.DAE_CODE=src.DAE_CODE) OR (target.DAE_CODE IS NULL AND src.DAE_CODE IS NULL)) AND
            ((target.DAE_RENTITY=src.DAE_RENTITY) OR (target.DAE_RENTITY IS NULL AND src.DAE_RENTITY IS NULL)) 
                when matched then update set
                    target.DAE_CODE=src.DAE_CODE, 
                    target.DAE_COPYTOPO=src.DAE_COPYTOPO, 
                    target.DAE_COPYTOWO=src.DAE_COPYTOWO, 
                    target.DAE_CREATECOPYTOWO=src.DAE_CREATECOPYTOWO, 
                    target.DAE_DOCUMENT=src.DAE_DOCUMENT, 
                    target.DAE_ENTITY=src.DAE_ENTITY, 
                    target.DAE_IDMCOPY=src.DAE_IDMCOPY, 
                    target.DAE_LASTSAVED=src.DAE_LASTSAVED, 
                    target.DAE_PRINTONPO=src.DAE_PRINTONPO, 
                    target.DAE_PRINTONWO=src.DAE_PRINTONWO, 
                    target.DAE_RENTITY=src.DAE_RENTITY, 
                    target.DAE_RTYPE=src.DAE_RTYPE, 
                    target.DAE_TYPE=src.DAE_TYPE, 
                    target.DAE_UPDATECOUNT=src.DAE_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DAE_CODE, DAE_COPYTOPO, DAE_COPYTOWO, DAE_CREATECOPYTOWO, DAE_DOCUMENT, DAE_ENTITY, DAE_IDMCOPY, DAE_LASTSAVED, DAE_PRINTONPO, DAE_PRINTONWO, DAE_RENTITY, DAE_RTYPE, DAE_TYPE, DAE_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DAE_CODE, 
                    src.DAE_COPYTOPO, 
                    src.DAE_COPYTOWO, 
                    src.DAE_CREATECOPYTOWO, 
                    src.DAE_DOCUMENT, 
                    src.DAE_ENTITY, 
                    src.DAE_IDMCOPY, 
                    src.DAE_LASTSAVED, 
                    src.DAE_PRINTONPO, 
                    src.DAE_PRINTONWO, 
                    src.DAE_RENTITY, 
                    src.DAE_RTYPE, 
                    src.DAE_TYPE, 
                    src.DAE_UPDATECOUNT, 
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
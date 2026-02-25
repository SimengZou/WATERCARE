create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CODESTRUCTURE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CODESTRUCTURE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CODESTRUCTURE as target using (SELECT * FROM (SELECT 
            strm.CSR_CODE, 
            strm.CSR_DESC, 
            strm.CSR_ENTITY, 
            strm.CSR_ICON, 
            strm.CSR_ICONPATH, 
            strm.CSR_IMAGE, 
            strm.CSR_IMPORTANCE, 
            strm.CSR_LASTSAVED, 
            strm.CSR_MATERIALTYPE, 
            strm.CSR_NOTUSED, 
            strm.CSR_RENTITY, 
            strm.CSR_STRLEVEL1, 
            strm.CSR_STRLEVEL2, 
            strm.CSR_STRLEVEL3, 
            strm.CSR_STRLEVEL4, 
            strm.CSR_STRLEVEL5, 
            strm.CSR_STRLEVEL6, 
            strm.CSR_STRLEVEL7, 
            strm.CSR_STRLEVEL8, 
            strm.CSR_STRUCTURE, 
            strm.CSR_UPDATECOUNT, 
            strm.CSR_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CSR_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CODESTRUCTURE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CSR_CODE=src.CSR_CODE) OR (target.CSR_CODE IS NULL AND src.CSR_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CSR_CODE=src.CSR_CODE, 
                    target.CSR_DESC=src.CSR_DESC, 
                    target.CSR_ENTITY=src.CSR_ENTITY, 
                    target.CSR_ICON=src.CSR_ICON, 
                    target.CSR_ICONPATH=src.CSR_ICONPATH, 
                    target.CSR_IMAGE=src.CSR_IMAGE, 
                    target.CSR_IMPORTANCE=src.CSR_IMPORTANCE, 
                    target.CSR_LASTSAVED=src.CSR_LASTSAVED, 
                    target.CSR_MATERIALTYPE=src.CSR_MATERIALTYPE, 
                    target.CSR_NOTUSED=src.CSR_NOTUSED, 
                    target.CSR_RENTITY=src.CSR_RENTITY, 
                    target.CSR_STRLEVEL1=src.CSR_STRLEVEL1, 
                    target.CSR_STRLEVEL2=src.CSR_STRLEVEL2, 
                    target.CSR_STRLEVEL3=src.CSR_STRLEVEL3, 
                    target.CSR_STRLEVEL4=src.CSR_STRLEVEL4, 
                    target.CSR_STRLEVEL5=src.CSR_STRLEVEL5, 
                    target.CSR_STRLEVEL6=src.CSR_STRLEVEL6, 
                    target.CSR_STRLEVEL7=src.CSR_STRLEVEL7, 
                    target.CSR_STRLEVEL8=src.CSR_STRLEVEL8, 
                    target.CSR_STRUCTURE=src.CSR_STRUCTURE, 
                    target.CSR_UPDATECOUNT=src.CSR_UPDATECOUNT, 
                    target.CSR_UPDATED=src.CSR_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CSR_CODE, 
                    CSR_DESC, 
                    CSR_ENTITY, 
                    CSR_ICON, 
                    CSR_ICONPATH, 
                    CSR_IMAGE, 
                    CSR_IMPORTANCE, 
                    CSR_LASTSAVED, 
                    CSR_MATERIALTYPE, 
                    CSR_NOTUSED, 
                    CSR_RENTITY, 
                    CSR_STRLEVEL1, 
                    CSR_STRLEVEL2, 
                    CSR_STRLEVEL3, 
                    CSR_STRLEVEL4, 
                    CSR_STRLEVEL5, 
                    CSR_STRLEVEL6, 
                    CSR_STRLEVEL7, 
                    CSR_STRLEVEL8, 
                    CSR_STRUCTURE, 
                    CSR_UPDATECOUNT, 
                    CSR_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CSR_CODE, 
                    src.CSR_DESC, 
                    src.CSR_ENTITY, 
                    src.CSR_ICON, 
                    src.CSR_ICONPATH, 
                    src.CSR_IMAGE, 
                    src.CSR_IMPORTANCE, 
                    src.CSR_LASTSAVED, 
                    src.CSR_MATERIALTYPE, 
                    src.CSR_NOTUSED, 
                    src.CSR_RENTITY, 
                    src.CSR_STRLEVEL1, 
                    src.CSR_STRLEVEL2, 
                    src.CSR_STRLEVEL3, 
                    src.CSR_STRLEVEL4, 
                    src.CSR_STRLEVEL5, 
                    src.CSR_STRLEVEL6, 
                    src.CSR_STRLEVEL7, 
                    src.CSR_STRLEVEL8, 
                    src.CSR_STRUCTURE, 
                    src.CSR_UPDATECOUNT, 
                    src.CSR_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CODESTRUCTURE_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CSR_CODE, 
            strm.CSR_DESC, 
            strm.CSR_ENTITY, 
            strm.CSR_ICON, 
            strm.CSR_ICONPATH, 
            strm.CSR_IMAGE, 
            strm.CSR_IMPORTANCE, 
            strm.CSR_LASTSAVED, 
            strm.CSR_MATERIALTYPE, 
            strm.CSR_NOTUSED, 
            strm.CSR_RENTITY, 
            strm.CSR_STRLEVEL1, 
            strm.CSR_STRLEVEL2, 
            strm.CSR_STRLEVEL3, 
            strm.CSR_STRLEVEL4, 
            strm.CSR_STRLEVEL5, 
            strm.CSR_STRLEVEL6, 
            strm.CSR_STRLEVEL7, 
            strm.CSR_STRLEVEL8, 
            strm.CSR_STRUCTURE, 
            strm.CSR_UPDATECOUNT, 
            strm.CSR_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CSR_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CODESTRUCTURE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CSR_CODE=src.CSR_CODE) OR (target.CSR_CODE IS NULL AND src.CSR_CODE IS NULL)) 
                when matched then update set
                    target.CSR_CODE=src.CSR_CODE, 
                    target.CSR_DESC=src.CSR_DESC, 
                    target.CSR_ENTITY=src.CSR_ENTITY, 
                    target.CSR_ICON=src.CSR_ICON, 
                    target.CSR_ICONPATH=src.CSR_ICONPATH, 
                    target.CSR_IMAGE=src.CSR_IMAGE, 
                    target.CSR_IMPORTANCE=src.CSR_IMPORTANCE, 
                    target.CSR_LASTSAVED=src.CSR_LASTSAVED, 
                    target.CSR_MATERIALTYPE=src.CSR_MATERIALTYPE, 
                    target.CSR_NOTUSED=src.CSR_NOTUSED, 
                    target.CSR_RENTITY=src.CSR_RENTITY, 
                    target.CSR_STRLEVEL1=src.CSR_STRLEVEL1, 
                    target.CSR_STRLEVEL2=src.CSR_STRLEVEL2, 
                    target.CSR_STRLEVEL3=src.CSR_STRLEVEL3, 
                    target.CSR_STRLEVEL4=src.CSR_STRLEVEL4, 
                    target.CSR_STRLEVEL5=src.CSR_STRLEVEL5, 
                    target.CSR_STRLEVEL6=src.CSR_STRLEVEL6, 
                    target.CSR_STRLEVEL7=src.CSR_STRLEVEL7, 
                    target.CSR_STRLEVEL8=src.CSR_STRLEVEL8, 
                    target.CSR_STRUCTURE=src.CSR_STRUCTURE, 
                    target.CSR_UPDATECOUNT=src.CSR_UPDATECOUNT, 
                    target.CSR_UPDATED=src.CSR_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CSR_CODE, CSR_DESC, CSR_ENTITY, CSR_ICON, CSR_ICONPATH, CSR_IMAGE, CSR_IMPORTANCE, CSR_LASTSAVED, CSR_MATERIALTYPE, CSR_NOTUSED, CSR_RENTITY, CSR_STRLEVEL1, CSR_STRLEVEL2, CSR_STRLEVEL3, CSR_STRLEVEL4, CSR_STRLEVEL5, CSR_STRLEVEL6, CSR_STRLEVEL7, CSR_STRLEVEL8, CSR_STRUCTURE, CSR_UPDATECOUNT, CSR_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CSR_CODE, 
                    src.CSR_DESC, 
                    src.CSR_ENTITY, 
                    src.CSR_ICON, 
                    src.CSR_ICONPATH, 
                    src.CSR_IMAGE, 
                    src.CSR_IMPORTANCE, 
                    src.CSR_LASTSAVED, 
                    src.CSR_MATERIALTYPE, 
                    src.CSR_NOTUSED, 
                    src.CSR_RENTITY, 
                    src.CSR_STRLEVEL1, 
                    src.CSR_STRLEVEL2, 
                    src.CSR_STRLEVEL3, 
                    src.CSR_STRLEVEL4, 
                    src.CSR_STRLEVEL5, 
                    src.CSR_STRLEVEL6, 
                    src.CSR_STRLEVEL7, 
                    src.CSR_STRLEVEL8, 
                    src.CSR_STRUCTURE, 
                    src.CSR_UPDATECOUNT, 
                    src.CSR_UPDATED, 
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
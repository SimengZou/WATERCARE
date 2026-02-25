create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DESCRIPTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DESCRIPTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DESCRIPTIONS as target using (SELECT * FROM (SELECT 
            strm.DES_CODE, 
            strm.DES_ENTITY, 
            strm.DES_LANG, 
            strm.DES_LASTSAVED, 
            strm.DES_ORG, 
            strm.DES_RENTITY, 
            strm.DES_RTYPE, 
            strm.DES_TEXT, 
            strm.DES_TRANS, 
            strm.DES_TYPE, 
            strm.DES_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DES_RENTITY,
            strm.DES_CODE,
            strm.DES_ORG,
            strm.DES_LANG,
            strm.DES_RTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DESCRIPTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DES_RENTITY=src.DES_RENTITY) OR (target.DES_RENTITY IS NULL AND src.DES_RENTITY IS NULL)) AND
            ((target.DES_CODE=src.DES_CODE) OR (target.DES_CODE IS NULL AND src.DES_CODE IS NULL)) AND
            ((target.DES_ORG=src.DES_ORG) OR (target.DES_ORG IS NULL AND src.DES_ORG IS NULL)) AND
            ((target.DES_LANG=src.DES_LANG) OR (target.DES_LANG IS NULL AND src.DES_LANG IS NULL)) AND
            ((target.DES_RTYPE=src.DES_RTYPE) OR (target.DES_RTYPE IS NULL AND src.DES_RTYPE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DES_CODE=src.DES_CODE, 
                    target.DES_ENTITY=src.DES_ENTITY, 
                    target.DES_LANG=src.DES_LANG, 
                    target.DES_LASTSAVED=src.DES_LASTSAVED, 
                    target.DES_ORG=src.DES_ORG, 
                    target.DES_RENTITY=src.DES_RENTITY, 
                    target.DES_RTYPE=src.DES_RTYPE, 
                    target.DES_TEXT=src.DES_TEXT, 
                    target.DES_TRANS=src.DES_TRANS, 
                    target.DES_TYPE=src.DES_TYPE, 
                    target.DES_UPDATECOUNT=src.DES_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DES_CODE, 
                    DES_ENTITY, 
                    DES_LANG, 
                    DES_LASTSAVED, 
                    DES_ORG, 
                    DES_RENTITY, 
                    DES_RTYPE, 
                    DES_TEXT, 
                    DES_TRANS, 
                    DES_TYPE, 
                    DES_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DES_CODE, 
                    src.DES_ENTITY, 
                    src.DES_LANG, 
                    src.DES_LASTSAVED, 
                    src.DES_ORG, 
                    src.DES_RENTITY, 
                    src.DES_RTYPE, 
                    src.DES_TEXT, 
                    src.DES_TRANS, 
                    src.DES_TYPE, 
                    src.DES_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DESCRIPTIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DES_CODE, 
            strm.DES_ENTITY, 
            strm.DES_LANG, 
            strm.DES_LASTSAVED, 
            strm.DES_ORG, 
            strm.DES_RENTITY, 
            strm.DES_RTYPE, 
            strm.DES_TEXT, 
            strm.DES_TRANS, 
            strm.DES_TYPE, 
            strm.DES_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DES_RENTITY,
            strm.DES_CODE,
            strm.DES_ORG,
            strm.DES_LANG,
            strm.DES_RTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DESCRIPTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DES_RENTITY=src.DES_RENTITY) OR (target.DES_RENTITY IS NULL AND src.DES_RENTITY IS NULL)) AND
            ((target.DES_CODE=src.DES_CODE) OR (target.DES_CODE IS NULL AND src.DES_CODE IS NULL)) AND
            ((target.DES_ORG=src.DES_ORG) OR (target.DES_ORG IS NULL AND src.DES_ORG IS NULL)) AND
            ((target.DES_LANG=src.DES_LANG) OR (target.DES_LANG IS NULL AND src.DES_LANG IS NULL)) AND
            ((target.DES_RTYPE=src.DES_RTYPE) OR (target.DES_RTYPE IS NULL AND src.DES_RTYPE IS NULL)) 
                when matched then update set
                    target.DES_CODE=src.DES_CODE, 
                    target.DES_ENTITY=src.DES_ENTITY, 
                    target.DES_LANG=src.DES_LANG, 
                    target.DES_LASTSAVED=src.DES_LASTSAVED, 
                    target.DES_ORG=src.DES_ORG, 
                    target.DES_RENTITY=src.DES_RENTITY, 
                    target.DES_RTYPE=src.DES_RTYPE, 
                    target.DES_TEXT=src.DES_TEXT, 
                    target.DES_TRANS=src.DES_TRANS, 
                    target.DES_TYPE=src.DES_TYPE, 
                    target.DES_UPDATECOUNT=src.DES_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DES_CODE, DES_ENTITY, DES_LANG, DES_LASTSAVED, DES_ORG, DES_RENTITY, DES_RTYPE, DES_TEXT, DES_TRANS, DES_TYPE, DES_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DES_CODE, 
                    src.DES_ENTITY, 
                    src.DES_LANG, 
                    src.DES_LASTSAVED, 
                    src.DES_ORG, 
                    src.DES_RENTITY, 
                    src.DES_RTYPE, 
                    src.DES_TEXT, 
                    src.DES_TRANS, 
                    src.DES_TYPE, 
                    src.DES_UPDATECOUNT, 
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
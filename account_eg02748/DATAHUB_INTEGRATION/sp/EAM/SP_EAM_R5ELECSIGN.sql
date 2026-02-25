create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ELECSIGN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ELECSIGN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ELECSIGN as target using (SELECT * FROM (SELECT 
            strm.ELS_CERTIFYNUM, 
            strm.ELS_CERTIFYTYPE, 
            strm.ELS_CODE, 
            strm.ELS_DATE, 
            strm.ELS_ENTCODE, 
            strm.ELS_ENTCODE2, 
            strm.ELS_ENTITY, 
            strm.ELS_EXTERNALDATE, 
            strm.ELS_LASTCHANGED, 
            strm.ELS_LASTSAVED, 
            strm.ELS_ORG, 
            strm.ELS_PARENT, 
            strm.ELS_SIGNTYPE, 
            strm.ELS_SIGNTYPEDESC, 
            strm.ELS_STATUS, 
            strm.ELS_USER, 
            strm.ELS_USERDESC, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ELS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ELECSIGN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ELS_CODE=src.ELS_CODE) OR (target.ELS_CODE IS NULL AND src.ELS_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ELS_CERTIFYNUM=src.ELS_CERTIFYNUM, 
                    target.ELS_CERTIFYTYPE=src.ELS_CERTIFYTYPE, 
                    target.ELS_CODE=src.ELS_CODE, 
                    target.ELS_DATE=src.ELS_DATE, 
                    target.ELS_ENTCODE=src.ELS_ENTCODE, 
                    target.ELS_ENTCODE2=src.ELS_ENTCODE2, 
                    target.ELS_ENTITY=src.ELS_ENTITY, 
                    target.ELS_EXTERNALDATE=src.ELS_EXTERNALDATE, 
                    target.ELS_LASTCHANGED=src.ELS_LASTCHANGED, 
                    target.ELS_LASTSAVED=src.ELS_LASTSAVED, 
                    target.ELS_ORG=src.ELS_ORG, 
                    target.ELS_PARENT=src.ELS_PARENT, 
                    target.ELS_SIGNTYPE=src.ELS_SIGNTYPE, 
                    target.ELS_SIGNTYPEDESC=src.ELS_SIGNTYPEDESC, 
                    target.ELS_STATUS=src.ELS_STATUS, 
                    target.ELS_USER=src.ELS_USER, 
                    target.ELS_USERDESC=src.ELS_USERDESC, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ELS_CERTIFYNUM, 
                    ELS_CERTIFYTYPE, 
                    ELS_CODE, 
                    ELS_DATE, 
                    ELS_ENTCODE, 
                    ELS_ENTCODE2, 
                    ELS_ENTITY, 
                    ELS_EXTERNALDATE, 
                    ELS_LASTCHANGED, 
                    ELS_LASTSAVED, 
                    ELS_ORG, 
                    ELS_PARENT, 
                    ELS_SIGNTYPE, 
                    ELS_SIGNTYPEDESC, 
                    ELS_STATUS, 
                    ELS_USER, 
                    ELS_USERDESC, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ELS_CERTIFYNUM, 
                    src.ELS_CERTIFYTYPE, 
                    src.ELS_CODE, 
                    src.ELS_DATE, 
                    src.ELS_ENTCODE, 
                    src.ELS_ENTCODE2, 
                    src.ELS_ENTITY, 
                    src.ELS_EXTERNALDATE, 
                    src.ELS_LASTCHANGED, 
                    src.ELS_LASTSAVED, 
                    src.ELS_ORG, 
                    src.ELS_PARENT, 
                    src.ELS_SIGNTYPE, 
                    src.ELS_SIGNTYPEDESC, 
                    src.ELS_STATUS, 
                    src.ELS_USER, 
                    src.ELS_USERDESC, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ELECSIGN_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ELS_CERTIFYNUM, 
            strm.ELS_CERTIFYTYPE, 
            strm.ELS_CODE, 
            strm.ELS_DATE, 
            strm.ELS_ENTCODE, 
            strm.ELS_ENTCODE2, 
            strm.ELS_ENTITY, 
            strm.ELS_EXTERNALDATE, 
            strm.ELS_LASTCHANGED, 
            strm.ELS_LASTSAVED, 
            strm.ELS_ORG, 
            strm.ELS_PARENT, 
            strm.ELS_SIGNTYPE, 
            strm.ELS_SIGNTYPEDESC, 
            strm.ELS_STATUS, 
            strm.ELS_USER, 
            strm.ELS_USERDESC, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ELS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ELECSIGN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ELS_CODE=src.ELS_CODE) OR (target.ELS_CODE IS NULL AND src.ELS_CODE IS NULL)) 
                when matched then update set
                    target.ELS_CERTIFYNUM=src.ELS_CERTIFYNUM, 
                    target.ELS_CERTIFYTYPE=src.ELS_CERTIFYTYPE, 
                    target.ELS_CODE=src.ELS_CODE, 
                    target.ELS_DATE=src.ELS_DATE, 
                    target.ELS_ENTCODE=src.ELS_ENTCODE, 
                    target.ELS_ENTCODE2=src.ELS_ENTCODE2, 
                    target.ELS_ENTITY=src.ELS_ENTITY, 
                    target.ELS_EXTERNALDATE=src.ELS_EXTERNALDATE, 
                    target.ELS_LASTCHANGED=src.ELS_LASTCHANGED, 
                    target.ELS_LASTSAVED=src.ELS_LASTSAVED, 
                    target.ELS_ORG=src.ELS_ORG, 
                    target.ELS_PARENT=src.ELS_PARENT, 
                    target.ELS_SIGNTYPE=src.ELS_SIGNTYPE, 
                    target.ELS_SIGNTYPEDESC=src.ELS_SIGNTYPEDESC, 
                    target.ELS_STATUS=src.ELS_STATUS, 
                    target.ELS_USER=src.ELS_USER, 
                    target.ELS_USERDESC=src.ELS_USERDESC, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ELS_CERTIFYNUM, ELS_CERTIFYTYPE, ELS_CODE, ELS_DATE, ELS_ENTCODE, ELS_ENTCODE2, ELS_ENTITY, ELS_EXTERNALDATE, ELS_LASTCHANGED, ELS_LASTSAVED, ELS_ORG, ELS_PARENT, ELS_SIGNTYPE, ELS_SIGNTYPEDESC, ELS_STATUS, ELS_USER, ELS_USERDESC, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ELS_CERTIFYNUM, 
                    src.ELS_CERTIFYTYPE, 
                    src.ELS_CODE, 
                    src.ELS_DATE, 
                    src.ELS_ENTCODE, 
                    src.ELS_ENTCODE2, 
                    src.ELS_ENTITY, 
                    src.ELS_EXTERNALDATE, 
                    src.ELS_LASTCHANGED, 
                    src.ELS_LASTSAVED, 
                    src.ELS_ORG, 
                    src.ELS_PARENT, 
                    src.ELS_SIGNTYPE, 
                    src.ELS_SIGNTYPEDESC, 
                    src.ELS_STATUS, 
                    src.ELS_USER, 
                    src.ELS_USERDESC, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ROLES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ROLES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ROLES as target using (SELECT * FROM (SELECT 
            strm.ROL_ACTIVE, 
            strm.ROL_ADVREPAUTHOR, 
            strm.ROL_ADVREPCONSUMER, 
            strm.ROL_ANALYTICS, 
            strm.ROL_APPROVER, 
            strm.ROL_BARCODE, 
            strm.ROL_BUYER, 
            strm.ROL_CODE, 
            strm.ROL_CONNECTOR, 
            strm.ROL_DEFAULTORG, 
            strm.ROL_DESC, 
            strm.ROL_EWSFIRSTFUNC, 
            strm.ROL_FIRSTFUNC, 
            strm.ROL_GROUP, 
            strm.ROL_INVAPPVLIMIT, 
            strm.ROL_INVAPPVLIMITNONPO, 
            strm.ROL_LANG, 
            strm.ROL_LASTSAVED, 
            strm.ROL_LOCALE, 
            strm.ROL_MOBILE, 
            strm.ROL_MOBILEADM, 
            strm.ROL_MRC, 
            strm.ROL_PICAPPVLIMIT, 
            strm.ROL_PORDAPPVLIMIT, 
            strm.ROL_PORDAUTHAPPVLIMIT, 
            strm.ROL_REQAPPVLIMIT, 
            strm.ROL_REQAUTHAPPVLIMIT, 
            strm.ROL_REQUESTOR, 
            strm.ROL_ROUTER, 
            strm.ROL_SUCCESSMSGTIMEOUT, 
            strm.ROL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROL_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROLES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROL_CODE=src.ROL_CODE) OR (target.ROL_CODE IS NULL AND src.ROL_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ROL_ACTIVE=src.ROL_ACTIVE, 
                    target.ROL_ADVREPAUTHOR=src.ROL_ADVREPAUTHOR, 
                    target.ROL_ADVREPCONSUMER=src.ROL_ADVREPCONSUMER, 
                    target.ROL_ANALYTICS=src.ROL_ANALYTICS, 
                    target.ROL_APPROVER=src.ROL_APPROVER, 
                    target.ROL_BARCODE=src.ROL_BARCODE, 
                    target.ROL_BUYER=src.ROL_BUYER, 
                    target.ROL_CODE=src.ROL_CODE, 
                    target.ROL_CONNECTOR=src.ROL_CONNECTOR, 
                    target.ROL_DEFAULTORG=src.ROL_DEFAULTORG, 
                    target.ROL_DESC=src.ROL_DESC, 
                    target.ROL_EWSFIRSTFUNC=src.ROL_EWSFIRSTFUNC, 
                    target.ROL_FIRSTFUNC=src.ROL_FIRSTFUNC, 
                    target.ROL_GROUP=src.ROL_GROUP, 
                    target.ROL_INVAPPVLIMIT=src.ROL_INVAPPVLIMIT, 
                    target.ROL_INVAPPVLIMITNONPO=src.ROL_INVAPPVLIMITNONPO, 
                    target.ROL_LANG=src.ROL_LANG, 
                    target.ROL_LASTSAVED=src.ROL_LASTSAVED, 
                    target.ROL_LOCALE=src.ROL_LOCALE, 
                    target.ROL_MOBILE=src.ROL_MOBILE, 
                    target.ROL_MOBILEADM=src.ROL_MOBILEADM, 
                    target.ROL_MRC=src.ROL_MRC, 
                    target.ROL_PICAPPVLIMIT=src.ROL_PICAPPVLIMIT, 
                    target.ROL_PORDAPPVLIMIT=src.ROL_PORDAPPVLIMIT, 
                    target.ROL_PORDAUTHAPPVLIMIT=src.ROL_PORDAUTHAPPVLIMIT, 
                    target.ROL_REQAPPVLIMIT=src.ROL_REQAPPVLIMIT, 
                    target.ROL_REQAUTHAPPVLIMIT=src.ROL_REQAUTHAPPVLIMIT, 
                    target.ROL_REQUESTOR=src.ROL_REQUESTOR, 
                    target.ROL_ROUTER=src.ROL_ROUTER, 
                    target.ROL_SUCCESSMSGTIMEOUT=src.ROL_SUCCESSMSGTIMEOUT, 
                    target.ROL_UPDATECOUNT=src.ROL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ROL_ACTIVE, 
                    ROL_ADVREPAUTHOR, 
                    ROL_ADVREPCONSUMER, 
                    ROL_ANALYTICS, 
                    ROL_APPROVER, 
                    ROL_BARCODE, 
                    ROL_BUYER, 
                    ROL_CODE, 
                    ROL_CONNECTOR, 
                    ROL_DEFAULTORG, 
                    ROL_DESC, 
                    ROL_EWSFIRSTFUNC, 
                    ROL_FIRSTFUNC, 
                    ROL_GROUP, 
                    ROL_INVAPPVLIMIT, 
                    ROL_INVAPPVLIMITNONPO, 
                    ROL_LANG, 
                    ROL_LASTSAVED, 
                    ROL_LOCALE, 
                    ROL_MOBILE, 
                    ROL_MOBILEADM, 
                    ROL_MRC, 
                    ROL_PICAPPVLIMIT, 
                    ROL_PORDAPPVLIMIT, 
                    ROL_PORDAUTHAPPVLIMIT, 
                    ROL_REQAPPVLIMIT, 
                    ROL_REQAUTHAPPVLIMIT, 
                    ROL_REQUESTOR, 
                    ROL_ROUTER, 
                    ROL_SUCCESSMSGTIMEOUT, 
                    ROL_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ROL_ACTIVE, 
                    src.ROL_ADVREPAUTHOR, 
                    src.ROL_ADVREPCONSUMER, 
                    src.ROL_ANALYTICS, 
                    src.ROL_APPROVER, 
                    src.ROL_BARCODE, 
                    src.ROL_BUYER, 
                    src.ROL_CODE, 
                    src.ROL_CONNECTOR, 
                    src.ROL_DEFAULTORG, 
                    src.ROL_DESC, 
                    src.ROL_EWSFIRSTFUNC, 
                    src.ROL_FIRSTFUNC, 
                    src.ROL_GROUP, 
                    src.ROL_INVAPPVLIMIT, 
                    src.ROL_INVAPPVLIMITNONPO, 
                    src.ROL_LANG, 
                    src.ROL_LASTSAVED, 
                    src.ROL_LOCALE, 
                    src.ROL_MOBILE, 
                    src.ROL_MOBILEADM, 
                    src.ROL_MRC, 
                    src.ROL_PICAPPVLIMIT, 
                    src.ROL_PORDAPPVLIMIT, 
                    src.ROL_PORDAUTHAPPVLIMIT, 
                    src.ROL_REQAPPVLIMIT, 
                    src.ROL_REQAUTHAPPVLIMIT, 
                    src.ROL_REQUESTOR, 
                    src.ROL_ROUTER, 
                    src.ROL_SUCCESSMSGTIMEOUT, 
                    src.ROL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ROLES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ROL_ACTIVE, 
            strm.ROL_ADVREPAUTHOR, 
            strm.ROL_ADVREPCONSUMER, 
            strm.ROL_ANALYTICS, 
            strm.ROL_APPROVER, 
            strm.ROL_BARCODE, 
            strm.ROL_BUYER, 
            strm.ROL_CODE, 
            strm.ROL_CONNECTOR, 
            strm.ROL_DEFAULTORG, 
            strm.ROL_DESC, 
            strm.ROL_EWSFIRSTFUNC, 
            strm.ROL_FIRSTFUNC, 
            strm.ROL_GROUP, 
            strm.ROL_INVAPPVLIMIT, 
            strm.ROL_INVAPPVLIMITNONPO, 
            strm.ROL_LANG, 
            strm.ROL_LASTSAVED, 
            strm.ROL_LOCALE, 
            strm.ROL_MOBILE, 
            strm.ROL_MOBILEADM, 
            strm.ROL_MRC, 
            strm.ROL_PICAPPVLIMIT, 
            strm.ROL_PORDAPPVLIMIT, 
            strm.ROL_PORDAUTHAPPVLIMIT, 
            strm.ROL_REQAPPVLIMIT, 
            strm.ROL_REQAUTHAPPVLIMIT, 
            strm.ROL_REQUESTOR, 
            strm.ROL_ROUTER, 
            strm.ROL_SUCCESSMSGTIMEOUT, 
            strm.ROL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ROL_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROLES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ROL_CODE=src.ROL_CODE) OR (target.ROL_CODE IS NULL AND src.ROL_CODE IS NULL)) 
                when matched then update set
                    target.ROL_ACTIVE=src.ROL_ACTIVE, 
                    target.ROL_ADVREPAUTHOR=src.ROL_ADVREPAUTHOR, 
                    target.ROL_ADVREPCONSUMER=src.ROL_ADVREPCONSUMER, 
                    target.ROL_ANALYTICS=src.ROL_ANALYTICS, 
                    target.ROL_APPROVER=src.ROL_APPROVER, 
                    target.ROL_BARCODE=src.ROL_BARCODE, 
                    target.ROL_BUYER=src.ROL_BUYER, 
                    target.ROL_CODE=src.ROL_CODE, 
                    target.ROL_CONNECTOR=src.ROL_CONNECTOR, 
                    target.ROL_DEFAULTORG=src.ROL_DEFAULTORG, 
                    target.ROL_DESC=src.ROL_DESC, 
                    target.ROL_EWSFIRSTFUNC=src.ROL_EWSFIRSTFUNC, 
                    target.ROL_FIRSTFUNC=src.ROL_FIRSTFUNC, 
                    target.ROL_GROUP=src.ROL_GROUP, 
                    target.ROL_INVAPPVLIMIT=src.ROL_INVAPPVLIMIT, 
                    target.ROL_INVAPPVLIMITNONPO=src.ROL_INVAPPVLIMITNONPO, 
                    target.ROL_LANG=src.ROL_LANG, 
                    target.ROL_LASTSAVED=src.ROL_LASTSAVED, 
                    target.ROL_LOCALE=src.ROL_LOCALE, 
                    target.ROL_MOBILE=src.ROL_MOBILE, 
                    target.ROL_MOBILEADM=src.ROL_MOBILEADM, 
                    target.ROL_MRC=src.ROL_MRC, 
                    target.ROL_PICAPPVLIMIT=src.ROL_PICAPPVLIMIT, 
                    target.ROL_PORDAPPVLIMIT=src.ROL_PORDAPPVLIMIT, 
                    target.ROL_PORDAUTHAPPVLIMIT=src.ROL_PORDAUTHAPPVLIMIT, 
                    target.ROL_REQAPPVLIMIT=src.ROL_REQAPPVLIMIT, 
                    target.ROL_REQAUTHAPPVLIMIT=src.ROL_REQAUTHAPPVLIMIT, 
                    target.ROL_REQUESTOR=src.ROL_REQUESTOR, 
                    target.ROL_ROUTER=src.ROL_ROUTER, 
                    target.ROL_SUCCESSMSGTIMEOUT=src.ROL_SUCCESSMSGTIMEOUT, 
                    target.ROL_UPDATECOUNT=src.ROL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ROL_ACTIVE, ROL_ADVREPAUTHOR, ROL_ADVREPCONSUMER, ROL_ANALYTICS, ROL_APPROVER, ROL_BARCODE, ROL_BUYER, ROL_CODE, ROL_CONNECTOR, ROL_DEFAULTORG, ROL_DESC, ROL_EWSFIRSTFUNC, ROL_FIRSTFUNC, ROL_GROUP, ROL_INVAPPVLIMIT, ROL_INVAPPVLIMITNONPO, ROL_LANG, ROL_LASTSAVED, ROL_LOCALE, ROL_MOBILE, ROL_MOBILEADM, ROL_MRC, ROL_PICAPPVLIMIT, ROL_PORDAPPVLIMIT, ROL_PORDAUTHAPPVLIMIT, ROL_REQAPPVLIMIT, ROL_REQAUTHAPPVLIMIT, ROL_REQUESTOR, ROL_ROUTER, ROL_SUCCESSMSGTIMEOUT, ROL_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ROL_ACTIVE, 
                    src.ROL_ADVREPAUTHOR, 
                    src.ROL_ADVREPCONSUMER, 
                    src.ROL_ANALYTICS, 
                    src.ROL_APPROVER, 
                    src.ROL_BARCODE, 
                    src.ROL_BUYER, 
                    src.ROL_CODE, 
                    src.ROL_CONNECTOR, 
                    src.ROL_DEFAULTORG, 
                    src.ROL_DESC, 
                    src.ROL_EWSFIRSTFUNC, 
                    src.ROL_FIRSTFUNC, 
                    src.ROL_GROUP, 
                    src.ROL_INVAPPVLIMIT, 
                    src.ROL_INVAPPVLIMITNONPO, 
                    src.ROL_LANG, 
                    src.ROL_LASTSAVED, 
                    src.ROL_LOCALE, 
                    src.ROL_MOBILE, 
                    src.ROL_MOBILEADM, 
                    src.ROL_MRC, 
                    src.ROL_PICAPPVLIMIT, 
                    src.ROL_PORDAPPVLIMIT, 
                    src.ROL_PORDAUTHAPPVLIMIT, 
                    src.ROL_REQAPPVLIMIT, 
                    src.ROL_REQAUTHAPPVLIMIT, 
                    src.ROL_REQUESTOR, 
                    src.ROL_ROUTER, 
                    src.ROL_SUCCESSMSGTIMEOUT, 
                    src.ROL_UPDATECOUNT, 
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
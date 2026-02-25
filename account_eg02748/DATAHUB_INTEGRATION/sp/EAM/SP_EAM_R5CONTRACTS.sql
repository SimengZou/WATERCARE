create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CONTRACTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CONTRACTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CONTRACTS as target using (SELECT * FROM (SELECT 
            strm.CON_CLASS, 
            strm.CON_CLASS_ORG, 
            strm.CON_CODE, 
            strm.CON_CONTACT, 
            strm.CON_COPY, 
            strm.CON_CREATE, 
            strm.CON_CURR, 
            strm.CON_DESC, 
            strm.CON_END, 
            strm.CON_LANG, 
            strm.CON_LASTSAVED, 
            strm.CON_ORG, 
            strm.CON_OWN, 
            strm.CON_OWNCONTACT, 
            strm.CON_PRINTED, 
            strm.CON_REF, 
            strm.CON_RENEW, 
            strm.CON_RSTATUS, 
            strm.CON_START, 
            strm.CON_STATUS, 
            strm.CON_STORE, 
            strm.CON_SUPPLIER, 
            strm.CON_SUPPLIER_ORG, 
            strm.CON_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CON_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONTRACTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CON_CODE=src.CON_CODE) OR (target.CON_CODE IS NULL AND src.CON_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CON_CLASS=src.CON_CLASS, 
                    target.CON_CLASS_ORG=src.CON_CLASS_ORG, 
                    target.CON_CODE=src.CON_CODE, 
                    target.CON_CONTACT=src.CON_CONTACT, 
                    target.CON_COPY=src.CON_COPY, 
                    target.CON_CREATE=src.CON_CREATE, 
                    target.CON_CURR=src.CON_CURR, 
                    target.CON_DESC=src.CON_DESC, 
                    target.CON_END=src.CON_END, 
                    target.CON_LANG=src.CON_LANG, 
                    target.CON_LASTSAVED=src.CON_LASTSAVED, 
                    target.CON_ORG=src.CON_ORG, 
                    target.CON_OWN=src.CON_OWN, 
                    target.CON_OWNCONTACT=src.CON_OWNCONTACT, 
                    target.CON_PRINTED=src.CON_PRINTED, 
                    target.CON_REF=src.CON_REF, 
                    target.CON_RENEW=src.CON_RENEW, 
                    target.CON_RSTATUS=src.CON_RSTATUS, 
                    target.CON_START=src.CON_START, 
                    target.CON_STATUS=src.CON_STATUS, 
                    target.CON_STORE=src.CON_STORE, 
                    target.CON_SUPPLIER=src.CON_SUPPLIER, 
                    target.CON_SUPPLIER_ORG=src.CON_SUPPLIER_ORG, 
                    target.CON_UPDATECOUNT=src.CON_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CON_CLASS, 
                    CON_CLASS_ORG, 
                    CON_CODE, 
                    CON_CONTACT, 
                    CON_COPY, 
                    CON_CREATE, 
                    CON_CURR, 
                    CON_DESC, 
                    CON_END, 
                    CON_LANG, 
                    CON_LASTSAVED, 
                    CON_ORG, 
                    CON_OWN, 
                    CON_OWNCONTACT, 
                    CON_PRINTED, 
                    CON_REF, 
                    CON_RENEW, 
                    CON_RSTATUS, 
                    CON_START, 
                    CON_STATUS, 
                    CON_STORE, 
                    CON_SUPPLIER, 
                    CON_SUPPLIER_ORG, 
                    CON_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CON_CLASS, 
                    src.CON_CLASS_ORG, 
                    src.CON_CODE, 
                    src.CON_CONTACT, 
                    src.CON_COPY, 
                    src.CON_CREATE, 
                    src.CON_CURR, 
                    src.CON_DESC, 
                    src.CON_END, 
                    src.CON_LANG, 
                    src.CON_LASTSAVED, 
                    src.CON_ORG, 
                    src.CON_OWN, 
                    src.CON_OWNCONTACT, 
                    src.CON_PRINTED, 
                    src.CON_REF, 
                    src.CON_RENEW, 
                    src.CON_RSTATUS, 
                    src.CON_START, 
                    src.CON_STATUS, 
                    src.CON_STORE, 
                    src.CON_SUPPLIER, 
                    src.CON_SUPPLIER_ORG, 
                    src.CON_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CONTRACTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CON_CLASS, 
            strm.CON_CLASS_ORG, 
            strm.CON_CODE, 
            strm.CON_CONTACT, 
            strm.CON_COPY, 
            strm.CON_CREATE, 
            strm.CON_CURR, 
            strm.CON_DESC, 
            strm.CON_END, 
            strm.CON_LANG, 
            strm.CON_LASTSAVED, 
            strm.CON_ORG, 
            strm.CON_OWN, 
            strm.CON_OWNCONTACT, 
            strm.CON_PRINTED, 
            strm.CON_REF, 
            strm.CON_RENEW, 
            strm.CON_RSTATUS, 
            strm.CON_START, 
            strm.CON_STATUS, 
            strm.CON_STORE, 
            strm.CON_SUPPLIER, 
            strm.CON_SUPPLIER_ORG, 
            strm.CON_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CON_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONTRACTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CON_CODE=src.CON_CODE) OR (target.CON_CODE IS NULL AND src.CON_CODE IS NULL)) 
                when matched then update set
                    target.CON_CLASS=src.CON_CLASS, 
                    target.CON_CLASS_ORG=src.CON_CLASS_ORG, 
                    target.CON_CODE=src.CON_CODE, 
                    target.CON_CONTACT=src.CON_CONTACT, 
                    target.CON_COPY=src.CON_COPY, 
                    target.CON_CREATE=src.CON_CREATE, 
                    target.CON_CURR=src.CON_CURR, 
                    target.CON_DESC=src.CON_DESC, 
                    target.CON_END=src.CON_END, 
                    target.CON_LANG=src.CON_LANG, 
                    target.CON_LASTSAVED=src.CON_LASTSAVED, 
                    target.CON_ORG=src.CON_ORG, 
                    target.CON_OWN=src.CON_OWN, 
                    target.CON_OWNCONTACT=src.CON_OWNCONTACT, 
                    target.CON_PRINTED=src.CON_PRINTED, 
                    target.CON_REF=src.CON_REF, 
                    target.CON_RENEW=src.CON_RENEW, 
                    target.CON_RSTATUS=src.CON_RSTATUS, 
                    target.CON_START=src.CON_START, 
                    target.CON_STATUS=src.CON_STATUS, 
                    target.CON_STORE=src.CON_STORE, 
                    target.CON_SUPPLIER=src.CON_SUPPLIER, 
                    target.CON_SUPPLIER_ORG=src.CON_SUPPLIER_ORG, 
                    target.CON_UPDATECOUNT=src.CON_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CON_CLASS, CON_CLASS_ORG, CON_CODE, CON_CONTACT, CON_COPY, CON_CREATE, CON_CURR, CON_DESC, CON_END, CON_LANG, CON_LASTSAVED, CON_ORG, CON_OWN, CON_OWNCONTACT, CON_PRINTED, CON_REF, CON_RENEW, CON_RSTATUS, CON_START, CON_STATUS, CON_STORE, CON_SUPPLIER, CON_SUPPLIER_ORG, CON_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CON_CLASS, 
                    src.CON_CLASS_ORG, 
                    src.CON_CODE, 
                    src.CON_CONTACT, 
                    src.CON_COPY, 
                    src.CON_CREATE, 
                    src.CON_CURR, 
                    src.CON_DESC, 
                    src.CON_END, 
                    src.CON_LANG, 
                    src.CON_LASTSAVED, 
                    src.CON_ORG, 
                    src.CON_OWN, 
                    src.CON_OWNCONTACT, 
                    src.CON_PRINTED, 
                    src.CON_REF, 
                    src.CON_RENEW, 
                    src.CON_RSTATUS, 
                    src.CON_START, 
                    src.CON_STATUS, 
                    src.CON_STORE, 
                    src.CON_SUPPLIER, 
                    src.CON_SUPPLIER_ORG, 
                    src.CON_UPDATECOUNT, 
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
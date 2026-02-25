create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DOCKRECEIPTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DOCKRECEIPTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DOCKRECEIPTS as target using (SELECT * FROM (SELECT 
            strm.DCK_CLASS, 
            strm.DCK_CLASS_ORG, 
            strm.DCK_CODE, 
            strm.DCK_CREATED, 
            strm.DCK_DESC, 
            strm.DCK_DOCKID, 
            strm.DCK_FREIGHT, 
            strm.DCK_LASTSAVED, 
            strm.DCK_LOCATION, 
            strm.DCK_MRC, 
            strm.DCK_ORDER, 
            strm.DCK_ORDER_ORG, 
            strm.DCK_ORG, 
            strm.DCK_PACKSLIP, 
            strm.DCK_RECEIVER, 
            strm.DCK_RECVDATE, 
            strm.DCK_REFERENCE, 
            strm.DCK_RETRIEVEALL, 
            strm.DCK_RSTATUS, 
            strm.DCK_SHIPVIA, 
            strm.DCK_STATUS, 
            strm.DCK_SUPPLIER, 
            strm.DCK_SUPPLIER_ORG, 
            strm.DCK_UPDATECOUNT, 
            strm.DCK_UPDATED, 
            strm.DCK_UPDUSER, 
            strm.DCK_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DCK_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCKRECEIPTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DCK_CODE=src.DCK_CODE) OR (target.DCK_CODE IS NULL AND src.DCK_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.DCK_CLASS=src.DCK_CLASS, 
                    target.DCK_CLASS_ORG=src.DCK_CLASS_ORG, 
                    target.DCK_CODE=src.DCK_CODE, 
                    target.DCK_CREATED=src.DCK_CREATED, 
                    target.DCK_DESC=src.DCK_DESC, 
                    target.DCK_DOCKID=src.DCK_DOCKID, 
                    target.DCK_FREIGHT=src.DCK_FREIGHT, 
                    target.DCK_LASTSAVED=src.DCK_LASTSAVED, 
                    target.DCK_LOCATION=src.DCK_LOCATION, 
                    target.DCK_MRC=src.DCK_MRC, 
                    target.DCK_ORDER=src.DCK_ORDER, 
                    target.DCK_ORDER_ORG=src.DCK_ORDER_ORG, 
                    target.DCK_ORG=src.DCK_ORG, 
                    target.DCK_PACKSLIP=src.DCK_PACKSLIP, 
                    target.DCK_RECEIVER=src.DCK_RECEIVER, 
                    target.DCK_RECVDATE=src.DCK_RECVDATE, 
                    target.DCK_REFERENCE=src.DCK_REFERENCE, 
                    target.DCK_RETRIEVEALL=src.DCK_RETRIEVEALL, 
                    target.DCK_RSTATUS=src.DCK_RSTATUS, 
                    target.DCK_SHIPVIA=src.DCK_SHIPVIA, 
                    target.DCK_STATUS=src.DCK_STATUS, 
                    target.DCK_SUPPLIER=src.DCK_SUPPLIER, 
                    target.DCK_SUPPLIER_ORG=src.DCK_SUPPLIER_ORG, 
                    target.DCK_UPDATECOUNT=src.DCK_UPDATECOUNT, 
                    target.DCK_UPDATED=src.DCK_UPDATED, 
                    target.DCK_UPDUSER=src.DCK_UPDUSER, 
                    target.DCK_USER=src.DCK_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    DCK_CLASS, 
                    DCK_CLASS_ORG, 
                    DCK_CODE, 
                    DCK_CREATED, 
                    DCK_DESC, 
                    DCK_DOCKID, 
                    DCK_FREIGHT, 
                    DCK_LASTSAVED, 
                    DCK_LOCATION, 
                    DCK_MRC, 
                    DCK_ORDER, 
                    DCK_ORDER_ORG, 
                    DCK_ORG, 
                    DCK_PACKSLIP, 
                    DCK_RECEIVER, 
                    DCK_RECVDATE, 
                    DCK_REFERENCE, 
                    DCK_RETRIEVEALL, 
                    DCK_RSTATUS, 
                    DCK_SHIPVIA, 
                    DCK_STATUS, 
                    DCK_SUPPLIER, 
                    DCK_SUPPLIER_ORG, 
                    DCK_UPDATECOUNT, 
                    DCK_UPDATED, 
                    DCK_UPDUSER, 
                    DCK_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.DCK_CLASS, 
                    src.DCK_CLASS_ORG, 
                    src.DCK_CODE, 
                    src.DCK_CREATED, 
                    src.DCK_DESC, 
                    src.DCK_DOCKID, 
                    src.DCK_FREIGHT, 
                    src.DCK_LASTSAVED, 
                    src.DCK_LOCATION, 
                    src.DCK_MRC, 
                    src.DCK_ORDER, 
                    src.DCK_ORDER_ORG, 
                    src.DCK_ORG, 
                    src.DCK_PACKSLIP, 
                    src.DCK_RECEIVER, 
                    src.DCK_RECVDATE, 
                    src.DCK_REFERENCE, 
                    src.DCK_RETRIEVEALL, 
                    src.DCK_RSTATUS, 
                    src.DCK_SHIPVIA, 
                    src.DCK_STATUS, 
                    src.DCK_SUPPLIER, 
                    src.DCK_SUPPLIER_ORG, 
                    src.DCK_UPDATECOUNT, 
                    src.DCK_UPDATED, 
                    src.DCK_UPDUSER, 
                    src.DCK_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DOCKRECEIPTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.DCK_CLASS, 
            strm.DCK_CLASS_ORG, 
            strm.DCK_CODE, 
            strm.DCK_CREATED, 
            strm.DCK_DESC, 
            strm.DCK_DOCKID, 
            strm.DCK_FREIGHT, 
            strm.DCK_LASTSAVED, 
            strm.DCK_LOCATION, 
            strm.DCK_MRC, 
            strm.DCK_ORDER, 
            strm.DCK_ORDER_ORG, 
            strm.DCK_ORG, 
            strm.DCK_PACKSLIP, 
            strm.DCK_RECEIVER, 
            strm.DCK_RECVDATE, 
            strm.DCK_REFERENCE, 
            strm.DCK_RETRIEVEALL, 
            strm.DCK_RSTATUS, 
            strm.DCK_SHIPVIA, 
            strm.DCK_STATUS, 
            strm.DCK_SUPPLIER, 
            strm.DCK_SUPPLIER_ORG, 
            strm.DCK_UPDATECOUNT, 
            strm.DCK_UPDATED, 
            strm.DCK_UPDUSER, 
            strm.DCK_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DCK_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCKRECEIPTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DCK_CODE=src.DCK_CODE) OR (target.DCK_CODE IS NULL AND src.DCK_CODE IS NULL)) 
                when matched then update set
                    target.DCK_CLASS=src.DCK_CLASS, 
                    target.DCK_CLASS_ORG=src.DCK_CLASS_ORG, 
                    target.DCK_CODE=src.DCK_CODE, 
                    target.DCK_CREATED=src.DCK_CREATED, 
                    target.DCK_DESC=src.DCK_DESC, 
                    target.DCK_DOCKID=src.DCK_DOCKID, 
                    target.DCK_FREIGHT=src.DCK_FREIGHT, 
                    target.DCK_LASTSAVED=src.DCK_LASTSAVED, 
                    target.DCK_LOCATION=src.DCK_LOCATION, 
                    target.DCK_MRC=src.DCK_MRC, 
                    target.DCK_ORDER=src.DCK_ORDER, 
                    target.DCK_ORDER_ORG=src.DCK_ORDER_ORG, 
                    target.DCK_ORG=src.DCK_ORG, 
                    target.DCK_PACKSLIP=src.DCK_PACKSLIP, 
                    target.DCK_RECEIVER=src.DCK_RECEIVER, 
                    target.DCK_RECVDATE=src.DCK_RECVDATE, 
                    target.DCK_REFERENCE=src.DCK_REFERENCE, 
                    target.DCK_RETRIEVEALL=src.DCK_RETRIEVEALL, 
                    target.DCK_RSTATUS=src.DCK_RSTATUS, 
                    target.DCK_SHIPVIA=src.DCK_SHIPVIA, 
                    target.DCK_STATUS=src.DCK_STATUS, 
                    target.DCK_SUPPLIER=src.DCK_SUPPLIER, 
                    target.DCK_SUPPLIER_ORG=src.DCK_SUPPLIER_ORG, 
                    target.DCK_UPDATECOUNT=src.DCK_UPDATECOUNT, 
                    target.DCK_UPDATED=src.DCK_UPDATED, 
                    target.DCK_UPDUSER=src.DCK_UPDUSER, 
                    target.DCK_USER=src.DCK_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( DCK_CLASS, DCK_CLASS_ORG, DCK_CODE, DCK_CREATED, DCK_DESC, DCK_DOCKID, DCK_FREIGHT, DCK_LASTSAVED, DCK_LOCATION, DCK_MRC, DCK_ORDER, DCK_ORDER_ORG, DCK_ORG, DCK_PACKSLIP, DCK_RECEIVER, DCK_RECVDATE, DCK_REFERENCE, DCK_RETRIEVEALL, DCK_RSTATUS, DCK_SHIPVIA, DCK_STATUS, DCK_SUPPLIER, DCK_SUPPLIER_ORG, DCK_UPDATECOUNT, DCK_UPDATED, DCK_UPDUSER, DCK_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.DCK_CLASS, 
                    src.DCK_CLASS_ORG, 
                    src.DCK_CODE, 
                    src.DCK_CREATED, 
                    src.DCK_DESC, 
                    src.DCK_DOCKID, 
                    src.DCK_FREIGHT, 
                    src.DCK_LASTSAVED, 
                    src.DCK_LOCATION, 
                    src.DCK_MRC, 
                    src.DCK_ORDER, 
                    src.DCK_ORDER_ORG, 
                    src.DCK_ORG, 
                    src.DCK_PACKSLIP, 
                    src.DCK_RECEIVER, 
                    src.DCK_RECVDATE, 
                    src.DCK_REFERENCE, 
                    src.DCK_RETRIEVEALL, 
                    src.DCK_RSTATUS, 
                    src.DCK_SHIPVIA, 
                    src.DCK_STATUS, 
                    src.DCK_SUPPLIER, 
                    src.DCK_SUPPLIER_ORG, 
                    src.DCK_UPDATECOUNT, 
                    src.DCK_UPDATED, 
                    src.DCK_UPDUSER, 
                    src.DCK_USER, 
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
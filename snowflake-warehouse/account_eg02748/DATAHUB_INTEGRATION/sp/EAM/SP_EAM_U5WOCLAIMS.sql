create or replace procedure DATAHUB_INTEGRATION.SP_EAM_U5WOCLAIMS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_U5WOCLAIMS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_U5WOCLAIMS as target using (SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm.WCO_ACTIVITY, 
            strm.WCO_ACTIVITY_DESC, 
            strm.WCO_CHARGEDATE, 
            strm.WCO_COMMENTS, 
            strm.WCO_COMMENTS_INT, 
            strm.WCO_CONTRACTOR, 
            strm.WCO_CONTRACTOR_QTY, 
            strm.WCO_CONTRACTOR_RATE, 
            strm.WCO_CONTRACT_CODE, 
            strm.WCO_DERPROJACT, 
            strm.WCO_DERPROJNUM, 
            strm.WCO_EVENT, 
            strm.WCO_EXPPROJACT, 
            strm.WCO_EXPPROJNUM, 
            strm.WCO_LINETOTAL, 
            strm.WCO_LINE_STATUS, 
            strm.WCO_ORG, 
            strm.WCO_PK, 
            strm.WCO_PROCESSED_ERROR, 
            strm.WCO_PROCESSED_STATUS, 
            strm.WCO_SCHEDITEM_DESC, 
            strm.WCO_SCHEDITEM_RATE, 
            strm.WCO_SCHEDULE_ITEM, 
            strm.WCO_TRANSID, 
            strm.WCO_TRANS_FLAG, 
            strm.WCO_WOPARENT, 
            strm.WCO_WOSCHEDITEM, 
            strm.WCO_WOTYPE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WCO_EVENT,
            strm.WCO_PK,
            strm.WCO_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5WOCLAIMS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WCO_EVENT=src.WCO_EVENT) OR (target.WCO_EVENT IS NULL AND src.WCO_EVENT IS NULL)) AND
            ((target.WCO_PK=src.WCO_PK) OR (target.WCO_PK IS NULL AND src.WCO_PK IS NULL)) AND
            ((target.WCO_ORG=src.WCO_ORG) OR (target.WCO_ORG IS NULL AND src.WCO_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target.WCO_ACTIVITY=src.WCO_ACTIVITY, 
                    target.WCO_ACTIVITY_DESC=src.WCO_ACTIVITY_DESC, 
                    target.WCO_CHARGEDATE=src.WCO_CHARGEDATE, 
                    target.WCO_COMMENTS=src.WCO_COMMENTS, 
                    target.WCO_COMMENTS_INT=src.WCO_COMMENTS_INT, 
                    target.WCO_CONTRACTOR=src.WCO_CONTRACTOR, 
                    target.WCO_CONTRACTOR_QTY=src.WCO_CONTRACTOR_QTY, 
                    target.WCO_CONTRACTOR_RATE=src.WCO_CONTRACTOR_RATE, 
                    target.WCO_CONTRACT_CODE=src.WCO_CONTRACT_CODE, 
                    target.WCO_DERPROJACT=src.WCO_DERPROJACT, 
                    target.WCO_DERPROJNUM=src.WCO_DERPROJNUM, 
                    target.WCO_EVENT=src.WCO_EVENT, 
                    target.WCO_EXPPROJACT=src.WCO_EXPPROJACT, 
                    target.WCO_EXPPROJNUM=src.WCO_EXPPROJNUM, 
                    target.WCO_LINETOTAL=src.WCO_LINETOTAL, 
                    target.WCO_LINE_STATUS=src.WCO_LINE_STATUS, 
                    target.WCO_ORG=src.WCO_ORG, 
                    target.WCO_PK=src.WCO_PK, 
                    target.WCO_PROCESSED_ERROR=src.WCO_PROCESSED_ERROR, 
                    target.WCO_PROCESSED_STATUS=src.WCO_PROCESSED_STATUS, 
                    target.WCO_SCHEDITEM_DESC=src.WCO_SCHEDITEM_DESC, 
                    target.WCO_SCHEDITEM_RATE=src.WCO_SCHEDITEM_RATE, 
                    target.WCO_SCHEDULE_ITEM=src.WCO_SCHEDULE_ITEM, 
                    target.WCO_TRANSID=src.WCO_TRANSID, 
                    target.WCO_TRANS_FLAG=src.WCO_TRANS_FLAG, 
                    target.WCO_WOPARENT=src.WCO_WOPARENT, 
                    target.WCO_WOSCHEDITEM=src.WCO_WOSCHEDITEM, 
                    target.WCO_WOTYPE=src.WCO_WOTYPE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CREATED, 
                    CREATEDBY, 
                    LASTSAVED, 
                    UPDATECOUNT, 
                    UPDATED, 
                    UPDATEDBY, 
                    WCO_ACTIVITY, 
                    WCO_ACTIVITY_DESC, 
                    WCO_CHARGEDATE, 
                    WCO_COMMENTS, 
                    WCO_COMMENTS_INT, 
                    WCO_CONTRACTOR, 
                    WCO_CONTRACTOR_QTY, 
                    WCO_CONTRACTOR_RATE, 
                    WCO_CONTRACT_CODE, 
                    WCO_DERPROJACT, 
                    WCO_DERPROJNUM, 
                    WCO_EVENT, 
                    WCO_EXPPROJACT, 
                    WCO_EXPPROJNUM, 
                    WCO_LINETOTAL, 
                    WCO_LINE_STATUS, 
                    WCO_ORG, 
                    WCO_PK, 
                    WCO_PROCESSED_ERROR, 
                    WCO_PROCESSED_STATUS, 
                    WCO_SCHEDITEM_DESC, 
                    WCO_SCHEDITEM_RATE, 
                    WCO_SCHEDULE_ITEM, 
                    WCO_TRANSID, 
                    WCO_TRANS_FLAG, 
                    WCO_WOPARENT, 
                    WCO_WOSCHEDITEM, 
                    WCO_WOTYPE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src.WCO_ACTIVITY, 
                    src.WCO_ACTIVITY_DESC, 
                    src.WCO_CHARGEDATE, 
                    src.WCO_COMMENTS, 
                    src.WCO_COMMENTS_INT, 
                    src.WCO_CONTRACTOR, 
                    src.WCO_CONTRACTOR_QTY, 
                    src.WCO_CONTRACTOR_RATE, 
                    src.WCO_CONTRACT_CODE, 
                    src.WCO_DERPROJACT, 
                    src.WCO_DERPROJNUM, 
                    src.WCO_EVENT, 
                    src.WCO_EXPPROJACT, 
                    src.WCO_EXPPROJNUM, 
                    src.WCO_LINETOTAL, 
                    src.WCO_LINE_STATUS, 
                    src.WCO_ORG, 
                    src.WCO_PK, 
                    src.WCO_PROCESSED_ERROR, 
                    src.WCO_PROCESSED_STATUS, 
                    src.WCO_SCHEDITEM_DESC, 
                    src.WCO_SCHEDITEM_RATE, 
                    src.WCO_SCHEDULE_ITEM, 
                    src.WCO_TRANSID, 
                    src.WCO_TRANS_FLAG, 
                    src.WCO_WOPARENT, 
                    src.WCO_WOSCHEDITEM, 
                    src.WCO_WOTYPE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_U5WOCLAIMS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm.WCO_ACTIVITY, 
            strm.WCO_ACTIVITY_DESC, 
            strm.WCO_CHARGEDATE, 
            strm.WCO_COMMENTS, 
            strm.WCO_COMMENTS_INT, 
            strm.WCO_CONTRACTOR, 
            strm.WCO_CONTRACTOR_QTY, 
            strm.WCO_CONTRACTOR_RATE, 
            strm.WCO_CONTRACT_CODE, 
            strm.WCO_DERPROJACT, 
            strm.WCO_DERPROJNUM, 
            strm.WCO_EVENT, 
            strm.WCO_EXPPROJACT, 
            strm.WCO_EXPPROJNUM, 
            strm.WCO_LINETOTAL, 
            strm.WCO_LINE_STATUS, 
            strm.WCO_ORG, 
            strm.WCO_PK, 
            strm.WCO_PROCESSED_ERROR, 
            strm.WCO_PROCESSED_STATUS, 
            strm.WCO_SCHEDITEM_DESC, 
            strm.WCO_SCHEDITEM_RATE, 
            strm.WCO_SCHEDULE_ITEM, 
            strm.WCO_TRANSID, 
            strm.WCO_TRANS_FLAG, 
            strm.WCO_WOPARENT, 
            strm.WCO_WOSCHEDITEM, 
            strm.WCO_WOTYPE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WCO_EVENT,
            strm.WCO_PK,
            strm.WCO_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5WOCLAIMS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WCO_EVENT=src.WCO_EVENT) OR (target.WCO_EVENT IS NULL AND src.WCO_EVENT IS NULL)) AND
            ((target.WCO_PK=src.WCO_PK) OR (target.WCO_PK IS NULL AND src.WCO_PK IS NULL)) AND
            ((target.WCO_ORG=src.WCO_ORG) OR (target.WCO_ORG IS NULL AND src.WCO_ORG IS NULL)) 
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target.WCO_ACTIVITY=src.WCO_ACTIVITY, 
                    target.WCO_ACTIVITY_DESC=src.WCO_ACTIVITY_DESC, 
                    target.WCO_CHARGEDATE=src.WCO_CHARGEDATE, 
                    target.WCO_COMMENTS=src.WCO_COMMENTS, 
                    target.WCO_COMMENTS_INT=src.WCO_COMMENTS_INT, 
                    target.WCO_CONTRACTOR=src.WCO_CONTRACTOR, 
                    target.WCO_CONTRACTOR_QTY=src.WCO_CONTRACTOR_QTY, 
                    target.WCO_CONTRACTOR_RATE=src.WCO_CONTRACTOR_RATE, 
                    target.WCO_CONTRACT_CODE=src.WCO_CONTRACT_CODE, 
                    target.WCO_DERPROJACT=src.WCO_DERPROJACT, 
                    target.WCO_DERPROJNUM=src.WCO_DERPROJNUM, 
                    target.WCO_EVENT=src.WCO_EVENT, 
                    target.WCO_EXPPROJACT=src.WCO_EXPPROJACT, 
                    target.WCO_EXPPROJNUM=src.WCO_EXPPROJNUM, 
                    target.WCO_LINETOTAL=src.WCO_LINETOTAL, 
                    target.WCO_LINE_STATUS=src.WCO_LINE_STATUS, 
                    target.WCO_ORG=src.WCO_ORG, 
                    target.WCO_PK=src.WCO_PK, 
                    target.WCO_PROCESSED_ERROR=src.WCO_PROCESSED_ERROR, 
                    target.WCO_PROCESSED_STATUS=src.WCO_PROCESSED_STATUS, 
                    target.WCO_SCHEDITEM_DESC=src.WCO_SCHEDITEM_DESC, 
                    target.WCO_SCHEDITEM_RATE=src.WCO_SCHEDITEM_RATE, 
                    target.WCO_SCHEDULE_ITEM=src.WCO_SCHEDULE_ITEM, 
                    target.WCO_TRANSID=src.WCO_TRANSID, 
                    target.WCO_TRANS_FLAG=src.WCO_TRANS_FLAG, 
                    target.WCO_WOPARENT=src.WCO_WOPARENT, 
                    target.WCO_WOSCHEDITEM=src.WCO_WOSCHEDITEM, 
                    target.WCO_WOTYPE=src.WCO_WOTYPE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CREATED, CREATEDBY, LASTSAVED, UPDATECOUNT, UPDATED, UPDATEDBY, WCO_ACTIVITY, WCO_ACTIVITY_DESC, WCO_CHARGEDATE, WCO_COMMENTS, WCO_COMMENTS_INT, WCO_CONTRACTOR, WCO_CONTRACTOR_QTY, WCO_CONTRACTOR_RATE, WCO_CONTRACT_CODE, WCO_DERPROJACT, WCO_DERPROJNUM, WCO_EVENT, WCO_EXPPROJACT, WCO_EXPPROJNUM, WCO_LINETOTAL, WCO_LINE_STATUS, WCO_ORG, WCO_PK, WCO_PROCESSED_ERROR, WCO_PROCESSED_STATUS, WCO_SCHEDITEM_DESC, WCO_SCHEDITEM_RATE, WCO_SCHEDULE_ITEM, WCO_TRANSID, WCO_TRANS_FLAG, WCO_WOPARENT, WCO_WOSCHEDITEM, WCO_WOTYPE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src.WCO_ACTIVITY, 
                    src.WCO_ACTIVITY_DESC, 
                    src.WCO_CHARGEDATE, 
                    src.WCO_COMMENTS, 
                    src.WCO_COMMENTS_INT, 
                    src.WCO_CONTRACTOR, 
                    src.WCO_CONTRACTOR_QTY, 
                    src.WCO_CONTRACTOR_RATE, 
                    src.WCO_CONTRACT_CODE, 
                    src.WCO_DERPROJACT, 
                    src.WCO_DERPROJNUM, 
                    src.WCO_EVENT, 
                    src.WCO_EXPPROJACT, 
                    src.WCO_EXPPROJNUM, 
                    src.WCO_LINETOTAL, 
                    src.WCO_LINE_STATUS, 
                    src.WCO_ORG, 
                    src.WCO_PK, 
                    src.WCO_PROCESSED_ERROR, 
                    src.WCO_PROCESSED_STATUS, 
                    src.WCO_SCHEDITEM_DESC, 
                    src.WCO_SCHEDITEM_RATE, 
                    src.WCO_SCHEDULE_ITEM, 
                    src.WCO_TRANSID, 
                    src.WCO_TRANS_FLAG, 
                    src.WCO_WOPARENT, 
                    src.WCO_WOSCHEDITEM, 
                    src.WCO_WOTYPE, 
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
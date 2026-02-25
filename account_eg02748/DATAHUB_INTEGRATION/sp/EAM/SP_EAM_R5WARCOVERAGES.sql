create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5WARCOVERAGES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5WARCOVERAGES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5WARCOVERAGES as target using (SELECT * FROM (SELECT 
            strm.WCV_ACTIVE, 
            strm.WCV_CREATED, 
            strm.WCV_DURATION, 
            strm.WCV_EXPIRATION, 
            strm.WCV_EXPIRATIONDATE, 
            strm.WCV_LASTSAVED, 
            strm.WCV_NEARTHRESHOLD, 
            strm.WCV_OBJECT, 
            strm.WCV_OBJECT_ORG, 
            strm.WCV_OBRTYPE, 
            strm.WCV_OBTYPE, 
            strm.WCV_RECORDED, 
            strm.WCV_SEQNO, 
            strm.WCV_STARTDATE, 
            strm.WCV_STARTUSAGE, 
            strm.WCV_UOM, 
            strm.WCV_UPDATECOUNT, 
            strm.WCV_UPDATED, 
            strm.WCV_USER, 
            strm.WCV_WARRANTY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WCV_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WARCOVERAGES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WCV_SEQNO=src.WCV_SEQNO) OR (target.WCV_SEQNO IS NULL AND src.WCV_SEQNO IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.WCV_ACTIVE=src.WCV_ACTIVE, 
                    target.WCV_CREATED=src.WCV_CREATED, 
                    target.WCV_DURATION=src.WCV_DURATION, 
                    target.WCV_EXPIRATION=src.WCV_EXPIRATION, 
                    target.WCV_EXPIRATIONDATE=src.WCV_EXPIRATIONDATE, 
                    target.WCV_LASTSAVED=src.WCV_LASTSAVED, 
                    target.WCV_NEARTHRESHOLD=src.WCV_NEARTHRESHOLD, 
                    target.WCV_OBJECT=src.WCV_OBJECT, 
                    target.WCV_OBJECT_ORG=src.WCV_OBJECT_ORG, 
                    target.WCV_OBRTYPE=src.WCV_OBRTYPE, 
                    target.WCV_OBTYPE=src.WCV_OBTYPE, 
                    target.WCV_RECORDED=src.WCV_RECORDED, 
                    target.WCV_SEQNO=src.WCV_SEQNO, 
                    target.WCV_STARTDATE=src.WCV_STARTDATE, 
                    target.WCV_STARTUSAGE=src.WCV_STARTUSAGE, 
                    target.WCV_UOM=src.WCV_UOM, 
                    target.WCV_UPDATECOUNT=src.WCV_UPDATECOUNT, 
                    target.WCV_UPDATED=src.WCV_UPDATED, 
                    target.WCV_USER=src.WCV_USER, 
                    target.WCV_WARRANTY=src.WCV_WARRANTY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    WCV_ACTIVE, 
                    WCV_CREATED, 
                    WCV_DURATION, 
                    WCV_EXPIRATION, 
                    WCV_EXPIRATIONDATE, 
                    WCV_LASTSAVED, 
                    WCV_NEARTHRESHOLD, 
                    WCV_OBJECT, 
                    WCV_OBJECT_ORG, 
                    WCV_OBRTYPE, 
                    WCV_OBTYPE, 
                    WCV_RECORDED, 
                    WCV_SEQNO, 
                    WCV_STARTDATE, 
                    WCV_STARTUSAGE, 
                    WCV_UOM, 
                    WCV_UPDATECOUNT, 
                    WCV_UPDATED, 
                    WCV_USER, 
                    WCV_WARRANTY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.WCV_ACTIVE, 
                    src.WCV_CREATED, 
                    src.WCV_DURATION, 
                    src.WCV_EXPIRATION, 
                    src.WCV_EXPIRATIONDATE, 
                    src.WCV_LASTSAVED, 
                    src.WCV_NEARTHRESHOLD, 
                    src.WCV_OBJECT, 
                    src.WCV_OBJECT_ORG, 
                    src.WCV_OBRTYPE, 
                    src.WCV_OBTYPE, 
                    src.WCV_RECORDED, 
                    src.WCV_SEQNO, 
                    src.WCV_STARTDATE, 
                    src.WCV_STARTUSAGE, 
                    src.WCV_UOM, 
                    src.WCV_UPDATECOUNT, 
                    src.WCV_UPDATED, 
                    src.WCV_USER, 
                    src.WCV_WARRANTY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5WARCOVERAGES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.WCV_ACTIVE, 
            strm.WCV_CREATED, 
            strm.WCV_DURATION, 
            strm.WCV_EXPIRATION, 
            strm.WCV_EXPIRATIONDATE, 
            strm.WCV_LASTSAVED, 
            strm.WCV_NEARTHRESHOLD, 
            strm.WCV_OBJECT, 
            strm.WCV_OBJECT_ORG, 
            strm.WCV_OBRTYPE, 
            strm.WCV_OBTYPE, 
            strm.WCV_RECORDED, 
            strm.WCV_SEQNO, 
            strm.WCV_STARTDATE, 
            strm.WCV_STARTUSAGE, 
            strm.WCV_UOM, 
            strm.WCV_UPDATECOUNT, 
            strm.WCV_UPDATED, 
            strm.WCV_USER, 
            strm.WCV_WARRANTY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WCV_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WARCOVERAGES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WCV_SEQNO=src.WCV_SEQNO) OR (target.WCV_SEQNO IS NULL AND src.WCV_SEQNO IS NULL)) 
                when matched then update set
                    target.WCV_ACTIVE=src.WCV_ACTIVE, 
                    target.WCV_CREATED=src.WCV_CREATED, 
                    target.WCV_DURATION=src.WCV_DURATION, 
                    target.WCV_EXPIRATION=src.WCV_EXPIRATION, 
                    target.WCV_EXPIRATIONDATE=src.WCV_EXPIRATIONDATE, 
                    target.WCV_LASTSAVED=src.WCV_LASTSAVED, 
                    target.WCV_NEARTHRESHOLD=src.WCV_NEARTHRESHOLD, 
                    target.WCV_OBJECT=src.WCV_OBJECT, 
                    target.WCV_OBJECT_ORG=src.WCV_OBJECT_ORG, 
                    target.WCV_OBRTYPE=src.WCV_OBRTYPE, 
                    target.WCV_OBTYPE=src.WCV_OBTYPE, 
                    target.WCV_RECORDED=src.WCV_RECORDED, 
                    target.WCV_SEQNO=src.WCV_SEQNO, 
                    target.WCV_STARTDATE=src.WCV_STARTDATE, 
                    target.WCV_STARTUSAGE=src.WCV_STARTUSAGE, 
                    target.WCV_UOM=src.WCV_UOM, 
                    target.WCV_UPDATECOUNT=src.WCV_UPDATECOUNT, 
                    target.WCV_UPDATED=src.WCV_UPDATED, 
                    target.WCV_USER=src.WCV_USER, 
                    target.WCV_WARRANTY=src.WCV_WARRANTY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( WCV_ACTIVE, WCV_CREATED, WCV_DURATION, WCV_EXPIRATION, WCV_EXPIRATIONDATE, WCV_LASTSAVED, WCV_NEARTHRESHOLD, WCV_OBJECT, WCV_OBJECT_ORG, WCV_OBRTYPE, WCV_OBTYPE, WCV_RECORDED, WCV_SEQNO, WCV_STARTDATE, WCV_STARTUSAGE, WCV_UOM, WCV_UPDATECOUNT, WCV_UPDATED, WCV_USER, WCV_WARRANTY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.WCV_ACTIVE, 
                    src.WCV_CREATED, 
                    src.WCV_DURATION, 
                    src.WCV_EXPIRATION, 
                    src.WCV_EXPIRATIONDATE, 
                    src.WCV_LASTSAVED, 
                    src.WCV_NEARTHRESHOLD, 
                    src.WCV_OBJECT, 
                    src.WCV_OBJECT_ORG, 
                    src.WCV_OBRTYPE, 
                    src.WCV_OBTYPE, 
                    src.WCV_RECORDED, 
                    src.WCV_SEQNO, 
                    src.WCV_STARTDATE, 
                    src.WCV_STARTUSAGE, 
                    src.WCV_UOM, 
                    src.WCV_UPDATECOUNT, 
                    src.WCV_UPDATED, 
                    src.WCV_USER, 
                    src.WCV_WARRANTY, 
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
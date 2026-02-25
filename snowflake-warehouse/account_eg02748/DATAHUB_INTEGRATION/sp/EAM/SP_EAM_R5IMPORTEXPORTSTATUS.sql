create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5IMPORTEXPORTSTATUS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5IMPORTEXPORTSTATUS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5IMPORTEXPORTSTATUS as target using (SELECT * FROM (SELECT 
            strm.IES_COMPLETED, 
            strm.IES_DATECREATED, 
            strm.IES_DESC, 
            strm.IES_EMAIL, 
            strm.IES_EMAILSENT, 
            strm.IES_ESTTIMETOEND, 
            strm.IES_ESTTIMETOSTART, 
            strm.IES_FILELOCATION, 
            strm.IES_FILESENT, 
            strm.IES_INCLUDEEMAIL, 
            strm.IES_LASTSAVED, 
            strm.IES_PREVIEW, 
            strm.IES_RECORDCOUNT, 
            strm.IES_SESSIONID, 
            strm.IES_STARTED, 
            strm.IES_STATUS, 
            strm.IES_TYPE, 
            strm.IES_UPDATECOUNT, 
            strm.IES_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.IES_SESSIONID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IMPORTEXPORTSTATUS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.IES_SESSIONID=src.IES_SESSIONID) OR (target.IES_SESSIONID IS NULL AND src.IES_SESSIONID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.IES_COMPLETED=src.IES_COMPLETED, 
                    target.IES_DATECREATED=src.IES_DATECREATED, 
                    target.IES_DESC=src.IES_DESC, 
                    target.IES_EMAIL=src.IES_EMAIL, 
                    target.IES_EMAILSENT=src.IES_EMAILSENT, 
                    target.IES_ESTTIMETOEND=src.IES_ESTTIMETOEND, 
                    target.IES_ESTTIMETOSTART=src.IES_ESTTIMETOSTART, 
                    target.IES_FILELOCATION=src.IES_FILELOCATION, 
                    target.IES_FILESENT=src.IES_FILESENT, 
                    target.IES_INCLUDEEMAIL=src.IES_INCLUDEEMAIL, 
                    target.IES_LASTSAVED=src.IES_LASTSAVED, 
                    target.IES_PREVIEW=src.IES_PREVIEW, 
                    target.IES_RECORDCOUNT=src.IES_RECORDCOUNT, 
                    target.IES_SESSIONID=src.IES_SESSIONID, 
                    target.IES_STARTED=src.IES_STARTED, 
                    target.IES_STATUS=src.IES_STATUS, 
                    target.IES_TYPE=src.IES_TYPE, 
                    target.IES_UPDATECOUNT=src.IES_UPDATECOUNT, 
                    target.IES_USERID=src.IES_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    IES_COMPLETED, 
                    IES_DATECREATED, 
                    IES_DESC, 
                    IES_EMAIL, 
                    IES_EMAILSENT, 
                    IES_ESTTIMETOEND, 
                    IES_ESTTIMETOSTART, 
                    IES_FILELOCATION, 
                    IES_FILESENT, 
                    IES_INCLUDEEMAIL, 
                    IES_LASTSAVED, 
                    IES_PREVIEW, 
                    IES_RECORDCOUNT, 
                    IES_SESSIONID, 
                    IES_STARTED, 
                    IES_STATUS, 
                    IES_TYPE, 
                    IES_UPDATECOUNT, 
                    IES_USERID, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.IES_COMPLETED, 
                    src.IES_DATECREATED, 
                    src.IES_DESC, 
                    src.IES_EMAIL, 
                    src.IES_EMAILSENT, 
                    src.IES_ESTTIMETOEND, 
                    src.IES_ESTTIMETOSTART, 
                    src.IES_FILELOCATION, 
                    src.IES_FILESENT, 
                    src.IES_INCLUDEEMAIL, 
                    src.IES_LASTSAVED, 
                    src.IES_PREVIEW, 
                    src.IES_RECORDCOUNT, 
                    src.IES_SESSIONID, 
                    src.IES_STARTED, 
                    src.IES_STATUS, 
                    src.IES_TYPE, 
                    src.IES_UPDATECOUNT, 
                    src.IES_USERID, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5IMPORTEXPORTSTATUS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.IES_COMPLETED, 
            strm.IES_DATECREATED, 
            strm.IES_DESC, 
            strm.IES_EMAIL, 
            strm.IES_EMAILSENT, 
            strm.IES_ESTTIMETOEND, 
            strm.IES_ESTTIMETOSTART, 
            strm.IES_FILELOCATION, 
            strm.IES_FILESENT, 
            strm.IES_INCLUDEEMAIL, 
            strm.IES_LASTSAVED, 
            strm.IES_PREVIEW, 
            strm.IES_RECORDCOUNT, 
            strm.IES_SESSIONID, 
            strm.IES_STARTED, 
            strm.IES_STATUS, 
            strm.IES_TYPE, 
            strm.IES_UPDATECOUNT, 
            strm.IES_USERID, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.IES_SESSIONID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IMPORTEXPORTSTATUS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.IES_SESSIONID=src.IES_SESSIONID) OR (target.IES_SESSIONID IS NULL AND src.IES_SESSIONID IS NULL)) 
                when matched then update set
                    target.IES_COMPLETED=src.IES_COMPLETED, 
                    target.IES_DATECREATED=src.IES_DATECREATED, 
                    target.IES_DESC=src.IES_DESC, 
                    target.IES_EMAIL=src.IES_EMAIL, 
                    target.IES_EMAILSENT=src.IES_EMAILSENT, 
                    target.IES_ESTTIMETOEND=src.IES_ESTTIMETOEND, 
                    target.IES_ESTTIMETOSTART=src.IES_ESTTIMETOSTART, 
                    target.IES_FILELOCATION=src.IES_FILELOCATION, 
                    target.IES_FILESENT=src.IES_FILESENT, 
                    target.IES_INCLUDEEMAIL=src.IES_INCLUDEEMAIL, 
                    target.IES_LASTSAVED=src.IES_LASTSAVED, 
                    target.IES_PREVIEW=src.IES_PREVIEW, 
                    target.IES_RECORDCOUNT=src.IES_RECORDCOUNT, 
                    target.IES_SESSIONID=src.IES_SESSIONID, 
                    target.IES_STARTED=src.IES_STARTED, 
                    target.IES_STATUS=src.IES_STATUS, 
                    target.IES_TYPE=src.IES_TYPE, 
                    target.IES_UPDATECOUNT=src.IES_UPDATECOUNT, 
                    target.IES_USERID=src.IES_USERID, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( IES_COMPLETED, IES_DATECREATED, IES_DESC, IES_EMAIL, IES_EMAILSENT, IES_ESTTIMETOEND, IES_ESTTIMETOSTART, IES_FILELOCATION, IES_FILESENT, IES_INCLUDEEMAIL, IES_LASTSAVED, IES_PREVIEW, IES_RECORDCOUNT, IES_SESSIONID, IES_STARTED, IES_STATUS, IES_TYPE, IES_UPDATECOUNT, IES_USERID, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.IES_COMPLETED, 
                    src.IES_DATECREATED, 
                    src.IES_DESC, 
                    src.IES_EMAIL, 
                    src.IES_EMAILSENT, 
                    src.IES_ESTTIMETOEND, 
                    src.IES_ESTTIMETOSTART, 
                    src.IES_FILELOCATION, 
                    src.IES_FILESENT, 
                    src.IES_INCLUDEEMAIL, 
                    src.IES_LASTSAVED, 
                    src.IES_PREVIEW, 
                    src.IES_RECORDCOUNT, 
                    src.IES_SESSIONID, 
                    src.IES_STARTED, 
                    src.IES_STATUS, 
                    src.IES_TYPE, 
                    src.IES_UPDATECOUNT, 
                    src.IES_USERID, 
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
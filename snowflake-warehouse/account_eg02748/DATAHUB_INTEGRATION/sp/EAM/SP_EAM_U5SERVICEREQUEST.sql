create or replace procedure DATAHUB_INTEGRATION.SP_EAM_U5SERVICEREQUEST()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_U5SERVICEREQUEST_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_U5SERVICEREQUEST as target using (SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.CRM_ADDRESSKEY, 
            strm.CRM_ALTERNATECONTACT, 
            strm.CRM_CALLERFISTNAME, 
            strm.CRM_CALLERLASTNAME, 
            strm.CRM_CALLTAKENBY, 
            strm.CRM_CITY, 
            strm.CRM_CONTACTEMAIL, 
            strm.CRM_CONTACTNUMBER, 
            strm.CRM_CONTACTREQUESTED, 
            strm.CRM_EVENT, 
            strm.CRM_FLATNO, 
            strm.CRM_NOTES, 
            strm.CRM_POSTALCODE, 
            strm.CRM_PRIORITY, 
            strm.CRM_PROBLEMCODE, 
            strm.CRM_REPORTEDDATE, 
            strm.CRM_SERVICEREQUEST, 
            strm.CRM_SOURCE, 
            strm.CRM_STNAME, 
            strm.CRM_STREETNO, 
            strm.CRM_SUBURB, 
            strm.CRM_TRANSACTIONID, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CRM_TRANSACTIONID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5SERVICEREQUEST as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CRM_TRANSACTIONID=src.CRM_TRANSACTIONID) OR (target.CRM_TRANSACTIONID IS NULL AND src.CRM_TRANSACTIONID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.CRM_ADDRESSKEY=src.CRM_ADDRESSKEY, 
                    target.CRM_ALTERNATECONTACT=src.CRM_ALTERNATECONTACT, 
                    target.CRM_CALLERFISTNAME=src.CRM_CALLERFISTNAME, 
                    target.CRM_CALLERLASTNAME=src.CRM_CALLERLASTNAME, 
                    target.CRM_CALLTAKENBY=src.CRM_CALLTAKENBY, 
                    target.CRM_CITY=src.CRM_CITY, 
                    target.CRM_CONTACTEMAIL=src.CRM_CONTACTEMAIL, 
                    target.CRM_CONTACTNUMBER=src.CRM_CONTACTNUMBER, 
                    target.CRM_CONTACTREQUESTED=src.CRM_CONTACTREQUESTED, 
                    target.CRM_EVENT=src.CRM_EVENT, 
                    target.CRM_FLATNO=src.CRM_FLATNO, 
                    target.CRM_NOTES=src.CRM_NOTES, 
                    target.CRM_POSTALCODE=src.CRM_POSTALCODE, 
                    target.CRM_PRIORITY=src.CRM_PRIORITY, 
                    target.CRM_PROBLEMCODE=src.CRM_PROBLEMCODE, 
                    target.CRM_REPORTEDDATE=src.CRM_REPORTEDDATE, 
                    target.CRM_SERVICEREQUEST=src.CRM_SERVICEREQUEST, 
                    target.CRM_SOURCE=src.CRM_SOURCE, 
                    target.CRM_STNAME=src.CRM_STNAME, 
                    target.CRM_STREETNO=src.CRM_STREETNO, 
                    target.CRM_SUBURB=src.CRM_SUBURB, 
                    target.CRM_TRANSACTIONID=src.CRM_TRANSACTIONID, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CREATED, 
                    CREATEDBY, 
                    CRM_ADDRESSKEY, 
                    CRM_ALTERNATECONTACT, 
                    CRM_CALLERFISTNAME, 
                    CRM_CALLERLASTNAME, 
                    CRM_CALLTAKENBY, 
                    CRM_CITY, 
                    CRM_CONTACTEMAIL, 
                    CRM_CONTACTNUMBER, 
                    CRM_CONTACTREQUESTED, 
                    CRM_EVENT, 
                    CRM_FLATNO, 
                    CRM_NOTES, 
                    CRM_POSTALCODE, 
                    CRM_PRIORITY, 
                    CRM_PROBLEMCODE, 
                    CRM_REPORTEDDATE, 
                    CRM_SERVICEREQUEST, 
                    CRM_SOURCE, 
                    CRM_STNAME, 
                    CRM_STREETNO, 
                    CRM_SUBURB, 
                    CRM_TRANSACTIONID, 
                    LASTSAVED, 
                    UPDATECOUNT, 
                    UPDATED, 
                    UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.CRM_ADDRESSKEY, 
                    src.CRM_ALTERNATECONTACT, 
                    src.CRM_CALLERFISTNAME, 
                    src.CRM_CALLERLASTNAME, 
                    src.CRM_CALLTAKENBY, 
                    src.CRM_CITY, 
                    src.CRM_CONTACTEMAIL, 
                    src.CRM_CONTACTNUMBER, 
                    src.CRM_CONTACTREQUESTED, 
                    src.CRM_EVENT, 
                    src.CRM_FLATNO, 
                    src.CRM_NOTES, 
                    src.CRM_POSTALCODE, 
                    src.CRM_PRIORITY, 
                    src.CRM_PROBLEMCODE, 
                    src.CRM_REPORTEDDATE, 
                    src.CRM_SERVICEREQUEST, 
                    src.CRM_SOURCE, 
                    src.CRM_STNAME, 
                    src.CRM_STREETNO, 
                    src.CRM_SUBURB, 
                    src.CRM_TRANSACTIONID, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_U5SERVICEREQUEST_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CREATED, 
            strm.CREATEDBY, 
            strm.CRM_ADDRESSKEY, 
            strm.CRM_ALTERNATECONTACT, 
            strm.CRM_CALLERFISTNAME, 
            strm.CRM_CALLERLASTNAME, 
            strm.CRM_CALLTAKENBY, 
            strm.CRM_CITY, 
            strm.CRM_CONTACTEMAIL, 
            strm.CRM_CONTACTNUMBER, 
            strm.CRM_CONTACTREQUESTED, 
            strm.CRM_EVENT, 
            strm.CRM_FLATNO, 
            strm.CRM_NOTES, 
            strm.CRM_POSTALCODE, 
            strm.CRM_PRIORITY, 
            strm.CRM_PROBLEMCODE, 
            strm.CRM_REPORTEDDATE, 
            strm.CRM_SERVICEREQUEST, 
            strm.CRM_SOURCE, 
            strm.CRM_STNAME, 
            strm.CRM_STREETNO, 
            strm.CRM_SUBURB, 
            strm.CRM_TRANSACTIONID, 
            strm.LASTSAVED, 
            strm.UPDATECOUNT, 
            strm.UPDATED, 
            strm.UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CRM_TRANSACTIONID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5SERVICEREQUEST as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CRM_TRANSACTIONID=src.CRM_TRANSACTIONID) OR (target.CRM_TRANSACTIONID IS NULL AND src.CRM_TRANSACTIONID IS NULL)) 
                when matched then update set
                    target.CREATED=src.CREATED, 
                    target.CREATEDBY=src.CREATEDBY, 
                    target.CRM_ADDRESSKEY=src.CRM_ADDRESSKEY, 
                    target.CRM_ALTERNATECONTACT=src.CRM_ALTERNATECONTACT, 
                    target.CRM_CALLERFISTNAME=src.CRM_CALLERFISTNAME, 
                    target.CRM_CALLERLASTNAME=src.CRM_CALLERLASTNAME, 
                    target.CRM_CALLTAKENBY=src.CRM_CALLTAKENBY, 
                    target.CRM_CITY=src.CRM_CITY, 
                    target.CRM_CONTACTEMAIL=src.CRM_CONTACTEMAIL, 
                    target.CRM_CONTACTNUMBER=src.CRM_CONTACTNUMBER, 
                    target.CRM_CONTACTREQUESTED=src.CRM_CONTACTREQUESTED, 
                    target.CRM_EVENT=src.CRM_EVENT, 
                    target.CRM_FLATNO=src.CRM_FLATNO, 
                    target.CRM_NOTES=src.CRM_NOTES, 
                    target.CRM_POSTALCODE=src.CRM_POSTALCODE, 
                    target.CRM_PRIORITY=src.CRM_PRIORITY, 
                    target.CRM_PROBLEMCODE=src.CRM_PROBLEMCODE, 
                    target.CRM_REPORTEDDATE=src.CRM_REPORTEDDATE, 
                    target.CRM_SERVICEREQUEST=src.CRM_SERVICEREQUEST, 
                    target.CRM_SOURCE=src.CRM_SOURCE, 
                    target.CRM_STNAME=src.CRM_STNAME, 
                    target.CRM_STREETNO=src.CRM_STREETNO, 
                    target.CRM_SUBURB=src.CRM_SUBURB, 
                    target.CRM_TRANSACTIONID=src.CRM_TRANSACTIONID, 
                    target.LASTSAVED=src.LASTSAVED, 
                    target.UPDATECOUNT=src.UPDATECOUNT, 
                    target.UPDATED=src.UPDATED, 
                    target.UPDATEDBY=src.UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CREATED, CREATEDBY, CRM_ADDRESSKEY, CRM_ALTERNATECONTACT, CRM_CALLERFISTNAME, CRM_CALLERLASTNAME, CRM_CALLTAKENBY, CRM_CITY, CRM_CONTACTEMAIL, CRM_CONTACTNUMBER, CRM_CONTACTREQUESTED, CRM_EVENT, CRM_FLATNO, CRM_NOTES, CRM_POSTALCODE, CRM_PRIORITY, CRM_PROBLEMCODE, CRM_REPORTEDDATE, CRM_SERVICEREQUEST, CRM_SOURCE, CRM_STNAME, CRM_STREETNO, CRM_SUBURB, CRM_TRANSACTIONID, LASTSAVED, UPDATECOUNT, UPDATED, UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CREATED, 
                    src.CREATEDBY, 
                    src.CRM_ADDRESSKEY, 
                    src.CRM_ALTERNATECONTACT, 
                    src.CRM_CALLERFISTNAME, 
                    src.CRM_CALLERLASTNAME, 
                    src.CRM_CALLTAKENBY, 
                    src.CRM_CITY, 
                    src.CRM_CONTACTEMAIL, 
                    src.CRM_CONTACTNUMBER, 
                    src.CRM_CONTACTREQUESTED, 
                    src.CRM_EVENT, 
                    src.CRM_FLATNO, 
                    src.CRM_NOTES, 
                    src.CRM_POSTALCODE, 
                    src.CRM_PRIORITY, 
                    src.CRM_PROBLEMCODE, 
                    src.CRM_REPORTEDDATE, 
                    src.CRM_SERVICEREQUEST, 
                    src.CRM_SOURCE, 
                    src.CRM_STNAME, 
                    src.CRM_STREETNO, 
                    src.CRM_SUBURB, 
                    src.CRM_TRANSACTIONID, 
                    src.LASTSAVED, 
                    src.UPDATECOUNT, 
                    src.UPDATED, 
                    src.UPDATEDBY, 
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
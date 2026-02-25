create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST()
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
                            INSERT INTO LANDING_ERROR.IPS_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICANTISPOC, 
            strm.APPROJAPPLDTLKEY, 
            strm.BILLCITY, 
            strm.BILLCOMPANY, 
            strm.BILLEMAIL, 
            strm.BILLFIRSTNAME, 
            strm.BILLLASTNAME, 
            strm.BILLMOBILE, 
            strm.BILLPHONE, 
            strm.BILLPOSTCODE, 
            strm.BILLSTREETNAME, 
            strm.BILLSTREETNUMBER, 
            strm.BILLSUBURB, 
            strm.BILLTITLE, 
            strm.DATALAKE_DELETED, 
            strm.DECLARATION, 
            strm.DETAILCONNMETHODATTACH, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NETWORKSHUTDOWNREQUESTKEY, 
            strm.PAYEETYPE, 
            strm.POCCITY, 
            strm.POCCOMPANY, 
            strm.POCEMAIL, 
            strm.POCFIRSTNMAE, 
            strm.POCLASTNAME, 
            strm.POCMOBILE, 
            strm.POCPOSTCODE, 
            strm.POCSTREETNAME, 
            strm.POCSTREETNUMBER, 
            strm.POCSUBURB, 
            strm.POCTITLE, 
            strm.POCWORKPHONE, 
            strm.PROPOSEDCONNECTIONDATE, 
            strm.REQUESTDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NETWORKSHUTDOWNREQUESTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NETWORKSHUTDOWNREQUESTKEY=src.NETWORKSHUTDOWNREQUESTKEY) OR (target.NETWORKSHUTDOWNREQUESTKEY IS NULL AND src.NETWORKSHUTDOWNREQUESTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICANTISPOC=src.APPLICANTISPOC, 
                    target.APPROJAPPLDTLKEY=src.APPROJAPPLDTLKEY, 
                    target.BILLCITY=src.BILLCITY, 
                    target.BILLCOMPANY=src.BILLCOMPANY, 
                    target.BILLEMAIL=src.BILLEMAIL, 
                    target.BILLFIRSTNAME=src.BILLFIRSTNAME, 
                    target.BILLLASTNAME=src.BILLLASTNAME, 
                    target.BILLMOBILE=src.BILLMOBILE, 
                    target.BILLPHONE=src.BILLPHONE, 
                    target.BILLPOSTCODE=src.BILLPOSTCODE, 
                    target.BILLSTREETNAME=src.BILLSTREETNAME, 
                    target.BILLSTREETNUMBER=src.BILLSTREETNUMBER, 
                    target.BILLSUBURB=src.BILLSUBURB, 
                    target.BILLTITLE=src.BILLTITLE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DECLARATION=src.DECLARATION, 
                    target.DETAILCONNMETHODATTACH=src.DETAILCONNMETHODATTACH, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NETWORKSHUTDOWNREQUESTKEY=src.NETWORKSHUTDOWNREQUESTKEY, 
                    target.PAYEETYPE=src.PAYEETYPE, 
                    target.POCCITY=src.POCCITY, 
                    target.POCCOMPANY=src.POCCOMPANY, 
                    target.POCEMAIL=src.POCEMAIL, 
                    target.POCFIRSTNMAE=src.POCFIRSTNMAE, 
                    target.POCLASTNAME=src.POCLASTNAME, 
                    target.POCMOBILE=src.POCMOBILE, 
                    target.POCPOSTCODE=src.POCPOSTCODE, 
                    target.POCSTREETNAME=src.POCSTREETNAME, 
                    target.POCSTREETNUMBER=src.POCSTREETNUMBER, 
                    target.POCSUBURB=src.POCSUBURB, 
                    target.POCTITLE=src.POCTITLE, 
                    target.POCWORKPHONE=src.POCWORKPHONE, 
                    target.PROPOSEDCONNECTIONDATE=src.PROPOSEDCONNECTIONDATE, 
                    target.REQUESTDATE=src.REQUESTDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPLICANTISPOC, 
                    APPROJAPPLDTLKEY, 
                    BILLCITY, 
                    BILLCOMPANY, 
                    BILLEMAIL, 
                    BILLFIRSTNAME, 
                    BILLLASTNAME, 
                    BILLMOBILE, 
                    BILLPHONE, 
                    BILLPOSTCODE, 
                    BILLSTREETNAME, 
                    BILLSTREETNUMBER, 
                    BILLSUBURB, 
                    BILLTITLE, 
                    DATALAKE_DELETED, 
                    DECLARATION, 
                    DETAILCONNMETHODATTACH, 
                    MODBY, 
                    MODDTTM, 
                    NETWORKSHUTDOWNREQUESTKEY, 
                    PAYEETYPE, 
                    POCCITY, 
                    POCCOMPANY, 
                    POCEMAIL, 
                    POCFIRSTNMAE, 
                    POCLASTNAME, 
                    POCMOBILE, 
                    POCPOSTCODE, 
                    POCSTREETNAME, 
                    POCSTREETNUMBER, 
                    POCSUBURB, 
                    POCTITLE, 
                    POCWORKPHONE, 
                    PROPOSEDCONNECTIONDATE, 
                    REQUESTDATE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICANTISPOC, 
                    src.APPROJAPPLDTLKEY, 
                    src.BILLCITY, 
                    src.BILLCOMPANY, 
                    src.BILLEMAIL, 
                    src.BILLFIRSTNAME, 
                    src.BILLLASTNAME, 
                    src.BILLMOBILE, 
                    src.BILLPHONE, 
                    src.BILLPOSTCODE, 
                    src.BILLSTREETNAME, 
                    src.BILLSTREETNUMBER, 
                    src.BILLSUBURB, 
                    src.BILLTITLE, 
                    src.DATALAKE_DELETED, 
                    src.DECLARATION, 
                    src.DETAILCONNMETHODATTACH, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NETWORKSHUTDOWNREQUESTKEY, 
                    src.PAYEETYPE, 
                    src.POCCITY, 
                    src.POCCOMPANY, 
                    src.POCEMAIL, 
                    src.POCFIRSTNMAE, 
                    src.POCLASTNAME, 
                    src.POCMOBILE, 
                    src.POCPOSTCODE, 
                    src.POCSTREETNAME, 
                    src.POCSTREETNUMBER, 
                    src.POCSUBURB, 
                    src.POCTITLE, 
                    src.POCWORKPHONE, 
                    src.PROPOSEDCONNECTIONDATE, 
                    src.REQUESTDATE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICANTISPOC, 
            strm.APPROJAPPLDTLKEY, 
            strm.BILLCITY, 
            strm.BILLCOMPANY, 
            strm.BILLEMAIL, 
            strm.BILLFIRSTNAME, 
            strm.BILLLASTNAME, 
            strm.BILLMOBILE, 
            strm.BILLPHONE, 
            strm.BILLPOSTCODE, 
            strm.BILLSTREETNAME, 
            strm.BILLSTREETNUMBER, 
            strm.BILLSUBURB, 
            strm.BILLTITLE, 
            strm.DATALAKE_DELETED, 
            strm.DECLARATION, 
            strm.DETAILCONNMETHODATTACH, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NETWORKSHUTDOWNREQUESTKEY, 
            strm.PAYEETYPE, 
            strm.POCCITY, 
            strm.POCCOMPANY, 
            strm.POCEMAIL, 
            strm.POCFIRSTNMAE, 
            strm.POCLASTNAME, 
            strm.POCMOBILE, 
            strm.POCPOSTCODE, 
            strm.POCSTREETNAME, 
            strm.POCSTREETNUMBER, 
            strm.POCSUBURB, 
            strm.POCTITLE, 
            strm.POCWORKPHONE, 
            strm.PROPOSEDCONNECTIONDATE, 
            strm.REQUESTDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NETWORKSHUTDOWNREQUESTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NETWORKSHUTDOWNREQUESTKEY=src.NETWORKSHUTDOWNREQUESTKEY) OR (target.NETWORKSHUTDOWNREQUESTKEY IS NULL AND src.NETWORKSHUTDOWNREQUESTKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICANTISPOC=src.APPLICANTISPOC, 
                    target.APPROJAPPLDTLKEY=src.APPROJAPPLDTLKEY, 
                    target.BILLCITY=src.BILLCITY, 
                    target.BILLCOMPANY=src.BILLCOMPANY, 
                    target.BILLEMAIL=src.BILLEMAIL, 
                    target.BILLFIRSTNAME=src.BILLFIRSTNAME, 
                    target.BILLLASTNAME=src.BILLLASTNAME, 
                    target.BILLMOBILE=src.BILLMOBILE, 
                    target.BILLPHONE=src.BILLPHONE, 
                    target.BILLPOSTCODE=src.BILLPOSTCODE, 
                    target.BILLSTREETNAME=src.BILLSTREETNAME, 
                    target.BILLSTREETNUMBER=src.BILLSTREETNUMBER, 
                    target.BILLSUBURB=src.BILLSUBURB, 
                    target.BILLTITLE=src.BILLTITLE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DECLARATION=src.DECLARATION, 
                    target.DETAILCONNMETHODATTACH=src.DETAILCONNMETHODATTACH, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NETWORKSHUTDOWNREQUESTKEY=src.NETWORKSHUTDOWNREQUESTKEY, 
                    target.PAYEETYPE=src.PAYEETYPE, 
                    target.POCCITY=src.POCCITY, 
                    target.POCCOMPANY=src.POCCOMPANY, 
                    target.POCEMAIL=src.POCEMAIL, 
                    target.POCFIRSTNMAE=src.POCFIRSTNMAE, 
                    target.POCLASTNAME=src.POCLASTNAME, 
                    target.POCMOBILE=src.POCMOBILE, 
                    target.POCPOSTCODE=src.POCPOSTCODE, 
                    target.POCSTREETNAME=src.POCSTREETNAME, 
                    target.POCSTREETNUMBER=src.POCSTREETNUMBER, 
                    target.POCSUBURB=src.POCSUBURB, 
                    target.POCTITLE=src.POCTITLE, 
                    target.POCWORKPHONE=src.POCWORKPHONE, 
                    target.PROPOSEDCONNECTIONDATE=src.PROPOSEDCONNECTIONDATE, 
                    target.REQUESTDATE=src.REQUESTDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPLICANTISPOC, APPROJAPPLDTLKEY, BILLCITY, BILLCOMPANY, BILLEMAIL, BILLFIRSTNAME, BILLLASTNAME, BILLMOBILE, BILLPHONE, BILLPOSTCODE, BILLSTREETNAME, BILLSTREETNUMBER, BILLSUBURB, BILLTITLE, DATALAKE_DELETED, DECLARATION, DETAILCONNMETHODATTACH, MODBY, MODDTTM, NETWORKSHUTDOWNREQUESTKEY, PAYEETYPE, POCCITY, POCCOMPANY, POCEMAIL, POCFIRSTNMAE, POCLASTNAME, POCMOBILE, POCPOSTCODE, POCSTREETNAME, POCSTREETNUMBER, POCSUBURB, POCTITLE, POCWORKPHONE, PROPOSEDCONNECTIONDATE, REQUESTDATE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICANTISPOC, 
                    src.APPROJAPPLDTLKEY, 
                    src.BILLCITY, 
                    src.BILLCOMPANY, 
                    src.BILLEMAIL, 
                    src.BILLFIRSTNAME, 
                    src.BILLLASTNAME, 
                    src.BILLMOBILE, 
                    src.BILLPHONE, 
                    src.BILLPOSTCODE, 
                    src.BILLSTREETNAME, 
                    src.BILLSTREETNUMBER, 
                    src.BILLSUBURB, 
                    src.BILLTITLE, 
                    src.DATALAKE_DELETED, 
                    src.DECLARATION, 
                    src.DETAILCONNMETHODATTACH, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NETWORKSHUTDOWNREQUESTKEY, 
                    src.PAYEETYPE, 
                    src.POCCITY, 
                    src.POCCOMPANY, 
                    src.POCEMAIL, 
                    src.POCFIRSTNMAE, 
                    src.POCLASTNAME, 
                    src.POCMOBILE, 
                    src.POCPOSTCODE, 
                    src.POCSTREETNAME, 
                    src.POCSTREETNUMBER, 
                    src.POCSUBURB, 
                    src.POCTITLE, 
                    src.POCWORKPHONE, 
                    src.PROPOSEDCONNECTIONDATE, 
                    src.REQUESTDATE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
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
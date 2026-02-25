create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLINTERFACE_NOTIFICATIONSTAGING()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLINTERFACE_NOTIFICATIONSTAGING_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLINTERFACE_NOTIFICATIONSTAGING as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICATIONNUMBER, 
            strm.APPLICATIONTYPE, 
            strm.CATEGORY, 
            strm.CONTACTEMAILADDRESS, 
            strm.CONTACTFULLNAME, 
            strm.DATALAKE_DELETED, 
            strm.DOCUMENTID, 
            strm.EPA, 
            strm.FROMEMAIL, 
            strm.KEYCOLUMN, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONIKER, 
            strm.NAME, 
            strm.NOTIFICATIONSTAGINGKEY, 
            strm.R4CURL, 
            strm.REVIEWASSIGNDAYPHONE, 
            strm.REVIEWASSIGNEMAIL, 
            strm.REVIEWASSIGNFULLNAME, 
            strm.REVIEWCOMPLETEDAYPHONE, 
            strm.REVIEWCOMPLETEEMAIL, 
            strm.REVIEWCOMPLETEFULLNAME, 
            strm.REVIEWERROLE, 
            strm.SOURCE, 
            strm.SUBJECT, 
            strm.TEMPLATENAME, 
            strm.TITLE, 
            strm.TOEMAIL, 
            strm.USERTYPE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NOTIFICATIONSTAGINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NOTIFICATIONSTAGING as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NOTIFICATIONSTAGINGKEY=src.NOTIFICATIONSTAGINGKEY) OR (target.NOTIFICATIONSTAGINGKEY IS NULL AND src.NOTIFICATIONSTAGINGKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICATIONNUMBER=src.APPLICATIONNUMBER, 
                    target.APPLICATIONTYPE=src.APPLICATIONTYPE, 
                    target.CATEGORY=src.CATEGORY, 
                    target.CONTACTEMAILADDRESS=src.CONTACTEMAILADDRESS, 
                    target.CONTACTFULLNAME=src.CONTACTFULLNAME, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DOCUMENTID=src.DOCUMENTID, 
                    target.EPA=src.EPA, 
                    target.FROMEMAIL=src.FROMEMAIL, 
                    target.KEYCOLUMN=src.KEYCOLUMN, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONIKER=src.MONIKER, 
                    target.NAME=src.NAME, 
                    target.NOTIFICATIONSTAGINGKEY=src.NOTIFICATIONSTAGINGKEY, 
                    target.R4CURL=src.R4CURL, 
                    target.REVIEWASSIGNDAYPHONE=src.REVIEWASSIGNDAYPHONE, 
                    target.REVIEWASSIGNEMAIL=src.REVIEWASSIGNEMAIL, 
                    target.REVIEWASSIGNFULLNAME=src.REVIEWASSIGNFULLNAME, 
                    target.REVIEWCOMPLETEDAYPHONE=src.REVIEWCOMPLETEDAYPHONE, 
                    target.REVIEWCOMPLETEEMAIL=src.REVIEWCOMPLETEEMAIL, 
                    target.REVIEWCOMPLETEFULLNAME=src.REVIEWCOMPLETEFULLNAME, 
                    target.REVIEWERROLE=src.REVIEWERROLE, 
                    target.SOURCE=src.SOURCE, 
                    target.SUBJECT=src.SUBJECT, 
                    target.TEMPLATENAME=src.TEMPLATENAME, 
                    target.TITLE=src.TITLE, 
                    target.TOEMAIL=src.TOEMAIL, 
                    target.USERTYPE=src.USERTYPE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPLICATIONNUMBER, 
                    APPLICATIONTYPE, 
                    CATEGORY, 
                    CONTACTEMAILADDRESS, 
                    CONTACTFULLNAME, 
                    DATALAKE_DELETED, 
                    DOCUMENTID, 
                    EPA, 
                    FROMEMAIL, 
                    KEYCOLUMN, 
                    MODBY, 
                    MODDTTM, 
                    MONIKER, 
                    NAME, 
                    NOTIFICATIONSTAGINGKEY, 
                    R4CURL, 
                    REVIEWASSIGNDAYPHONE, 
                    REVIEWASSIGNEMAIL, 
                    REVIEWASSIGNFULLNAME, 
                    REVIEWCOMPLETEDAYPHONE, 
                    REVIEWCOMPLETEEMAIL, 
                    REVIEWCOMPLETEFULLNAME, 
                    REVIEWERROLE, 
                    SOURCE, 
                    SUBJECT, 
                    TEMPLATENAME, 
                    TITLE, 
                    TOEMAIL, 
                    USERTYPE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICATIONNUMBER, 
                    src.APPLICATIONTYPE, 
                    src.CATEGORY, 
                    src.CONTACTEMAILADDRESS, 
                    src.CONTACTFULLNAME, 
                    src.DATALAKE_DELETED, 
                    src.DOCUMENTID, 
                    src.EPA, 
                    src.FROMEMAIL, 
                    src.KEYCOLUMN, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONIKER, 
                    src.NAME, 
                    src.NOTIFICATIONSTAGINGKEY, 
                    src.R4CURL, 
                    src.REVIEWASSIGNDAYPHONE, 
                    src.REVIEWASSIGNEMAIL, 
                    src.REVIEWASSIGNFULLNAME, 
                    src.REVIEWCOMPLETEDAYPHONE, 
                    src.REVIEWCOMPLETEEMAIL, 
                    src.REVIEWCOMPLETEFULLNAME, 
                    src.REVIEWERROLE, 
                    src.SOURCE, 
                    src.SUBJECT, 
                    src.TEMPLATENAME, 
                    src.TITLE, 
                    src.TOEMAIL, 
                    src.USERTYPE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_NOTIFICATIONSTAGING as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLICATIONNUMBER, 
            strm.APPLICATIONTYPE, 
            strm.CATEGORY, 
            strm.CONTACTEMAILADDRESS, 
            strm.CONTACTFULLNAME, 
            strm.DATALAKE_DELETED, 
            strm.DOCUMENTID, 
            strm.EPA, 
            strm.FROMEMAIL, 
            strm.KEYCOLUMN, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONIKER, 
            strm.NAME, 
            strm.NOTIFICATIONSTAGINGKEY, 
            strm.R4CURL, 
            strm.REVIEWASSIGNDAYPHONE, 
            strm.REVIEWASSIGNEMAIL, 
            strm.REVIEWASSIGNFULLNAME, 
            strm.REVIEWCOMPLETEDAYPHONE, 
            strm.REVIEWCOMPLETEEMAIL, 
            strm.REVIEWCOMPLETEFULLNAME, 
            strm.REVIEWERROLE, 
            strm.SOURCE, 
            strm.SUBJECT, 
            strm.TEMPLATENAME, 
            strm.TITLE, 
            strm.TOEMAIL, 
            strm.USERTYPE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NOTIFICATIONSTAGINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NOTIFICATIONSTAGING as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NOTIFICATIONSTAGINGKEY=src.NOTIFICATIONSTAGINGKEY) OR (target.NOTIFICATIONSTAGINGKEY IS NULL AND src.NOTIFICATIONSTAGINGKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLICATIONNUMBER=src.APPLICATIONNUMBER, 
                    target.APPLICATIONTYPE=src.APPLICATIONTYPE, 
                    target.CATEGORY=src.CATEGORY, 
                    target.CONTACTEMAILADDRESS=src.CONTACTEMAILADDRESS, 
                    target.CONTACTFULLNAME=src.CONTACTFULLNAME, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DOCUMENTID=src.DOCUMENTID, 
                    target.EPA=src.EPA, 
                    target.FROMEMAIL=src.FROMEMAIL, 
                    target.KEYCOLUMN=src.KEYCOLUMN, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONIKER=src.MONIKER, 
                    target.NAME=src.NAME, 
                    target.NOTIFICATIONSTAGINGKEY=src.NOTIFICATIONSTAGINGKEY, 
                    target.R4CURL=src.R4CURL, 
                    target.REVIEWASSIGNDAYPHONE=src.REVIEWASSIGNDAYPHONE, 
                    target.REVIEWASSIGNEMAIL=src.REVIEWASSIGNEMAIL, 
                    target.REVIEWASSIGNFULLNAME=src.REVIEWASSIGNFULLNAME, 
                    target.REVIEWCOMPLETEDAYPHONE=src.REVIEWCOMPLETEDAYPHONE, 
                    target.REVIEWCOMPLETEEMAIL=src.REVIEWCOMPLETEEMAIL, 
                    target.REVIEWCOMPLETEFULLNAME=src.REVIEWCOMPLETEFULLNAME, 
                    target.REVIEWERROLE=src.REVIEWERROLE, 
                    target.SOURCE=src.SOURCE, 
                    target.SUBJECT=src.SUBJECT, 
                    target.TEMPLATENAME=src.TEMPLATENAME, 
                    target.TITLE=src.TITLE, 
                    target.TOEMAIL=src.TOEMAIL, 
                    target.USERTYPE=src.USERTYPE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPLICATIONNUMBER, APPLICATIONTYPE, CATEGORY, CONTACTEMAILADDRESS, CONTACTFULLNAME, DATALAKE_DELETED, DOCUMENTID, EPA, FROMEMAIL, KEYCOLUMN, MODBY, MODDTTM, MONIKER, NAME, NOTIFICATIONSTAGINGKEY, R4CURL, REVIEWASSIGNDAYPHONE, REVIEWASSIGNEMAIL, REVIEWASSIGNFULLNAME, REVIEWCOMPLETEDAYPHONE, REVIEWCOMPLETEEMAIL, REVIEWCOMPLETEFULLNAME, REVIEWERROLE, SOURCE, SUBJECT, TEMPLATENAME, TITLE, TOEMAIL, USERTYPE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLICATIONNUMBER, 
                    src.APPLICATIONTYPE, 
                    src.CATEGORY, 
                    src.CONTACTEMAILADDRESS, 
                    src.CONTACTFULLNAME, 
                    src.DATALAKE_DELETED, 
                    src.DOCUMENTID, 
                    src.EPA, 
                    src.FROMEMAIL, 
                    src.KEYCOLUMN, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONIKER, 
                    src.NAME, 
                    src.NOTIFICATIONSTAGINGKEY, 
                    src.R4CURL, 
                    src.REVIEWASSIGNDAYPHONE, 
                    src.REVIEWASSIGNEMAIL, 
                    src.REVIEWASSIGNFULLNAME, 
                    src.REVIEWCOMPLETEDAYPHONE, 
                    src.REVIEWCOMPLETEEMAIL, 
                    src.REVIEWCOMPLETEFULLNAME, 
                    src.REVIEWERROLE, 
                    src.SOURCE, 
                    src.SUBJECT, 
                    src.TEMPLATENAME, 
                    src.TITLE, 
                    src.TOEMAIL, 
                    src.USERTYPE, 
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
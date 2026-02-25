create or replace procedure DATAHUB_INTEGRATION.SP_IPS_PORTAL_MEMBERSHIP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_PORTAL_MEMBERSHIP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_PORTAL_MEMBERSHIP as target using (SELECT * FROM (SELECT 
            strm.ACTIVATEDDATE, 
            strm.ACTIVATIONCODE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVEDDATE, 
            strm.CONTACT, 
            strm.DATALAKE_DELETED, 
            strm.EXPDATE, 
            strm.FAILEDPASSWORDANSWERCOUNT, 
            strm.FAILEDPASSWORDCOUNT, 
            strm.ISACTIVATED, 
            strm.ISAPPROVED, 
            strm.ISLOCKEDOUT, 
            strm.ISPWDRESETREQ, 
            strm.LASTLOGINDATE, 
            strm.LASTPASSWORDCHANGEDDATE, 
            strm.LASTPASSWORDRESETDATE, 
            strm.LOCKEDOUTDATE, 
            strm.MEMBERSHIPKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARENTUSER, 
            strm.PASSWORDANSWERHASH, 
            strm.PASSWORDANSWERSALT, 
            strm.PASSWORDHASH, 
            strm.PASSWORDQUESTION, 
            strm.PASSWORDSALT, 
            strm.USERNAME, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MEMBERSHIPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PORTAL_MEMBERSHIP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MEMBERSHIPKEY=src.MEMBERSHIPKEY) OR (target.MEMBERSHIPKEY IS NULL AND src.MEMBERSHIPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTIVATEDDATE=src.ACTIVATEDDATE, 
                    target.ACTIVATIONCODE=src.ACTIVATIONCODE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVEDDATE=src.APPROVEDDATE, 
                    target.CONTACT=src.CONTACT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FAILEDPASSWORDANSWERCOUNT=src.FAILEDPASSWORDANSWERCOUNT, 
                    target.FAILEDPASSWORDCOUNT=src.FAILEDPASSWORDCOUNT, 
                    target.ISACTIVATED=src.ISACTIVATED, 
                    target.ISAPPROVED=src.ISAPPROVED, 
                    target.ISLOCKEDOUT=src.ISLOCKEDOUT, 
                    target.ISPWDRESETREQ=src.ISPWDRESETREQ, 
                    target.LASTLOGINDATE=src.LASTLOGINDATE, 
                    target.LASTPASSWORDCHANGEDDATE=src.LASTPASSWORDCHANGEDDATE, 
                    target.LASTPASSWORDRESETDATE=src.LASTPASSWORDRESETDATE, 
                    target.LOCKEDOUTDATE=src.LOCKEDOUTDATE, 
                    target.MEMBERSHIPKEY=src.MEMBERSHIPKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARENTUSER=src.PARENTUSER, 
                    target.PASSWORDANSWERHASH=src.PASSWORDANSWERHASH, 
                    target.PASSWORDANSWERSALT=src.PASSWORDANSWERSALT, 
                    target.PASSWORDHASH=src.PASSWORDHASH, 
                    target.PASSWORDQUESTION=src.PASSWORDQUESTION, 
                    target.PASSWORDSALT=src.PASSWORDSALT, 
                    target.USERNAME=src.USERNAME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTIVATEDDATE, 
                    ACTIVATIONCODE, 
                    ADDBY, 
                    ADDDTTM, 
                    APPROVEDDATE, 
                    CONTACT, 
                    DATALAKE_DELETED, 
                    EXPDATE, 
                    FAILEDPASSWORDANSWERCOUNT, 
                    FAILEDPASSWORDCOUNT, 
                    ISACTIVATED, 
                    ISAPPROVED, 
                    ISLOCKEDOUT, 
                    ISPWDRESETREQ, 
                    LASTLOGINDATE, 
                    LASTPASSWORDCHANGEDDATE, 
                    LASTPASSWORDRESETDATE, 
                    LOCKEDOUTDATE, 
                    MEMBERSHIPKEY, 
                    MODBY, 
                    MODDTTM, 
                    PARENTUSER, 
                    PASSWORDANSWERHASH, 
                    PASSWORDANSWERSALT, 
                    PASSWORDHASH, 
                    PASSWORDQUESTION, 
                    PASSWORDSALT, 
                    USERNAME, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTIVATEDDATE, 
                    src.ACTIVATIONCODE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVEDDATE, 
                    src.CONTACT, 
                    src.DATALAKE_DELETED, 
                    src.EXPDATE, 
                    src.FAILEDPASSWORDANSWERCOUNT, 
                    src.FAILEDPASSWORDCOUNT, 
                    src.ISACTIVATED, 
                    src.ISAPPROVED, 
                    src.ISLOCKEDOUT, 
                    src.ISPWDRESETREQ, 
                    src.LASTLOGINDATE, 
                    src.LASTPASSWORDCHANGEDDATE, 
                    src.LASTPASSWORDRESETDATE, 
                    src.LOCKEDOUTDATE, 
                    src.MEMBERSHIPKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARENTUSER, 
                    src.PASSWORDANSWERHASH, 
                    src.PASSWORDANSWERSALT, 
                    src.PASSWORDHASH, 
                    src.PASSWORDQUESTION, 
                    src.PASSWORDSALT, 
                    src.USERNAME, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_PORTAL_MEMBERSHIP as target using (
                SELECT * FROM (SELECT 
            strm.ACTIVATEDDATE, 
            strm.ACTIVATIONCODE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROVEDDATE, 
            strm.CONTACT, 
            strm.DATALAKE_DELETED, 
            strm.EXPDATE, 
            strm.FAILEDPASSWORDANSWERCOUNT, 
            strm.FAILEDPASSWORDCOUNT, 
            strm.ISACTIVATED, 
            strm.ISAPPROVED, 
            strm.ISLOCKEDOUT, 
            strm.ISPWDRESETREQ, 
            strm.LASTLOGINDATE, 
            strm.LASTPASSWORDCHANGEDDATE, 
            strm.LASTPASSWORDRESETDATE, 
            strm.LOCKEDOUTDATE, 
            strm.MEMBERSHIPKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARENTUSER, 
            strm.PASSWORDANSWERHASH, 
            strm.PASSWORDANSWERSALT, 
            strm.PASSWORDHASH, 
            strm.PASSWORDQUESTION, 
            strm.PASSWORDSALT, 
            strm.USERNAME, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MEMBERSHIPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PORTAL_MEMBERSHIP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MEMBERSHIPKEY=src.MEMBERSHIPKEY) OR (target.MEMBERSHIPKEY IS NULL AND src.MEMBERSHIPKEY IS NULL)) 
                when matched then update set
                    target.ACTIVATEDDATE=src.ACTIVATEDDATE, 
                    target.ACTIVATIONCODE=src.ACTIVATIONCODE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROVEDDATE=src.APPROVEDDATE, 
                    target.CONTACT=src.CONTACT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FAILEDPASSWORDANSWERCOUNT=src.FAILEDPASSWORDANSWERCOUNT, 
                    target.FAILEDPASSWORDCOUNT=src.FAILEDPASSWORDCOUNT, 
                    target.ISACTIVATED=src.ISACTIVATED, 
                    target.ISAPPROVED=src.ISAPPROVED, 
                    target.ISLOCKEDOUT=src.ISLOCKEDOUT, 
                    target.ISPWDRESETREQ=src.ISPWDRESETREQ, 
                    target.LASTLOGINDATE=src.LASTLOGINDATE, 
                    target.LASTPASSWORDCHANGEDDATE=src.LASTPASSWORDCHANGEDDATE, 
                    target.LASTPASSWORDRESETDATE=src.LASTPASSWORDRESETDATE, 
                    target.LOCKEDOUTDATE=src.LOCKEDOUTDATE, 
                    target.MEMBERSHIPKEY=src.MEMBERSHIPKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARENTUSER=src.PARENTUSER, 
                    target.PASSWORDANSWERHASH=src.PASSWORDANSWERHASH, 
                    target.PASSWORDANSWERSALT=src.PASSWORDANSWERSALT, 
                    target.PASSWORDHASH=src.PASSWORDHASH, 
                    target.PASSWORDQUESTION=src.PASSWORDQUESTION, 
                    target.PASSWORDSALT=src.PASSWORDSALT, 
                    target.USERNAME=src.USERNAME, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTIVATEDDATE, ACTIVATIONCODE, ADDBY, ADDDTTM, APPROVEDDATE, CONTACT, DATALAKE_DELETED, EXPDATE, FAILEDPASSWORDANSWERCOUNT, FAILEDPASSWORDCOUNT, ISACTIVATED, ISAPPROVED, ISLOCKEDOUT, ISPWDRESETREQ, LASTLOGINDATE, LASTPASSWORDCHANGEDDATE, LASTPASSWORDRESETDATE, LOCKEDOUTDATE, MEMBERSHIPKEY, MODBY, MODDTTM, PARENTUSER, PASSWORDANSWERHASH, PASSWORDANSWERSALT, PASSWORDHASH, PASSWORDQUESTION, PASSWORDSALT, USERNAME, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTIVATEDDATE, 
                    src.ACTIVATIONCODE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROVEDDATE, 
                    src.CONTACT, 
                    src.DATALAKE_DELETED, 
                    src.EXPDATE, 
                    src.FAILEDPASSWORDANSWERCOUNT, 
                    src.FAILEDPASSWORDCOUNT, 
                    src.ISACTIVATED, 
                    src.ISAPPROVED, 
                    src.ISLOCKEDOUT, 
                    src.ISPWDRESETREQ, 
                    src.LASTLOGINDATE, 
                    src.LASTPASSWORDCHANGEDDATE, 
                    src.LASTPASSWORDRESETDATE, 
                    src.LOCKEDOUTDATE, 
                    src.MEMBERSHIPKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARENTUSER, 
                    src.PASSWORDANSWERHASH, 
                    src.PASSWORDANSWERSALT, 
                    src.PASSWORDHASH, 
                    src.PASSWORDQUESTION, 
                    src.PASSWORDSALT, 
                    src.USERNAME, 
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
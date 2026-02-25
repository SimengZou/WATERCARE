create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_DIRECTDEBITRUN()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_DIRECTDEBITRUN_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_DIRECTDEBITRUN as target using (SELECT * FROM (SELECT 
            strm.ACCTGRP, 
            strm.ACHENTRYAMOUNT, 
            strm.ACHFILENAME, 
            strm.ACHFORMATFILE, 
            strm.ACHOUTPUTDIRECTORY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLINGCYCLE, 
            strm.BILLRUNKEY, 
            strm.BILLTYPEKEY, 
            strm.COMMENTSKEY, 
            strm.CREATEACHFILE, 
            strm.DELETED, 
            strm.DIRECTDEBITRUNKEY, 
            strm.EXTRACTTHROUGHDATE, 
            strm.INCLUDEDEBITSCHEDULE, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFACCOUNTS, 
            strm.NUMBEROFACHENTRIES, 
            strm.NUMBEROFBILLS, 
            strm.NUMBEROFDEBITSCHEDULE, 
            strm.PAYMENTBATCHCOUNT, 
            strm.PRENOTEBANKACCOUNTCOUNT, 
            strm.PROCESSINGFLAG, 
            strm.RERUN, 
            strm.SCHEDULEKEY, 
            strm.STARTDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DIRECTDEBITRUN as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITRUNKEY=src.DIRECTDEBITRUNKEY) OR (target.DIRECTDEBITRUNKEY IS NULL AND src.DIRECTDEBITRUNKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCTGRP=src.ACCTGRP, 
                    target.ACHENTRYAMOUNT=src.ACHENTRYAMOUNT, 
                    target.ACHFILENAME=src.ACHFILENAME, 
                    target.ACHFORMATFILE=src.ACHFORMATFILE, 
                    target.ACHOUTPUTDIRECTORY=src.ACHOUTPUTDIRECTORY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLINGCYCLE=src.BILLINGCYCLE, 
                    target.BILLRUNKEY=src.BILLRUNKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CREATEACHFILE=src.CREATEACHFILE, 
                    target.DELETED=src.DELETED, 
                    target.DIRECTDEBITRUNKEY=src.DIRECTDEBITRUNKEY, 
                    target.EXTRACTTHROUGHDATE=src.EXTRACTTHROUGHDATE, 
                    target.INCLUDEDEBITSCHEDULE=src.INCLUDEDEBITSCHEDULE, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFACCOUNTS=src.NUMBEROFACCOUNTS, 
                    target.NUMBEROFACHENTRIES=src.NUMBEROFACHENTRIES, 
                    target.NUMBEROFBILLS=src.NUMBEROFBILLS, 
                    target.NUMBEROFDEBITSCHEDULE=src.NUMBEROFDEBITSCHEDULE, 
                    target.PAYMENTBATCHCOUNT=src.PAYMENTBATCHCOUNT, 
                    target.PRENOTEBANKACCOUNTCOUNT=src.PRENOTEBANKACCOUNTCOUNT, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.RERUN=src.RERUN, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCTGRP, 
                    ACHENTRYAMOUNT, 
                    ACHFILENAME, 
                    ACHFORMATFILE, 
                    ACHOUTPUTDIRECTORY, 
                    ADDBY, 
                    ADDDTTM, 
                    BILLINGCYCLE, 
                    BILLRUNKEY, 
                    BILLTYPEKEY, 
                    COMMENTSKEY, 
                    CREATEACHFILE, 
                    DELETED, 
                    DIRECTDEBITRUNKEY, 
                    EXTRACTTHROUGHDATE, 
                    INCLUDEDEBITSCHEDULE, 
                    LASTINVOCATIONSTATUS, 
                    MODBY, 
                    MODDTTM, 
                    NUMBEROFACCOUNTS, 
                    NUMBEROFACHENTRIES, 
                    NUMBEROFBILLS, 
                    NUMBEROFDEBITSCHEDULE, 
                    PAYMENTBATCHCOUNT, 
                    PRENOTEBANKACCOUNTCOUNT, 
                    PROCESSINGFLAG, 
                    RERUN, 
                    SCHEDULEKEY, 
                    STARTDTTM, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCTGRP, 
                    src.ACHENTRYAMOUNT, 
                    src.ACHFILENAME, 
                    src.ACHFORMATFILE, 
                    src.ACHOUTPUTDIRECTORY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLINGCYCLE, 
                    src.BILLRUNKEY, 
                    src.BILLTYPEKEY, 
                    src.COMMENTSKEY, 
                    src.CREATEACHFILE, 
                    src.DELETED, 
                    src.DIRECTDEBITRUNKEY, 
                    src.EXTRACTTHROUGHDATE, 
                    src.INCLUDEDEBITSCHEDULE, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFACCOUNTS, 
                    src.NUMBEROFACHENTRIES, 
                    src.NUMBEROFBILLS, 
                    src.NUMBEROFDEBITSCHEDULE, 
                    src.PAYMENTBATCHCOUNT, 
                    src.PRENOTEBANKACCOUNTCOUNT, 
                    src.PROCESSINGFLAG, 
                    src.RERUN, 
                    src.SCHEDULEKEY, 
                    src.STARTDTTM, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DIRECTDEBITRUN as target using (
                SELECT * FROM (SELECT 
            strm.ACCTGRP, 
            strm.ACHENTRYAMOUNT, 
            strm.ACHFILENAME, 
            strm.ACHFORMATFILE, 
            strm.ACHOUTPUTDIRECTORY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLINGCYCLE, 
            strm.BILLRUNKEY, 
            strm.BILLTYPEKEY, 
            strm.COMMENTSKEY, 
            strm.CREATEACHFILE, 
            strm.DELETED, 
            strm.DIRECTDEBITRUNKEY, 
            strm.EXTRACTTHROUGHDATE, 
            strm.INCLUDEDEBITSCHEDULE, 
            strm.LASTINVOCATIONSTATUS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NUMBEROFACCOUNTS, 
            strm.NUMBEROFACHENTRIES, 
            strm.NUMBEROFBILLS, 
            strm.NUMBEROFDEBITSCHEDULE, 
            strm.PAYMENTBATCHCOUNT, 
            strm.PRENOTEBANKACCOUNTCOUNT, 
            strm.PROCESSINGFLAG, 
            strm.RERUN, 
            strm.SCHEDULEKEY, 
            strm.STARTDTTM, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITRUNKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DIRECTDEBITRUN as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITRUNKEY=src.DIRECTDEBITRUNKEY) OR (target.DIRECTDEBITRUNKEY IS NULL AND src.DIRECTDEBITRUNKEY IS NULL)) 
                when matched then update set
                    target.ACCTGRP=src.ACCTGRP, 
                    target.ACHENTRYAMOUNT=src.ACHENTRYAMOUNT, 
                    target.ACHFILENAME=src.ACHFILENAME, 
                    target.ACHFORMATFILE=src.ACHFORMATFILE, 
                    target.ACHOUTPUTDIRECTORY=src.ACHOUTPUTDIRECTORY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLINGCYCLE=src.BILLINGCYCLE, 
                    target.BILLRUNKEY=src.BILLRUNKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CREATEACHFILE=src.CREATEACHFILE, 
                    target.DELETED=src.DELETED, 
                    target.DIRECTDEBITRUNKEY=src.DIRECTDEBITRUNKEY, 
                    target.EXTRACTTHROUGHDATE=src.EXTRACTTHROUGHDATE, 
                    target.INCLUDEDEBITSCHEDULE=src.INCLUDEDEBITSCHEDULE, 
                    target.LASTINVOCATIONSTATUS=src.LASTINVOCATIONSTATUS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NUMBEROFACCOUNTS=src.NUMBEROFACCOUNTS, 
                    target.NUMBEROFACHENTRIES=src.NUMBEROFACHENTRIES, 
                    target.NUMBEROFBILLS=src.NUMBEROFBILLS, 
                    target.NUMBEROFDEBITSCHEDULE=src.NUMBEROFDEBITSCHEDULE, 
                    target.PAYMENTBATCHCOUNT=src.PAYMENTBATCHCOUNT, 
                    target.PRENOTEBANKACCOUNTCOUNT=src.PRENOTEBANKACCOUNTCOUNT, 
                    target.PROCESSINGFLAG=src.PROCESSINGFLAG, 
                    target.RERUN=src.RERUN, 
                    target.SCHEDULEKEY=src.SCHEDULEKEY, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCTGRP, ACHENTRYAMOUNT, ACHFILENAME, ACHFORMATFILE, ACHOUTPUTDIRECTORY, ADDBY, ADDDTTM, BILLINGCYCLE, BILLRUNKEY, BILLTYPEKEY, COMMENTSKEY, CREATEACHFILE, DELETED, DIRECTDEBITRUNKEY, EXTRACTTHROUGHDATE, INCLUDEDEBITSCHEDULE, LASTINVOCATIONSTATUS, MODBY, MODDTTM, NUMBEROFACCOUNTS, NUMBEROFACHENTRIES, NUMBEROFBILLS, NUMBEROFDEBITSCHEDULE, PAYMENTBATCHCOUNT, PRENOTEBANKACCOUNTCOUNT, PROCESSINGFLAG, RERUN, SCHEDULEKEY, STARTDTTM, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCTGRP, 
                    src.ACHENTRYAMOUNT, 
                    src.ACHFILENAME, 
                    src.ACHFORMATFILE, 
                    src.ACHOUTPUTDIRECTORY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLINGCYCLE, 
                    src.BILLRUNKEY, 
                    src.BILLTYPEKEY, 
                    src.COMMENTSKEY, 
                    src.CREATEACHFILE, 
                    src.DELETED, 
                    src.DIRECTDEBITRUNKEY, 
                    src.EXTRACTTHROUGHDATE, 
                    src.INCLUDEDEBITSCHEDULE, 
                    src.LASTINVOCATIONSTATUS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NUMBEROFACCOUNTS, 
                    src.NUMBEROFACHENTRIES, 
                    src.NUMBEROFBILLS, 
                    src.NUMBEROFDEBITSCHEDULE, 
                    src.PAYMENTBATCHCOUNT, 
                    src.PRENOTEBANKACCOUNTCOUNT, 
                    src.PROCESSINGFLAG, 
                    src.RERUN, 
                    src.SCHEDULEKEY, 
                    src.STARTDTTM, 
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